# -*- coding: utf-8 -*-
"""
jobs/query_history_k_data_plus.py

功能：
- 调用 BaoStock query_history_k_data_plus 接口
- 自 2015-01-01 起拉取 A 股日 K 线及估值指标
- 写入：dbo.dwd_kline_daily_raw
- 增量策略（修复历史缺口）：
    1) 若无数据：拉 [start_date, today]
    2) 若已有数据：
        - 若 MIN(trade_date) > start_date：回补 [start_date, MIN-1]
        - 若 MAX(trade_date) < today：追更 [MAX+1, today]
- API 调用统计 + 8 万熔断（DB 持久化）
"""

import logging
import datetime as dt
from typing import Dict, List, Optional, Tuple

import baostock as bs

from common.db import db_conn  # type: ignore
from common.baostock_quota import BaoStockDailyQuota, BaoStockQuotaExceeded  # type: ignore

logger = logging.getLogger("jobs.query_history_k_data_plus")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] [query_history_k_data_plus] %(message)s")
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

DEFAULT_START_DATE = "2020-01-01"
BATCH_SIZE = 5000
API_DAILY_LIMIT = 80000


# ----------------- 工具函数 ----------------- #

def safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s or s.lower() in ("nan", "null", "none"):
            return None
        return float(s)
    except Exception:
        return None


def safe_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def safe_date(v) -> Optional[dt.date]:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    try:
        s = str(v).strip()[:10]
        if not s:
            return None
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


# ----------------- 股票池 ----------------- #

def load_codes_from_db() -> List[str]:
    """
    优先从库里取股票池（避免额外 BaoStock 调用）
    依赖：dbo.dwd_stock_basic_all(code) 或者你已有的股票维表
    """
    sql_try = [
        # 你的基础资料表：query_stock_basic.py 写入的是 dwd_stock_basic_all(code)
        "SELECT code FROM dbo.dwd_stock_basic_all WHERE code LIKE 'sh.%' OR code LIKE 'sz.%' OR code LIKE 'bj.%';",
    ]
    with db_conn() as conn:
        cur = conn.cursor()
        for sql in sql_try:
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                codes = [r[0] for r in rows if r and r[0]]
                if codes:
                    logger.info("从 DB 获取股票列表 %d 只", len(codes))
                    return codes
            except Exception:
                continue
    return []


def get_all_stock_codes_from_baostock(quota: BaoStockDailyQuota) -> List[str]:
    """
    DB 没股票池时，才走 BaoStock query_all_stock
    """
    quota.incr(1, api_name="query_all_stock")
    today = ymd(dt.date.today())
    rs = bs.query_all_stock(day=today)
    if rs.error_code != "0":
        raise RuntimeError(f"query_all_stock failed: {rs.error_code}, {rs.error_msg}")

    codes: List[str] = []
    while rs.next():
        row = rs.get_row_data()
        code = row[0]
        if code.startswith(("sh.", "sz.", "bj.")):
            codes.append(code)

    logger.info("从 BaoStock 获取股票列表 %d 只", len(codes))
    return codes


# ----------------- BaoStock 拉取 ----------------- #

def fetch_kline_for_code(
    code: str,
    start_date: dt.date,
    end_date: dt.date,
    quota: BaoStockDailyQuota,
) -> List[Dict[str, Optional[str]]]:
    """
    单次调用 query_history_k_data_plus
    """
    fields = ",".join(
        [
            "date", "code",
            "open", "high", "low", "close", "preclose",
            "volume", "amount",
            "adjustflag", "turn", "tradestatus", "pctChg",
            "peTTM", "pbMRQ", "psTTM", "pcfNcfTTM",
            "isST",
        ]
    )

    quota.incr(1, api_name="query_history_k_data_plus")

    rs = bs.query_history_k_data_plus(
        code,
        start_date=ymd(start_date),
        end_date=ymd(end_date),
        fields=fields,
        frequency="d",
        adjustflag="3",
    )
    if rs.error_code != "0":
        logger.warning("query_history_k_data_plus 失败 code=%s, %s, %s", code, rs.error_code, rs.error_msg)
        return []

    result: List[Dict[str, Optional[str]]] = []
    columns = rs.fields
    while rs.next():
        data = rs.get_row_data()
        result.append({col: val for col, val in zip(columns, data)})
    return result


# ----------------- DB 侧增量判断 ----------------- #

def load_min_max_trade_date_by_code() -> Dict[str, Tuple[Optional[dt.date], Optional[dt.date]]]:
    """
    读取每只股票已入库区间：[min_date, max_date]
    """
    sql = """
    SELECT code, MIN(trade_date) AS min_date, MAX(trade_date) AS max_date
    FROM dbo.dwd_kline_daily_raw
    GROUP BY code
    """
    mp: Dict[str, Tuple[Optional[dt.date], Optional[dt.date]]] = {}
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        for code, min_d, max_d in cur.fetchall():
            mp[str(code)] = (safe_date(min_d), safe_date(max_d))
    logger.info("已存在 K 线股票数：%d", len(mp))
    return mp


# ----------------- 写库（临时表去重插入，幂等） ----------------- #

def insert_kline_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0

    with db_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE #tmp_kline(
                code nvarchar(16) NOT NULL,
                trade_date date NOT NULL,
                [open] float NULL,
                [high] float NULL,
                [low] float NULL,
                [close] float NULL,
                preclose float NULL,
                volume float NULL,
                amount float NULL,
                adjustflag int NULL,
                turn float NULL,
                tradestatus int NULL,
                pct_chg float NULL,
                pe_ttm float NULL,
                pb_mrq float NULL,
                ps_ttm float NULL,
                pcf_ncf_ttm float NULL,
                is_st int NULL
            );
            """)

            ins_tmp = """
            INSERT INTO #tmp_kline(
                code, trade_date,
                [open], [high], [low], [close], preclose,
                volume, amount,
                adjustflag, turn, tradestatus, pct_chg,
                pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, is_st
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            try:
                cur.fast_executemany = True  # type: ignore[attr-defined]
            except Exception:
                pass
            cur.executemany(ins_tmp, rows)

            cur.execute("""
            INSERT INTO dbo.dwd_kline_daily_raw(
                code, trade_date,
                [open], [high], [low], [close], preclose,
                volume, amount,
                adjustflag, turn, tradestatus, pct_chg,
                pe_ttm, pb_mrq, ps_ttm, pcf_ncf_ttm, is_st
            )
            SELECT
                t.code, t.trade_date,
                t.[open], t.[high], t.[low], t.[close], t.preclose,
                t.volume, t.amount,
                t.adjustflag, t.turn, t.tradestatus, t.pct_chg,
                t.pe_ttm, t.pb_mrq, t.ps_ttm, t.pcf_ncf_ttm, t.is_st
            FROM #tmp_kline t
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.dwd_kline_daily_raw d
                WHERE d.code = t.code AND d.trade_date = t.trade_date
            );
            """)

            cur.execute("SELECT @@ROWCOUNT;")
            inserted = int(cur.fetchone()[0])
            return inserted
        finally:
            try:
                cur.execute("DROP TABLE IF EXISTS #tmp_kline;")
            except Exception:
                pass


# ----------------- 主流程 ----------------- #

def main(start_date: str = DEFAULT_START_DATE) -> int:
    logger.info("开始执行日 K 线拉取 Job，start_date=%s，API_LIMIT=%s/日", start_date, API_DAILY_LIMIT)

    start_dt = safe_date(start_date)
    if not start_dt:
        raise ValueError(f"start_date 非法：{start_date}")

    today = dt.date.today()
    quota = BaoStockDailyQuota(limit=API_DAILY_LIMIT)

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock login failed: {lg.error_code}, {lg.error_msg}")

    total_inserted = 0
    batch: List[Tuple] = []

    try:
        codes = load_codes_from_db()
        if not codes:
            codes = get_all_stock_codes_from_baostock(quota)

        date_map = load_min_max_trade_date_by_code()

        for idx, code in enumerate(codes, start=1):
            min_date, max_date = date_map.get(code, (None, None))

            tasks: List[Tuple[dt.date, dt.date]] = []

            if max_date is None:
                # 完全没数据：全量拉
                tasks.append((start_dt, today))
            else:
                # 回补历史缺口
                if min_date and min_date > start_dt:
                    tasks.append((start_dt, min_date - dt.timedelta(days=1)))
                # 追更最新
                if max_date < today:
                    tasks.append((max_date + dt.timedelta(days=1), today))

            for s, e in tasks:
                if s > e:
                    continue

                try:
                    k_rows = fetch_kline_for_code(code, s, e, quota)
                except BaoStockQuotaExceeded:
                    # 熔断直接抛出，让 finally 走 logout
                    raise

                if not k_rows:
                    continue

                for r in k_rows:
                    trade_date = safe_date(r.get("date"))
                    if not trade_date:
                        continue

                    code_val = r.get("code")
                    open_v = safe_float(r.get("open"))
                    high_v = safe_float(r.get("high"))
                    low_v = safe_float(r.get("low"))
                    close_v = safe_float(r.get("close"))
                    preclose_v = safe_float(r.get("preclose"))
                    tradestatus_v = safe_int(r.get("tradestatus"))

                    # close 不允许 NULL：优先用 preclose，其次用 open；都没有就丢弃该行
                    if close_v is None:
                        if preclose_v is not None:
                            close_v = preclose_v
                            # 停牌/空数据时，补齐 OH L，避免后续指标计算出现大量 NULL
                            if open_v is None:
                                open_v = close_v
                            if high_v is None:
                                high_v = close_v
                            if low_v is None:
                                low_v = close_v
                        elif open_v is not None:
                            close_v = open_v
                        else:
                            # 极端脏数据：跳过
                            continue

                    row_tuple: Tuple = (
                        code_val,
                        trade_date,
                        open_v,
                        high_v,
                        low_v,
                        close_v,
                        preclose_v,
                        safe_float(r.get("volume")),
                        safe_float(r.get("amount")),
                        safe_int(r.get("adjustflag")),
                        safe_float(r.get("turn")),
                        tradestatus_v,
                        safe_float(r.get("pctChg")),
                        safe_float(r.get("peTTM")),
                        safe_float(r.get("pbMRQ")),
                        safe_float(r.get("psTTM")),
                        safe_float(r.get("pcfNcfTTM")),
                        safe_int(r.get("isST")),
                    )

                    batch.append(row_tuple)

                    if len(batch) >= BATCH_SIZE:
                        inserted = insert_kline_rows(batch)
                        total_inserted += inserted
                        logger.info(
                            "进度 %d/%d | code=%s | 本批写入 %d 行 | 累计 %d 行 | 今日API=%d",
                            idx, len(codes), code, inserted, total_inserted, quota.get_today_count()
                        )
                        batch = []

        if batch:
            inserted = insert_kline_rows(batch)
            total_inserted += inserted
            logger.info("最终批次写入 %d 行，累计 %d 行", inserted, total_inserted)

        logger.info("日 K 线拉取 Job 完成，累计写入 %d 行", total_inserted)
        return total_inserted

    finally:
        try:
            bs.logout()
        except Exception as e:
            logger.warning("BaoStock 登出异常: %s", e)
        logger.info("BaoStock 已登出")


if __name__ == "__main__":
    main()
