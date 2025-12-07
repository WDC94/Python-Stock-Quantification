# -*- coding: utf-8 -*-
"""
jobs/fetch_kline.py

拉取 A 股日 K 线 + 估值指标（增量模式），并写入：
- fact_daily: 基础 OHLCV（仅插入新增交易日，不覆盖历史）
- dwm_indicators_daily: 当日 close / MA250 / PB / PE_TTM / total_mv（市值）

数据源：BaoStock
"""

import logging
import datetime as dt
from collections import deque
from typing import Dict, Any, List, Tuple, Optional

import baostock as bs

from common.db import db_conn, query

logger = logging.getLogger("jobs.fetch_kline")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [fetch_kline] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# 全量抓取的起始日期（针对从零起步的股票）
START_DATE = "2015-01-01"
# 每多少行批量写入一次数据库
BATCH_COMMIT = 3000
# 计算 MA250 时，历史种子窗口长度（最多取 249 个历史收盘价）
MA_WINDOW = 250
MA_SEED_LEN = MA_WINDOW - 1


def _to_baostock_code(ts_code: str) -> Optional[str]:
    """Tushare 风格 ts_code -> BaoStock 代码。例如 600000.SH -> sh.600000"""
    if not ts_code or "." not in ts_code:
        return None
    symbol, exch = ts_code.split(".")
    exch = exch.upper()
    if exch == "SH":
        prefix = "sh"
    elif exch == "SZ":
        prefix = "sz"
    else:
        return None
    return f"{prefix}.{symbol}"


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(",", "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _load_universe() -> Dict[str, Dict[str, Any]]:
    """从 dim_security 读取股票池及股本信息"""
    rows = query(
        "SELECT ts_code, symbol, exchange, total_shares, float_shares "
        "FROM dbo.dim_security;"
    )
    universe: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ts_code = r["ts_code"]
        universe[ts_code] = {
            "symbol": r.get("symbol"),
            "exchange": r.get("exchange"),
            "total_shares": _safe_float(r.get("total_shares")),
            "float_shares": _safe_float(r.get("float_shares")),
        }
    return universe


def _load_last_trade_dates() -> Dict[str, dt.date]:
    """
    从 fact_daily 查询各股票最新交易日，用于确定增量起始日期。
    """
    rows = query(
        "SELECT ts_code, MAX(trade_date) AS max_trade_date "
        "FROM dbo.fact_daily "
        "GROUP BY ts_code;"
    )
    result: Dict[str, dt.date] = {}
    for r in rows:
        d = r.get("max_trade_date")
        if d is None:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        result[r["ts_code"]] = d
    return result


def _load_recent_closes(ts_code: str, limit: int = MA_SEED_LEN) -> List[float]:
    """
    从 fact_daily 加载该股票最近 limit 个收盘价（按日期升序），
    作为计算 MA250 的历史种子窗口。
    """
    if limit <= 0:
        return []

    sql = f"""
    SELECT TOP {limit} trade_date, [close]
    FROM dbo.fact_daily
    WHERE ts_code = ?
    ORDER BY trade_date DESC;
    """
    rows = query(sql, [ts_code])
    if not rows:
        return []

    # 当前 rows 为按日期倒序，反转成升序，保证 deque 的时间顺序正确
    rows = list(reversed(rows))
    closes: List[float] = []
    for r in rows:
        v = _safe_float(r.get("close"))
        if v is not None:
            closes.append(v)
    return closes


def _fetch_kline_one(bs_code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """拉取单只股票日 K + 估值指标"""
    fields = (
        "date,code,open,high,low,close,preclose,volume,amount,"
        "adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ"
    )
    rs = bs.query_history_k_data_plus(
        bs_code,
        fields,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="3",  # 3=不复权；如需前复权可改为 "1"
    )
    if rs.error_code != "0":
        logger.warning(
            "query_history_k_data_plus 失败: code=%s, msg=%s, start=%s, end=%s",
            bs_code,
            rs.error_msg,
            start_date,
            end_date,
        )
        return []

    result: List[Dict[str, Any]] = []
    fields_list = rs.fields
    while rs.error_code == "0" and rs.next():
        data = rs.get_row_data()
        row = dict(zip(fields_list, data))
        result.append(row)
    return result


def _build_records_for_stock(
    ts_code: str,
    base_info: Dict[str, Any],
    raw_rows: List[Dict[str, Any]],
    seed_closes: Optional[List[float]] = None,
) -> Tuple[List[Tuple], List[Tuple]]:
    """
    将单只股票的原始 K 线转为 fact_daily & dwm_indicators_daily 插入参数（增量）：

    - seed_closes: 历史收盘价种子（按日期升序，最多 249 个），用于保证 MA250 的连续性；
                   不会对历史记录做任何更新，仅用于计算新增交易日的 MA。
    """
    fact_rows: List[Tuple] = []
    dwm_rows: List[Tuple] = []

    closes: deque = deque(maxlen=MA_WINDOW)

    # 预加载历史 MA 种子窗口
    if seed_closes:
        for v in seed_closes:
            fv = _safe_float(v)
            if fv is not None:
                closes.append(fv)

    total_shares = base_info.get("total_shares") or 0.0
    float_shares = base_info.get("float_shares") or 0.0

    for row in raw_rows:
        trade_date = row.get("date")
        if not trade_date:
            continue

        open_ = _safe_float(row.get("open"))
        high = _safe_float(row.get("high"))
        low = _safe_float(row.get("low"))
        close = _safe_float(row.get("close"))
        vol = _safe_float(row.get("volume"))
        amount = _safe_float(row.get("amount"))

        if close is None:
            # 没有收盘价视为无效
            continue

        closes.append(close)
        ma250 = sum(closes) / len(closes) if closes else None

        pe_ttm = _safe_float(row.get("peTTM"))
        pb = _safe_float(row.get("pbMRQ"))

        # 市值：优先用流通股本 * 收盘价，其次总股本 * 收盘价
        mv_shares = float_shares or total_shares or 0.0
        total_mv = close * mv_shares if mv_shares > 0 else None

        fact_rows.append(
            (
                ts_code,
                trade_date,
                open_,
                high,
                low,
                close,
                vol,
                amount,
            )
        )

        dwm_rows.append(
            (
                ts_code,
                trade_date,
                close,
                ma250,
                pb,
                pe_ttm,
                total_mv,
            )
        )

    return fact_rows, dwm_rows


def _insert_batches(
    fact_rows: List[Tuple],
    dwm_rows: List[Tuple],
):
    """将累计的 fact/dwm 数据批量写入 SQL Server"""
    if not fact_rows and not dwm_rows:
        return

    with db_conn() as conn:
        cur = conn.cursor()

        if fact_rows:
            insert_fact = """
            INSERT INTO dbo.fact_daily(
                ts_code, trade_date, [open], high, low, [close], vol, amount
            ) VALUES(?,?,?,?,?,?,?,?)
            """
            cur.fast_executemany = True
            cur.executemany(insert_fact, fact_rows)

        if dwm_rows:
            insert_dwm = """
            INSERT INTO dbo.dwm_indicators_daily(
                ts_code, trade_date, [close], ma250, pb, pe_ttm, total_mv
            ) VALUES(?,?,?,?,?,?,?)
            """
            cur.fast_executemany = True
            cur.executemany(insert_dwm, dwm_rows)


def main() -> int:
    logger.info("====== 开始执行 fetch_kline（BaoStock 日 K + 估值，增量模式）======")

    universe = _load_universe()
    if not universe:
        logger.warning("dim_security 为空，跳过 fetch_kline")
        return 0

    today = dt.date.today()
    end_date = today.strftime("%Y-%m-%d")

    # 读取各股票历史最新交易日
    last_dates = _load_last_trade_dates()
    logger.info(
        "股票数量：%s，其中已存在日线数据的股票数：%s",
        len(universe),
        len(last_dates),
    )

    # 登录 BaoStock
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")
    logger.info("BaoStock login success!")

    total_fact = 0
    total_dwm = 0

    try:
        buffer_fact: List[Tuple] = []
        buffer_dwm: List[Tuple] = []

        start_date_default = dt.datetime.strptime(START_DATE, "%Y-%m-%d").date()

        for idx, (ts_code, base_info) in enumerate(universe.items(), start=1):
            bs_code = _to_baostock_code(ts_code)
            if not bs_code:
                if idx % 500 == 0:
                    logger.warning("无法转换 ts_code=%s 为 BaoStock 代码，跳过", ts_code)
                continue

            # 增量起始日期：已有历史则从 max(trade_date)+1，否则从 START_DATE
            last_date = last_dates.get(ts_code)
            if last_date:
                start_dt = last_date + dt.timedelta(days=1)
                mode = "incremental"
            else:
                start_dt = start_date_default
                mode = "full"

            if start_dt > today:
                # 没有新交易日，跳过
                continue

            start_date = start_dt.strftime("%Y-%m-%d")

            # 仅在增量模式下，加载历史 MA 种子
            seed_closes: Optional[List[float]] = None
            if last_date:
                seed_closes = _load_recent_closes(ts_code, MA_SEED_LEN)

            rows = _fetch_kline_one(bs_code, start_date, end_date)
            if not rows:
                continue

            fact_rows, dwm_rows = _build_records_for_stock(
                ts_code, base_info, rows, seed_closes=seed_closes
            )
            if not fact_rows and not dwm_rows:
                continue

            buffer_fact.extend(fact_rows)
            buffer_dwm.extend(dwm_rows)

            if len(buffer_fact) >= BATCH_COMMIT or len(buffer_dwm) >= BATCH_COMMIT:
                _insert_batches(buffer_fact, buffer_dwm)
                total_fact += len(buffer_fact)
                total_dwm += len(buffer_dwm)
                logger.info(
                    "模式=%s, 已写入 fact_daily=%s 行, dwm_indicators_daily=%s 行 (处理到第 %s 只)",
                    mode,
                    total_fact,
                    total_dwm,
                    idx,
                )
                buffer_fact.clear()
                buffer_dwm.clear()

        # flush buffer
        if buffer_fact or buffer_dwm:
            _insert_batches(buffer_fact, buffer_dwm)
            total_fact += len(buffer_fact)
            total_dwm += len(buffer_dwm)
            logger.info(
                "最终写入 fact_daily=%s 行, dwm_indicators_daily=%s 行",
                total_fact,
                total_dwm,
            )

    finally:
        try:
            bs.logout()
        except Exception as e:  # noqa
            logger.warning("BaoStock logout 异常: %s", e)
        logger.info("BaoStock logout success")

    logger.info(
        "====== fetch_kline 完成（增量），fact_daily=%s 行, dwm_indicators_daily=%s 行 ======",
        total_fact,
        total_dwm,
    )
    return total_dwm


if __name__ == "__main__":
    main()
