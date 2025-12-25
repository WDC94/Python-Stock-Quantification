# -*- coding: utf-8 -*-
"""
jobs/query_profit_data.py

功能：
- 基于 BaoStock 的 query_profit_data() 接口，按【年度 × 季度】抓取季频盈利能力数据；
- 写入 SQL Server 表：dbo.fact_profit_quarterly；
- 增量模式：仅插入表中不存在的 (ts_code, fiscal_year, quarter)，避免重复覆盖。

新增：BaoStock 当日 API 调用计数 + 阈值熔断
- 计数表：dbo.sys_baostock_api_counter（自动创建）
- 当日调用达到 80000 次：立即 bs.logout() 并中断后续请求（抛 BaoStockQuotaExceeded）
- 可用环境变量覆盖：
    BAOSTOCK_STOP_AT（默认 80000）
"""

import os
import logging
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set, Callable

import baostock as bs

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.query_profit_data")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [query_profit_data] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# 每批处理股票数量（按 ts_code 维度切分）
BATCH_SIZE = 100

# ---------------- 配额守卫（SQL Server 计数 + 熔断） ----------------

BAOSTOCK_STOP_AT = int(os.getenv("BAOSTOCK_STOP_AT", "80000"))


class BaoStockQuotaExceeded(RuntimeError):
    """当日 BaoStock API 调用达到阈值，强制中断。"""


def _ensure_counter_table() -> None:
    sql = """
    IF OBJECT_ID('dbo.sys_baostock_api_counter','U') IS NULL
    BEGIN
        CREATE TABLE dbo.sys_baostock_api_counter(
            call_date  date          NOT NULL PRIMARY KEY,
            req_count  int           NOT NULL,
            updated_at datetime2(0)  NOT NULL DEFAULT SYSDATETIME()
        );
    END
    """
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()


def get_today_api_count() -> int:
    """查询当日累计次数（无记录则 0）。"""
    _ensure_counter_table()
    today = dt.date.today()
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT req_count FROM dbo.sys_baostock_api_counter WHERE call_date = ?;",
            today,
        )
        row = cur.fetchone()
        cur.close()
    return int(row[0]) if row else 0


def _reserve_one_call(stop_at: int) -> Tuple[int, bool]:
    """
    原子预占 1 次配额（多 Job 并发也安全）：
    - 若当前 req_count < stop_at：+1，allowed=True
    - 若当前 req_count >= stop_at：不再增长，allowed=False
    返回：(当前/更新后 req_count, allowed)
    """
    _ensure_counter_table()
    today = dt.date.today()

    sql = """
    SET NOCOUNT ON;

    DECLARE @d date = ?;
    DECLARE @stop int = ?;

    IF NOT EXISTS (
        SELECT 1 FROM dbo.sys_baostock_api_counter WITH (UPDLOCK, HOLDLOCK)
        WHERE call_date = @d
    )
    BEGIN
        INSERT INTO dbo.sys_baostock_api_counter(call_date, req_count, updated_at)
        VALUES(@d, 0, SYSDATETIME());
    END

    DECLARE @updated int = 0;

    UPDATE dbo.sys_baostock_api_counter
    SET req_count = req_count + 1,
        updated_at = SYSDATETIME()
    WHERE call_date = @d
      AND req_count < @stop;

    SET @updated = @@ROWCOUNT;

    SELECT req_count, @updated
    FROM dbo.sys_baostock_api_counter
    WHERE call_date = @d;
    """

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, (today, stop_at))
        row = cur.fetchone()
        cur.close()

    if not row:
        return 0, True

    cnt = int(row[0])
    allowed = int(row[1]) == 1
    return cnt, allowed


def _force_stop(cnt: int, stop_at: int) -> None:
    """强制停止：logout + 抛异常。"""
    try:
        bs.logout()
    except Exception:
        pass
    raise BaoStockQuotaExceeded(
        f"BaoStock 当日 API 调用已达到阈值：{cnt} / {stop_at}，已强制 logout 并中断后续请求"
    )


def bs_call(func: Callable[..., Any], *args, stop_at: int = BAOSTOCK_STOP_AT, **kwargs) -> Any:
    """
    BaoStock 调用统一入口：先预占配额，超阈值直接熔断。
    说明：这里统计的是“query_*”请求次数；login/logout 不计入。
    """
    cnt, allowed = _reserve_one_call(stop_at=stop_at)
    if not allowed:
        _force_stop(cnt, stop_at)
    return func(*args, **kwargs)


# ---------------- 通用工具 ----------------

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


def safe_date(v) -> Optional[dt.date]:
    if v is None:
        return None
    if isinstance(v, dt.date):
        return v
    try:
        s = str(v).strip()
        if not s:
            return None
        s = s[:10]
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _to_baostock_code(ts_code: str) -> Optional[str]:
    """
    ts_code: 600000.SH -> sh.600000
    """
    if not ts_code or "." not in ts_code:
        return None
    num, exch = ts_code.split(".")
    exch = exch.upper()
    if exch == "SH":
        prefix = "sh"
    elif exch == "SZ":
        prefix = "sz"
    else:
        return None
    return f"{prefix}.{num}"


# ---------------- 维度 & 已有数据 ----------------

def load_universe_from_db() -> List[str]:
    """
    从 dwd_stock_basic_all 读取股票池（全市场 A 股）
    """
    rows = query("""
        SELECT DISTINCT ts_code
        FROM dbo.dwd_stock_basic_all
        WHERE ts_code IS NOT NULL AND LTRIM(RTRIM(ts_code)) <> '';
    """)
    return [r["ts_code"] for r in rows]


def get_target_years() -> List[int]:
    """
    默认抓取近 6 年（含当年）
    """
    today = dt.date.today()
    end_year = today.year
    start_year = max(2022, end_year - 5)
    return list(range(start_year, end_year + 1))


def load_existing_profit_quarters() -> Dict[str, Set[Tuple[int, int]]]:
    """
    从 fact_profit_quarterly 读取已存在的季度组合：
    返回：{ ts_code: {(year, quarter), ...}, ... }
    """
    sql = """
    SELECT ts_code, fiscal_year, quarter
    FROM dbo.fact_profit_quarterly;
    """
    rows = query(sql)

    existing: Dict[str, Set[Tuple[int, int]]] = {}
    for r in rows:
        ts = r.get("ts_code")
        fy = r.get("fiscal_year")
        q = r.get("quarter")
        if not ts or fy is None or q is None:
            continue
        try:
            year = int(str(fy).strip())
            quarter = int(q)
        except Exception:
            continue
        existing.setdefault(ts, set()).add((year, quarter))

    logger.info(
        "已存在季度盈利记录：股票数=%s，记录总数≈%s",
        len(existing),
        len(rows),
    )
    return existing


# ---------------- BaoStock 调用 ----------------

def _fetch_profit_one(
    bs_code: str,
    year: int,
    quarter: int,
) -> Optional[Dict[str, Any]]:
    """
    单只股票某一季度盈利能力：query_profit_data（已接入配额守卫）
    """
    rs = bs_call(bs.query_profit_data, code=bs_code, year=year, quarter=quarter)

    if rs.error_code != "0":
        logger.debug(
            "query_profit_data 失败: code=%s, year=%s, q=%s, msg=%s",
            bs_code, year, quarter, rs.error_msg
        )
        return None

    fields = rs.fields
    last_row = None
    while rs.next():
        last_row = rs.get_row_data()
    if not last_row:
        return None
    return dict(zip(fields, last_row))


def _build_row(
    ts_code: str,
    year: int,
    quarter: int,
    rec: Dict[str, Any],
) -> Tuple:
    stat_date = safe_date(rec.get("statDate"))
    pub_date = safe_date(rec.get("pubDate"))

    roe_pct = safe_float(rec.get("roeAvg"))
    np_margin_pct = safe_float(rec.get("npMargin"))
    gp_margin_pct = safe_float(rec.get("gpMargin"))

    roe = roe_pct / 100.0 if roe_pct is not None else None
    np_margin = np_margin_pct / 100.0 if np_margin_pct is not None else None
    gp_margin = gp_margin_pct / 100.0 if gp_margin_pct is not None else None

    net_profit = safe_float(rec.get("netProfit"))
    eps_ttm = safe_float(rec.get("epsTTM"))
    mbr_revenue = safe_float(rec.get("MBRevenue"))

    total_share = safe_float(rec.get("totalShare"))
    liqa_share = safe_float(rec.get("liqaShare"))

    return (
        ts_code,
        str(year),
        quarter,
        stat_date,
        pub_date,
        roe,
        np_margin,
        gp_margin,
        net_profit,
        eps_ttm,
        mbr_revenue,
        total_share,
        liqa_share,
    )


def _insert_quarter_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0

    with db_conn() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO dbo.fact_profit_quarterly(
            ts_code, fiscal_year, quarter,
            stat_date, pub_date,
            roe, np_margin, gp_margin,
            net_profit, eps_ttm, mbr_revenue,
            total_share, liqa_share
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.fast_executemany = True
        cur.executemany(sql, rows)
    return len(rows)


def _supplement_from_baostock_batch(
    batch_ts_codes: List[str],
    years: List[int],
    missing_quarters_map: Dict[str, List[Tuple[int, int]]],
) -> int:
    if not batch_ts_codes or not missing_quarters_map:
        return 0

    ts_to_bs: Dict[str, str] = {}
    for ts in batch_ts_codes:
        b = _to_baostock_code(ts)
        if b:
            ts_to_bs[ts] = b

    if not ts_to_bs:
        return 0

    total_tasks = sum(len(v) for v in missing_quarters_map.values())
    done = 0
    rows_to_insert: List[Tuple] = []

    for ts_code, bs_code in ts_to_bs.items():
        combos = missing_quarters_map.get(ts_code, [])
        if not combos:
            continue

        for year, q in combos:
            profit = _fetch_profit_one(bs_code, year, q)
            if not profit:
                done += 1
                continue
            row = _build_row(ts_code, year, q, profit)
            rows_to_insert.append(row)
            done += 1

    inserted = _insert_quarter_rows(rows_to_insert)

    logger.info(
        "BaoStock 季度盈利补充（当前批次）：股票数=%s，总任务=%s，已处理=%s，写入记录=%s",
        len(ts_to_bs),
        total_tasks,
        done,
        inserted,
    )
    return inserted


# ---------------- 主入口（按 100 股分批处理，增量） ----------------

def main() -> int:
    logger.info("====== 开始执行 query_profit_data（BaoStock 季度盈利，增量模式）======")

    # 0) 输出当日配额使用情况
    today_cnt = get_today_api_count()
    logger.info(
        "BaoStock 当日 API 计数：已用=%s，阈值=%s，剩余=%s",
        today_cnt, BAOSTOCK_STOP_AT, max(0, BAOSTOCK_STOP_AT - today_cnt)
    )

    # 1) 股票池 + 年度范围
    ts_codes = load_universe_from_db()
    if not ts_codes:
        logger.warning("dwd_stock_basic_all 为空，跳过季频盈利抓取")
        return 0

    years = get_target_years()
    existing_quarters = load_existing_profit_quarters()

    logger.info(
        "目标股票数：%s，年度范围：%s-%s",
        len(ts_codes),
        min(years),
        max(years),
    )

    # 2) 登录 BaoStock
    logger.info("BaoStock 登录 ...")
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")
    logger.info("BaoStock login success!")

    total_inserted = 0

    try:
        n = len(ts_codes)
        for start in range(0, n, BATCH_SIZE):
            batch_ts = ts_codes[start: start + BATCH_SIZE]

            missing_quarters_map: Dict[str, List[Tuple[int, int]]] = {}
            for ts in batch_ts:
                existed = existing_quarters.get(ts, set())
                missing: List[Tuple[int, int]] = []
                for y in years:
                    for q in (1, 2, 3, 4):
                        if (y, q) not in existed:
                            missing.append((y, q))
                if missing:
                    missing_quarters_map[ts] = missing

            if not missing_quarters_map:
                logger.info(
                    "批次：%s - %s / %s 所有年度季度已存在，跳过。",
                    start + 1,
                    min(start + BATCH_SIZE, n),
                    n,
                )
                continue

            logger.info(
                "处理批次：%s - %s / %s，需增量补充股票数=%s",
                start + 1,
                min(start + BATCH_SIZE, n),
                n,
                len(missing_quarters_map),
            )

            inserted = _supplement_from_baostock_batch(
                batch_ts, years, missing_quarters_map
            )
            total_inserted += inserted

            # 本地 map 增量更新，避免后续批次重复计算
            for ts, combos in missing_quarters_map.items():
                if not combos:
                    continue
                existed = existing_quarters.setdefault(ts, set())
                existed.update(combos)

    except BaoStockQuotaExceeded as e:
        logger.warning(str(e))
        logger.warning("已触发当日配额熔断：本次任务提前结束（已写入=%s）", total_inserted)

    finally:
        try:
            bs.logout()
        except Exception as e:  # noqa
            logger.warning("BaoStock 登出异常: %s", e)
        logger.info("BaoStock 登出完成")

    logger.info(
        "====== query_profit_data 完成（增量），累计写入 fact_profit_quarterly 记录数: %s ======",
        total_inserted,
    )
    return total_inserted


if __name__ == "__main__":
    main()
