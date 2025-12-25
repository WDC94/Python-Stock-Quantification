# -*- coding: utf-8 -*-
"""
jobs/query_balance_data.py

季度偿债能力数据拉取 Job（增量模式）

数据源：BaoStock query_balance_data()
目标表：dbo.fact_balance_quarterly

增量策略（节省 API 次数）：
- 每个批次（<=100 只股票）先查 dbo.fact_balance_quarterly 已存在 (ts_code, fiscal_year, quarter)
- 仅对缺失组合调用 BaoStock 并 INSERT

API 配额熔断（全局）：
- 计数表：dbo.sys_baostock_api_counter（自动创建）
- 当日 query_* 调用次数达到 80000：立即 bs.logout() + raise BaoStockQuotaExceeded
"""

import os
import logging
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set, Callable

import baostock as bs

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.query_balance_data")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] [query_balance_data] %(message)s")
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# 每批处理股票数量
BATCH_SIZE = 100


# ---------------- BaoStock 当日调用计数 + 熔断 ----------------

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
    BaoStock query_* 调用统一入口：先预占配额，达到阈值则熔断。
    说明：只统计 query_* 的请求次数；login/logout 不计入。
    """
    cnt, allowed = _reserve_one_call(stop_at=stop_at)
    if not allowed:
        _force_stop(cnt, stop_at)
    return func(*args, **kwargs)


# ---------- 通用工具 ----------

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
    """ts_code: 600000.SH -> sh.600000"""
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


# ---------- 股票池 & 目标季度 ----------

def load_universe_from_db() -> List[str]:
    """
    股票池：dwd_stock_basic_all（项目已替代 dim_security）
    """
    rows = query("""
        SELECT DISTINCT ts_code
        FROM dbo.dwd_stock_basic_all
        WHERE ts_code IS NOT NULL AND LTRIM(RTRIM(ts_code)) <> '';
    """)
    return [r["ts_code"] for r in rows]


def get_target_periods() -> Tuple[int, int, List[Tuple[int, int]]]:
    """
    默认抓取近 6 年（含当年），当年只抓已结束季度（减少无效请求）。
    """
    today = dt.date.today()
    end_year = today.year
    start_year = max(2020, end_year - 5)

    completed_q = (today.month - 1) // 3  # 0~3
    periods: List[Tuple[int, int]] = []

    for y in range(start_year, end_year + 1):
        if y < end_year:
            for q in (1, 2, 3, 4):
                periods.append((y, q))
        else:
            for q in range(1, completed_q + 1):
                periods.append((y, q))

    return start_year, end_year, periods


# ---------- 先查已存在季度（按批次 IN 查询） ----------

def load_existing_balance_quarters_for_batch(
    batch_ts_codes: List[str],
    start_year: int,
    end_year: int,
) -> Dict[str, Set[Tuple[int, int]]]:
    """
    查询当前批次已存在的季度组合：
    返回：{ ts_code: {(year, quarter), ...}, ... }
    """
    if not batch_ts_codes:
        return {}

    placeholders = ",".join(["?"] * len(batch_ts_codes))
    sql = f"""
    SELECT ts_code, fiscal_year, quarter
    FROM dbo.fact_balance_quarterly
    WHERE ts_code IN ({placeholders})
      AND TRY_CONVERT(int, fiscal_year) BETWEEN ? AND ?;
    """
    params = list(batch_ts_codes) + [start_year, end_year]
    rows = query(sql, params)

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

    return existing


# ---------- BaoStock 调用 ----------

def _fetch_balance_one(bs_code: str, year: int, quarter: int) -> Optional[Dict[str, Any]]:
    """
    单只股票某一季度偿债能力：query_balance_data（接入 bs_call 计数/熔断）
    """
    rs = bs_call(bs.query_balance_data, code=bs_code, year=year, quarter=quarter)

    if rs.error_code != "0":
        logger.debug(
            "query_balance_data 失败: code=%s, year=%s, q=%s, msg=%s",
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


def _build_row(ts_code: str, year: int, quarter: int, rec: Dict[str, Any]) -> Tuple:
    stat_date = safe_date(rec.get("statDate"))
    pub_date = safe_date(rec.get("pubDate"))

    current_ratio = safe_float(rec.get("currentRatio"))
    quick_ratio = safe_float(rec.get("quickRatio"))
    cash_ratio = safe_float(rec.get("cashRatio"))

    yoy_liab_pct = safe_float(rec.get("YOYLiability"))
    yoy_liability = yoy_liab_pct / 100.0 if yoy_liab_pct is not None else None

    liability_to_asset = safe_float(rec.get("liabilityToAsset"))
    asset_to_equity = safe_float(rec.get("assetToEquity"))

    return (
        ts_code,
        str(year),
        quarter,
        stat_date,
        pub_date,
        current_ratio,
        quick_ratio,
        cash_ratio,
        yoy_liability,
        liability_to_asset,
        asset_to_equity,
    )


def _insert_quarter_rows(rows: List[Tuple]) -> int:
    if not rows:
        return 0

    with db_conn() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO dbo.fact_balance_quarterly(
            ts_code, fiscal_year, quarter,
            stat_date, pub_date,
            current_ratio, quick_ratio, cash_ratio,
            yoy_liability, liability_to_asset, asset_to_equity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.fast_executemany = True
        cur.executemany(sql, rows)

    return len(rows)


def _supplement_from_baostock_batch(
    batch_ts_codes: List[str],
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
            rec = _fetch_balance_one(bs_code, year, q)
            if not rec:
                done += 1
                continue
            rows_to_insert.append(_build_row(ts_code, year, q, rec))
            done += 1

    inserted = _insert_quarter_rows(rows_to_insert)

    logger.info(
        "BaoStock 季度偿债能力补充（当前批次）：股票数=%s，总任务=%s，已处理=%s，写入记录=%s",
        len(ts_to_bs),
        total_tasks,
        done,
        inserted,
    )
    return inserted


# ---------- 主入口 ----------

def main() -> int:
    logger.info("====== 开始执行 query_balance_data（BaoStock 季度偿债能力，增量模式）======")

    # 输出当日配额使用情况（便于任务中心监控）
    today_cnt = get_today_api_count()
    logger.info(
        "BaoStock 当日 API 计数：已用=%s，阈值=%s，剩余=%s",
        today_cnt, BAOSTOCK_STOP_AT, max(0, BAOSTOCK_STOP_AT - today_cnt)
    )

    ts_codes = load_universe_from_db()
    if not ts_codes:
        logger.warning("dwd_stock_basic_all 为空，跳过季频偿债能力抓取")
        return 0

    start_year, end_year, target_periods = get_target_periods()

    logger.info(
        "目标股票数=%s，年度范围=%s-%s，目标季度数=%s",
        len(ts_codes),
        start_year,
        end_year,
        len(target_periods),
    )

    # 登录 BaoStock
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

            # 关键：先查已存在季度 -> 存在则不请求 API
            existing_map = load_existing_balance_quarters_for_batch(batch_ts, start_year, end_year)

            missing_quarters_map: Dict[str, List[Tuple[int, int]]] = {}
            missing_total = 0

            for ts in batch_ts:
                existed = existing_map.get(ts, set())
                missing = [(y, q) for (y, q) in target_periods if (y, q) not in existed]
                if missing:
                    missing_quarters_map[ts] = missing
                    missing_total += len(missing)

            if not missing_quarters_map:
                logger.info(
                    "批次：%s - %s / %s 已全部存在，跳过（本批次无 API 请求）。",
                    start + 1,
                    min(start + BATCH_SIZE, n),
                    n,
                )
                continue

            logger.info(
                "处理批次：%s - %s / %s，需拉取缺失季度=%s（存在即跳过已生效）",
                start + 1,
                min(start + BATCH_SIZE, n),
                n,
                missing_total,
            )

            inserted = _supplement_from_baostock_batch(batch_ts, missing_quarters_map)
            total_inserted += inserted

    except BaoStockQuotaExceeded:
        # 熔断要求：抛出异常中断（这里记录后继续抛出）
        logger.error("已触发 BaoStock 当日调用阈值熔断（>= %s），任务中断。", BAOSTOCK_STOP_AT)
        raise

    finally:
        try:
            bs.logout()
        except Exception as e:  # noqa
            logger.warning("BaoStock 登出异常: %s", e)
        logger.info("BaoStock 登出完成")

    logger.info(
        "====== query_balance_data 完成（增量），累计写入 fact_balance_quarterly 记录数: %s ======",
        total_inserted,
    )
    return total_inserted


if __name__ == "__main__":
    main()
