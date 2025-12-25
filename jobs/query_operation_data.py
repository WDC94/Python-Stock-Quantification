# -*- coding: utf-8 -*-
"""
jobs/query_operation_data.py

季度营运能力数据拉取 Job（增量模式）

数据源：BaoStock query_operation_data()
写入表：dbo.fact_operation_quarterly

增量策略：
- 启动时从 dbo.fact_operation_quarterly 读取已存在的 (ts_code, fiscal_year, quarter)
- 仅对缺失组合做拉取和 INSERT
"""

import logging
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set

import baostock as bs

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.query_operation_data")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [query_operation_data] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# 每批处理股票数量
BATCH_SIZE = 100


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
        # 只取前 10 位，适配 "YYYY-MM-DD" 或带时间格式
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


# ---------- 维度 & 已有数据 ----------

def load_universe_from_db() -> List[str]:
    """
    从 dim_security 读取股票池
    """
    rows = query("SELECT ts_code FROM dbo.dim_security;")
    return [r["ts_code"] for r in rows]


def get_target_years() -> List[int]:
    """
    默认抓取近 8 个完整年度（含当年）
    """
    today = dt.date.today()
    end_year = today.year
    start_year = max(2020, end_year - 5)
    return list(range(start_year, end_year+1))


def load_existing_operation_quarters() -> Dict[str, Set[Tuple[int, int]]]:
    """
    读取 fact_operation_quarterly 已有 (ts_code, year, quarter)
    用于增量判断
    """
    sql = """
    SELECT ts_code, fiscal_year, quarter
    FROM dbo.fact_operation_quarterly;
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
        "已存在营运能力季度记录：股票数=%s，记录数≈%s",
        len(existing),
        len(rows),
    )
    return existing


# ---------- BaoStock 调用 ----------

def _fetch_operation_one(
    bs_code: str,
    year: int,
    quarter: int,
) -> Optional[Dict[str, Any]]:
    """
    单只股票某一季度营运能力：query_operation_data
    """
    rs = bs.query_operation_data(code=bs_code, year=year, quarter=quarter)
    if rs.error_code != "0":
        logger.debug(
            "query_operation_data 失败: code=%s, year=%s, q=%s, msg=%s",
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
    """
    将 BaoStock 返回记录映射到 fact_operation_quarterly 插入行
    """
    stat_date = safe_date(rec.get("statDate"))
    pub_date = safe_date(rec.get("pubDate"))

    nr_turn_ratio = safe_float(rec.get("NRTurnRatio"))
    nr_turn_days = safe_float(rec.get("NRTurnDays"))
    inv_turn_ratio = safe_float(rec.get("INVTurnRatio"))
    inv_turn_days = safe_float(rec.get("INVTurnDays"))
    ca_turn_ratio = safe_float(rec.get("CATurnRatio"))
    asset_turn_ratio = safe_float(rec.get("AssetTurnRatio"))

    return (
        ts_code,
        str(year),
        quarter,
        stat_date,
        pub_date,
        nr_turn_ratio,
        nr_turn_days,
        inv_turn_ratio,
        inv_turn_days,
        ca_turn_ratio,
        asset_turn_ratio,
    )


def _insert_quarter_rows(rows: List[Tuple]) -> int:
    """
    批量写入 fact_operation_quarterly
    """
    if not rows:
        return 0

    with db_conn() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO dbo.fact_operation_quarterly(
            ts_code, fiscal_year, quarter,
            stat_date, pub_date,
            nr_turn_ratio, nr_turn_days,
            inv_turn_ratio, inv_turn_days,
            ca_turn_ratio, asset_turn_ratio
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cur.fast_executemany = True
        cur.executemany(sql, rows)
    return len(rows)


def _supplement_from_baostock_batch(
    batch_ts_codes: List[str],
    years: List[int],
    missing_quarters_map: Dict[str, List[Tuple[int, int]]],
) -> int:
    """
    当前批次（<=100 只股票）的营运能力增量补数
    """
    if not batch_ts_codes or not missing_quarters_map:
        return 0

    # ts_code -> baostock code
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
            rec = _fetch_operation_one(bs_code, year, q)
            if not rec:
                done += 1
                continue
            row = _build_row(ts_code, year, q, rec)
            rows_to_insert.append(row)
            done += 1

    inserted = _insert_quarter_rows(rows_to_insert)

    logger.info(
        "BaoStock 季度营运能力补充（当前批次）：股票数=%s，总任务=%s，已处理=%s，写入记录=%s",
        len(ts_to_bs),
        total_tasks,
        done,
        inserted,
    )
    return inserted


# ---------- 主入口 ----------

def main() -> int:
    logger.info("====== 开始执行 query_operation_data（BaoStock 季度营运能力，增量模式）======")

    # 1) 股票池 + 年度范围
    ts_codes = load_universe_from_db()
    if not ts_codes:
        logger.warning("dim_security 为空，跳过季频营运能力抓取")
        return 0

    years = get_target_years()
    existing_quarters = load_existing_operation_quarters()

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

            # 计算当前批次每个 ts_code 缺失的 (year, quarter) 组合
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

            # 本地 existing_quarters 增量更新，避免重复拉取
            for ts, combos in missing_quarters_map.items():
                if not combos:
                    continue
                existed = existing_quarters.setdefault(ts, set())
                existed.update(combos)

    finally:
        try:
            bs.logout()
        except Exception as e:  # noqa
            logger.warning("BaoStock 登出异常: %s", e)
        logger.info("BaoStock 登出完成")

    logger.info(
        "====== query_operation_data 完成（增量），累计写入 fact_operation_quarterly 记录数: %s ======",
        total_inserted,
    )
    return total_inserted


if __name__ == "__main__":
    main()
