# -*- coding: utf-8 -*-
"""
jobs/query_growth_data.py

季度成长能力数据拉取 Job（增量模式）

数据源：BaoStock query_growth_data()
目标表：dbo.fact_growth_quarterly

字段映射（BaoStock -> DB）：
- code        -> ts_code (600000.SH <-> sh.600000 转换)
- pubDate     -> pub_date
- statDate    -> stat_date
- YOYEquity   -> yoy_equity     (除以 100 存 0~1)
- YOYAsset    -> yoy_asset      (除以 100 存 0~1)
- YOYNI       -> yoy_ni         (除以 100 存 0~1)
- YOYEPSBasic -> yoy_eps_basic  (除以 100 存 0~1)
- YOYPNI      -> yoy_pni        (除以 100 存 0~1)

增量策略：
- 启动时，从 dbo.fact_growth_quarterly 读取已存在的 (ts_code, fiscal_year, quarter)
- 仅对缺失组合做拉取和 INSERT
"""

import logging
import datetime as dt
from typing import Dict, Any, List, Tuple, Optional, Set

import baostock as bs

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.query_growth_data")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [query_growth_data] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# 每批处理股票数量
BATCH_SIZE = 200


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
    默认抓取近 6 个完整年度（含当年）
    """
    today = dt.date.today()
    end_year = today.year
    start_year = max(2020, end_year - 5)
    return list(range(start_year, end_year+1))


def load_existing_growth_quarters() -> Dict[str, Set[Tuple[int, int]]]:
    """
    已有季度成长数据，用于增量判断
    返回:
        { ts_code: {(year, quarter), ...}, ... }
    """
    sql = """
    SELECT ts_code, fiscal_year, quarter
    FROM dbo.fact_growth_quarterly;
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
        "已存在季度成长记录：股票数=%s，记录数≈%s",
        len(existing),
        len(rows),
    )
    return existing


# ---------- BaoStock 调用 ----------

def _fetch_growth_one(
    bs_code: str,
    year: int,
    quarter: int,
) -> Optional[Dict[str, Any]]:
    """
    单只股票某一季度成长能力：query_growth_data
    """
    rs = bs.query_growth_data(code=bs_code, year=year, quarter=quarter)
    if rs.error_code != "0":
        logger.debug(
            "query_growth_data 失败: code=%s, year=%s, q=%s, msg=%s",
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
    将 BaoStock 返回记录映射到 fact_growth_quarterly 插入行
    """
    stat_date = safe_date(rec.get("statDate"))
    pub_date = safe_date(rec.get("pubDate"))

    def pct_to_ratio(key: str) -> Optional[float]:
        v = safe_float(rec.get(key))
        return v / 100.0 if v is not None else None

    yoy_equity = pct_to_ratio("YOYEquity")
    yoy_asset = pct_to_ratio("YOYAsset")
    yoy_ni = pct_to_ratio("YOYNI")
    yoy_eps_basic = pct_to_ratio("YOYEPSBasic")
    yoy_pni = pct_to_ratio("YOYPNI")

    return (
        ts_code,
        str(year),
        quarter,
        stat_date,
        pub_date,
        yoy_equity,
        yoy_asset,
        yoy_ni,
        yoy_eps_basic,
        yoy_pni,
    )


def _insert_quarter_rows(rows: List[Tuple]) -> int:
    """
    批量写入 fact_growth_quarterly
    """
    if not rows:
        return 0

    with db_conn() as conn:
        cur = conn.cursor()
        sql = """
        INSERT INTO dbo.fact_growth_quarterly(
            ts_code, fiscal_year, quarter,
            stat_date, pub_date,
            yoy_equity, yoy_asset, yoy_ni,
            yoy_eps_basic, yoy_pni
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    当前批次（<=100 只股票）的季度成长能力增量补数
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
            rec = _fetch_growth_one(bs_code, year, q)
            if not rec:
                done += 1
                continue
            row = _build_row(ts_code, year, q, rec)
            rows_to_insert.append(row)
            done += 1

    inserted = _insert_quarter_rows(rows_to_insert)

    logger.info(
        "BaoStock 季度成长能力补充（当前批次）：股票数=%s，总任务=%s，已处理=%s，写入记录=%s",
        len(ts_to_bs),
        total_tasks,
        done,
        inserted,
    )
    return inserted


# ---------- 主入口 ----------

def main() -> int:
    logger.info("====== 开始执行 query_growth_data（BaoStock 季度成长能力，增量模式）======")

    # 1) 股票池 + 年度范围
    ts_codes = load_universe_from_db()
    if not ts_codes:
        logger.warning("dim_security 为空，跳过季频成长能力抓取")
        return 0

    years = get_target_years()
    existing_quarters = load_existing_growth_quarters()

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

            # 本地 existing_quarters 增量更新，避免后续重复拉取
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
        "====== query_growth_data 完成（增量），累计写入 fact_growth_quarterly 记录数: %s ======",
        total_inserted,
    )
    return total_inserted


if __name__ == "__main__":
    main()
