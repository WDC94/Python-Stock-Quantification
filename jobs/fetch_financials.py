# -*- coding: utf-8 -*-
"""
jobs/fetch_financials.py

年度财务数据拉取 Job（增量模式）

数据源策略（当前版本）：
- 东财 Choice(EmQuantAPI)：全部代码保留为注释，不执行；
- 实际取数全部走 BaoStock 财报接口；
- 无法获取到的数据字段：写入 NULL。

写入策略（增量）：
- 启动时从 dbo.fact_finance_annual 读取已存在的 (ts_code, fiscal_year)；
- 仅对缺失的 ts_code+年度做拉取和 INSERT；
- 对已存在数据不做覆盖，提升整体执行效率。
"""

import logging
import datetime as dt
from typing import Dict, Any, Tuple, List, Optional, Set

import baostock as bs

# ================== Choice (EmQuantAPI) 预留代码：全部注释，待权限开通后启用 ==================
"""
# 东财 Choice 量化接口
try:
    from EmQuantAPI import c  # type: ignore
    HAS_EMQUANT = True
except Exception:
    HAS_EMQUANT = False


def supplement_from_choice(
    store: Dict[Tuple[str, str], Dict[str, Any]],
    ts_codes: List[str],
    years: List[int],
) -> bool:
    '''
    预留：通过 EmQuantAPI 补充年度财务数据。
    当前版本已禁用，待 Choice 权限开通后，再按指标手册补充实现。

    返回:
    - True: Choice 已成功写入部分数据
    - False: Choice 不可用（全部走 BaoStock）
    '''
    if not HAS_EMQUANT:
        return False

    login_result = c.start()
    if login_result.ErrorCode != 0:
        return False

    try:
        # TODO: 在此实现 c.csd / c.css 调用，写入 store
        return False
    finally:
        c.stop()
"""
# ============================================================================

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.fetch_financials")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [fetch_financials] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# 每批处理股票数量
BATCH_SIZE = 100


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


def _pick_first(rec: Dict[str, Any], *keys: str) -> Optional[float]:
    """
    从 dict 中按顺序挑选第一个可解析为 float 的字段
    """
    for k in keys:
        if k in rec:
            v = safe_float(rec.get(k))
            if v is not None:
                return v
    return None


# ----------------- 维度 & 年度范围 ----------------- #

def load_universe_from_db() -> List[str]:
    """
    从 dim_security 读取股票池
    """
    rows = query("SELECT ts_code FROM dbo.dim_security;")
    return [r["ts_code"] for r in rows]


def get_target_years() -> List[int]:
    """
    默认抓取近 8 个完整会计年度：
    - 截止到上一年度（因为当年年报未必披露完成）
    - 例如当前 2025 年，则抓 2017–2024
    """
    today = dt.date.today()
    end_year = today.year - 1
    start_year = max(2007, end_year - 7)  # BaoStock 财务数据一般自 2007 起
    return list(range(start_year, end_year + 1))


def load_existing_finance_years() -> Dict[str, Set[str]]:
    """
    从 fact_finance_annual 读取已存在的 (ts_code, fiscal_year)，
    用于后续增量判断。
    """
    sql = """
    SELECT ts_code, fiscal_year
    FROM dbo.fact_finance_annual;
    """
    rows = query(sql)
    existing: Dict[str, Set[str]] = {}
    for r in rows:
        ts = r.get("ts_code")
        fy = r.get("fiscal_year")
        if not ts or fy is None:
            continue
        fy_str = str(fy).strip()
        if not fy_str:
            continue
        if ts not in existing:
            existing[ts] = set()
        existing[ts].add(fy_str)
    logger.info(
        "已存在财报记录：股票数=%s，记录总数≈%s",
        len(existing),
        len(rows),
    )
    return existing


# ----------------- 记录容器 ----------------- #

def _default_record(ts_code: str, year: int) -> Dict[str, Any]:
    rec = {
        "ts_code": ts_code,
        "fiscal_year": str(year),
    }
    rec.update(
        {
            "net_profit": None,
            "net_assets": None,
            "total_assets": None,
            "total_liab": None,
            "bps": None,
            "total_mv": None,
            "roe": None,
            "debt_asset_ratio": None,
            "pe": None,
            "pb": None,
            "dividend_cash": None,
            "dividend_flag": 0,
            "profit_flag": 0,
        }
    )
    return rec


def get_or_create_record(
    store: Dict[Tuple[str, str], Dict[str, Any]], ts_code: str, year: int
) -> Dict[str, Any]:
    key = (ts_code, str(year))
    if key not in store:
        store[key] = _default_record(ts_code, year)
    return store[key]


# ----------------- BaoStock 财报：单码调用，批次汇总 ----------------- #

def _fetch_profit_one(
    bs_code: str,
    year: int,
    quarter: int = 4,
) -> Optional[Dict[str, Any]]:
    """
    单只股票盈利能力：query_profit_data
    取该年度 Q4 作为年报口径。
    """
    rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
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


def _fetch_balance_one(
    bs_code: str,
    year: int,
    quarter: int = 4,
) -> Optional[Dict[str, Any]]:
    """
    单只股票资产负债表：query_balance_data
    取该年度 Q4 作为年末资产负债表。
    """
    rs = bs.query_balance_data(code=bs_code, year=year, quarter=quarter)
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


def supplement_from_baostock_batch(
    batch_ts_codes: List[str],
    years: List[int],
    missing_years_map: Optional[Dict[str, List[int]]] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    单个批次（<=100 只股票）的年度财务补充逻辑（支持增量）：

    参数：
    - batch_ts_codes: 本批次需要处理的 ts_code 列表；
    - years: 默认年度范围（例如近 8 年）；
    - missing_years_map: 若为增量模式，传入 {ts_code: [缺失年份列表]}，
      则仅对指定年份做拉取；若为 None，则对 years 全量处理。
    """
    store: Dict[Tuple[str, str], Dict[str, Any]] = {}
    if not batch_ts_codes or not years:
        return store

    # ts_code (600000.SH) -> baostock code (sh.600000)
    ts_to_bs: Dict[str, str] = {}
    for ts in batch_ts_codes:
        b = _to_baostock_code(ts)
        if not b:
            continue
        ts_to_bs[ts] = b

    if not ts_to_bs:
        return store

    total_tasks = 0
    for ts_code in ts_to_bs.keys():
        if missing_years_map is not None:
            ys = missing_years_map.get(ts_code, [])
            total_tasks += len(ys)
        else:
            total_tasks += len(years)

    done = 0

    for ts_code, bs_code in ts_to_bs.items():
        # 决定该股票要处理的年度列表
        if missing_years_map is not None:
            years_for_ts = missing_years_map.get(ts_code, [])
            if not years_for_ts:
                continue
        else:
            years_for_ts = years

        for year in years_for_ts:
            profit = _fetch_profit_one(bs_code, year)
            balance = _fetch_balance_one(bs_code, year)

            if not profit and not balance:
                done += 1
                continue

            rec = get_or_create_record(store, ts_code, year)

            # --- 利润表 ---
            if profit:
                # 归母净利润
                net_profit = _pick_first(
                    profit,
                    "NPParentCompanyOwners",  # 兼容其它数据源
                    "netProfit",              # BaoStock: 净利润(元/万元，视版本而定)
                )
                if net_profit is not None:
                    rec["net_profit"] = net_profit
                    rec["profit_flag"] = 1 if net_profit > 0 else 0

                # ROE：BaoStock 提供 roeAvg（百分比）
                roe_pct = _pick_first(
                    profit,
                    "roeAvg",  # BaoStock: 净资产收益率(平均)(%)
                    "roe",     # 兼容其它接口命名
                )
                if roe_pct is not None:
                    # 统一存成 0–1 比例，例如 15% -> 0.15
                    rec["roe"] = roe_pct / 100.0

            # --- 资产负债表 ---
            if balance:
                # 归母净资产
                net_assets = _pick_first(
                    balance,
                    "nAsset",
                    "netAsset",
                    "netAssets",
                    "equityParentCompanyOwners",
                )
                if net_assets is not None:
                    rec["net_assets"] = net_assets

                # 资产总计
                total_assets = _pick_first(balance, "totalAssets")
                if total_assets is not None:
                    rec["total_assets"] = total_assets

                # 负债合计
                total_liab = _pick_first(
                    balance,
                    "totalLiability",
                    "totalLiab",
                )
                if total_liab is not None:
                    rec["total_liab"] = total_liab

                # 每股净资产（若接口提供）
                bps = _pick_first(
                    balance,
                    "netAssetPerShare",
                    "NAPS",
                    "naps",
                    "netAssetsPerShare",
                )
                if bps is not None:
                    rec["bps"] = bps

                # 资产负债率：BaoStock 提供 liabilityToAsset（百分比）
                debt_asset_pct = _pick_first(
                    balance,
                    "liabilityToAsset",  # 偿债能力：资产负债率(%)
                )
                if debt_asset_pct is not None:
                    # 同样统一转为 0–1 比例
                    rec["debt_asset_ratio"] = debt_asset_pct / 100.0

            # --- 比率指标的兜底逻辑 ---
            # 如果 BaoStock 没给 roeAvg，但拿到了净利润和净资产，则自己算一遍
            if rec.get("roe") is None and \
               rec.get("net_profit") is not None and \
               rec.get("net_assets") not in (None, 0):
                rec["roe"] = rec["net_profit"] / rec["net_assets"]

            # 如果 BaoStock 没给 liabilityToAsset，但有总资产+总负债，同样兜底计算
            if rec.get("debt_asset_ratio") is None and \
               rec.get("total_liab") is not None and \
               rec.get("total_assets") not in (None, 0):
                rec["debt_asset_ratio"] = rec["total_liab"] / rec["total_assets"]

            done += 1

    logger.info(
        "BaoStock 财报补充（当前批次）：股票数=%s，总任务=%s，已处理=%s",
        len(ts_to_bs), total_tasks, done
    )
    return store


# ----------------- 写入 SQL Server（分批插入） ----------------- #

def truncate_fact_finance_annual() -> None:
    """
    整表清空工具方法：
    - 默认增量模式不再自动调用；
    - 如需全量重算，可在单独脚本或运维操作中显式调用。
    """
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE dbo.fact_finance_annual;")


def save_to_fact_finance_annual_batch(
    store: Dict[Tuple[str, str], Dict[str, Any]]
) -> int:
    """
    将当前批次的记录插入 fact_finance_annual。
    不做 TRUNCATE，由 main 控制整体写入策略。
    """
    if not store:
        return 0

    rows = list(store.values())

    with db_conn() as conn:
        cur = conn.cursor()

        insert_sql = """
        INSERT INTO dbo.fact_finance_annual(
            ts_code, fiscal_year,
            net_profit, net_assets, total_assets, total_liab,
            bps, total_mv,
            roe, debt_asset_ratio, pe, pb,
            dividend_cash, dividend_flag, profit_flag
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                r["ts_code"],
                r["fiscal_year"],
                r.get("net_profit"),
                r.get("net_assets"),
                r.get("total_assets"),
                r.get("total_liab"),
                r.get("bps"),
                r.get("total_mv"),
                r.get("roe"),
                r.get("debt_asset_ratio"),
                r.get("pe"),
                r.get("pb"),
                r.get("dividend_cash"),
                r.get("dividend_flag"),
                r.get("profit_flag"),
            )
            for r in rows
        ]

        cur.fast_executemany = True
        cur.executemany(insert_sql, params)

    return len(rows)


# ----------------- 主入口（按 100 股分批处理，增量） ----------------- #

def main() -> int:
    logger.info("====== 开始执行 fetch_financials（BaoStock 分批写入，增量模式）======")

    # 1) 准备股票池 + 年度范围
    ts_codes = load_universe_from_db()
    if not ts_codes:
        logger.warning("dim_security 为空，跳过财务抓取")
        return 0

    years = get_target_years()
    existing_years_map = load_existing_finance_years()

    logger.info(
        "目标股票数：%s，年度范围：%s-%s",
        len(ts_codes), min(years), max(years)
    )

    # 2) BaoStock 登录，一次会话内完成所有批次
    logger.info("BaoStock 登录 ...")
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")
    logger.info("login success!")

    total_inserted = 0

    try:
        n = len(ts_codes)
        for start in range(0, n, BATCH_SIZE):
            batch_ts = ts_codes[start: start + BATCH_SIZE]

            # 针对当前批次，计算每个 ts_code 缺失的年度列表
            missing_years_map: Dict[str, List[int]] = {}
            for ts in batch_ts:
                existed = existing_years_map.get(ts, set())
                # 只拉取表中不存在的年度
                missing_years = [y for y in years if str(y) not in existed]
                if missing_years:
                    missing_years_map[ts] = missing_years

            if not missing_years_map:
                logger.info(
                    "批次：%s - %s / %s 所有年度已存在，跳过。",
                    start + 1, min(start + BATCH_SIZE, n), n
                )
                continue

            logger.info(
                "处理批次：%s - %s / %s，实际需增量补充股票数=%s",
                start + 1, min(start + BATCH_SIZE, n), n, len(missing_years_map)
            )

            pending_ts = list(missing_years_map.keys())

            # 当前批次：BaoStock 单码财报 + 本地计算（仅缺失年度）
            store = supplement_from_baostock_batch(
                pending_ts, years, missing_years_map=missing_years_map
            )

            # 批次写库（插入缺失记录）
            inserted = save_to_fact_finance_annual_batch(store)
            total_inserted += inserted

            logger.info(
                "批次完成：股票数=%s，写入记录=%s，累计写入=%s",
                len(pending_ts), inserted, total_inserted
            )

    finally:
        try:
            bs.logout()
        except Exception as e:
            logger.warning("BaoStock 登出异常: %s", e)
        logger.info("BaoStock 登出完成")

    logger.info(
        "====== fetch_financials 完成（增量），累计写入 fact_finance_annual 记录数: %s ======",
        total_inserted,
    )
    return total_inserted


if __name__ == "__main__":
    main()
