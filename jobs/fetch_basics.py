# -*- coding: utf-8 -*-
"""
jobs/fetch_basics.py

数据源策略：
- 优先：东财 Choice 量化接口（EmQuantAPI）
- 兜底：BaoStock
- 两者都获取失败：相关字段记为 NULL

功能：
- 通过 BaoStock 获取全市场 A 股基础信息（一次性 query_stock_basic）
- 通过 BaoStock 获取行业分类（query_stock_industry）
- 通过 EmQuantAPI 补充总股本(TOTALSHARE)、流通股本(FREESHARE)
- 若 EmQuant 获取失败或部分股票缺失，则通过 BaoStock query_profit_data(totalShare, liqaShare) 兜底
- 写入 SQL Server 表：dbo.dim_security
"""

import logging
import datetime as dt
from typing import Dict, Any, Optional, List

import baostock as bs

# 东财 Choice 量化接口
try:
    from EmQuantAPI import c  # type: ignore
    HAS_EMQUANT = True
except Exception:
    HAS_EMQUANT = False

from common.db import db_conn

logger = logging.getLogger("jobs.fetch_basics")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [fetch_basics] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# ---------- 通用工具函数 ----------

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


def _format_ts_code(baostock_code: str) -> Optional[str]:
    """
    baostock 代码格式: sh.600000 / sz.000001
    转换为 ts_code: 600000.SH / 000001.SZ
    """
    if not baostock_code or "." not in baostock_code:
        return None
    prefix, num = baostock_code.split(".")
    prefix = prefix.lower()
    if prefix == "sh":
        exch = "SH"
    elif prefix == "sz":
        exch = "SZ"
    else:
        return None
    return f"{num}.{exch}"


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


# ---------- BaoStock：基础信息 + 行业 ----------

def fetch_universe_from_baostock() -> Dict[str, Dict[str, Any]]:
    """
    使用一次性 query_stock_basic() 获取 A 股列表
    调用前必须已 bs.login()。
    """
    logger.info("BaoStock 拉取全部 A 股基础信息 (query_stock_basic) ...")
    rs = bs.query_stock_basic()
    if rs.error_code != "0":
        raise RuntimeError(f"query_stock_basic 失败: {rs.error_msg}")

    universe: Dict[str, Dict[str, Any]] = {}

    while rs.next():
        # 官方字段顺序：code, code_name, ipoDate, outDate, type, status
        code, name, ipo_date, out_date, type_, status = rs.get_row_data()

        # 只保留 A 股：type = 1（股票），status = 1（在市）
        if type_ != "1" or status != "1":
            continue

        if not (code.startswith("sh.") or code.startswith("sz.")):
            continue

        ts_code = _format_ts_code(code)
        if not ts_code:
            continue

        symbol, exchange = ts_code.split(".")

        # status 字段：1=在市，这里再结合 out_date 兜个底
        status_flag = "L" if status == "1" and not out_date else "D"

        universe[ts_code] = {
            "ts_code": ts_code,
            "symbol": symbol,
            "name": name,
            "exchange": exchange,
            "industry": None,        # 后续补
            "list_date": ipo_date or None,
            "status": status_flag,
            "total_shares": None,    # Choice / BaoStock 补充
            "float_shares": None,    # Choice / BaoStock 补充
        }

    logger.info("BaoStock 有效 A 股记录数: %s", len(universe))
    return universe


def supplement_industry_from_baostock(universe: Dict[str, Dict[str, Any]]) -> None:
    """
    使用 query_stock_industry 补充行业字段
    调用前必须已 bs.login()。
    """
    if not universe:
        logger.warning("universe 为空，跳过行业补充")
        return

    logger.info("BaoStock 拉取行业分类 (query_stock_industry) ...")
    rs = bs.query_stock_industry()
    if rs.error_code != "0":
        logger.warning("query_stock_industry 失败: %s", rs.error_msg)
        return

    industry_map: Dict[str, str] = {}
    while rs.next():
        data = rs.get_row_data()
        fields = rs.fields
        rec = dict(zip(fields, data))
        code = rec.get("code")
        industry = rec.get("industry")
        if not code or not industry:
            continue
        ts_code = _format_ts_code(code)
        if not ts_code:
            continue
        industry_map[ts_code] = industry

    logger.info("行业分类覆盖股票数: %s", len(industry_map))

    for ts_code, info in universe.items():
        if info.get("industry"):
            continue
        if ts_code in industry_map:
            info["industry"] = industry_map[ts_code]


# ---------- 东财 Choice：总股本 / 流通股本（优先） ----------

def supplement_shares_from_emquant(universe: Dict[str, Dict[str, Any]]) -> bool:
    """
    通过 EmQuantAPI 补充：
    - 总股本：TOTALSHARE
    - 流通股本：FREESHARE

    返回：
    - True: 登录成功且已尝试写入
    - False: 登录失败或 EmQuant 不可用（需走 BaoStock 兜底）
    """
    if not universe:
        logger.warning("universe 为空，跳过 EmQuant 补充")
        return False

    if not HAS_EMQUANT:
        logger.warning("EmQuantAPI 未安装或导入失败，跳过 Choice 补充股本")
        return False

    logger.info("EmQuantAPI 登录 ...")
    login_result = c.start()
    if login_result.ErrorCode != 0:
        logger.error("EmQuant 登录失败：%s", login_result.ErrorMsg)
        # 明确返回 False，后续走 BaoStock 兜底
        return False

    try:
        codes = list(universe.keys())  # 600000.SH 格式
        batch_size = 200

        for i in range(0, len(codes), batch_size):
            batch = codes[i: i + batch_size]
            codes_str = ",".join(batch)

            try:
                # enddate 留空则使用服务器默认最近日期
                data = c.css(codes_str, "TOTALSHARE,FREESHARE", "")
            except Exception as e:
                logger.error("EmQuant css 调用异常: %s", e)
                continue

            if data.ErrorCode != 0:
                logger.warning("EmQuant css 失败: %s", data.ErrorMsg)
                continue

            indicators: List[str] = list(data.Indicators)
            try:
                idx_total = indicators.index("TOTALSHARE")
            except ValueError:
                logger.warning("EmQuant 返回未包含 TOTALSHARE 指标: %s", indicators)
                continue

            try:
                idx_free = indicators.index("FREESHARE")
            except ValueError:
                idx_free = None
                logger.warning("EmQuant 返回未包含 FREESHARE 指标，仅写入总股本")

            for code in data.Codes:
                if code not in universe:
                    continue
                vals = data.Data.get(code)
                if vals is None:
                    continue

                total_val = None
                free_val = None
                try:
                    total_val = safe_float(vals[idx_total])
                    if idx_free is not None:
                        free_val = safe_float(vals[idx_free])
                except Exception:
                    pass

                # 只在为空时写入，避免后续有其他更精确来源覆盖
                if total_val is not None and universe[code].get("total_shares") is None:
                    universe[code]["total_shares"] = total_val
                if free_val is not None and universe[code].get("float_shares") is None:
                    universe[code]["float_shares"] = free_val

            logger.info(
                "EmQuant 补充股本进度: %s / %s",
                i + len(batch),
                len(codes),
            )

        return True

    finally:
        try:
            logout_result = c.stop()
            logger.info(
                "EmQuant 登出：ErrorCode=%s, Msg=%s",
                getattr(logout_result, "ErrorCode", None),
                getattr(logout_result, "ErrorMsg", None),
            )
        except Exception:
            pass


# ---------- BaoStock：总股本 / 流通股本兜底 ----------

def _current_report_period() -> (int, int):
    """
    按当前日期推一个最近已披露的季报周期，用于 query_profit_data：
    - 1~3 月：取上一年度 Q4
    - 4~6 月：取当年度 Q1
    - 7~9 月：取当年度 Q2
    - 10~12 月：取当年度 Q3
    """
    today = dt.date.today()
    year = today.year
    m = today.month
    if 1 <= m <= 3:
        return year - 1, 4
    elif 4 <= m <= 6:
        return year, 1
    elif 7 <= m <= 9:
        return year, 2
    else:
        return year, 3


def supplement_shares_from_baostock(universe: Dict[str, Dict[str, Any]]) -> None:
    """
    使用 BaoStock 季频盈利能力接口 query_profit_data 兜底补充：
    - totalShare -> 总股本
    - liqaShare  -> 流通股本

    要求：调用前必须已 bs.login()。
    只对当前 total_shares / float_shares 为空的股票进行补充。
    """
    if not universe:
        logger.warning("universe 为空，跳过 BaoStock 股本兜底")
        return

    year, quarter = _current_report_period()
    logger.info("BaoStock 兜底补充股本：year=%s, quarter=%s ...", year, quarter)

    # 只处理缺失股本信息的股票
    targets = [
        ts_code for ts_code, info in universe.items()
        if info.get("total_shares") is None or info.get("float_shares") is None
    ]

    logger.info("需要 BaoStock 兜底补充股本的股票数量: %s", len(targets))
    if not targets:
        return

    for idx, ts_code in enumerate(targets, start=1):
        bs_code = _to_baostock_code(ts_code)
        if not bs_code:
            continue

        rs = bs.query_profit_data(code=bs_code, year=year, quarter=quarter)
        if rs.error_code != "0":
            # 单票失败，跳过
            if idx % 200 == 0:
                logger.warning(
                    "query_profit_data 失败: %s, year=%s, quarter=%s, msg=%s",
                    bs_code, year, quarter, rs.error_msg
                )
            continue

        last_row = None
        while rs.next():
            last_row = rs.get_row_data()
        if not last_row:
            continue

        fields = rs.fields
        rec = dict(zip(fields, last_row))

        total_val = safe_float(rec.get("totalShare"))
        free_val = safe_float(rec.get("liqaShare"))

        info = universe[ts_code]
        if total_val is not None and info.get("total_shares") is None:
            info["total_shares"] = total_val
        if free_val is not None and info.get("float_shares") is None:
            info["float_shares"] = free_val

        if idx % 500 == 0:
            logger.info("BaoStock 股本兜底进度: %s / %s", idx, len(targets))


# ---------- 写库 ----------

def save_to_dim_security(universe: Dict[str, Dict[str, Any]]) -> int:
    if not universe:
        logger.warning("universe 为空，跳过写入 dim_security")
        return 0

    rows = list(universe.values())

    with db_conn() as conn:
        cur = conn.cursor()
        # 全量重刷
        cur.execute("TRUNCATE TABLE dbo.dim_security;")

        insert_sql = """
        INSERT INTO dbo.dim_security(
            ts_code, symbol, name, exchange, industry,
            list_date, status, total_shares, float_shares
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        params = [
            (
                r["ts_code"],
                r["symbol"],
                r["name"],
                r["exchange"],
                r.get("industry"),
                r.get("list_date"),
                r.get("status"),
                r.get("total_shares"),
                r.get("float_shares"),
            )
            for r in rows
        ]

        cur.fast_executemany = True
        cur.executemany(insert_sql, params)

    return len(rows)


# ---------- 主入口 ----------

def main() -> int:
    logger.info("====== 开始执行 fetch_basics（Choice + BaoStock）======")

    try:
        # 1) BaoStock 登录
        logger.info("BaoStock 登录 ...")
        try:
            lg = bs.login()
        except OSError as e:
            logger.error("BaoStock 登录阶段发生 OSError: %r", e, exc_info=True)
            # 转成更清晰的业务异常抛出去，前端也能看懂
            raise RuntimeError(f"BaoStock 登录阶段发生系统级异常: {e!r}")

        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {lg.error_msg}")

        try:
            # 1.1 基础信息 + 行业
            universe = fetch_universe_from_baostock()
            supplement_industry_from_baostock(universe)

            # 1.2 Choice 优先补充股本
            em_ok = supplement_shares_from_emquant(universe)
            if not em_ok:
                logger.warning("Choice 股本补充不可用，全部改由 BaoStock 兜底")

            # 1.3 BaoStock 兜底补充股本
            supplement_shares_from_baostock(universe)
        finally:
            try:
                bs.logout()
            except Exception as e:
                logger.warning("BaoStock 登出异常: %s", e)
            logger.info("BaoStock 登出完成")

        # 2) 写入 dim_security
        count = save_to_dim_security(universe)
        logger.info("====== fetch_basics 完成，写入 dim_security 记录数: %s ======", count)
        return count

    except OSError as e:
        # 兜底再打一遍，防止中间环节抛 OSError
        logger.error("fetch_basics 运行过程中捕获到 OSError: %r", e, exc_info=True)
        # 继续往外抛，让 /api/jobs 接口返回 error 字符串
        raise



if __name__ == "__main__":
    main()
