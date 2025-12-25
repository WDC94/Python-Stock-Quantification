# -*- coding: utf-8 -*-
"""jobs/query_dividend_data.py

功能：
- 调用 BaoStock query_dividend_data() 拉取除权分红信息；
- 写入 SQL Server 表：dbo.dwd_dividend_raw；
- 增量策略：按 (code, div_year) 维度，只拉取库里不存在的年度。

本次改造（对应你的两个诉求）：
1) 修复“前端提示成功但无数据写入”的空跑场景
   - 股票池优先从库里读取 dwd_stock_basic_all(code)，避免 query_all_stock 在非交易日/异常时返回空导致 codes=0
   - DB 无股票池时，再回退 BaoStock query_all_stock，并回退尝试最近 N 天
2) 增加 BaoStock API 调用统计 + 单日 8 万熔断
   - 计数表：dbo.sys_baostock_api_counter（自动创建）
   - 当日调用达到 BAOSTOCK_STOP_AT（默认 80000）：立即 bs.logout() + raise BaoStockQuotaExceeded

可选环境变量：
- BAOSTOCK_STOP_AT              熔断阈值，默认 80000
- STOCK_DB_NAME                 目标库名，默认 stock（用于强制写入 [stock].[dbo]）
- DIVIDEND_START_YEAR           起始年度，默认 2015
- DIVIDEND_BATCH_SIZE           批量写库行数，默认 1000
- DIVIDEND_LOOKBACK_DAYS        query_all_stock 回退天数，默认 10
- DIVIDEND_YEAR_TYPE            report|operate|auto，默认 auto（report 空则 fallback operate）
"""

import os
import logging
import datetime as dt
from typing import Dict, List, Tuple, Optional, Any, Callable, Set

import baostock as bs

from common.db import db_conn, query  # type: ignore

logger = logging.getLogger("jobs.query_dividend_data")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter("%(asctime)s [%(levelname)s] [query_dividend_data] %(message)s")
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


# ---------------- 配置 ----------------

BAOSTOCK_STOP_AT = int(os.getenv("BAOSTOCK_STOP_AT", "80000"))
STOCK_DB_NAME = os.getenv("STOCK_DB_NAME", "stock")

START_YEAR = int(os.getenv("DIVIDEND_START_YEAR", "2020"))
BATCH_SIZE = int(os.getenv("DIVIDEND_BATCH_SIZE", "1000"))
LOOKBACK_DAYS = int(os.getenv("DIVIDEND_LOOKBACK_DAYS", "10"))
DIVIDEND_YEAR_TYPE = os.getenv("DIVIDEND_YEAR_TYPE", "auto").strip().lower()  # report | operate | auto


# ---------------- BaoStock 配额守卫（SQL Server 计数 + 熔断） ----------------

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
    _ensure_counter_table()
    today = dt.date.today()
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT req_count FROM dbo.sys_baostock_api_counter WHERE call_date = ?;", today)
        row = cur.fetchone()
        cur.close()
    return int(row[0]) if row else 0


def _reserve_one_call(stop_at: int) -> Tuple[int, bool]:
    """原子预占 1 次配额（多 Job 并发安全）。返回：(更新后 req_count, allowed)。"""
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
    try:
        bs.logout()
    except Exception:
        pass
    raise BaoStockQuotaExceeded(
        f"BaoStock 当日 API 调用已达到阈值：{cnt} / {stop_at}，已强制 logout 并中断后续请求"
    )


def bs_call(func: Callable[..., Any], *args, stop_at: int = BAOSTOCK_STOP_AT, **kwargs) -> Any:
    """BaoStock query_* 调用统一入口：先计数/预占，超阈值直接熔断。"""
    cnt, allowed = _reserve_one_call(stop_at=stop_at)
    if not allowed:
        _force_stop(cnt, stop_at)
    return func(*args, **kwargs)


# ---------------- 工具函数 ----------------

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


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
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _current_db_name() -> str:
    try:
        rows = query("SELECT DB_NAME() AS dbname;")
        if rows and rows[0].get("dbname"):
            return str(rows[0]["dbname"])
    except Exception:
        pass
    return ""


def _qualify_to_stock(two_part: str) -> str:
    """确保写入 [stock].[dbo].*（即使当前连接库不是 stock）。"""
    cur_db = _current_db_name().strip().lower()
    target_db = STOCK_DB_NAME.strip().lower()
    if cur_db and cur_db == target_db:
        return two_part
    return f"{STOCK_DB_NAME}.{two_part}"


# ---------------- DB 侧辅助（目标表/股票池/增量） ----------------

def load_universe_from_db() -> List[str]:
    """优先用 DB 股票池，避免额外 BaoStock 调用。"""
    tbl = _qualify_to_stock("dbo.dwd_stock_basic_all")
    try:
        rows = query(
            f"""
            SELECT DISTINCT code
            FROM {tbl}
            WHERE code IS NOT NULL AND LTRIM(RTRIM(code)) <> ''
              AND (code LIKE 'sh.%' OR code LIKE 'sz.%' OR code LIKE 'bj.%');
            """
        )
        codes = [str(r["code"]).strip() for r in rows if r.get("code")]
        if codes:
            logger.info("股票池：从 DB 获取 %d 支", len(codes))
        return codes
    except Exception as e:
        logger.warning("股票池：从 DB 读取失败，将回退 BaoStock query_all_stock。err=%s", e)
        return []


def get_all_stock_codes_from_baostock(lookback_days: int = LOOKBACK_DAYS) -> List[str]:
    """DB 无股票池时才走 BaoStock。对最近 N 天做回退，提升鲁棒性。"""
    for i in range(0, max(1, lookback_days) + 1):
        day = dt.date.today() - dt.timedelta(days=i)
        day_str = ymd(day)
        rs = bs_call(bs.query_all_stock, day=day_str)
        if rs.error_code != "0":
            logger.warning("query_all_stock 失败 day=%s: %s", day_str, rs.error_msg)
            continue

        codes: List[str] = []
        while rs.next():
            data = rs.get_row_data()
            code = data[0]
            if code.startswith(("sh.", "sz.", "bj.")):
                codes.append(code)

        if codes:
            logger.info("股票池：从 BaoStock 获取 %d 支（day=%s）", len(codes), day_str)
            return codes

        logger.warning("query_all_stock 返回空列表，day=%s，继续回退…", day_str)

    return []


def load_existing_code_year() -> Dict[Tuple[str, str], bool]:
    """已入库的 (code, div_year) 组合，用于增量判断。"""
    tbl = _qualify_to_stock("dbo.dwd_dividend_raw")
    sql = f"SELECT code, div_year FROM {tbl} GROUP BY code, div_year"
    rows = query(sql)
    exists: Dict[Tuple[str, str], bool] = {}
    for r in rows:
        code = r.get("code")
        year = r.get("div_year")
        if code and year:
            exists[(str(code).strip(), str(year).strip())] = True
    logger.info("增量基线：已存在 (code,year) 组合数=%d", len(exists))
    return exists


def insert_dividend_rows(rows: List[Tuple]) -> int:
    """批量写入 dwd_dividend_raw。返回：本批次写入行数（以输入 rows 为准，避免 pyodbc rowcount=-1）。"""
    if not rows:
        return 0

    tbl = _qualify_to_stock("dbo.dwd_dividend_raw")
    sql = f"""
    INSERT INTO {tbl}(
        code, div_year,
        divid_pre_notice_date,
        divid_agm_pum_date,
        divid_plan_announce_date,
        divid_plan_date,
        divid_regist_date,
        divid_operate_date,
        divid_pay_date,
        divid_stock_market_date,
        divid_cash_ps_before_tax,
        divid_cash_ps_after_tax,
        divid_stocks_ps,
        divid_cash_stock,
        divid_reserve_to_stock_ps
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """

    with db_conn() as conn:
        cur = conn.cursor()
        try:
            try:
                cur.fast_executemany = True
            except Exception:
                pass
            cur.executemany(sql, rows)
            conn.commit()
            return len(rows)
        finally:
            cur.close()


def get_dividend_table_count() -> int:
    tbl = _qualify_to_stock("dbo.dwd_dividend_raw")
    try:
        rows = query(f"SELECT COUNT(1) AS cnt FROM {tbl};")
        if rows:
            return int(rows[0].get("cnt") or 0)
    except Exception:
        pass
    return 0


# ---------------- BaoStock 侧取数 ----------------

def _fetch_dividend(code: str, year: int, year_type: str) -> List[dict]:
    rs = bs_call(bs.query_dividend_data, code=code, year=str(year), yearType=year_type)
    if rs.error_code != "0":
        logger.warning("query_dividend_data 失败: code=%s, year=%s, yearType=%s, msg=%s", code, year, year_type, rs.error_msg)
        return []

    fields = rs.fields
    result: List[dict] = []
    while rs.next():
        row = rs.get_row_data()
        result.append(dict(zip(fields, row)))
    return result


def fetch_dividend_for_code_year(code: str, year: int) -> List[dict]:
    """支持 report/operate/auto。auto：report 无结果则 fallback operate。"""
    year_type = DIVIDEND_YEAR_TYPE

    if year_type in ("report", "operate"):
        return _fetch_dividend(code, year, year_type)

    # auto
    recs = _fetch_dividend(code, year, "report")
    if recs:
        return recs
    return _fetch_dividend(code, year, "operate")


def build_row_tuple(code: str, year: int, rec: dict) -> Tuple:
    return (
        code,
        str(year),
        safe_date(rec.get("dividPreNoticeDate")),
        safe_date(rec.get("dividAgmPumDate")),
        safe_date(rec.get("dividPlanAnnounceDate")),
        safe_date(rec.get("dividPlanDate")),
        safe_date(rec.get("dividRegistDate")),
        safe_date(rec.get("dividOperateDate")),
        safe_date(rec.get("dividPayDate")),
        safe_date(rec.get("dividStockMarketDate")),
        safe_float(rec.get("dividCashPsBeforeTax")),
        safe_float(rec.get("dividCashPsAfterTax")),
        safe_float(rec.get("dividStocksPs")),
        safe_float(rec.get("dividCashStock")),
        safe_float(rec.get("dividReserveToStockPs")),
    )


def _dedup_records(recs: List[dict]) -> List[dict]:
    """同一年可能重复返回（report/operate 回退），做轻量去重。"""
    seen: Set[Tuple] = set()
    out: List[dict] = []
    for r in recs:
        k = (
            (r.get("dividPreNoticeDate") or ""),
            (r.get("dividPlanAnnounceDate") or ""),
            (r.get("dividRegistDate") or ""),
            (r.get("dividOperateDate") or ""),
            (r.get("dividPayDate") or ""),
            (r.get("dividCashPsBeforeTax") or ""),
            (r.get("dividStocksPs") or ""),
        )
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


# ---------------- 主流程 ----------------

def main(start_year: int = START_YEAR) -> None:
    logger.info("====== 开始执行除权分红拉取 Job：start_year=%s, yearType=%s, stop_at=%s ======", start_year, DIVIDEND_YEAR_TYPE, BAOSTOCK_STOP_AT)
    logger.info("当前连接库=%s，目标写入库=%s", _current_db_name() or "(unknown)", STOCK_DB_NAME)

    # 登录 BaoStock
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_code}, {lg.error_msg}")

    try:
        # 1) 股票池
        codes = load_universe_from_db()
        if not codes:
            codes = get_all_stock_codes_from_baostock()
        if not codes:
            raise RuntimeError("股票池为空：DB 未加载 dwd_stock_basic_all，且 BaoStock query_all_stock 多次回退仍为空。")

        # 2) 增量基线
        existing = load_existing_code_year()

        current_year = dt.date.today().year
        years = list(range(int(start_year), current_year + 1))

        total_inserted = 0
        batch: List[Tuple] = []

        start_cnt = get_dividend_table_count()
        logger.info("目标表当前行数=%d", start_cnt)

        for idx, code in enumerate(codes, start=1):
            for year in years:
                key = (code, str(year))
                if key in existing:
                    continue

                recs = fetch_dividend_for_code_year(code, year)
                recs = _dedup_records(recs)

                if not recs:
                    # 不写库也不报错：可能该公司该年没有分红/除权
                    existing[key] = True
                    continue

                for rec in recs:
                    batch.append(build_row_tuple(code, year, rec))

                existing[key] = True

                if len(batch) >= BATCH_SIZE:
                    inserted = insert_dividend_rows(batch)
                    total_inserted += inserted
                    batch = []

            # 进度日志（按股票粒度）
            if idx % 200 == 0:
                logger.info(
                    "进度：%d/%d 支股票；累计写入=%d；当日 BaoStock 调用=%d",
                    idx, len(codes), total_inserted, get_today_api_count()
                )

        if batch:
            inserted = insert_dividend_rows(batch)
            total_inserted += inserted

        end_cnt = get_dividend_table_count()
        logger.info("====== Job 完成：本次写入=%d；表行数 %d -> %d；当日 BaoStock 调用=%d ======", total_inserted, start_cnt, end_cnt, get_today_api_count())

    finally:
        try:
            bs.logout()
        except Exception:
            pass
        logger.info("BaoStock 已登出")


if __name__ == "__main__":
    main()
