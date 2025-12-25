# -*- coding: utf-8 -*-
"""
jobs/query_stock_basic.py

功能：
- 通过 BaoStock 获取“全部证券基本资料”：
    • query_stock_basic()
    • query_stock_industry()
- 按 code 关联两个接口的结果，整合为一份证券维度表；
- 全量覆盖写入 SQL Server 表：dbo.dwd_stock_basic_all
  （先 TRUNCATE，再批量 INSERT）

与现有 fetch_* 系列无耦合。
"""

import logging
import datetime as dt
from typing import Dict, List, Optional, Tuple, Any

import baostock as bs

from common.db import db_conn  # 按你项目里 db 封装路径，如果是 db.py 就改成 "from db import db_conn"

logger = logging.getLogger("jobs.query_stock_basic")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] [query_stock_basic] %(message)s"
    )
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

BATCH_SIZE = 1000


# ---------- 工具函数 ----------

def safe_date(v: Any) -> Optional[dt.date]:
    if v is None:
        return None
    if isinstance(v, dt.date):
        return v
    s = str(v).strip()
    if not s or s in ("0", "0000-00-00"):
        return None
    try:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


# ---------- BaoStock 取数 ----------

def fetch_stock_basic() -> List[Dict[str, Any]]:
    """
    调用 query_stock_basic()，获取全部证券基础资料。
    返回字段：code, code_name, ipoDate, outDate, type, status
    """
    logger.info("调用 BaoStock query_stock_basic() 获取全部证券基本资料 ...")
    rs = bs.query_stock_basic()
    if rs.error_code != "0":
        raise RuntimeError(f"query_stock_basic 失败: {rs.error_code}, {rs.error_msg}")

    result: List[Dict[str, Any]] = []
    fields = rs.fields
    while rs.next():
        row = dict(zip(fields, rs.get_row_data()))
        result.append(row)

    logger.info("query_stock_basic 返回记录数：%s", len(result))
    return result


def fetch_stock_industry() -> Dict[str, Dict[str, Any]]:
    """
    调用 query_stock_industry()，按 code 聚合行业信息。
    返回 dict: { code: {industry, industryClassification, updateDate, ...}, ... }
    """
    logger.info("调用 BaoStock query_stock_industry() 获取行业分类 ...")
    rs = bs.query_stock_industry()
    if rs.error_code != "0":
        raise RuntimeError(f"query_stock_industry 失败: {rs.error_code}, {rs.error_msg}")

    fields = rs.fields
    result: Dict[str, Dict[str, Any]] = {}

    while rs.next():
        row = dict(zip(fields, rs.get_row_data()))
        code = row.get("code")
        if not code:
            continue
        result[code] = row

    logger.info("query_stock_industry 覆盖股票数量：%s", len(result))
    return result


# ---------- DB 写入 ----------

def truncate_target_table() -> None:
    """清空目标表，保证本 Job 是全量覆盖。"""
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("IF OBJECT_ID('dbo.dwd_stock_basic_all','U') IS NOT NULL "
                    "TRUNCATE TABLE dbo.dwd_stock_basic_all;")
        conn.commit()
    logger.info("已清空目标表 dbo.dwd_stock_basic_all")


def insert_records(records: List[Tuple]) -> int:
    """批量插入 dwd_stock_basic_all。"""
    if not records:
        return 0

    sql = """
    INSERT INTO dbo.dwd_stock_basic_all(
        code, code_name, ipo_date, out_date,
        sec_type, status,
        industry, industry_class, industry_update_date
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """

    with db_conn() as conn:
        cur = conn.cursor()
        try:
            try:
                cur.fast_executemany = True  # pyodbc 性能优化
            except Exception:
                pass
            cur.executemany(sql, records)
            conn.commit()
            return cur.rowcount or 0
        finally:
            cur.close()


# ---------- 主流程 ----------

def main() -> None:
    logger.info("====== 开始执行证券基础资料全量同步 Job ======")

    # 1. 登录 BaoStock
    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {lg.error_code}, {lg.error_msg}")
    logger.info("BaoStock login success")

    try:
        # 2. 获取两份原始数据
        basic_list = fetch_stock_basic()
        industry_map = fetch_stock_industry()

        if not basic_list:
            logger.warning("query_stock_basic 返回为空，直接结束")
            return

        # 3. 先清空目标表
        truncate_target_table()

        # 4. 组装整合后的记录并分批写入
        batch: List[Tuple] = []
        total_inserted = 0

        for i, b in enumerate(basic_list, start=1):
            code = b.get("code")
            if not code:
                continue

            code_name = b.get("code_name") or ""
            ipo_date = safe_date(b.get("ipoDate"))
            out_date = safe_date(b.get("outDate"))
            sec_type = safe_int(b.get("type"))
            status = safe_int(b.get("status"))

            ind = industry_map.get(code, {})
            industry = ind.get("industry")
            industry_class = ind.get("industryClassification")
            industry_update_date = safe_date(ind.get("updateDate"))

            batch.append(
                (
                    code,
                    code_name,
                    ipo_date,
                    out_date,
                    sec_type,
                    status,
                    industry,
                    industry_class,
                    industry_update_date,
                )
            )

            if len(batch) >= BATCH_SIZE:
                inserted = insert_records(batch)
                total_inserted += inserted
                logger.info(
                    "已处理 %d 条基础记录，本批写入 %d 条，累计 %d 条",
                    i, inserted, total_inserted
                )
                batch = []

        # 最后一批
        if batch:
            inserted = insert_records(batch)
            total_inserted += inserted

        logger.info("====== 证券基础资料同步完成，最终写入记录数：%s ======", total_inserted)

    finally:
        bs.logout()
        logger.info("BaoStock 已登出")


if __name__ == "__main__":
    main()
