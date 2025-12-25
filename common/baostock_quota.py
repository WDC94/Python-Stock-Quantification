# -*- coding: utf-8 -*-
"""
common/baostock_quota.py

目的：
- 统一统计 BaoStock 当日 API 调用次数（写入 SQL Server）
- 达到阈值（默认 80000）后：bs.logout() + 抛异常，中断后续所有请求

使用方式：
- 把所有 bs.query_* 调用改为：bs_call(bs.query_xxx, **kwargs)
"""

import os
import datetime as dt
import logging
from typing import Any, Callable, Tuple

import baostock as bs
from common.db import db_conn  # 你项目现有封装

logger = logging.getLogger("common.baostock_quota")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [baostock_quota] %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)

# 软阈值：达到即停止（留 2 万冗余）
BAOSTOCK_STOP_AT = int(os.getenv("BAOSTOCK_STOP_AT", "80000"))
# 硬阈值：理论上限（一般不用触发，仅做兜底）
BAOSTOCK_HARD_LIMIT = int(os.getenv("BAOSTOCK_HARD_LIMIT", "100000"))


class BaoStockQuotaExceeded(RuntimeError):
    """当日 BaoStock API 调用达到阈值，强制中断。"""


def _ensure_table() -> None:
    """确保计数表存在。"""
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


def get_today_count() -> int:
    """获取当日累计调用次数（不存在则 0）。"""
    _ensure_table()
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


def _reserve_one_call(stop_at: int = BAOSTOCK_STOP_AT) -> Tuple[int, bool]:
    """
    预占 1 次调用配额（原子操作）：
    - 若当前 < stop_at：req_count += 1，allowed=True
    - 若当前 >= stop_at：不再增长，allowed=False
    返回：(当前/更新后 req_count, allowed)
    """
    _ensure_table()
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
        f"BaoStock 当日 API 调用已达到阈值：{cnt} / {stop_at}（已强制 logout 并中断后续请求）"
    )


def bs_call(func: Callable[..., Any], *args, stop_at: int = BAOSTOCK_STOP_AT, **kwargs) -> Any:
    """
    BaoStock API 调用统一入口：
    - 先预占配额
    - 达到 stop_at：立刻 logout + 中断
    - 否则执行真实 API 调用
    """
    cnt, allowed = _reserve_one_call(stop_at=stop_at)

    # 硬上限兜底（极端情况下）
    if cnt >= BAOSTOCK_HARD_LIMIT:
        _force_stop(cnt, stop_at)

    if not allowed:
        _force_stop(cnt, stop_at)

    # 允许调用
    return func(*args, **kwargs)
# -----------------------------------------------------------------------------
# 兼容旧版：BaoStockDailyQuota
# - 旧代码会先 quota.incr() 再调用 bs.query_*，这里提供同名类避免 ImportError。
# - 新代码推荐直接使用 bs_call(...)。
# -----------------------------------------------------------------------------

class BaoStockDailyQuota:
    """兼容旧版配额对象（incr/get_today_count），内部复用当前的计数表逻辑。"""

    def __init__(self, limit: int = BAOSTOCK_STOP_AT):
        self.limit = int(limit)

    def incr(self, n: int = 1, api_name: str = "") -> int:
        """
        预占 n 次调用额度。
        - 达到阈值：立刻 bs.logout() + 抛 BaoStockQuotaExceeded
        返回：当前累计次数（最后一次预占后的 cnt）
        """
        try:
            times = int(n)
        except Exception:
            times = 1
        if times <= 0:
            times = 1

        cnt = 0
        for _ in range(times):
            cnt, allowed = _reserve_one_call(stop_at=self.limit)

            # 硬上限兜底
            if cnt >= BAOSTOCK_HARD_LIMIT:
                _force_stop(cnt, self.limit)

            if not allowed:
                _force_stop(cnt, self.limit)

        if api_name:
            logger.debug("quota incr: api=%s, cnt=%s/%s", api_name, cnt, self.limit)
        return cnt

    def get_today_count(self) -> int:
        return get_today_count()

