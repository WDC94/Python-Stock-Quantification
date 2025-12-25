# -*- coding: utf-8 -*-
"""backtest_mvp.py

回测 MVP（IC + 分层收益 + 组合净值）

口径（MVP）：
- 未来收益：close-to-close（不含分红再投资；如需含分红需要复权或现金分红回补）
- Universe：dwd_stock_basic_all(sec_type=1 & status=1) 且日K tradestatus=1 且非ST(is_st=0)
- 组合：TopN 等权，日频再平衡

依赖表：
- dbo.dws_stock_score_daily
- dbo.dwd_kline_daily_raw
- dbo.dwd_stock_basic_all

输出表：
- dbo.dws_factor_ic_daily
- dbo.dws_factor_layer_ret_daily
- dbo.dws_portfolio_nav_daily
- dbo.dws_portfolio_holdings_daily
- dbo.sys_backtest_run_log

运行示例：
  python backtest_mvp.py --factor total_score_ind --start 2018-01-01 --end 2025-12-31 \
      --ic_horizon 5 --layers 5 --topn 50 --nav_horizon 1
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import time
from typing import Dict, Iterable, List, Optional, Tuple


# ---------------------- DB 适配（兼容 common.db / db.py） ----------------------
try:
    from common.db import query  # type: ignore
    try:
        from common.db import db_conn  # type: ignore
    except Exception:
        db_conn = None
    try:
        from common.db import execute as _execute  # type: ignore
    except Exception:
        _execute = None
except Exception:
    from db import query  # type: ignore
    try:
        from db import db_conn  # type: ignore
    except Exception:
        db_conn = None
    try:
        from db import execute as _execute  # type: ignore
    except Exception:
        _execute = None


def _exec(conn, sql: str, params: Optional[Iterable] = None):
    """单连接执行，避免频繁建连。"""
    params = list(params or [])
    cur = conn.cursor()
    cur.execute(sql, params)


def _executemany(conn, sql: str, rows: List[Tuple]):
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, rows)


def _get_conn():
    if not db_conn:
        raise RuntimeError("db_conn not found in common.db / db.py")
    return db_conn()


# ---------------------- Schema ----------------------

def ensure_schema(conn):
    """确保回测落地表存在（可重复执行）。"""

    _exec(
        conn,
        """
        IF OBJECT_ID('dbo.dws_factor_ic_daily','U') IS NULL
        BEGIN
            CREATE TABLE dbo.dws_factor_ic_daily(
                factor_name   VARCHAR(64)  NOT NULL,
                horizon       INT          NOT NULL,
                trade_date    DATE         NOT NULL,
                ic            FLOAT        NULL,
                rank_ic       FLOAT        NULL,
                n             INT          NOT NULL,
                mean_ret      FLOAT        NULL,
                std_ret       FLOAT        NULL,
                updated_at    DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_dws_factor_ic_daily PRIMARY KEY(factor_name, horizon, trade_date)
            );
            CREATE INDEX IX_factor_ic_date
                ON dbo.dws_factor_ic_daily(trade_date DESC, factor_name, horizon);
        END
        """,
    )

    _exec(
        conn,
        """
        IF OBJECT_ID('dbo.dws_factor_layer_ret_daily','U') IS NULL
        BEGIN
            CREATE TABLE dbo.dws_factor_layer_ret_daily(
                factor_name   VARCHAR(64)  NOT NULL,
                horizon       INT          NOT NULL,
                trade_date    DATE         NOT NULL,
                layer         INT          NOT NULL,
                n             INT          NOT NULL,
                avg_ret       FLOAT        NULL,
                updated_at    DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_dws_factor_layer_ret_daily PRIMARY KEY(factor_name, horizon, trade_date, layer)
            );
            CREATE INDEX IX_layer_ret_date
                ON dbo.dws_factor_layer_ret_daily(trade_date DESC, factor_name, horizon, layer);
        END
        """,
    )

    _exec(
        conn,
        """
        IF OBJECT_ID('dbo.dws_portfolio_nav_daily','U') IS NULL
        BEGIN
            CREATE TABLE dbo.dws_portfolio_nav_daily(
                portfolio_code VARCHAR(128) NOT NULL,
                trade_date     DATE         NOT NULL,
                nav            DECIMAL(20,10) NOT NULL,
                daily_ret      FLOAT        NULL,
                hold_cnt       INT          NULL,
                updated_at     DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_dws_portfolio_nav_daily PRIMARY KEY(portfolio_code, trade_date)
            );
            CREATE INDEX IX_nav_date
                ON dbo.dws_portfolio_nav_daily(trade_date DESC, portfolio_code);
        END
        """,
    )

    _exec(
        conn,
        """
        IF OBJECT_ID('dbo.dws_portfolio_holdings_daily','U') IS NULL
        BEGIN
            CREATE TABLE dbo.dws_portfolio_holdings_daily(
                portfolio_code VARCHAR(128) NOT NULL,
                trade_date     DATE         NOT NULL,
                ts_code        VARCHAR(20)  NOT NULL,
                weight         FLOAT        NOT NULL,
                rank_in_day    INT          NULL,
                updated_at     DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                CONSTRAINT PK_dws_portfolio_holdings_daily PRIMARY KEY(portfolio_code, trade_date, ts_code)
            );
            CREATE INDEX IX_holdings_date
                ON dbo.dws_portfolio_holdings_daily(trade_date DESC, portfolio_code);
        END
        """,
    )

    _exec(
        conn,
        """
        IF OBJECT_ID('dbo.sys_backtest_run_log','U') IS NULL
        BEGIN
            CREATE TABLE dbo.sys_backtest_run_log(
                run_id       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                run_ts       DATETIME2(0) NOT NULL DEFAULT SYSDATETIME(),
                module       VARCHAR(64)  NOT NULL,
                params_json  NVARCHAR(MAX) NULL,
                ok           TINYINT      NOT NULL,
                duration_sec FLOAT       NULL,
                msg          NVARCHAR(1000) NULL
            );
            CREATE INDEX IX_backtest_log_ts
                ON dbo.sys_backtest_run_log(run_ts DESC);
        END
        """,
    )


# ---------------------- Utils ----------------------

def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def get_min_max_score_date() -> Tuple[Optional[dt.date], Optional[dt.date]]:
    rows = query("SELECT MIN(trade_date) AS min_d, MAX(trade_date) AS max_d FROM dbo.dws_stock_score_daily;")
    if not rows:
        return None, None
    return rows[0].get("min_d"), rows[0].get("max_d")


def list_score_columns() -> List[str]:
    rows = query(
        """
        SELECT c.name
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'dbo' AND t.name = 'dws_stock_score_daily'
        ORDER BY c.column_id;
        """
    )
    return [r.get("name") for r in rows if r.get("name")]


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_factor_name(factor: str, cols: List[str]) -> str:
    factor = (factor or "").strip()
    if not factor:
        raise ValueError("factor is empty")
    if not _IDENT_RE.match(factor):
        raise ValueError(f"invalid factor name: {factor}")
    if factor not in cols:
        raise ValueError(f"factor column not found in dbo.dws_stock_score_daily: {factor}")
    return factor


# ---------------------- Core: IC + Layer ----------------------

def calc_ic_and_layer(conn, factor: str, horizon: int, layers: int, start: dt.date, end: dt.date, min_n: int):
    """落地：IC + 分层日收益"""

    # 清理历史结果（幂等）
    _exec(
        conn,
        "DELETE FROM dbo.dws_factor_ic_daily WHERE factor_name=? AND horizon=? AND trade_date BETWEEN ? AND ?;",
        [factor, horizon, start, end],
    )
    _exec(
        conn,
        "DELETE FROM dbo.dws_factor_layer_ret_daily WHERE factor_name=? AND horizon=? AND trade_date BETWEEN ? AND ?;",
        [factor, horizon, start, end],
    )

    # IC（Pearson + RankIC）
    sql_ic = f"""
    DECLARE @start DATE = ?;
    DECLARE @end   DATE = ?;
    DECLARE @h INT = {int(horizon)};

    WITH score AS (
        SELECT s.trade_date, s.ts_code, CAST(s.[{factor}] AS FLOAT) AS factor_val
        FROM dbo.dws_stock_score_daily s
        JOIN dbo.dwd_stock_basic_all b
          ON b.code = s.ts_code AND ISNULL(b.sec_type,1)=1 AND ISNULL(b.status,1)=1
        WHERE s.trade_date BETWEEN @start AND @end
          AND s.[{factor}] IS NOT NULL
    ), ret AS (
        SELECT k.code, k.trade_date,
               CAST(
                 (LEAD(k.[close], @h) OVER(PARTITION BY k.code ORDER BY k.trade_date) - k.[close])
                 / NULLIF(k.[close],0)
               AS FLOAT) AS ret_fwd
        FROM dbo.dwd_kline_daily_raw k
        WHERE k.trade_date BETWEEN @start AND DATEADD(DAY, 400, @end)
          AND ISNULL(k.tradestatus,1)=1
          AND ISNULL(k.is_st,0)=0
    ), base AS (
        SELECT sc.trade_date, sc.ts_code, sc.factor_val, rt.ret_fwd
        FROM score sc
        JOIN ret rt ON rt.code = sc.ts_code AND rt.trade_date = sc.trade_date
        WHERE rt.ret_fwd IS NOT NULL
    ), agg_raw AS (
        SELECT
            trade_date,
            COUNT(1) AS n,
            AVG(ret_fwd) AS mean_ret,
            STDEV(ret_fwd) AS std_ret,
            SUM(factor_val) AS sumx,
            SUM(ret_fwd) AS sumy,
            SUM(factor_val*factor_val) AS sumx2,
            SUM(ret_fwd*ret_fwd) AS sumy2,
            SUM(factor_val*ret_fwd) AS sumxy
        FROM base
        GROUP BY trade_date
    ), ranked AS (
        SELECT
            trade_date,
            DENSE_RANK() OVER(PARTITION BY trade_date ORDER BY factor_val) AS rx,
            DENSE_RANK() OVER(PARTITION BY trade_date ORDER BY ret_fwd)    AS ry
        FROM base
    ), agg_rank AS (
        SELECT
            trade_date,
            COUNT(1) AS n,
            SUM(CAST(rx AS FLOAT)) AS sumx,
            SUM(CAST(ry AS FLOAT)) AS sumy,
            SUM(CAST(rx AS FLOAT)*CAST(rx AS FLOAT)) AS sumx2,
            SUM(CAST(ry AS FLOAT)*CAST(ry AS FLOAT)) AS sumy2,
            SUM(CAST(rx AS FLOAT)*CAST(ry AS FLOAT)) AS sumxy
        FROM ranked
        GROUP BY trade_date
    )

    INSERT INTO dbo.dws_factor_ic_daily(
        factor_name, horizon, trade_date, ic, rank_ic, n, mean_ret, std_ret
    )
    SELECT
        '{factor}' AS factor_name,
        @h AS horizon,
        a.trade_date,

        CASE
          WHEN (a.n*a.sumx2 - a.sumx*a.sumx) = 0 OR (a.n*a.sumy2 - a.sumy*a.sumy) = 0 THEN NULL
          ELSE (a.n*a.sumxy - a.sumx*a.sumy)
               / SQRT((a.n*a.sumx2 - a.sumx*a.sumx) * (a.n*a.sumy2 - a.sumy*a.sumy))
        END AS ic,

        CASE
          WHEN r.n IS NULL THEN NULL
          WHEN (r.n*r.sumx2 - r.sumx*r.sumx) = 0 OR (r.n*r.sumy2 - r.sumy*r.sumy) = 0 THEN NULL
          ELSE (r.n*r.sumxy - r.sumx*r.sumy)
               / SQRT((r.n*r.sumx2 - r.sumx*r.sumx) * (r.n*r.sumy2 - r.sumy*r.sumy))
        END AS rank_ic,

        a.n,
        a.mean_ret,
        a.std_ret
    FROM agg_raw a
    LEFT JOIN agg_rank r ON r.trade_date = a.trade_date
    WHERE a.n >= {int(min_n)};
    """

    _exec(conn, sql_ic, [start, end])

    # 分层收益（1=最优组，因子值最大）
    sql_layer = f"""
    DECLARE @start DATE = ?;
    DECLARE @end   DATE = ?;
    DECLARE @h INT = {int(horizon)};
    DECLARE @layers INT = {int(layers)};

    WITH score AS (
        SELECT s.trade_date, s.ts_code, CAST(s.[{factor}] AS FLOAT) AS factor_val
        FROM dbo.dws_stock_score_daily s
        JOIN dbo.dwd_stock_basic_all b
          ON b.code = s.ts_code AND ISNULL(b.sec_type,1)=1 AND ISNULL(b.status,1)=1
        WHERE s.trade_date BETWEEN @start AND @end
          AND s.[{factor}] IS NOT NULL
    ), ret AS (
        SELECT k.code, k.trade_date,
               CAST(
                 (LEAD(k.[close], @h) OVER(PARTITION BY k.code ORDER BY k.trade_date) - k.[close])
                 / NULLIF(k.[close],0)
               AS FLOAT) AS ret_fwd
        FROM dbo.dwd_kline_daily_raw k
        WHERE k.trade_date BETWEEN @start AND DATEADD(DAY, 400, @end)
          AND ISNULL(k.tradestatus,1)=1
          AND ISNULL(k.is_st,0)=0
    ), base AS (
        SELECT sc.trade_date, sc.ts_code, sc.factor_val, rt.ret_fwd
        FROM score sc
        JOIN ret rt ON rt.code = sc.ts_code AND rt.trade_date = sc.trade_date
        WHERE rt.ret_fwd IS NOT NULL
    ), layered AS (
        SELECT
            trade_date,
            NTILE(@layers) OVER(PARTITION BY trade_date ORDER BY factor_val DESC) AS layer,
            ret_fwd
        FROM base
    )

    INSERT INTO dbo.dws_factor_layer_ret_daily(
        factor_name, horizon, trade_date, layer, n, avg_ret
    )
    SELECT
        '{factor}' AS factor_name,
        @h AS horizon,
        trade_date,
        layer,
        COUNT(1) AS n,
        AVG(ret_fwd) AS avg_ret
    FROM layered
    GROUP BY trade_date, layer;
    """

    _exec(conn, sql_layer, [start, end])


# ---------------------- Core: Portfolio NAV ----------------------

def calc_portfolio_nav(conn, factor: str, nav_horizon: int, topn: int, start: dt.date, end: dt.date) -> str:
    """落地：TopN 等权组合净值（日频再平衡）"""

    portfolio_code = f"TOP{int(topn)}_{factor}_H{int(nav_horizon)}_D"

    # 清理旧数据
    _exec(
        conn,
        "DELETE FROM dbo.dws_portfolio_nav_daily WHERE portfolio_code=? AND trade_date BETWEEN ? AND ?;",
        [portfolio_code, start, end],
    )
    _exec(
        conn,
        "DELETE FROM dbo.dws_portfolio_holdings_daily WHERE portfolio_code=? AND trade_date BETWEEN ? AND ?;",
        [portfolio_code, start, end],
    )

    # 写入持仓快照（再平衡日）
    sql_hold = f"""
    DECLARE @start DATE = ?;
    DECLARE @end   DATE = ?;
    DECLARE @topn INT = {int(topn)};

    WITH score AS (
        SELECT s.trade_date, s.ts_code, CAST(s.[{factor}] AS FLOAT) AS factor_val
        FROM dbo.dws_stock_score_daily s
        JOIN dbo.dwd_stock_basic_all b
          ON b.code = s.ts_code AND ISNULL(b.sec_type,1)=1 AND ISNULL(b.status,1)=1
        WHERE s.trade_date BETWEEN @start AND @end
          AND s.[{factor}] IS NOT NULL
    ), ranked AS (
        SELECT
            trade_date,
            ts_code,
            ROW_NUMBER() OVER(PARTITION BY trade_date ORDER BY factor_val DESC) AS rn
        FROM score
    ), picked AS (
        SELECT trade_date, ts_code, rn
        FROM ranked
        WHERE rn <= @topn
    ), cnt AS (
        SELECT trade_date, COUNT(1) AS cnt
        FROM picked
        GROUP BY trade_date
    )

    INSERT INTO dbo.dws_portfolio_holdings_daily(portfolio_code, trade_date, ts_code, weight, rank_in_day)
    SELECT
        ?,
        p.trade_date,
        p.ts_code,
        CAST(1.0 / NULLIF(c.cnt,0) AS FLOAT) AS weight,
        p.rn
    FROM picked p
    JOIN cnt c ON c.trade_date = p.trade_date;
    """
    _exec(conn, sql_hold, [start, end, portfolio_code])

    # 拉取组合日收益（以因子日 t 对应未来 nav_horizon 收益）
    sql_ret = f"""
    DECLARE @start DATE = ?;
    DECLARE @end   DATE = ?;
    DECLARE @h INT = {int(nav_horizon)};
    DECLARE @topn INT = {int(topn)};

    WITH score AS (
        SELECT s.trade_date, s.ts_code, CAST(s.[{factor}] AS FLOAT) AS factor_val
        FROM dbo.dws_stock_score_daily s
        JOIN dbo.dwd_stock_basic_all b
          ON b.code = s.ts_code AND ISNULL(b.sec_type,1)=1 AND ISNULL(b.status,1)=1
        WHERE s.trade_date BETWEEN @start AND @end
          AND s.[{factor}] IS NOT NULL
    ), ret AS (
        SELECT k.code, k.trade_date,
               CAST(
                 (LEAD(k.[close], @h) OVER(PARTITION BY k.code ORDER BY k.trade_date) - k.[close])
                 / NULLIF(k.[close],0)
               AS FLOAT) AS ret_fwd
        FROM dbo.dwd_kline_daily_raw k
        WHERE k.trade_date BETWEEN @start AND DATEADD(DAY, 400, @end)
          AND ISNULL(k.tradestatus,1)=1
          AND ISNULL(k.is_st,0)=0
    ), base AS (
        SELECT sc.trade_date, sc.ts_code, sc.factor_val, rt.ret_fwd
        FROM score sc
        JOIN ret rt ON rt.code = sc.ts_code AND rt.trade_date = sc.trade_date
        WHERE rt.ret_fwd IS NOT NULL
    ), ranked AS (
        SELECT
            trade_date,
            ret_fwd,
            ROW_NUMBER() OVER(PARTITION BY trade_date ORDER BY factor_val DESC) AS rn
        FROM base
    )

    SELECT trade_date,
           AVG(ret_fwd) AS daily_ret,
           COUNT(1) AS hold_cnt
    FROM ranked
    WHERE rn <= @topn
    GROUP BY trade_date
    ORDER BY trade_date;
    """

    ret_rows = query(sql_ret, [start, end])
    ret_map: Dict[dt.date, float] = {}
    cnt_map: Dict[dt.date, int] = {}
    for r in ret_rows:
        d = r.get("trade_date")
        if d is None:
            continue
        v = r.get("daily_ret")
        if v is None:
            continue
        ret_map[d] = float(v)
        cnt_map[d] = int(r.get("hold_cnt") or 0)

    # 交易日历（用于把 t -> t+1 的收益写到 t+1 日期的净值上）
    end_plus = end + dt.timedelta(days=400)
    cal_rows = query(
        "SELECT DISTINCT trade_date FROM dbo.dwd_kline_daily_raw WHERE trade_date BETWEEN ? AND ? ORDER BY trade_date;",
        [start, end_plus],
    )
    cal_dates = [r.get("trade_date") for r in cal_rows if r.get("trade_date")]
    if not cal_dates:
        return portfolio_code

    # 只写入 [start, end] 区间的净值序列
    cal_dates = [d for d in cal_dates if start <= d <= end]
    if not cal_dates:
        return portfolio_code

    nav = 1.0
    out_rows: List[Tuple[str, dt.date, float, Optional[float], Optional[int]]] = []
    for i, d in enumerate(cal_dates):
        if i == 0:
            out_rows.append((portfolio_code, d, float(nav), None, None))
            continue
        prev = cal_dates[i - 1]
        r = ret_map.get(prev)
        if r is None:
            out_rows.append((portfolio_code, d, float(nav), None, None))
        else:
            nav = nav * (1.0 + float(r))
            out_rows.append((portfolio_code, d, float(nav), float(r), cnt_map.get(prev)))

    _executemany(
        conn,
        """
        INSERT INTO dbo.dws_portfolio_nav_daily(portfolio_code, trade_date, nav, daily_ret, hold_cnt)
        VALUES (?, ?, ?, ?, ?);
        """,
        out_rows,
    )

    return portfolio_code


# ---------------------- Run / Log ----------------------

def log_run(conn, module: str, params: dict, ok: bool, duration_sec: float, msg: str):
    try:
        _exec(
            conn,
            """
            INSERT INTO dbo.sys_backtest_run_log(module, params_json, ok, duration_sec, msg)
            VALUES (?, ?, ?, ?, ?);
            """,
            [module, json.dumps(params, ensure_ascii=False), 1 if ok else 0, float(duration_sec), msg[:1000]],
        )
    except Exception:
        # 日志失败不影响主流程
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", type=str, default="total_score_ind", help="单因子列名；多个用逗号分隔；或 ALL")
    ap.add_argument("--start", type=str, default="", help="YYYY-MM-DD")
    ap.add_argument("--end", type=str, default="", help="YYYY-MM-DD")
    ap.add_argument("--ic_horizon", type=int, default=5, help="IC / 分层收益 的未来收益周期（交易日）")
    ap.add_argument("--layers", type=int, default=5, help="分层数量")
    ap.add_argument("--min_n", type=int, default=200, help="单日最小样本数（过滤掉样本太少的日期）")
    ap.add_argument("--nav_horizon", type=int, default=1, help="组合净值的未来收益周期（交易日）")
    ap.add_argument("--topn", type=int, default=50, help="TopN 组合持仓数")
    args = ap.parse_args()

    t0 = time.time()
    ok = True
    msg = ""

    params = {
        "factor": args.factor,
        "start": args.start,
        "end": args.end,
        "ic_horizon": args.ic_horizon,
        "layers": args.layers,
        "min_n": args.min_n,
        "nav_horizon": args.nav_horizon,
        "topn": args.topn,
    }

    conn = _get_conn()
    try:
        ensure_schema(conn)
        cols = list_score_columns()

        min_d, max_d = get_min_max_score_date()
        if not min_d or not max_d:
            raise RuntimeError("dbo.dws_stock_score_daily is empty")

        start = parse_date(args.start) if args.start else min_d
        end = parse_date(args.end) if args.end else max_d

        if start > end:
            raise ValueError("start > end")

        # 因子列表
        if args.factor.strip().upper() == "ALL":
            # 机构常用：总分 + 8 维度
            candidates = [
                "total_score",
                "total_score_ind",
                "score_profit",
                "score_operation",
                "score_growth",
                "score_safety",
                "score_cashflow",
                "score_valuation",
                "score_dividend",
                "score_size",
            ]
            factors = [f for f in candidates if f in cols]
        else:
            factors = [f.strip() for f in args.factor.split(",") if f.strip()]

        factors = [validate_factor_name(f, cols) for f in factors]

        for f in factors:
            calc_ic_and_layer(conn, f, int(args.ic_horizon), int(args.layers), start, end, int(args.min_n))
            calc_portfolio_nav(conn, f, int(args.nav_horizon), int(args.topn), start, end)

        conn.commit()

    except Exception as e:
        ok = False
        msg = str(e)
        try:
            conn.rollback()
        except Exception:
            pass

    finally:
        duration = time.time() - t0
        log_run(conn, "backtest_mvp", params, ok, duration, msg)
        try:
            conn.commit()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    if not ok:
        raise SystemExit(msg)


if __name__ == "__main__":
    main()
