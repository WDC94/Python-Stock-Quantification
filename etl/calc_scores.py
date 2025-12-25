# -*- coding: utf-8 -*-
"""calc_scores.py

修复点：
1) db_conn() 兼容上下文管理器（GeneratorContextManager）与直连两种返回形态，避免 cursor 报错。
2) trade_date 未传入时，自动取 dbo.dwd_kline_daily_raw 的最新交易日，避免非交易日跑出 0 行评分。
"""

import datetime as dt
from typing import Optional, Set
from contextlib import ExitStack

from common.db import db_conn


TODAY = dt.date.today()

# 各维度权重（可按策略层配置下发；这里先按默认口径）
WEIGHTS = {
    "profit": 0.20,
    "growth": 0.15,
    "cashflow": 0.15,
    "safety": 0.15,
    "valuation": 0.15,
    "operation": 0.10,
    "dividend": 0.10,
}


def _safe_close(obj):
    try:
        obj.close()
    except Exception:
        pass


def _enter_conn(stack: ExitStack):
    """
    兼容两种 db_conn():
    - 返回 context manager（你现在实际触发的就是这种）
    - 返回 pyodbc connection（少数项目会这样写）
    """
    conn_obj = db_conn()
    if hasattr(conn_obj, "__enter__") and hasattr(conn_obj, "__exit__"):
        return stack.enter_context(conn_obj)
    # 当作直连
    stack.callback(_safe_close, conn_obj)
    return conn_obj


def _table_columns(conn, table_name: str) -> Set[str]:
    """读取 dbo.<table_name> 的字段集合；表不存在则返回空集合。"""
    sql = """
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME=?
    """
    try:
        cur = conn.cursor()
        cur.execute(sql, (table_name,))
        cols = {str(r[0]) for r in cur.fetchall()}
        _safe_close(cur)
        return cols
    except Exception:
        return set()


def _pick(cols: Set[str], *candidates: str) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _resolve_latest_trade_date(conn) -> dt.date:
    """
    默认用 K 线表最新交易日，避免 TODAY 不是交易日导致插入 0 行。
    兼容字段 trade_date / date 两种命名。
    """
    cur = conn.cursor()
    try:
        try:
            cur.execute("SELECT MAX(trade_date) FROM dbo.dwd_kline_daily_raw;")
            r = cur.fetchone()
            if r and r[0]:
                return r[0]
        except Exception:
            pass

        try:
            cur.execute("SELECT MAX([date]) FROM dbo.dwd_kline_daily_raw;")
            r = cur.fetchone()
            if r and r[0]:
                return r[0]
        except Exception:
            pass

        # 实在取不到就回退 TODAY（但一般说明K线表为空）
        return TODAY
    finally:
        _safe_close(cur)


def run_calc_scores(trade_date: Optional[dt.date] = None) -> None:
    """计算并落库 dbo.dws_stock_score_daily（按 trade_date 全量重算，幂等）。"""
    with ExitStack() as stack:
        conn = _enter_conn(stack)
        cur = conn.cursor()

        try:
            trade_date = trade_date or _resolve_latest_trade_date(conn)

            # -------- 1) 幂等清理 --------
            cur.execute("DELETE FROM dbo.dws_stock_score_daily WHERE trade_date = ?", (trade_date,))
            conn.commit()

            # -------- 2) 自适应字段映射（兼容历史 schema 演进） --------
            cols_profit = _table_columns(conn, "fact_profit_quarterly")
            cols_oper = _table_columns(conn, "fact_operation_quarterly")
            cols_growth = _table_columns(conn, "fact_growth_quarterly")
            cols_bal = _table_columns(conn, "fact_balance_quarterly")
            cols_cf = _table_columns(conn, "fact_cashflow_quarterly")

            # profit
            p_roe = _pick(cols_profit, "roe_ttm", "roe", "roeAvg")
            p_npm = _pick(cols_profit, "net_profit_margin", "np_margin", "npMargin")
            p_gpm = _pick(cols_profit, "gross_margin", "gp_margin", "gpMargin")

            # operation
            o_asset_turn = _pick(cols_oper, "asset_turnover", "asset_turn_ratio", "assetTurnRatio")
            o_inv_turn = _pick(cols_oper, "inventory_turnover", "inv_turn_ratio", "invTurnRatio")
            o_ar_turn = _pick(cols_oper, "ar_turnover", "nr_turn_ratio", "nrTurnRatio")

            # growth
            g_yoy = _pick(cols_growth, "revenue_yoy", "yoy_pni", "yoy_ni", "YOYPNI", "YOYNI")
            g_qoq = _pick(cols_growth, "revenue_qoq")

            # balance
            b_dar = _pick(cols_bal, "debt_asset_ratio", "liability_to_asset", "liabilityToAsset")
            b_cr = _pick(cols_bal, "current_ratio", "currentRatio")
            b_qr = _pick(cols_bal, "quick_ratio", "quickRatio")

            # cashflow
            c_ocf_np = _pick(cols_cf, "ocf_net_profit", "cfo_to_np", "CFOToNP")

            def col_or_null(alias: str, col: Optional[str]) -> str:
                return f"TRY_CONVERT(FLOAT, {alias}.[{col}])" if col else "CAST(NULL AS FLOAT)"

            # -------- 3) 落库：计算 MA + 取最新一期季报 + 计算分红 TTM --------
            sql = f"""
            SET NOCOUNT ON;

            DECLARE @trade_date date = ?;

            WITH k_hist AS (
                SELECT
                    k.code,
                    k.trade_date,
                    k.[close],
                    k.pe_ttm,
                    k.pb_mrq,
                    k.ps_ttm
                FROM dbo.dwd_kline_daily_raw k
                WHERE k.trade_date <= @trade_date
                  AND k.trade_date >= DATEADD(day, -420, @trade_date)
            ),
            k_ma AS (
                SELECT
                    code,
                    trade_date,
                    [close],
                    pe_ttm,
                    pb_mrq,
                    ps_ttm,
                    AVG([close]) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)  AS ma60,
                    AVG([close]) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) AS ma120,
                    AVG([close]) OVER (PARTITION BY code ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS ma250
                FROM k_hist
            ),
            k_today AS (
                SELECT
                    code,
                    trade_date,
                    [close],
                    pe_ttm,
                    pb_mrq,
                    ps_ttm,
                    ma60,
                    ma120,
                    ma250,
                    [close] / NULLIF(ma60, 0)  AS price_vs_ma60,
                    [close] / NULLIF(ma120, 0) AS price_vs_ma120,
                    [close] / NULLIF(ma250, 0) AS price_vs_ma250,
                    CONCAT(RIGHT(code, 6), '.', UPPER(LEFT(code, 2))) AS ts_code_alt
                FROM k_ma
                WHERE trade_date = @trade_date
            )
            INSERT INTO dbo.dws_stock_score_daily (
                ts_code, trade_date,

                roe_ttm, net_profit_margin, gross_margin,
                score_profit,

                asset_turnover, inventory_turnover, ar_turnover,
                score_operation,

                revenue_yoy, revenue_qoq,
                score_growth,

                debt_asset_ratio, current_ratio, quick_ratio,
                score_safety,

                ocf_net_profit, free_cash_flow,
                score_cashflow,

                pe_ttm, pb_mrq, ps_ttm,
                ma60, ma120, ma250,
                price_vs_ma60, price_vs_ma120, price_vs_ma250,
                score_valuation,

                dividend_yield_ttm, dividend_years,
                score_dividend,

                total_score, rating, rating_desc
            )
            SELECT
                k.code AS ts_code,
                k.trade_date,

                pf.roe_raw,
                pf.npm_raw,
                pf.gpm_raw,
                CASE
                    WHEN pf.roe_pct IS NULL THEN NULL
                    WHEN pf.roe_pct >= 20 THEN 100
                    WHEN pf.roe_pct >= 15 THEN 80
                    WHEN pf.roe_pct >= 10 THEN 60
                    WHEN pf.roe_pct >= 5  THEN 40
                    ELSE 20
                END AS score_profit,

                op.asset_turn_raw,
                op.inv_turn_raw,
                op.ar_turn_raw,
                CASE
                    WHEN op.asset_turn_raw IS NULL THEN NULL
                    WHEN op.asset_turn_raw >= 1.2 THEN 100
                    WHEN op.asset_turn_raw >= 0.8 THEN 70
                    ELSE 40
                END AS score_operation,

                gr.yoy_raw,
                gr.qoq_raw,
                CASE
                    WHEN gr.yoy_pct IS NULL THEN NULL
                    WHEN gr.yoy_pct >= 30 THEN 100
                    WHEN gr.yoy_pct >= 15 THEN 70
                    WHEN gr.yoy_pct >= 5  THEN 50
                    ELSE 30
                END AS score_growth,

                bl.dar_raw,
                bl.cr_raw,
                bl.qr_raw,
                CASE
                    WHEN bl.dar_pct IS NULL THEN NULL
                    WHEN bl.dar_pct <= 40 THEN 100
                    WHEN bl.dar_pct <= 60 THEN 70
                    ELSE 40
                END AS score_safety,

                cf.ocf_np_raw,
                CAST(NULL AS FLOAT) AS free_cash_flow,
                CASE
                    WHEN cf.ocf_np_raw IS NULL THEN NULL
                    WHEN cf.ocf_np_raw >= 1 THEN 100
                    WHEN cf.ocf_np_raw >= 0.7 THEN 70
                    ELSE 40
                END AS score_cashflow,

                TRY_CONVERT(FLOAT, k.pe_ttm) AS pe_ttm,
                TRY_CONVERT(FLOAT, k.pb_mrq) AS pb_mrq,
                TRY_CONVERT(FLOAT, k.ps_ttm) AS ps_ttm,
                TRY_CONVERT(FLOAT, k.ma60)   AS ma60,
                TRY_CONVERT(FLOAT, k.ma120)  AS ma120,
                TRY_CONVERT(FLOAT, k.ma250)  AS ma250,
                TRY_CONVERT(FLOAT, k.price_vs_ma60)  AS price_vs_ma60,
                TRY_CONVERT(FLOAT, k.price_vs_ma120) AS price_vs_ma120,
                TRY_CONVERT(FLOAT, k.price_vs_ma250) AS price_vs_ma250,
                CASE
                    WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN NULL
                    WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100
                    WHEN k.pe_ttm < 20 THEN 70
                    ELSE 40
                END AS score_valuation,

                dv.dividend_yield_ttm,
                dv.dividend_years,
                CASE
                    WHEN dv.dividend_yield_ttm IS NULL THEN NULL
                    WHEN dv.dividend_yield_ttm >= 6 THEN 100
                    WHEN dv.dividend_yield_ttm >= 4 THEN 70
                    ELSE 40
                END AS score_dividend,

                (
                    {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5  THEN 40 ELSE 20 END, 0)
                  + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5  THEN 50 ELSE 30 END, 0)
                  + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                  + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                  + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                  + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                  + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                ) AS total_score,

                CASE
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 85 THEN 'A'
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 70 THEN 'B'
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 55 THEN 'C'
                    ELSE 'D'
                END AS rating,

                CASE
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 85 THEN '核心资产'
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 70 THEN '稳健型'
                    WHEN (
                        {WEIGHTS['profit']}    * ISNULL(CASE WHEN pf.roe_pct IS NULL THEN 0 WHEN pf.roe_pct >= 20 THEN 100 WHEN pf.roe_pct >= 15 THEN 80 WHEN pf.roe_pct >= 10 THEN 60 WHEN pf.roe_pct >= 5 THEN 40 ELSE 20 END, 0)
                      + {WEIGHTS['growth']}    * ISNULL(CASE WHEN gr.yoy_pct IS NULL THEN 0 WHEN gr.yoy_pct >= 30 THEN 100 WHEN gr.yoy_pct >= 15 THEN 70 WHEN gr.yoy_pct >= 5 THEN 50 ELSE 30 END, 0)
                      + {WEIGHTS['cashflow']}  * ISNULL(CASE WHEN cf.ocf_np_raw IS NULL THEN 0 WHEN cf.ocf_np_raw >= 1 THEN 100 WHEN cf.ocf_np_raw >= 0.7 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['safety']}    * ISNULL(CASE WHEN bl.dar_pct IS NULL THEN 0 WHEN bl.dar_pct <= 40 THEN 100 WHEN bl.dar_pct <= 60 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['valuation']} * ISNULL(CASE WHEN k.pe_ttm IS NULL OR k.pb_mrq IS NULL THEN 0 WHEN k.pe_ttm < 10 AND k.pb_mrq < 1 THEN 100 WHEN k.pe_ttm < 20 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['operation']} * ISNULL(CASE WHEN op.asset_turn_raw IS NULL THEN 0 WHEN op.asset_turn_raw >= 1.2 THEN 100 WHEN op.asset_turn_raw >= 0.8 THEN 70 ELSE 40 END, 0)
                      + {WEIGHTS['dividend']}  * ISNULL(CASE WHEN dv.dividend_yield_ttm IS NULL THEN 0 WHEN dv.dividend_yield_ttm >= 6 THEN 100 WHEN dv.dividend_yield_ttm >= 4 THEN 70 ELSE 40 END, 0)
                    ) >= 55 THEN '一般'
                    ELSE '风险'
                END AS rating_desc

            FROM k_today k

            OUTER APPLY (
                SELECT TOP 1 *
                FROM dbo.fact_profit_quarterly p
                WHERE p.ts_code = k.code OR p.ts_code = k.ts_code_alt
                ORDER BY p.fiscal_year DESC, p.quarter DESC, ISNULL(p.pub_date, p.stat_date) DESC
            ) p
            CROSS APPLY (
                SELECT
                    {col_or_null('p', p_roe)} AS roe_raw,
                    {col_or_null('p', p_npm)} AS npm_raw,
                    {col_or_null('p', p_gpm)} AS gpm_raw,
                    CASE
                        WHEN {col_or_null('p', p_roe)} IS NULL THEN NULL
                        WHEN {col_or_null('p', p_roe)} <= 1 THEN {col_or_null('p', p_roe)} * 100
                        ELSE {col_or_null('p', p_roe)}
                    END AS roe_pct
            ) pf

            OUTER APPLY (
                SELECT TOP 1 *
                FROM dbo.fact_operation_quarterly o
                WHERE o.ts_code = k.code OR o.ts_code = k.ts_code_alt
                ORDER BY o.fiscal_year DESC, o.quarter DESC, ISNULL(o.pub_date, o.stat_date) DESC
            ) o
            CROSS APPLY (
                SELECT
                    {col_or_null('o', o_asset_turn)} AS asset_turn_raw,
                    {col_or_null('o', o_inv_turn)}   AS inv_turn_raw,
                    {col_or_null('o', o_ar_turn)}    AS ar_turn_raw
            ) op

            OUTER APPLY (
                SELECT TOP 1 *
                FROM dbo.fact_growth_quarterly g
                WHERE g.ts_code = k.code OR g.ts_code = k.ts_code_alt
                ORDER BY g.fiscal_year DESC, g.quarter DESC, ISNULL(g.pub_date, g.stat_date) DESC
            ) g
            CROSS APPLY (
                SELECT
                    {col_or_null('g', g_yoy)} AS yoy_raw,
                    {col_or_null('g', g_qoq)} AS qoq_raw,
                    CASE
                        WHEN {col_or_null('g', g_yoy)} IS NULL THEN NULL
                        WHEN {col_or_null('g', g_yoy)} <= 1 THEN {col_or_null('g', g_yoy)} * 100
                        ELSE {col_or_null('g', g_yoy)}
                    END AS yoy_pct
            ) gr

            OUTER APPLY (
                SELECT TOP 1 *
                FROM dbo.fact_balance_quarterly b
                WHERE b.ts_code = k.code OR b.ts_code = k.ts_code_alt
                ORDER BY b.fiscal_year DESC, b.quarter DESC, ISNULL(b.pub_date, b.stat_date) DESC
            ) b
            CROSS APPLY (
                SELECT
                    {col_or_null('b', b_dar)} AS dar_raw,
                    {col_or_null('b', b_cr)}  AS cr_raw,
                    {col_or_null('b', b_qr)}  AS qr_raw,
                    CASE
                        WHEN {col_or_null('b', b_dar)} IS NULL THEN NULL
                        WHEN {col_or_null('b', b_dar)} <= 1 THEN {col_or_null('b', b_dar)} * 100
                        ELSE {col_or_null('b', b_dar)}
                    END AS dar_pct
            ) bl

            OUTER APPLY (
                SELECT TOP 1 *
                FROM dbo.fact_cashflow_quarterly c
                WHERE c.ts_code = k.code OR c.ts_code = k.ts_code_alt
                ORDER BY c.fiscal_year DESC, c.quarter DESC, ISNULL(c.pub_date, c.stat_date) DESC
            ) c
            CROSS APPLY (
                SELECT
                    {col_or_null('c', c_ocf_np)} AS ocf_np_raw
            ) cf

            OUTER APPLY (
                SELECT
                    CAST(
                        CASE
                            WHEN k.[close] IS NULL OR k.[close] = 0 THEN NULL
                            WHEN SUM(d.divid_cash_ps_before_tax) IS NULL THEN NULL
                            ELSE (SUM(d.divid_cash_ps_before_tax) / k.[close]) * 100
                        END
                    AS FLOAT) AS dividend_yield_ttm,
                    CAST(
                        (
                            SELECT COUNT(DISTINCT d2.div_year)
                            FROM dbo.dwd_dividend_raw d2
                            WHERE d2.code = k.code
                              AND ISNULL(d2.divid_cash_ps_before_tax, 0) > 0
                              AND TRY_CONVERT(int, d2.div_year) >= YEAR(@trade_date) - 2
                        )
                    AS INT) AS dividend_years
                FROM dbo.dwd_dividend_raw d
                WHERE d.code = k.code
                  AND ISNULL(d.divid_cash_ps_before_tax, 0) > 0
                  AND COALESCE(d.divid_pay_date, d.divid_operate_date, d.divid_plan_date) > DATEADD(day, -365, @trade_date)
                  AND COALESCE(d.divid_pay_date, d.divid_operate_date, d.divid_plan_date) <= @trade_date
            ) dv;
            """

            cur.execute(sql, (trade_date,))
            conn.commit()

            print(f"[OK] calc_scores finished: {trade_date}")

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            raise RuntimeError(f"calc_scores failed: {e}")

        finally:
            _safe_close(cur)


def main() -> None:
    """app.py job runner 约定入口：无参调用。"""
    run_calc_scores(None)


if __name__ == "__main__":
    main()
