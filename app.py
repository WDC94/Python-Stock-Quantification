# -*- coding: utf-8 -*-
"""
app.py
Flask 主程序入口 & API 路由实现

职责：
- 提供前端页面路由（筛选页 / 个股详情页）
- 提供数据查询 API（筛选列表 / 个股详情 / K 线）
- 提供 Job 触发 API（fetch_basics / fetch_kline / fetch_financials / fetch_announcements /
  calc_indicators / calc_scores / run_screener）
"""

from flask import Flask, jsonify, request, render_template

from common.db import query
from etl.screener import run_screener  # 选股策略入口

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # JSON 输出保持中文


# ------------------ 页面路由 ------------------ #


@app.route("/")
def index_page():
    """股票筛选页"""
    return render_template("index.html")


@app.route("/stock/<ts_code>")
def stock_page(ts_code):
    """个股详情页"""
    return render_template("stock.html", ts_code=ts_code)


# ------------------ API: 股票筛选列表 ------------------ #


@app.route("/api/stocks", methods=["GET"])
def api_stocks():
    """
    股票全量列表（约 5000+ 只，供前端左侧表格使用）

    - 数据来源：dwm_indicators_daily + dim_security
    - 口径：取 dwm_indicators_daily 中最新 trade_date 的全量股票
    - 支持可选 Query 附加过滤（不影响全量行数，只是在后端先做一层过滤）：
        q:      代码/名称模糊搜索
        pb_max: 最大 PB
        mv_min: 最小总市值（元）
    """
    q = request.args.get("q", "").strip()
    pb_max = request.args.get("pb_max", type=float)
    mv_min = request.args.get("mv_min", type=float)

    sql = """
    WITH latest AS (
        SELECT MAX(trade_date) AS trade_date
        FROM dbo.dwm_indicators_daily
    )
    SELECT
        d.ts_code,
        s.name,
        d.pb,
        d.pe_ttm,
        d.total_mv,
        d.[close],
        d.ma250,
        d.below_ma250,
        d.score_mv,
        d.score_roe,
        d.score_dar,
        d.score_pe,
        d.score_pb,
        d.score_val,
        d.score_prof,
        d.score_safety,
        d.score_size,
        d.total_score,
        d.rating
    FROM dbo.dwm_indicators_daily AS d
    JOIN latest l
        ON d.trade_date = l.trade_date
    JOIN dbo.dim_security AS s
        ON d.ts_code = s.ts_code
    WHERE 1 = 1
    """
    params = []

    if pb_max is not None:
        sql += " AND d.pb <= ?"
        params.append(pb_max)

    if mv_min is not None:
        sql += " AND d.total_mv >= ?"
        params.append(mv_min)

    if q:
        sql += " AND (s.name LIKE ? OR d.ts_code LIKE ?)"
        kw = f"%{q}%"
        params.extend([kw, kw])

    # 默认：PB 从低到高，总分从高到低、市值从高到低
    sql += " ORDER BY d.pb ASC, d.total_score DESC, d.total_mv DESC;"

    rows = query(sql, params)
    return jsonify(rows)


# ------------------ API: 个股详情 ------------------ #


@app.route("/api/stocks/<ts_code>", methods=["GET"])
def api_stock_detail(ts_code):
    """
    返回个股基础信息 + 财务 + 分红 + 公告
    - basic: dim_security
    - finance: fact_finance_annual
    - dividend: fact_dividend
    - announcements: dwd_announcement
    """
    # 基础信息
    basic_sql = """
    SELECT ts_code, symbol, name, exchange, industry, list_date, status,
           total_shares, float_shares
    FROM dbo.dim_security
    WHERE ts_code = ?
    """
    basic_rows = query(basic_sql, [ts_code])
    basic = basic_rows[0] if basic_rows else None

    # 财务（年度）
    finance_sql = """
    SELECT fiscal_year,
           net_profit,
           net_assets,
           total_assets,
           total_liab,
           bps,
           total_mv,
           roe,
           debt_asset_ratio,
           pe,
           pb,
           dividend_cash,
           dividend_flag,
           profit_flag
    FROM dbo.fact_finance_annual
    WHERE ts_code = ?
    ORDER BY fiscal_year DESC;
    """
    finance = query(finance_sql, [ts_code])

    # 分红记录
    dividend_sql = """
    SELECT notice_date,
           plan_year,
           cash_per_share,
           ex_date,
           record_date,
           pay_date
    FROM dbo.fact_dividend
    WHERE ts_code = ?
    ORDER BY notice_date DESC;
    """
    dividend = query(dividend_sql, [ts_code])

    # 公告
    ann_sql = """
    SELECT notice_id,
           notice_date,
           title,
           category,
           url
    FROM dbo.dwd_announcement
    WHERE ts_code = ?
    ORDER BY notice_date DESC;
    """
    announcements = query(ann_sql, [ts_code])

    return jsonify(
        {
            "basic": basic,
            "finance": finance,
            "dividend": dividend,
            "announcements": announcements,
        }
    )


# ------------------ API: K 线 + MA250 + 估值 ------------------ #


@app.route("/api/kline/<ts_code>", methods=["GET"])
def api_kline(ts_code):
    """
    返回指定标的最近 N 日 K 线 + 技术指标 + 估值指标
    Query 参数:
    - limit: 返回条数，默认 260

    字段：
    - date, open, high, low, close, vol, amount
    - ma250, pb, pe_ttm, total_mv
    """
    limit = request.args.get("limit", default=260, type=int)

    # 使用 TOP + ORDER BY trade_date DESC，再在 Python 侧 reverse 成时间正序
    sql = f"""
    SELECT TOP {limit}
        d.trade_date AS [date],
        d.[open],
        d.high,
        d.low,
        d.[close],
        d.vol,
        d.amount,
        ind.ma250,
        ind.pb,
        ind.pe_ttm,
        ind.total_mv
    FROM dbo.fact_daily AS d
    LEFT JOIN dbo.dwm_indicators_daily AS ind
        ON d.ts_code = ind.ts_code AND d.trade_date = ind.trade_date
    WHERE d.ts_code = ?
    ORDER BY d.trade_date DESC;
    """
    rows = query(sql, [ts_code])

    # 前端画图习惯：按日期正序
    rows = list(reversed(rows))
    return jsonify(rows)


# ------------------ API: 手工触发选股 ------------------ #


@app.route("/api/run_screener", methods=["POST"])
def api_run_screener():
    """
    手动触发当日策略选股（右侧任务栏“执行策略选股”按钮）

    Body JSON:
    - pb_max:  最大 PB（默认 1.0）
    - mv_min:  最小市值（元，默认 100 亿）
    - below_ma: 是否仅选择低于 MA250（1/0，当前版本仍在 screener 中固定为仅低于 MA250，可预留）
    """
    data = request.get_json(silent=True) or {}
    pb_max = float(data.get("pb_max", 1.0))
    mv_min = float(data.get("mv_min", 1e10))
    # 预留字段，当前 screener 内部仍默认 below_ma250 = 1，如后续调整再透传
    # below_ma = int(data.get("below_ma", 1))

    count = run_screener(pb_max=pb_max, mv_min=mv_min)
    return jsonify({"count": count})


# ------------------ API: 手工触发数据拉取 / 计算 Job ------------------ #


@app.route("/api/jobs/<job_name>", methods=["POST"])
def api_run_job(job_name: str):
    """
    手工触发各类 ETL / 计算 Job（右侧任务栏按钮调用）

    约定 job_name：
    - fetch_basics        → jobs.fetch_basics.main()
    - fetch_kline         → jobs.fetch_kline.main()
    - fetch_financials    → jobs.fetch_financials.main()
    - fetch_announcements → jobs.fetch_announcements.main()
    - calc_indicators     → etl.calc_indicators.main()
    - calc_scores         → etl.calc_scores.main()

    返回：
    - ok:    bool
    - count: 影响/写入记录数（由各 job 返回）
    - error: 出错信息（ok = false 时）
    """
    try:
        if job_name == "fetch_basics":
            from jobs.fetch_basics import main as job_main  # type: ignore
        elif job_name == "fetch_kline":
            from jobs.fetch_kline import main as job_main  # type: ignore
        elif job_name == "fetch_financials":
            from jobs.fetch_financials import main as job_main  # type: ignore
        elif job_name == "fetch_announcements":
            from jobs.fetch_announcements import main as job_main  # type: ignore
        elif job_name == "calc_indicators":
            from etl.calc_indicators import main as job_main  # type: ignore
        elif job_name == "calc_scores":
            from etl.calc_scores import main as job_main  # type: ignore
        else:
            # 未定义的 job 名称
            return jsonify({"ok": False, "error": f"unknown job: {job_name}"}), 404
    except ImportError:
        # 对应模块暂未实现或文件尚未落地
        return jsonify({"ok": False, "error": "job not implemented"}), 500

    try:
        count = job_main()
        return jsonify({"ok": True, "count": count})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # 开发环境本地启动
    app.run(host="0.0.0.0", port=5000, debug=True)
