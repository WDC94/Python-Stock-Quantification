# -*- coding: utf-8 -*-
"""
app.py
Flask 主程序入口 & API 路由实现（已适配 query_* 模块 + 最新数据库结构）

关键修复点：
- 股票池不再依赖 dbo.dim_security，统一改为 dbo.dwd_stock_basic_all
- query_financials 组合任务：依次增量拉取 盈利/营运/成长/现金流/偿债（季度）
- 个股详情：改为季度财报表（fact_*_quarterly）+ 分红原始表（dwd_dividend_raw）
- 任务中心：提供 /api/jobs/list /api/jobs/status /api/jobs/logs
"""

import os
import json
import time
import traceback
import re
from datetime import datetime
from collections import deque
from importlib import import_module

from flask import Flask, jsonify, request, render_template
from jinja2 import TemplateNotFound


# ------------------ DB 适配（兼容 common.db / db.py） ------------------ #
try:
    from common.db import query  # 只要能查即可
    try:
        from common.db import db_conn  # 用于写入/事务
    except Exception:
        db_conn = None
    try:
        from common.db import execute as _execute  # 如果项目里有 execute
    except Exception:
        _execute = None
except Exception:
    from db import query  # noqa
    try:
        from db import db_conn  # noqa
    except Exception:
        db_conn = None
    try:
        from db import execute as _execute  # noqa
    except Exception:
        _execute = None


def execute(sql: str, params=None):
    """写操作：优先用 common.db.execute；没有则用 db_conn；都没有就抛错"""
    params = params or []
    if _execute:
        return _execute(sql, params)
    if not db_conn:
        raise RuntimeError("DB execute not available: missing execute() and db_conn()")
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return True


# ------------------ 通用工具 ------------------ #
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_db_ident(db: str) -> str:
    """将数据库名转换为安全的 [db] 引用，避免注入。"""
    db = (db or "").strip().strip("[]")
    if not re.fullmatch(r"[A-Za-z0-9_]+", db):
        raise ValueError(f"invalid database name: {db!r}")
    return f"[{db}]"


def _parse_table_name(name: str):
    """解析表名：
    - dbo.table
    - stock.dbo.table
    - table（默认 dbo）
    返回 (db, schema, table)
    """
    name = (name or "").strip()
    name = name.replace("[", "").replace("]", "")
    parts = [p for p in name.split(".") if p]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 1:
        return None, "dbo", parts[0]
    raise ValueError(f"invalid table name: {name!r}")


def current_db_name() -> str:
    try:
        rows = query("SELECT DB_NAME() AS db;")
        if rows:
            return str(rows[0].get("db") or "")
    except Exception:
        pass
    return ""


def table_exists(name: str) -> bool:
    """支持 1/2/3 段表名，可靠判断表是否存在。"""
    try:
        db, schema, table = _parse_table_name(name)
        if db:
            dbq = _safe_db_ident(db)
            sql = (
                f"SELECT TOP 1 1 AS ok "
                f"FROM {dbq}.sys.tables t "
                f"JOIN {dbq}.sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE s.name = ? AND t.name = ?;"
            )
            rows = query(sql, [schema, table])
        else:
            rows = query(
                "SELECT TOP 1 1 AS ok "
                "FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
                "WHERE s.name = ? AND t.name = ?;",
                [schema, table],
            )
        return bool(rows)
    except Exception:
        return False


def list_columns(name: str):
    """返回表字段列表（小写/大小写不做强制），支持 1/2/3 段表名。"""
    try:
        db, schema, table = _parse_table_name(name)
        if db:
            dbq = _safe_db_ident(db)
            sql = (
                f"SELECT c.name "
                f"FROM {dbq}.sys.columns c "
                f"JOIN {dbq}.sys.tables t ON c.object_id = t.object_id "
                f"JOIN {dbq}.sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE s.name = ? AND t.name = ? "
                f"ORDER BY c.column_id;"
            )
            rows = query(sql, [schema, table])
        else:
            rows = query(
                "SELECT c.name "
                "FROM sys.columns c "
                "JOIN sys.tables t ON c.object_id = t.object_id "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                "WHERE s.name = ? AND t.name = ? "
                "ORDER BY c.column_id;",
                [schema, table],
            )
        return [r.get("name") for r in rows]
    except Exception:
        return []

def pick_column(two_part_name: str, candidates):
    cols = set([c.lower() for c in list_columns(two_part_name)])
    for c in candidates:
        if c.lower() in cols:
            return c
    return None


def ts_to_bs_code(ts_code: str):
    """
    600000.SH -> sh.600000
    000001.SZ -> sz.000001
    430047.BJ -> bj.430047（如数据源支持）
    """
    if not ts_code or "." not in ts_code:
        return None
    code, ex = ts_code.split(".", 1)
    ex = ex.upper().strip()
    if ex == "SH":
        return f"sh.{code}"
    if ex == "SZ":
        return f"sz.{code}"
    if ex == "BJ":
        return f"bj.{code}"
    return None


def bs_to_ts_code(bs_code: str):
    """
    sh.600000 -> 600000.SH
    sz.000001 -> 000001.SZ
    bj.430047 -> 430047.BJ
    """
    if not bs_code or "." not in bs_code:
        return None
    ex, code = bs_code.split(".", 1)
    ex = ex.lower().strip()
    if ex == "sh":
        return f"{code}.SH"
    if ex == "sz":
        return f"{code}.SZ"
    if ex == "bj":
        return f"{code}.BJ"
    return None


def normalize_code_to_ts(code: str):
    """兼容输入：ts_code / bs_code / 纯数字"""
    if not code:
        return None
    code = code.strip()
    if "." in code and code.split(".")[0].lower() in ("sh", "sz", "bj"):
        return bs_to_ts_code(code)
    if "." in code and code.split(".")[-1].upper() in ("SH", "SZ", "BJ"):
        return code
    # 纯数字默认不猜交易所（避免误判）
    return None


def normalize_code_to_bs(code: str):
    """兼容输入：ts_code / bs_code"""
    if not code:
        return None
    code = code.strip()
    if "." in code and code.split(".")[0].lower() in ("sh", "sz", "bj"):
        return code
    if "." in code and code.split(".")[-1].upper() in ("SH", "SZ", "BJ"):
        return ts_to_bs_code(code)
    return None


# ------------------ 股票池（统一从 dwd_stock_basic_all） ------------------ #
def load_universe_from_dwd(for_ts_code: bool):
    """
    股票池来源：dbo.dwd_stock_basic_all
    - 表内常见字段：code（baostock 格式 sh.600000）/ code_name / industry / ...
    - 若存在 ts_code 字段则直接用
    """
    tbl = "dbo.dwd_stock_basic_all"
    if not table_exists(tbl):
        return []

    col = pick_column(tbl, ["ts_code", "code"])
    if not col:
        return []

    rows = query(f"SELECT {col} AS code FROM {tbl} WHERE {col} IS NOT NULL;")
    out = []
    for r in rows:
        v = r.get("code")
        if not v:
            continue
        v = str(v).strip()
        if for_ts_code:
            # 目标返回 ts_code
            if v.lower().startswith(("sh.", "sz.", "bj.")):
                ts = bs_to_ts_code(v)
                if ts:
                    out.append(ts)
            elif v.upper().endswith((".SH", ".SZ", ".BJ")):
                out.append(v)
        else:
            # 目标返回 bs_code
            if v.upper().endswith((".SH", ".SZ", ".BJ")):
                bs = ts_to_bs_code(v)
                if bs:
                    out.append(bs)
            elif v.lower().startswith(("sh.", "sz.", "bj.")):
                out.append(v)
    return out


def patch_job_universe(mod):
    """
    自动判断该作业期望的股票代码形态：
    - 若模块内存在 _to_baostock_code(ts_code) 这种转换函数，通常说明它期望 ts_code 输入
    - 否则默认返回 baostock code（sh.600000）
    """
    if not hasattr(mod, "load_universe_from_db"):
        return

    expects_ts = hasattr(mod, "_to_baostock_code")
    if expects_ts:
        mod.load_universe_from_db = lambda: load_universe_from_dwd(for_ts_code=True)
    else:
        mod.load_universe_from_db = lambda: load_universe_from_dwd(for_ts_code=False)


def safe_import_any(name: str):
    """兼容：根目录模块 / etl 子包 / jobs 子包"""
    candidates = [name, f"etl.{name}", f"jobs.{name}"]
    last_err = None
    for mn in candidates:
        try:
            return import_module(mn)
        except Exception as e:
            last_err = e
    raise last_err or ImportError(name)


# ------------------ Flask App ------------------ #
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False


# ------------------ 任务状态/日志（DB 可选 + 内存兜底） ------------------ #
_job_status_mem = {}  # job_code -> dict
_job_logs_mem = deque(maxlen=300)

JOB_STATUS_TABLE = "dbo.etl_job_status"
JOB_LOG_TABLE = "dbo.etl_job_log"


def write_job_status(job_code: str, status: str, duration_sec: float, msg: str = ""):
    payload = {
        "job_code": job_code,
        "status": status,
        "last_run_time": now_str(),
        "duration_sec": round(duration_sec, 3),
        "message": msg or "",
    }
    _job_status_mem[job_code] = payload

    if table_exists(JOB_STATUS_TABLE):
        # upsert
        try:
            execute(
                f"""
                MERGE {JOB_STATUS_TABLE} AS t
                USING (SELECT ? AS job_code) AS s
                ON t.job_code = s.job_code
                WHEN MATCHED THEN UPDATE SET
                    status = ?,
                    last_run_time = ?,
                    duration_sec = ?,
                    message = ?
                WHEN NOT MATCHED THEN
                    INSERT (job_code, status, last_run_time, duration_sec, message)
                    VALUES (?, ?, ?, ?, ?);
                """,
                [
                    job_code,
                    status,
                    payload["last_run_time"],
                    payload["duration_sec"],
                    payload["message"],
                    job_code,
                    status,
                    payload["last_run_time"],
                    payload["duration_sec"],
                    payload["message"],
                ],
            )
        except Exception:
            # 不阻塞主流程
            pass


def write_job_log(job_code: str, ok: bool, duration_sec: float, msg: str = ""):
    row = {
        "run_ts": now_str(),
        "job_code": job_code,
        "ok": bool(ok),
        "duration_sec": round(duration_sec, 3),
        "msg": msg or "",
    }
    _job_logs_mem.appendleft(row)

    if table_exists(JOB_LOG_TABLE):
        try:
            execute(
                f"""
                INSERT INTO {JOB_LOG_TABLE}(run_ts, job_code, ok, duration_sec, msg)
                VALUES (?, ?, ?, ?, ?);
                """,
                [row["run_ts"], job_code, 1 if ok else 0, row["duration_sec"], row["msg"]],
            )
        except Exception:
            pass


# ------------------ Job 实现（query_* + 兼容旧 fetch_*） ------------------ #
def run_job(job_code: str):
    start = time.time()
    ok = True
    msg = ""

    try:
        if job_code == "query_stock_basic":
            mod = safe_import_any("query_stock_basic")
            # 该作业自身不需要股票池
            mod.main()

        elif job_code == "query_history_k_data_plus":
            mod = safe_import_any("query_history_k_data_plus")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_dividend_data":
            mod = safe_import_any("query_dividend_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_profit_data":
            mod = safe_import_any("query_profit_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_operation_data":
            mod = safe_import_any("query_operation_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_growth_data":
            mod = safe_import_any("query_growth_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_cash_flow_data":
            mod = safe_import_any("query_cash_flow_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_balance_data":
            mod = safe_import_any("query_balance_data")
            patch_job_universe(mod)
            mod.main()

        elif job_code == "query_financials":
            # 组合任务：盈利/营运/成长/现金流/偿债（季度）
            seq = [
                "query_profit_data",
                "query_operation_data",
                "query_growth_data",
                "query_cash_flow_data",
                "query_balance_data",
            ]
            for j in seq:
                run_job(j)  # 复用同一套 patch & 记录

        elif job_code == "calc_indicators":
            # 兼容：可能在 etl.calc_indicators
            mod = safe_import_any("calc_indicators")
            if hasattr(mod, "main"):
                mod.main()
            else:
                raise RuntimeError("calc_indicators.main not found")

        elif job_code == "calc_scores":
            mod = safe_import_any("calc_scores")
            if hasattr(mod, "main"):
                mod.main()
            else:
                raise RuntimeError("calc_scores.main not found")

        elif job_code == "run_screener":
            # 动态导入，避免启动期缺模块直接崩
            mod = safe_import_any("screener")
            if hasattr(mod, "run_screener"):
                mod.run_screener()
            elif hasattr(mod, "main"):
                mod.main()
            else:
                raise RuntimeError("screener entry not found")

        elif job_code == "backtest_mvp":
            mod = safe_import_any("backtest_mvp")
            if hasattr(mod, "main"):
                mod.main()
            else:
                raise RuntimeError("backtest_mvp.main not found")


        # ---- 旧 fetch_* 兼容（别让老前端报 unknown job） ----
        elif job_code == "fetch_basics":
            return run_job("query_stock_basic")
        elif job_code == "fetch_kline":
            return run_job("query_history_k_data_plus")
        elif job_code == "fetch_financials":
            return run_job("query_financials")
        elif job_code == "fetch_announcements":
            # 你当前已不维护公告表，保留接口不报错（可按需接回 AkShare 公告作业）
            ok = True
            msg = "announcements job skipped (not implemented in current schema)"

        else:
            raise ValueError(f"unknown job: {job_code}")

    except Exception as e:
        ok = False
        msg = f"{job_code} failed: {str(e)}"
        traceback.print_exc()

    duration = time.time() - start
    write_job_status(job_code, "OK" if ok else "ERR", duration, msg)
    write_job_log(job_code, ok, duration, msg)

    return ok, msg, duration


# ------------------ 页面路由 ------------------ #
@app.route("/")
def index_page():
    return render_template("index.html")


@app.route("/stock/<code>")
def stock_page(code):
    # 兼容旧 stock.html；没有该模板就回到 index.html
    try:
        return render_template("stock.html", ts_code=code)
    except TemplateNotFound:
        return render_template("index.html")




# ------------------ API: DB 健康检查 ------------------ #
@app.route("/api/health/db", methods=["GET"])
def api_health_db():
    tbl = resolve_strategy_table()
    return jsonify(
        {
            "current_db": current_db_name() or None,
            "strategy_table": tbl,
            "strategy_table_exists": table_exists(tbl),
        }
    )

# ------------------ API: 总览 ------------------ #
@app.route("/api/overview", methods=["GET"])
def api_overview():
    stock_count = 0
    kline_stock_count = 0
    screen_count = 0
    latest_trade_date = None

    # 股票数
    try:
        if table_exists("dbo.dwd_stock_basic_all"):
            rows = query("SELECT COUNT(1) AS c FROM dbo.dwd_stock_basic_all;")
            stock_count = int(rows[0]["c"]) if rows else 0
    except Exception:
        pass

    # 行情覆盖数 & 最新交易日
    try:
        if table_exists("dbo.dwd_kline_daily_raw"):
            code_col = pick_column("dbo.dwd_kline_daily_raw", ["code", "ts_code"])
            dt_col = pick_column("dbo.dwd_kline_daily_raw", ["trade_date", "date"])
            if code_col:
                rows = query(f"SELECT COUNT(DISTINCT {code_col}) AS c FROM dbo.dwd_kline_daily_raw;")
                kline_stock_count = int(rows[0]["c"]) if rows else 0
            if dt_col:
                rows = query(f"SELECT MAX({dt_col}) AS d FROM dbo.dwd_kline_daily_raw;")
                latest_trade_date = rows[0]["d"] if rows else None
    except Exception:
        pass

    # 当日选股数量
    try:
        if table_exists("dbo.dm_screen_pick"):
            rows = query(
                """
                SELECT COUNT(1) AS c
                FROM dbo.dm_screen_pick
                WHERE run_date = (SELECT MAX(run_date) FROM dbo.dm_screen_pick);
                """
            )
            screen_count = int(rows[0]["c"]) if rows else 0
    except Exception:
        pass

    # 任务状态（DB 优先）
    jobs = []
    if table_exists(JOB_STATUS_TABLE):
        try:
            jobs = query(
                f"""
                SELECT job_code,
                       status,
                       last_run_time,
                       duration_sec,
                       message
                FROM {JOB_STATUS_TABLE}
                ORDER BY last_run_time DESC;
                """
            )
        except Exception:
            jobs = []
    else:
        jobs = list(_job_status_mem.values())

    # Top10
    screen_top = []
    try:
        if table_exists("dbo.dm_screen_pick"):
            screen_top = query(
                """
                SELECT TOP 10
                    ts_code,
                    name,
                    mv_100m,
                    pb,
                    pe_ttm,
                    roe,
                    div_yield,
                    score,
                    flag
                FROM dbo.dm_screen_pick
                WHERE run_date = (SELECT MAX(run_date) FROM dbo.dm_screen_pick)
                ORDER BY score DESC;
                """
            )
    except Exception:
        screen_top = []

    return jsonify(
        {
            "stock_count": stock_count,
            "kline_stock_count": kline_stock_count,
            "screen_count": screen_count,
            "latest_trade_date": latest_trade_date,
            "jobs": jobs,
            "screen_top": screen_top,
        }
    )


# ------------------ API: 股票列表（来源 dm_screen_pick） ------------------ #
@app.route("/api/stocks", methods=["GET"])
def api_stocks():
    q = request.args.get("q", "").strip()
    industry = request.args.get("industry", "").strip()
    pb_max = request.args.get("pb_max", type=float)
    pe_max = request.args.get("pe_max", type=float)
    mv_min = request.args.get("mv_min", type=float)
    div_min = request.args.get("div_min", type=float)
    roe_min = request.args.get("roe_min", type=float)

    if not table_exists("dbo.dm_screen_pick"):
        return jsonify([])

    # 为了前端兼容，直接返回较全字段；过滤条件按存在的列动态拼
    cols = set([c.lower() for c in list_columns("dbo.dm_screen_pick")])

    sql = """
    SELECT *
    FROM dbo.dm_screen_pick
    WHERE run_date = (SELECT MAX(run_date) FROM dbo.dm_screen_pick)
    """
    params = []

    def has(col):
        return col.lower() in cols

    if pb_max is not None and has("pb"):
        sql += " AND pb <= ?"
        params.append(pb_max)

    if pe_max is not None and (has("pe_ttm") or has("pe")):
        sql += f" AND {('pe_ttm' if has('pe_ttm') else 'pe')} <= ?"
        params.append(pe_max)

    if mv_min is not None and (has("total_mv") or has("mv_100m")):
        # mv_100m 是“亿”为单位；total_mv 可能是“元”
        if has("mv_100m"):
            sql += " AND mv_100m >= ?"
            params.append(mv_min)  # 前端一般填“亿”
        else:
            sql += " AND total_mv >= ?"
            params.append(mv_min)

    if div_min is not None and has("div_yield"):
        sql += " AND div_yield >= ?"
        params.append(div_min)

    if roe_min is not None and has("roe"):
        sql += " AND roe >= ?"
        params.append(roe_min)

    if industry and has("industry"):
        sql += " AND industry = ?"
        params.append(industry)

    if q:
        # 兼容 ts_code/name
        like_cols = []
        if has("name"):
            like_cols.append("name")
        if has("ts_code"):
            like_cols.append("ts_code")
        if like_cols:
            sql += " AND (" + " OR ".join([f"{c} LIKE ?" for c in like_cols]) + ")"
            kw = f"%{q}%"
            params.extend([kw] * len(like_cols))

    # 排序兜底
    if "pb" in cols and "score" in cols:
        sql += " ORDER BY pb ASC, score DESC;"
    elif "pb" in cols and "total_score" in cols:
        sql += " ORDER BY pb ASC, total_score DESC;"
    else:
        sql += " ORDER BY ts_code;"

    rows = query(sql, params)
    return jsonify(rows)


# ------------------ API: 个股详情（适配新表） ------------------ #
@app.route("/api/stocks/<code>", methods=["GET"])
def api_stock_detail(code):
    """
    返回个股基础信息 + 季度财报 + 分红（raw）
    - basic: dbo.dwd_stock_basic_all（替代 dim_security）
    - profit/balance/cashflow/growth/operation: dbo.fact_*_quarterly
    - dividend: dbo.dwd_dividend_raw（替代旧 fact_dividend）
    """
    ts_code = normalize_code_to_ts(code) or code
    bs_code = normalize_code_to_bs(code) or ts_to_bs_code(ts_code)

    # 基础信息：dwd_stock_basic_all（以 baostock code 关联更稳）
    basic = None
    if bs_code and table_exists("dbo.dwd_stock_basic_all"):
        col = pick_column("dbo.dwd_stock_basic_all", ["code", "ts_code"])
        if col:
            basic_rows = query(
                f"SELECT TOP 1 * FROM dbo.dwd_stock_basic_all WHERE {col} = ?;",
                [bs_code if col.lower() == "code" else ts_code],
            )
            basic = basic_rows[0] if basic_rows else None

    def q_table(tbl, where_col="ts_code", limit=24, order_col="end_date"):
        if not ts_code or not table_exists(tbl):
            return []
        # 列名兜底
        wc = pick_column(tbl, [where_col, "ts_code", "code"])
        oc = pick_column(tbl, [order_col, "end_date", "stat_date", "report_date"])
        if not wc:
            return []
        if not oc:
            # 无日期列就不排序
            sql = f"SELECT TOP {limit} * FROM {tbl} WHERE {wc} = ?;"
        else:
            sql = f"SELECT TOP {limit} * FROM {tbl} WHERE {wc} = ? ORDER BY {oc} DESC;"
        return query(sql, [ts_code])

    profit = q_table("dbo.fact_profit_quarterly", where_col="ts_code")
    operation = q_table("dbo.fact_operation_quarterly", where_col="ts_code")
    growth = q_table("dbo.fact_growth_quarterly", where_col="ts_code")
    cashflow = q_table("dbo.fact_cashflow_quarterly", where_col="ts_code")
    balance = q_table("dbo.fact_balance_quarterly", where_col="ts_code")

    # 分红 raw：通常用 baostock code 关联（sh.600000）
    dividend = []
    if bs_code and table_exists("dbo.dwd_dividend_raw"):
        dc = pick_column("dbo.dwd_dividend_raw", ["code", "ts_code"])
        od = pick_column("dbo.dwd_dividend_raw", ["divid_date", "ex_divid_date", "record_date", "date"])
        if dc:
            if od:
                dividend = query(
                    f"SELECT TOP 50 * FROM dbo.dwd_dividend_raw WHERE {dc} = ? ORDER BY {od} DESC;",
                    [bs_code if dc.lower() == "code" else ts_code],
                )
            else:
                dividend = query(
                    f"SELECT TOP 50 * FROM dbo.dwd_dividend_raw WHERE {dc} = ?;",
                    [bs_code if dc.lower() == "code" else ts_code],
                )

    return jsonify(
        {
            "ts_code": ts_code,
            "bs_code": bs_code,
            "basic": basic,
            "profit": profit,
            "operation": operation,
            "growth": growth,
            "cashflow": cashflow,
            "balance": balance,
            "dividend": dividend,
            "announcements": [],  # 当前口径未维护公告表，预留字段保证前端不炸
        }
    )


# ------------------ API: K 线（优先 raw 表；自动补 ma250） ------------------ #
@app.route("/api/kline/<code>", methods=["GET"])
def api_kline(code):
    limit = request.args.get("limit", default=260, type=int)
    ts_code = normalize_code_to_ts(code) or code
    bs_code = normalize_code_to_bs(code) or ts_to_bs_code(ts_code)

    # 优先 raw 表（query_history_k_data_plus）
    tbl = "dbo.dwd_kline_daily_raw"
    if not table_exists(tbl) or not bs_code:
        return jsonify([])

    # 自动匹配列名
    c_code = pick_column(tbl, ["code", "ts_code"])
    c_dt = pick_column(tbl, ["trade_date", "date"])
    c_open = pick_column(tbl, ["open", "Open"])
    c_high = pick_column(tbl, ["high", "High"])
    c_low = pick_column(tbl, ["low", "Low"])
    c_close = pick_column(tbl, ["close", "Close"])
    c_vol = pick_column(tbl, ["volume", "vol"])
    c_amt = pick_column(tbl, ["amount", "turnover", "turn"])
    c_pe = pick_column(tbl, ["pe_ttm", "peTTM"])
    c_pb = pick_column(tbl, ["pb", "pbMRQ"])

    if not (c_code and c_dt and c_close):
        return jsonify([])

    where_val = bs_code if c_code.lower() == "code" else ts_code

    sql = f"""
    SELECT TOP {limit}
        {c_dt} AS [date],
        {c_open} AS [open],
        {c_high} AS high,
        {c_low}  AS low,
        {c_close} AS [close],
        {c_vol}  AS volume,
        {c_amt}  AS amount,
        {c_pe}   AS pe_ttm,
        {c_pb}   AS pb
    FROM {tbl}
    WHERE {c_code} = ?
    ORDER BY {c_dt} DESC;
    """

    rows = query(sql, [where_val])
    rows = list(reversed(rows))  # 正序

    # 计算 ma250（简单移动平均）
    closes = []
    for r in rows:
        try:
            closes.append(float(r.get("close")) if r.get("close") is not None else None)
        except Exception:
            closes.append(None)

    window = 250
    for i in range(len(rows)):
        if i + 1 < window:
            rows[i]["ma250"] = None
            continue
        seg = closes[i + 1 - window : i + 1]
        if any(v is None for v in seg):
            rows[i]["ma250"] = None
        else:
            rows[i]["ma250"] = sum(seg) / window

    return jsonify(rows)


# ------------------ API: 手工触发选股 ------------------ #
@app.route("/api/run_screener", methods=["POST"])
def api_run_screener():
    try:
        ok, msg, _ = run_job("run_screener")
        return jsonify({"ok": ok, "error": msg if not ok else ""})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ------------------ API: 策略（dim_strategy / 内存兜底） ------------------ #
# 表结构参考 create_tables.sql：
# dbo.dim_strategy(strategy_code PK, strategy_name NOT NULL, strategy_type NOT NULL, filter_params, weight_params, ...)
STRATEGY_TABLE_CANDIDATES = [
    os.getenv("STRATEGY_TABLE", "").strip(),
    "dbo.dim_strategy",
    "stock.dbo.dim_strategy",
]


def resolve_strategy_table() -> str:
    """优先使用存在的 dim_strategy 表（支持跨库三段名）。"""
    for t in STRATEGY_TABLE_CANDIDATES:
        if not t:
            continue
        if table_exists(t):
            return t
    # 如果都不存在，返回默认候选（用于报错信息）
    return STRATEGY_TABLE_CANDIDATES[1]

_strategy_mem = {
    "strategy_code": "default",
    "strategy_type": "stock_pick",
    "name": "默认策略",
    "filter": {},
    "weight": {},
    "description": "",
}


def _ensure_dim_strategy_no_nulls(default_type: str = "stock_pick"):
    '''
    历史数据兜底：如果 dim_strategy 已经存在 strategy_type=NULL 的行，会导致任何 UPDATE 失败（515）。
    这里在保存策略前做一次修复，避免错误反复出现。
    '''
    tbl = resolve_strategy_table()
    if not table_exists(tbl):
        return
    try:
        execute(
            f'''
            UPDATE {tbl}
            SET strategy_type = ?
            WHERE strategy_type IS NULL;
            ''',
            [default_type],
        )
    except Exception:
        # 不阻塞主流程
        pass


@app.route("/api/strategy/profile", methods=["GET"])
def api_strategy_get():
    '''获取当前启用策略（优先：is_active=1 且 display_order 最小）。'''
    tbl = resolve_strategy_table()
    if table_exists(tbl):
        try:
            rows = query(
                f'''
                SELECT TOP 1
                    strategy_code,
                    strategy_name,
                    strategy_type,
                    description,
                    filter_params,
                    weight_params
                FROM {tbl}
                WHERE is_active = 1
                ORDER BY display_order ASC, updated_at DESC, created_at DESC;
                '''
            )
            if rows:
                r = rows[0]
                return jsonify(
                    {
                        "strategy_code": r.get("strategy_code") or "default",
                        "strategy_type": r.get("strategy_type") or "stock_pick",
                        "name": r.get("strategy_name") or "默认策略",
                        "description": r.get("description") or "",
                        "filter": json.loads(r.get("filter_params") or "{}"),
                        "weight": json.loads(r.get("weight_params") or "{}"),
                    }
                )
        except Exception:
            pass
    return jsonify(_strategy_mem)


@app.route("/api/strategy/profile", methods=["POST"])
def api_strategy_save():
    '''
    保存策略配置

    关键点：dim_strategy.strategy_type 为 NOT NULL。
    - 若前端没传 strategy_type，则后端默认 stock_pick
    - 对历史 NULL 数据做一次修复，避免 UPDATE 直接失败
    '''
    payload = request.get_json(force=True, silent=True) or {}

    strategy_code = (payload.get("strategy_code") or "default").strip()
    strategy_type = (payload.get("strategy_type") or "stock_pick").strip()
    name = (payload.get("name") or "默认策略").strip()
    desc = (payload.get("description") or "").strip()

    flt = payload.get("filter") or {}
    wgt = payload.get("weight") or {}

    # 展示顺序/启用状态（可选）
    display_order = payload.get("display_order", 1)
    try:
        display_order = int(display_order)
    except Exception:
        display_order = 1

    is_active = payload.get("is_active", 1)
    try:
        is_active = 1 if int(is_active) else 0
    except Exception:
        is_active = 1

    # 内存兜底
    _strategy_mem.update(
        {
        "strategy_code": strategy_code,
        "strategy_type": strategy_type,
        "name": name,
        "description": desc,
        "filter": flt,
        "weight": wgt,
        }
    )

    tbl = resolve_strategy_table()
    if not table_exists(tbl):
        return jsonify({"ok": False, "error": f"dim_strategy not found (current_db={current_db_name() or 'unknown'}; table={tbl})"}), 500

    try:
        _ensure_dim_strategy_no_nulls(default_type="stock_pick")

        execute(
            f'''
            MERGE {tbl} AS t
            USING (SELECT ? AS strategy_code) AS s
              ON t.strategy_code = s.strategy_code
            WHEN MATCHED THEN UPDATE SET
                strategy_name = ?,
                strategy_type = ?,
                description   = ?,
                filter_params = ?,
                weight_params = ?,
                is_active     = ?,
                display_order = ?,
                updated_at    = SYSDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (
                    strategy_code, strategy_name, strategy_type, description,
                    filter_params, weight_params,
                    is_active, display_order, created_at, updated_at
                )
                VALUES (
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, SYSDATETIME(), SYSDATETIME()
                );
            ''',
            [
                # MATCHED
                strategy_code,
                name,
                strategy_type,
                desc,
                json.dumps(flt, ensure_ascii=False),
                json.dumps(wgt, ensure_ascii=False),
                is_active,
                display_order,
                # NOT MATCHED
                strategy_code,
                name,
                strategy_type,
                desc,
                json.dumps(flt, ensure_ascii=False),
                json.dumps(wgt, ensure_ascii=False),
                is_active,
                display_order,
            ],
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

    return jsonify({"ok": True})

@app.route("/api/strategy/preview", methods=["GET"])
def api_strategy_preview():
    # 预览：直接返回当日 dm_screen_pick Top20（前端提示就是这样）
    if not table_exists("dbo.dm_screen_pick"):
        return jsonify([])
    try:
        rows = query(
            '''
            SELECT TOP 20
                ts_code, name,
                mv_100m, pb, pe_ttm, roe, div_yield,
                score
            FROM dbo.dm_screen_pick
            WHERE run_date = (SELECT MAX(run_date) FROM dbo.dm_screen_pick)
            ORDER BY score DESC;
            '''
        )
        return jsonify(rows)
    except Exception:
        return jsonify([])


# ------------------ API: Job 管理 ------------------ #

JOBS_META = [
    {"code": "query_stock_basic", "name": "拉取股票基础信息", "api": "/api/jobs/query_stock_basic"},
    {"code": "query_history_k_data_plus", "name": "拉取日线行情", "api": "/api/jobs/query_history_k_data_plus"},
    {"code": "query_dividend_data", "name": "拉取除权除息/分红", "api": "/api/jobs/query_dividend_data"},
    {"code": "query_financials", "name": "拉取财报数据（组合）", "api": "/api/jobs/query_financials"},
    {"code": "query_profit_data", "name": "拉取盈利能力（季度）", "api": "/api/jobs/query_profit_data"},
    {"code": "query_operation_data", "name": "拉取营运能力（季度）", "api": "/api/jobs/query_operation_data"},
    {"code": "query_growth_data", "name": "拉取成长能力（季度）", "api": "/api/jobs/query_growth_data"},
    {"code": "query_cash_flow_data", "name": "拉取现金流（季度）", "api": "/api/jobs/query_cash_flow_data"},
    {"code": "query_balance_data", "name": "拉取偿债能力（季度）", "api": "/api/jobs/query_balance_data"},
    {"code": "calc_indicators", "name": "计算技术&估值指标", "api": "/api/jobs/calc_indicators"},
    {"code": "calc_scores", "name": "计算综合评分", "api": "/api/jobs/calc_scores"},
    {"code": "run_screener", "name": "执行当日选股", "api": "/api/run_screener"},
    {"code": "backtest_mvp", "name": "回测MVP（IC/分层/净值）", "api": "/api/jobs/backtest_mvp"},

]


@app.route("/api/jobs/list", methods=["GET"])
def api_jobs_list():
    # 合并状态信息
    status_map = {}
    if table_exists(JOB_STATUS_TABLE):
        try:
            rows = query(
                f"SELECT job_code, status, last_run_time, duration_sec, message FROM {JOB_STATUS_TABLE};"
            )
            status_map = {r["job_code"]: r for r in rows if r.get("job_code")}
        except Exception:
            status_map = {}
    else:
        status_map = {k: v for k, v in _job_status_mem.items()}

    out = []
    for j in JOBS_META:
        s = status_map.get(j["code"], {})
        out.append(
            {
                **j,
                "status": s.get("status"),
                "last_run_time": s.get("last_run_time"),
                "duration_sec": s.get("duration_sec"),
                "message": s.get("message"),
            }
        )
    return jsonify(out)


@app.route("/api/jobs/status", methods=["GET"])
def api_jobs_status():
    if table_exists(JOB_STATUS_TABLE):
        try:
            rows = query(
                f"SELECT job_code, status, last_run_time, duration_sec, message FROM {JOB_STATUS_TABLE};"
            )
            return jsonify({r["job_code"]: r for r in rows if r.get("job_code")})
        except Exception:
            pass
    return jsonify(_job_status_mem)


@app.route("/api/jobs/logs", methods=["GET"])
def api_jobs_logs():
    limit = request.args.get("limit", default=50, type=int)
    limit = max(1, min(limit, 200))

    if table_exists(JOB_LOG_TABLE):
        try:
            rows = query(
                f"""
                SELECT TOP {limit}
                    run_ts, job_code, ok, duration_sec, msg
                FROM {JOB_LOG_TABLE}
                ORDER BY run_ts DESC;
                """
            )
            return jsonify(rows)
        except Exception:
            pass

    return jsonify(list(_job_logs_mem)[:limit])


@app.route("/api/jobs/<job_code>", methods=["POST"])
def api_run_job(job_code):
    ok, msg, duration = run_job(job_code)
    return jsonify({"ok": ok, "error": msg if not ok else "", "duration_sec": duration})


# ------------------ 启动 ------------------ #
if __name__ == "__main__":
    # 你也可以用：flask run
    app.run(host="0.0.0.0", port=5050, debug=False)
