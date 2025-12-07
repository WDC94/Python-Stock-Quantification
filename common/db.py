# -*- coding: utf-8 -*-
"""
common/db.py
SQL Server 连接与基础封装
"""

import os
from contextlib import contextmanager

import pyodbc

# 可以在 .env 里覆盖以下环境变量
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_DATABASE = os.getenv("SQL_DATABASE", "stock")
SQL_USERNAME = os.getenv("SQL_USERNAME", "sa")
SQL_PASSWORD = os.getenv("SQL_PASSWORD", "st123456")
SQL_DRIVER = os.getenv("SQL_DRIVER", "{ODBC Driver 18 for SQL Server}")


def _build_conn_str() -> str:
    return (
        f"DRIVER={SQL_DRIVER};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )


def get_connection() -> pyodbc.Connection:
    """获取原生连接，不做自动提交控制。"""
    return pyodbc.connect(_build_conn_str())


@contextmanager
def db_conn():
    """
    上下文管理事务：
    with db_conn() as conn:
        ...
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql: str, params=None):
    """
    执行查询，返回 list[dict]
    """
    params = params or []
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        columns = [c[0] for c in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]
    return rows


def execute(sql: str, params=None) -> int:
    """
    执行 DML，返回影响行数
    """
    params = params or []
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.rowcount
