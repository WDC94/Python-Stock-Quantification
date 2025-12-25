# 股票量化投资模型（BaoStock + Flask + SQL Server）

面向 A 股的低估值 + 高质量股票筛选与综合评分系统。  
数据入库至 SQL Server，通过 Flask 提供 API，前端用 HTML + JS + ECharts 展示。

---

## 1. 系统架构

```text
┌────────────────────────────────────────────────────────┐
                       前端展示层 (HTML + JS + ECharts)
    ├─ 股票筛选页：低 PB + 综合评分列表（支持条件筛选）
    └─ 个股详情页：K 线、财务、除权分红、综合评分拆解
└────────────────────────────────────────────────────────┘
                ▲                │
                │  JSON API      │
                ▼                │
┌────────────────────────────────────────────────────────┐
                       后端服务层 (Flask API)
    ├─ app.py
    ├─ routes:
    │    • GET  /api/stocks          股票列表 + 综合评分
    │    • GET  /api/stocks/{ts_code}个股详情 + 评分拆解
    │    • GET  /api/kline/{ts_code} 个股 K 线 + MA250
    │    • POST /api/run_screener    手动触发当日选股
    └─ common/db.py                  SQL Server 连接封装
└────────────────────────────────────────────────────────┘
                ▲
                │ 批处理 / ETL
                ▼
┌────────────────────────────────────────────────────────┐
                因子与综合评分层 (Python ETL + SQL)
    ├─ etl/calc_indicators.py
    │    • 盈利	企业赚钱能力  20%
    │      运营	资产运转效率  15%
    │      成长	业绩扩张能力  15%
    │      偿债	财务稳健性  10%
    │      现金流	盈利质量  10%
    │      估值 & 技术	定价与趋势  20%
    │      分红	股东回报  10%
    │  
    │    • 落表：dbo.dws_stock_score_daily
    │
    ├─ etl/calc_scores.py（预留/建议新增）
    │    • 基于市值、资产负债率、ROE、PE、PB 计算 0–100 分
    │    • 维度：估值 / 盈利 / 安全 / 规模
    │    • 落表字段（推荐）：
    │        - score_mv        市值得分
    │        - score_roe       ROE 得分
    │        - score_dar       资产负债率得分
    │        - score_pe        PE 得分
    │        - score_pb        PB 得分
    │        - score_val       估值得分 (PE+PB)
    │        - score_prof      盈利得分
    │        - score_safety    安全/杠杆得分
    │        - score_size      规模得分
    │        - total_score     综合评分
    │        - rating          评级(A/B/C/D)
    └─ etl/screener.py
         • 基于当日指标 + 综合评分
         • 按策略口径筛选股票
         • 落表：dm_screen_pick（当日入选清单）
└────────────────────────────────────────────────────────┘
                ▲
                │ 数据采集 (批量任务)
                ▼
┌────────────────────────────────────────────────────────┐
                       数据采集层 (BaoStock)
    ├─ jobs/query_stock_basic.py
    │    • BaoStock 股票基础信息 → [dbo].[dwd_stock_basic_all]
    ├─ jobs/query_history_k_data_plus.py
    │    • BaoStock 日K线 → [dbo].[dwd_kline_daily_raw]
    ├─ jobs/query_profit_data.py
    │    • BaoStock 盈利能力指标（含 净利率/毛利率/ROE等）→ [dbo].[fact_profit_quarterly]
    ├─ jobs/query_balance_data.py
    │    • BaoStock 偿债能力指标
    ├─ jobs/query_cash_flow_data.py
    │    • BaoStock 现金流指标
    └─ jobs/query_operation_data.py
         • BaoStock 运营能力指标 → [dbo].[fact_operation_quarterly]
└────────────────────────────────────────────────────────┘
                ▲
                │ SQL Server
                ▼
┌────────────────────────────────────────────────────────┐
                       数据存储层 (SQL Server)
    ├─ dim_security          股票基础信息
    ├─ fact_daily            日线行情（前复权）
    ├─ fact_finance_annual   年度财务指标（含 ROE、资产负债率等）
    ├─ dwd_announcement      公告明细
    ├─ dwm_indicators_daily  日度技术+估值指标 (PB、MA250、below_ma250、score_*)
    └─ dm_screen_pick        每日选股结果（含综合评分快照）
└────────────────────────────────────────────────────────┘

========================================================================================================================

#2、目录结构
project_root/
│
├─ app.py                      # Flask 主入口 & 路由
├─ requirements.txt            # python依赖库
│
├─ common/
│   ├─ baostock_quota.py       # BaoStock API调用管理
│   └─ db.py                   # SQL Server 连接封装 (pyodbc)
│
├─ etl/
│   ├─ calc_indicators.py      # 计算 PB、MA250 等因子 → dwm_indicators_daily
│   ├─ calc_scores.py          # 多指标综合评分（市值/杠杆/ROE/PE/PB）
│   └─ screener.py             # 选股逻辑 → dm_screen_pick
│
├─ jobs/
│   ├─ query_stock_basic.py            # 拉取股票基础信息 → [dbo].[dwd_stock_basic_all]
│   ├─ query_history_k_data_plus.py    # 拉取日 K 数据 → [dbo].[dwd_kline_daily_raw]
│   ├─ query_profit_data.py            # 拉取盈利能力数据 → [dbo].[fact_profit_quarterly]
│   └─ query_balance_data.py           # 拉取偿债能力数据 → [dbo].[fact_balance_quarterly]
│
├─ sql/
│   └─ create_tables.sql       # 建表脚本（SQL Server）
│
├─ templates/
│   └─ index.html              # 前端页面
│
└─ README.md                   # 项目自述文件（本文件）
