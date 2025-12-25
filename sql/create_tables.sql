/* 数据库：stock（请预先创建）
   本脚本可重复执行：先删视图/表，再重建 + 索引
*/

------------------------------------------------------------
-- 0) 先删除视图（如已存在），保证可重复执行
------------------------------------------------------------
IF OBJECT_ID('dbo.v_y3_profit_dividend','V') IS NOT NULL
    DROP VIEW dbo.v_y3_profit_dividend;
GO


IF OBJECT_ID('dbo.dim_strategy','U') IS NOT NULL
    DROP TABLE dbo.dim_strategy;
GO


--策略配置表：dim_strategy
--统一管理策略元数据 + JSON 参数，便于扩展

CREATE TABLE dbo.dim_strategy(
  strategy_code   VARCHAR(32)  NOT NULL,           -- LOW_PB_DIV / VAL_QUALITY / GARP / TREND_OVERLAY
  strategy_name   NVARCHAR(100) NOT NULL,
  strategy_type   VARCHAR(20)  NOT NULL,           -- 'stock_pick' / 'overlay'
  description     NVARCHAR(500) NULL,

  -- JSON 配置（筛选条件 & 因子权重），由后端解析
  filter_params   NVARCHAR(MAX) NULL,              -- 如 {"pb_max":1.0,"mv_min":100e8,...}
  weight_params   NVARCHAR(MAX) NULL,              -- 如 {"w_pb":0.3,"w_dividend":0.4,...}

  is_active       TINYINT      NOT NULL DEFAULT 1,
  display_order   INT          NOT NULL DEFAULT 0,
  created_at      DATETIME2    NOT NULL DEFAULT SYSDATETIME(),
  updated_at      DATETIME2    NULL,

  CONSTRAINT PK_dim_strategy PRIMARY KEY(strategy_code)
);
GO


------------------------------------------------------------
-- 3b) 季度盈利能力表 fact_profit_quarterly（BaoStock query_profit_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_profit_quarterly','U') IS NOT NULL
    DROP TABLE dbo.fact_profit_quarterly;
GO

CREATE TABLE dbo.fact_profit_quarterly(
  ts_code      VARCHAR(20)  NOT NULL,        -- 例: 600000.SH
  fiscal_year  CHAR(4)      NOT NULL,        -- 会计年度，例如 2021
  quarter      TINYINT      NOT NULL,        -- 季度 1~4

  stat_date    DATE         NULL,            -- 统计日期 statDate，如 2017-03-31
  pub_date     DATE         NULL,            -- 公告日期 pubDate

  -- 比率类统一存 0~1 比例（接口返回是百分比）
  roe          DECIMAL(9,4)  NULL,           -- 净资产收益率(平均)，roeAvg/100
  np_margin    DECIMAL(9,4)  NULL,           -- 销售净利率，npMargin/100
  gp_margin    DECIMAL(9,4)  NULL,           -- 销售毛利率，gpMargin/100

  -- 量值类按“元”口径存储
  net_profit   DECIMAL(18,2) NULL,           -- 净利润(元)，netProfit
  eps_ttm      DECIMAL(18,4) NULL,           -- 每股收益 TTM，epsTTM
  mbr_revenue  DECIMAL(18,2) NULL,           -- 主营业务收入(元)，MBRevenue

  total_share  DECIMAL(20,2) NULL,           -- 总股本(股)，totalShare
  liqa_share   DECIMAL(20,2) NULL,           -- 流通股本(股)，liqaShare

  CONSTRAINT PK_fact_profit_quarterly PRIMARY KEY(ts_code, fiscal_year, quarter)
);
GO

-- 季度盈利能力按 ts_code + 年度 + 季度倒序
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_profit_q_1' 
      AND object_id = OBJECT_ID('dbo.fact_profit_quarterly')
)
BEGIN
    CREATE INDEX IX_profit_q_1
        ON dbo.fact_profit_quarterly(ts_code,fiscal_year DESC, quarter DESC);
END;
GO

------------------------------------------------------------
-- 3c) 季度营运能力表 fact_operation_quarterly（BaoStock query_operation_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_operation_quarterly','U') IS NOT NULL
    DROP TABLE dbo.fact_operation_quarterly;
GO

CREATE TABLE dbo.fact_operation_quarterly(
  ts_code      VARCHAR(20)  NOT NULL,        -- 例: 600000.SH
  fiscal_year  CHAR(4)      NOT NULL,        -- 会计年度，例如 2021
  quarter      TINYINT      NOT NULL,        -- 季度 1~4

  stat_date    DATE         NULL,            -- 统计日期 statDate，例如 2017-03-31
  pub_date     DATE         NULL,            -- 公告日期 pubDate

  -- 以下字段直接按“次 / 天”的数值口径存储（非百分比）
  nr_turn_ratio    DECIMAL(18,4) NULL,       -- NRTurnRatio 应收账款周转率(次)
  nr_turn_days     DECIMAL(18,4) NULL,       -- NRTurnDays  应收账款周转天数(天)
  inv_turn_ratio   DECIMAL(18,4) NULL,       -- INVTurnRatio 存货周转率(次)
  inv_turn_days    DECIMAL(18,4) NULL,       -- INVTurnDays  存货周转天数(天)
  ca_turn_ratio    DECIMAL(18,4) NULL,       -- CATurnRatio  流动资产周转率(次)
  asset_turn_ratio DECIMAL(18,4) NULL,       -- AssetTurnRatio 总资产周转率(次)

  CONSTRAINT PK_fact_operation_quarterly PRIMARY KEY(ts_code, fiscal_year, quarter)
);
GO

------------------------------------------------------------
-- 季度营运能力索引：按 ts_code + 年度 + 季度倒序
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_operation_q_1' 
      AND object_id = OBJECT_ID('dbo.fact_operation_quarterly')
)
BEGIN
    CREATE INDEX IX_operation_q_1
        ON dbo.fact_operation_quarterly(ts_code, fiscal_year DESC, quarter DESC);
END;
GO

------------------------------------------------------------
-- 3d) 季度成长能力表 fact_growth_quarterly（BaoStock query_growth_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_growth_quarterly','U') IS NOT NULL
    DROP TABLE dbo.fact_growth_quarterly;
GO

CREATE TABLE dbo.fact_growth_quarterly(
  ts_code      VARCHAR(20)  NOT NULL,        -- 例: 600000.SH
  fiscal_year  CHAR(4)      NOT NULL,        -- 会计年度，例如 2021
  quarter      TINYINT      NOT NULL,        -- 季度 1~4

  stat_date    DATE         NULL,            -- 统计日期 statDate，例如 2017-03-31
  pub_date     DATE         NULL,            -- 公告日期 pubDate

  -- 接口返回为百分比，这里统一除以 100 存 0~1 比例，便于后续打分/建模
  yoy_equity     DECIMAL(9,4) NULL,          -- YOYEquity  净资产同比增长率
  yoy_asset      DECIMAL(9,4) NULL,          -- YOYAsset   总资产同比增长率
  yoy_ni         DECIMAL(9,4) NULL,          -- YOYNI      净利润同比增长率
  yoy_eps_basic  DECIMAL(9,4) NULL,          -- YOYEPSBasic 基本每股收益同比增长率
  yoy_pni        DECIMAL(9,4) NULL,          -- YOYPNI     归母净利润同比增长率

  CONSTRAINT PK_fact_growth_quarterly PRIMARY KEY(ts_code, fiscal_year, quarter)
);
GO

------------------------------------------------------------
-- 季度成长能力索引：按 ts_code + 年度 + 季度倒序
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_growth_q_1' 
      AND object_id = OBJECT_ID('dbo.fact_growth_quarterly')
)
BEGIN
    CREATE INDEX IX_growth_q_1
        ON dbo.fact_growth_quarterly(ts_code, fiscal_year DESC, quarter DESC);
END;
GO


------------------------------------------------------------
-- 3e) 季度偿债能力表 fact_balance_quarterly（BaoStock query_balance_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_balance_quarterly','U') IS NOT NULL
    DROP TABLE dbo.fact_balance_quarterly;
GO

CREATE TABLE dbo.fact_balance_quarterly(
  ts_code      VARCHAR(20)  NOT NULL,        -- 例: 600000.SH
  fiscal_year  CHAR(4)      NOT NULL,        -- 会计年度，例如 2021
  quarter      TINYINT      NOT NULL,        -- 季度 1~4

  stat_date    DATE         NULL,            -- 统计日期 statDate，例如 2017-03-31
  pub_date     DATE         NULL,            -- 公告日期 pubDate

  -- 偿债能力核心指标
  current_ratio      DECIMAL(18,4) NULL,     -- currentRatio   流动比率
  quick_ratio        DECIMAL(18,4) NULL,     -- quickRatio     速动比率
  cash_ratio         DECIMAL(18,4) NULL,     -- cashRatio      现金比率
  yoy_liability      DECIMAL(9,4)  NULL,     -- YOYLiability   总负债同比增长率（接口为百分比，这里存 0~1）
  liability_to_asset DECIMAL(18,4) NULL,     -- liabilityToAsset 资产负债率（保持接口原始口径）
  asset_to_equity    DECIMAL(18,4) NULL,     -- assetToEquity  权益乘数

  CONSTRAINT PK_fact_balance_quarterly PRIMARY KEY(ts_code, fiscal_year, quarter)
);
GO

------------------------------------------------------------
-- 季度偿债能力索引：按 ts_code + 年度 + 季度倒序
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_balance_q_1' 
      AND object_id = OBJECT_ID('dbo.fact_balance_quarterly')
)
BEGIN
    CREATE INDEX IX_balance_q_1
        ON dbo.fact_balance_quarterly(ts_code, fiscal_year DESC, quarter DESC);
END;
GO

------------------------------------------------------------
-- 3e) 季度现金流量表 fact_cashflow_quarterly（BaoStock query_cash_flow_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_cashflow_quarterly','U') IS NOT NULL
    DROP TABLE dbo.fact_cashflow_quarterly;
GO

CREATE TABLE dbo.fact_cashflow_quarterly(
  ts_code      VARCHAR(20)  NOT NULL,        -- 例: 600000.SH
  fiscal_year  CHAR(4)      NOT NULL,        -- 会计年度，例如 2021
  quarter      TINYINT      NOT NULL,        -- 季度 1~4

  stat_date    DATE         NULL,            -- 统计日期 statDate，例如 2017-06-30
  pub_date     DATE         NULL,            -- 公告日期 pubDate

  -- 现金流量相关比率（接口为比率值，直接按小数存储，不再 /100）
  ca_to_asset            DECIMAL(18,4) NULL,    -- CAToAsset          流动资产 / 总资产
  nca_to_asset           DECIMAL(18,4) NULL,    -- NCAToAsset         非流动资产 / 总资产
  tangible_asset_to_asset DECIMAL(18,4) NULL,   -- tangibleAssetToAsset 有形资产 / 总资产
  ebit_to_interest       DECIMAL(18,4) NULL,    -- ebitToInterest     已获利息倍数
  cfo_to_or              DECIMAL(18,4) NULL,    -- CFOToOR           经营现金净额 / 营业收入
  cfo_to_np              DECIMAL(18,4) NULL,    -- CFOToNP           经营现金净额 / 净利润
  cfo_to_gr              DECIMAL(18,4) NULL,    -- CFOToGr           经营现金净额 / 营业总收入

  CONSTRAINT PK_fact_cashflow_quarterly PRIMARY KEY(ts_code, fiscal_year, quarter)
);
GO

------------------------------------------------------------
-- 季度现金流量索引：按 ts_code + 年度 + 季度倒序
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_cashflow_q_1' 
      AND object_id = OBJECT_ID('dbo.fact_cashflow_quarterly')
)
BEGIN
    CREATE INDEX IX_cashflow_q_1
        ON dbo.fact_cashflow_quarterly(ts_code, fiscal_year DESC, quarter DESC);
END;
GO

------------------------------------------------------------
-- 10) 原始日 K 线明细表 dwd_kline_daily_raw
--     数据源：BaoStock query_history_k_data_plus
------------------------------------------------------------
IF OBJECT_ID('dbo.dwd_kline_daily_raw','U') IS NOT NULL
    DROP TABLE dbo.dwd_kline_daily_raw;
GO

CREATE TABLE dbo.dwd_kline_daily_raw(
  code        VARCHAR(12)  NOT NULL,      -- BaoStock 代码：sh.600000 / sz.000001
  trade_date  DATE         NOT NULL,      -- 交易日期，对应字段 date

  [open]      DECIMAL(18,4) NULL,         -- 开盘价
  [high]      DECIMAL(18,4) NULL,         -- 最高价
  [low]       DECIMAL(18,4) NULL,         -- 最低价
  [close]     DECIMAL(18,4) NOT NULL,     -- 收盘价
  preclose    DECIMAL(18,4) NULL,         -- 前收盘价

  volume      DECIMAL(20,2) NULL,         -- 成交量(股)
  amount      DECIMAL(20,2) NULL,         -- 成交金额(元)

  adjustflag  TINYINT       NULL,         -- 复权标志：1=后复权,2=前复权,3=不复权
  turn        DECIMAL(18,4) NULL,         -- 换手率(%)，接口原值
  tradestatus TINYINT       NULL,         -- 交易状态：1=正常；0=停牌
  pct_chg     DECIMAL(18,4) NULL,         -- 涨跌幅(%)，接口原值

  pe_ttm      DECIMAL(18,4) NULL,         -- 滚动市盈率
  pb_mrq      DECIMAL(18,4) NULL,         -- 市净率
  ps_ttm      DECIMAL(18,4) NULL,         -- 滚动市销率
  pcf_ncf_ttm DECIMAL(18,4) NULL,         -- 滚动市现率
  is_st       TINYINT       NULL,         -- 是否 ST：1=是ST股，0=非ST股

  CONSTRAINT PK_dwd_kline_daily_raw PRIMARY KEY(code, trade_date)
);
GO

------------------------------------------------------------
-- 日 K 线索引：按日期 + 代码查询
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_dwd_kline_raw_1'
      AND object_id = OBJECT_ID('dbo.dwd_kline_daily_raw')
)
BEGIN
    CREATE INDEX IX_dwd_kline_raw_1
        ON dbo.dwd_kline_daily_raw(trade_date DESC, code);
END;
GO


------------------------------------------------------------
-- 11) 除权除息原始表 dwd_dividend_raw（BaoStock query_dividend_data）
------------------------------------------------------------
IF OBJECT_ID('dbo.dwd_dividend_raw','U') IS NOT NULL
    DROP TABLE dbo.dwd_dividend_raw;
GO

CREATE TABLE dbo.dwd_dividend_raw(
  id                       BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

  code                     VARCHAR(12) NOT NULL,  -- BaoStock 代码：sh.600000 / sz.000001
  div_year                 CHAR(4)     NOT NULL,  -- 查询年份（year 参数，通常为预案公告年份）

  divid_pre_notice_date    DATE        NULL,      -- dividPreNoticeDate  预披露公告日
  divid_agm_pum_date       DATE        NULL,      -- dividAgmPumDate     股东大会公告日期
  divid_plan_announce_date DATE        NULL,      -- dividPlanAnnounceDate 预案公告日
  divid_plan_date          DATE        NULL,      -- dividPlanDate       分红实施公告日
  divid_regist_date        DATE        NULL,      -- dividRegistDate     股权登记日
  divid_operate_date       DATE        NULL,      -- dividOperateDate    除权除息日期
  divid_pay_date           DATE        NULL,      -- dividPayDate        派息日
  divid_stock_market_date  DATE        NULL,      -- dividStockMarketDate 红股上市交易日

  divid_cash_ps_before_tax DECIMAL(18,4) NULL,    -- dividCashPsBeforeTax 每股股利税前(元/股)
  divid_cash_ps_after_tax  DECIMAL(18,4) NULL,    -- dividCashPsAfterTax  每股股利税后(元/股)
  divid_stocks_ps          DECIMAL(18,4) NULL,    -- dividStocksPs        每股红股(股/股)
  divid_cash_stock         DECIMAL(18,4) NULL,    -- dividCashStock       分红送转=派息+送股+转增
  divid_reserve_to_stock_ps DECIMAL(18,4) NULL    -- dividReserveToStockPs 每股转增资本(股/股)
);
GO

------------------------------------------------------------
-- 常用索引：按 code + 年度检索
------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_dwd_dividend_raw_1'
      AND object_id = OBJECT_ID('dbo.dwd_dividend_raw')
)
BEGIN
    CREATE INDEX IX_dwd_dividend_raw_1
        ON dbo.dwd_dividend_raw(code, div_year);
END;
GO

-- 如后续需要按除权除息日筛选，可加一个索引：
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_dwd_dividend_raw_2'
      AND object_id = OBJECT_ID('dbo.dwd_dividend_raw')
)
BEGIN
    CREATE INDEX IX_dwd_dividend_raw_2
        ON dbo.dwd_dividend_raw(code, divid_operate_date);
END;
GO

------------------------------------------------------------
-- 12) 全部证券基础资料（BaoStock query_stock_basic + query_stock_industry）
--     原始维度表 dwd_stock_basic_all
------------------------------------------------------------
IF OBJECT_ID('dbo.dwd_stock_basic_all','U') IS NOT NULL
    DROP TABLE dbo.dwd_stock_basic_all;
GO

CREATE TABLE dbo.dwd_stock_basic_all(
  code                 VARCHAR(12)   NOT NULL,      -- BaoStock 代码：sh.600000 / sz.000001 / sz.399001 等
  code_name            NVARCHAR(100) NOT NULL,      -- 证券名称
  ipo_date             DATE          NULL,          -- 上市日期 ipoDate
  out_date             DATE          NULL,          -- 退市日期 outDate
  sec_type             TINYINT       NULL,          -- 证券类型：1股票、2指数、3其它、4可转债、5 ETF
  status               TINYINT       NULL,          -- 上市状态：1上市、0退市

  industry             NVARCHAR(64)  NULL,          -- 行业名称 industry
  industry_class       NVARCHAR(64)  NULL,          -- 行业分类 industryClassification
  industry_update_date DATE          NULL,          -- 行业信息更新日期 updateDate

  CONSTRAINT PK_dwd_stock_basic_all PRIMARY KEY(code)
);
GO

-- 常用查询：按类型 / 状态 / 行业快速过滤
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_dwd_stock_basic_all_1'
      AND object_id = OBJECT_ID('dbo.dwd_stock_basic_all')
)
BEGIN
    CREATE INDEX IX_dwd_stock_basic_all_1
        ON dbo.dwd_stock_basic_all(sec_type, status, industry);
END;
GO

---------------------股票评分表------------------------------

IF OBJECT_ID('dbo.dws_stock_score_daily', 'U') IS NOT NULL
    DROP TABLE dbo.dws_stock_score_daily;
GO

CREATE TABLE dbo.dws_stock_score_daily
(
    /* =========================
       基础主键信息
    ========================= */
    ts_code            VARCHAR(20)  NOT NULL,   -- sh.600000
    trade_date         DATE         NOT NULL,   -- 交易日

    /* =========================
       盈利能力因子（Profitability）
       来源：fact_profit_quarterly
    ========================= */
    roe_ttm            FLOAT NULL,
    net_profit_margin  FLOAT NULL,
    gross_margin       FLOAT NULL,
    profit_qoq         FLOAT NULL,
    profit_yoy         FLOAT NULL,
    score_profit       FLOAT NULL,

    /* =========================
       运营能力因子（Operation）
       来源：fact_operation_quarterly
    ========================= */
    asset_turnover     FLOAT NULL,
    inventory_turnover FLOAT NULL,
    ar_turnover        FLOAT NULL,
    score_operation    FLOAT NULL,

    /* =========================
       成长能力因子（Growth）
       来源：fact_growth_quarterly
    ========================= */
    revenue_yoy        FLOAT NULL,
    revenue_qoq        FLOAT NULL,
    profit_growth_3y   FLOAT NULL,
    score_growth       FLOAT NULL,

    /* =========================
       偿债 / 安全因子（Solvency）
       来源：fact_balance_quarterly
    ========================= */
    debt_asset_ratio   FLOAT NULL,
    current_ratio      FLOAT NULL,
    quick_ratio        FLOAT NULL,
    score_safety       FLOAT NULL,

    /* =========================
       现金流质量（Cash Flow）
       来源：fact_cashflow_quarterly
    ========================= */
    ocf_net_profit     FLOAT NULL,
    free_cash_flow     FLOAT NULL,
    ocf_growth         FLOAT NULL,
    score_cashflow     FLOAT NULL,

    /* =========================
       估值 & 技术因子（Valuation + Technical）
       来源：dwd_kline_daily_raw
    ========================= */
    pe_ttm             FLOAT NULL,
    pb_mrq             FLOAT NULL,
    ps_ttm             FLOAT NULL,

    ma60               FLOAT NULL,
    ma120              FLOAT NULL,
    ma250              FLOAT NULL,

    price_vs_ma60      FLOAT NULL,
    price_vs_ma120     FLOAT NULL,
    price_vs_ma250     FLOAT NULL,

    valuation_percentile FLOAT NULL,  -- 历史估值分位
    low_valuation_days   INT   NULL,  -- 低估持续天数

    score_valuation      FLOAT NULL,

    /* =========================
       分红因子（Dividend）
       来源：dwd_dividend_raw
    ========================= */
    dividend_yield_ttm FLOAT NULL,
    dividend_years     INT   NULL,
    payout_ratio       FLOAT NULL,
    score_dividend     FLOAT NULL,

    /* =========================
       规模 & 流动性（Size / Liquidity）
    ========================= */
    total_mv           FLOAT NULL,
    float_mv           FLOAT NULL,
    avg_turnover_20d   FLOAT NULL,
    score_size         FLOAT NULL,

    /* =========================
       综合评分 & 评级
    ========================= */
    total_score        FLOAT NULL,     -- 0–100
    rating             CHAR(1) NULL,    -- A/B/C/D
    rating_desc        VARCHAR(20) NULL,

    /* =========================
       元数据
    ========================= */
    etl_time           DATETIME2 DEFAULT SYSDATETIME(),

    CONSTRAINT PK_dws_stock_score_daily
        PRIMARY KEY (ts_code, trade_date)
);
GO

ALTER TABLE dbo.dws_stock_score_daily  ---新增行业中性综字段
ADD
    /* =========================
       盈利（行业中性）
    ========================= */
    roe_rank            FLOAT NULL,
    roe_zscore          FLOAT NULL,
    score_profit_ind    FLOAT NULL,

    /* =========================
       估值（行业中性，反向因子）
    ========================= */
    pe_rank             FLOAT NULL,
    pb_rank             FLOAT NULL,
    score_value_ind     FLOAT NULL,

    /* =========================
       偿债（行业中性，反向因子）
    ========================= */
    dar_rank            FLOAT NULL,
    score_safety_ind    FLOAT NULL,

    /* =========================
       分红（行业中性）
    ========================= */
    dividend_rank       FLOAT NULL,
    score_dividend_ind  FLOAT NULL,

    /* =========================
       行业中性综合评分
    ========================= */
    total_score_ind     FLOAT NULL;
GO

-- 按日期查全市场
CREATE INDEX IDX_dws_score_trade_date
ON dbo.dws_stock_score_daily(trade_date);

-- 按评分排序选股
CREATE INDEX IDX_dws_score_total_score
ON dbo.dws_stock_score_daily(total_score DESC);

-- 前端常用：评级筛选
CREATE INDEX IDX_dws_score_rating
ON dbo.dws_stock_score_daily(rating);


/*========================================================
  解释视图：评分 + 行业/名称 + 评级文案底座
  对齐表：dbo.dws_stock_score_daily（最新大表版本）
========================================================*/

-- 1) 可选：评级文案字典（如果你已建过，可跳过）
IF OBJECT_ID('dbo.dim_rating_copy','U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_rating_copy(
        rating        CHAR(1)        NOT NULL PRIMARY KEY,   -- A/B/C/D
        title         NVARCHAR(50)    NOT NULL,
        tagline       NVARCHAR(200)   NOT NULL,
        action_hint   NVARCHAR(200)   NOT NULL,
        risk_hint     NVARCHAR(200)   NOT NULL,
        updated_at    DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME()
    );

    INSERT INTO dbo.dim_rating_copy(rating,title,tagline,action_hint,risk_hint) VALUES
    ('A',N'优选',N'综合因子表现领先，估值与质量匹配度高。',N'可作为核心观察/配置标的，优先进入备选池并设置目标仓位。',N'关注行业景气与盈利兑现节奏，避免追高。'),
    ('B',N'可跟踪',N'存在明确亮点，但一致性不足或估值/风险存在约束。',N'纳入跟踪池，等待催化或关键维度改善后再提升权重。',N'重点关注拖累维度（估值偏贵/杠杆偏高/盈利波动）。'),
    ('C',N'观望',N'因子分布中性偏弱，缺少明确优势。',N'以观察为主，不建议主动加仓；等待基本面拐点或估值回归。',N'短期胜率与赔率均一般，注意时间成本。'),
    ('D',N'回避',N'风险或估值/盈利至少一项明显拖累，综合性价比偏低。',N'不进入备选池；仅在强催化或显著修复后再评估。',N'警惕业绩下修、负债压力、估值陷阱等。');
END
GO

-- 2) 重建视图
IF OBJECT_ID('dbo.vw_stock_score_explain_daily','V') IS NOT NULL
    DROP VIEW dbo.vw_stock_score_explain_daily;
GO

CREATE VIEW dbo.vw_stock_score_explain_daily
AS
SELECT
    s.ts_code,
    s.trade_date,

    -- 证券维度（名称/行业）
    b.code_name,
    b.sec_type,
    b.status,
    b.industry,
    b.industry_class,
    b.industry_update_date,

    /* =========================
       原始因子（用于解释）
    ========================= */
    -- 盈利
    s.roe_ttm,
    s.net_profit_margin,
    s.gross_margin,
    s.profit_qoq,
    s.profit_yoy,

    -- 运营
    s.asset_turnover,
    s.inventory_turnover,
    s.ar_turnover,

    -- 成长
    s.revenue_yoy,
    s.revenue_qoq,
    s.profit_growth_3y,

    -- 偿债/安全
    s.debt_asset_ratio,
    s.current_ratio,
    s.quick_ratio,

    -- 现金流
    s.ocf_net_profit,
    s.free_cash_flow,
    s.ocf_growth,

    -- 估值&技术
    s.pe_ttm,
    s.pb_mrq,
    s.ps_ttm,
    s.ma60,
    s.ma120,
    s.ma250,
    s.price_vs_ma60,
    s.price_vs_ma120,
    s.price_vs_ma250,
    s.valuation_percentile,
    s.low_valuation_days,

    -- 分红
    s.dividend_yield_ttm,
    s.dividend_years,
    s.payout_ratio,

    -- 规模&流动性
    s.total_mv,
    s.float_mv,
    s.avg_turnover_20d,

    /* =========================
       维度得分（用于动态亮点/风险）
    ========================= */
    s.score_profit,
    s.score_operation,
    s.score_growth,
    s.score_safety,
    s.score_cashflow,
    s.score_valuation,
    s.score_dividend,
    s.score_size,

    /* =========================
       综合评分与评级
    ========================= */
    s.total_score,
    s.rating,
    s.rating_desc,
    s.etl_time,

    /* =========================
       行业中性字段（你新增的）
    ========================= */
    s.roe_rank,
    s.roe_zscore,
    s.score_profit_ind,

    s.pe_rank,
    s.pb_rank,
    s.score_value_ind,

    s.dar_rank,
    s.score_safety_ind,

    s.dividend_rank,
    s.score_dividend_ind,

    s.total_score_ind,

    /* =========================
       评级文案底座（前端直接用）
    ========================= */
    c.title       AS rating_title,
    c.tagline     AS rating_tagline,
    c.action_hint AS rating_action_hint,
    c.risk_hint   AS rating_risk_hint

FROM dbo.dws_stock_score_daily s
LEFT JOIN dbo.dwd_stock_basic_all b
       ON b.code = s.ts_code
LEFT JOIN dbo.dim_rating_copy c
       ON c.rating = s.rating;
GO


-----------------API调用统计------------------------------
IF OBJECT_ID('dbo.sys_baostock_api_counter','U') IS NULL
BEGIN
    CREATE TABLE dbo.sys_baostock_api_counter(
        call_date  date          NOT NULL PRIMARY KEY,
        req_count  int           NOT NULL,
        updated_at datetime2(0)  NOT NULL DEFAULT SYSDATETIME()
    );
END
