/* 数据库：stock（请预先创建）
   本脚本可重复执行：先删视图/表，再重建 + 索引
*/

------------------------------------------------------------
-- 0) 先删除视图（如已存在），保证可重复执行
------------------------------------------------------------
IF OBJECT_ID('dbo.v_y3_profit_dividend','V') IS NOT NULL
    DROP VIEW dbo.v_y3_profit_dividend;
GO


------------------------------------------------------------
-- 1) 股票基础信息表 dim_security
------------------------------------------------------------
IF OBJECT_ID('dbo.dim_security','U') IS NOT NULL
    DROP TABLE dbo.dim_security;
GO

CREATE TABLE dbo.dim_security(
  ts_code        VARCHAR(20)  NOT NULL PRIMARY KEY,   -- 例: 600000.SH
  symbol         VARCHAR(10)  NOT NULL,               -- 例: 600000
  name           NVARCHAR(64) NOT NULL,
  exchange       VARCHAR(10)  NOT NULL,               -- SH/SZ
  industry       NVARCHAR(64) NULL,                   -- 行业名称
  list_date      DATE         NULL,                   -- 上市日期
  status         VARCHAR(10)  NULL,                   -- 上市状态
  total_shares   DECIMAL(20,2) NULL,                  -- 总股本(股)
  float_shares   DECIMAL(20,2) NULL                   -- 流通股本(股)
);
GO


------------------------------------------------------------
-- 2) 日线 K 表 fact_daily（前复权）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_daily','U') IS NOT NULL
    DROP TABLE dbo.fact_daily;
GO

CREATE TABLE dbo.fact_daily(
  ts_code    VARCHAR(20)  NOT NULL,
  trade_date DATE         NOT NULL,
  [open]     DECIMAL(18,4) NULL,
  high       DECIMAL(18,4) NULL,
  low        DECIMAL(18,4) NULL,
  [close]    DECIMAL(18,4) NOT NULL,
  vol        DECIMAL(20,2) NULL,   -- 成交量(股)
  amount     DECIMAL(20,2) NULL,   -- 成交额(元)
  CONSTRAINT PK_fact_daily PRIMARY KEY(ts_code, trade_date)
);
GO


------------------------------------------------------------
-- 3) 年度财务指标表 fact_finance_annual
--    只存年度口径核心科目 + 比率 + 分红/盈利标记
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_finance_annual','U') IS NOT NULL
    DROP TABLE dbo.fact_finance_annual;
GO

CREATE TABLE dbo.fact_finance_annual(
  ts_code      VARCHAR(20)  NOT NULL,
  fiscal_year  CHAR(4)      NOT NULL,           -- 会计年度，例如 2021

  -- 核心财务量（元）
  net_profit    DECIMAL(18,2) NULL,             -- 归母净利润
  net_assets    DECIMAL(18,2) NULL,             -- 归母净资产
  total_assets  DECIMAL(18,2) NULL,             -- 资产总计
  total_liab    DECIMAL(18,2) NULL,             -- 负债合计
  bps           DECIMAL(18,4) NULL,             -- 每股净资产(元/股)
  total_mv      DECIMAL(18,2) NULL,             -- 年末总市值(元，可选)

  -- 比率指标
  roe               DECIMAL(9,4)  NULL,         -- ROE
  debt_asset_ratio  DECIMAL(9,4)  NULL,         -- 资产负债率
  pe                DECIMAL(9,4)  NULL,         -- 年度 PE（如有）
  pb                DECIMAL(9,4)  NULL,         -- 年度 PB（如有）

  -- 分红 / 盈利标记
  dividend_cash     DECIMAL(18,2) NULL,         -- 当年现金分红总额(元)
  dividend_flag     TINYINT      NOT NULL DEFAULT 0,   -- 1=当年有现金分红
  profit_flag       TINYINT      NOT NULL DEFAULT 0,   -- 1=当年盈利

  CONSTRAINT PK_fact_finance_annual PRIMARY KEY(ts_code, fiscal_year)
);
GO


------------------------------------------------------------
-- 4) 分红明细表 fact_dividend（按公告维度）
------------------------------------------------------------
IF OBJECT_ID('dbo.fact_dividend','U') IS NOT NULL
    DROP TABLE dbo.fact_dividend;
GO

CREATE TABLE dbo.fact_dividend(
  ts_code        VARCHAR(20)  NOT NULL,
  notice_date    DATE         NOT NULL,        -- 公告日期
  plan_year      CHAR(4)      NOT NULL,        -- 分红方案对应年度
  cash_per_share DECIMAL(18,4) NULL,           -- 每股派现（元）
  ex_date        DATE         NULL,            -- 除权除息日
  record_date    DATE         NULL,            -- 股权登记日
  pay_date       DATE         NULL,            -- 现金发放日
  CONSTRAINT PK_fact_dividend PRIMARY KEY(ts_code, notice_date)
);
GO


------------------------------------------------------------
-- 5) 公告明细表 dwd_announcement
------------------------------------------------------------
IF OBJECT_ID('dbo.dwd_announcement','U') IS NOT NULL
    DROP TABLE dbo.dwd_announcement;
GO

CREATE TABLE dbo.dwd_announcement(
  ts_code     VARCHAR(20)    NOT NULL,
  notice_id   VARCHAR(64)    NOT NULL,         -- 公告唯一 ID（接口侧）
  notice_date DATE           NOT NULL,         -- 公告日期
  title       NVARCHAR(256)  NOT NULL,         -- 公告标题
  category    NVARCHAR(64)   NULL,             -- 分红/回购/年报/快报/其他
  url         NVARCHAR(512)  NULL,             -- 公告原文链接
  CONSTRAINT PK_dwd_announcement PRIMARY KEY(ts_code, notice_id)
);
GO


------------------------------------------------------------
-- 6) 日度指标 + 综合评分表 dwm_indicators_daily
--    close / ma250 / pb / pe_ttm / total_mv / below_ma250 + score_*
------------------------------------------------------------
IF OBJECT_ID('dbo.dwm_indicators_daily','U') IS NOT NULL
    DROP TABLE dbo.dwm_indicators_daily;
GO

CREATE TABLE dbo.dwm_indicators_daily(
  ts_code    VARCHAR(20)  NOT NULL,
  trade_date DATE         NOT NULL,

  -- 日度行情与技术指标（由 fetch_kline 维护）
  [close]    DECIMAL(18,4) NOT NULL,           -- 收盘价
  ma250      DECIMAL(18,4) NULL,               -- 250 日均线
  pb         DECIMAL(18,4) NULL,               -- 当日 PB（pbMRQ）
  pe_ttm     DECIMAL(18,4) NULL,               -- 当日滚动 PE（peTTM）
  total_mv   DECIMAL(18,2) NULL,               -- 当日总市值(元)，= close * 股本
  below_ma250 AS (CASE WHEN [close] < ma250 THEN 1 ELSE 0 END) PERSISTED,

  -- 综合评分维度（由 etl/calc_scores.py 维护）
  score_mv        DECIMAL(9,4) NULL,           -- 市值得分
  score_roe       DECIMAL(9,4) NULL,           -- ROE 得分
  score_dar       DECIMAL(9,4) NULL,           -- 资产负债率得分
  score_pe        DECIMAL(9,4) NULL,           -- PE 得分
  score_pb        DECIMAL(9,4) NULL,           -- PB 得分
  score_val       DECIMAL(9,4) NULL,           -- 估值得分 (PE+PB)
  score_prof      DECIMAL(9,4) NULL,           -- 盈利得分
  score_safety    DECIMAL(9,4) NULL,           -- 安全得分（杠杆）
  score_size      DECIMAL(9,4) NULL,           -- 规模得分
  total_score     DECIMAL(9,4) NULL,           -- 综合总分
  rating          VARCHAR(8)   NULL,           -- 评级（A/B/C/D 等）

  CONSTRAINT PK_dwm_indicators_daily PRIMARY KEY(ts_code, trade_date)
);
GO


------------------------------------------------------------
-- 7) 近 3 年连续盈利且分红视图 v_y3_profit_dividend（按财年）
------------------------------------------------------------
CREATE VIEW dbo.v_y3_profit_dividend
AS
SELECT f.ts_code
FROM dbo.fact_finance_annual f
JOIN (
    SELECT CONVERT(CHAR(4), YEAR(GETDATE()) - 1) AS y
    UNION ALL
    SELECT CONVERT(CHAR(4), YEAR(GETDATE()) - 2)
    UNION ALL
    SELECT CONVERT(CHAR(4), YEAR(GETDATE()) - 3)
) yrs ON f.fiscal_year = yrs.y
GROUP BY f.ts_code
HAVING SUM(CASE WHEN f.profit_flag = 1 THEN 1 ELSE 0 END) = 3
   AND SUM(CASE WHEN f.dividend_flag = 1 THEN 1 ELSE 0 END) = 3;
GO


------------------------------------------------------------
-- 8) 选股结果快照表 dm_screen_pick
--    run_screener 每次跑一遍，按 run_date 做快照
------------------------------------------------------------
IF OBJECT_ID('dbo.dm_screen_pick','U') IS NOT NULL
    DROP TABLE dbo.dm_screen_pick;
GO

CREATE TABLE dbo.dm_screen_pick(
  run_date          DATE         NOT NULL,      -- 选股运行日期（通常 = 最近交易日）
  ts_code           VARCHAR(20)  NOT NULL,
  name              NVARCHAR(64) NOT NULL,

  pb                DECIMAL(18,4) NOT NULL,    -- 当日 PB
  total_mv          DECIMAL(18,2) NOT NULL,    -- 当日总市值(元)
  below_ma250       TINYINT       NOT NULL,    -- 1=低于 MA250
  y3_profit_dividend TINYINT      NOT NULL,    -- 1=近三年连续盈利+分红

  score_mv        DECIMAL(9,4) NULL,
  score_roe       DECIMAL(9,4) NULL,
  score_dar       DECIMAL(9,4) NULL,
  score_pe        DECIMAL(9,4) NULL,
  score_pb        DECIMAL(9,4) NULL,
  score_val       DECIMAL(9,4) NULL,
  score_prof      DECIMAL(9,4) NULL,
  score_safety    DECIMAL(9,4) NULL,
  score_size      DECIMAL(9,4) NULL,
  total_score     DECIMAL(9,4) NULL,
  rating          VARCHAR(8)   NULL,

  CONSTRAINT PK_dm_screen_pick PRIMARY KEY(run_date, ts_code)
);
GO


------------------------------------------------------------
-- 9) 索引（常用查询模式）
------------------------------------------------------------

-- 日线行情按 ts_code + 日期倒序
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_fact_daily_1' 
      AND object_id = OBJECT_ID('dbo.fact_daily')
)
BEGIN
    CREATE INDEX IX_fact_daily_1
        ON dbo.fact_daily(ts_code, trade_date DESC);
END;
GO

-- 日度指标 + 评分按 ts_code + 日期倒序
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_dwm_ind_1' 
      AND object_id = OBJECT_ID('dbo.dwm_indicators_daily')
)
BEGIN
    CREATE INDEX IX_dwm_ind_1
        ON dbo.dwm_indicators_daily(ts_code, trade_date DESC);
END;
GO

-- 年度财务按 ts_code + 年度倒序
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes 
    WHERE name = 'IX_fin_yr_1' 
      AND object_id = OBJECT_ID('dbo.fact_finance_annual')
)
BEGIN
    CREATE INDEX IX_fin_yr_1
        ON dbo.fact_finance_annual(ts_code, fiscal_year DESC);
END;
GO
