-- stk_factor_pro，日线前复权，带技术指标 MA, MACD, KDJ

CREATE TABLE `stk_factor_pro`
(
    `ts_code`          varchar(16)        DEFAULT NULL COMMENT '股票代码',
    `trade_date`       int                DEFAULT NULL COMMENT '交易日期',
    `open_qfq`         double             DEFAULT NULL COMMENT '开盘价',
    `high_qfq`         double             DEFAULT NULL COMMENT '最高价',
    `low_qfq`          double             DEFAULT NULL COMMENT '最低价',
    `close_qfq`        double             DEFAULT NULL COMMENT '收盘价',
    `pre_close_qfq`    double             DEFAULT NULL COMMENT '昨收价(前复权)',
    `change`           double         DEFAULT NULL COMMENT '涨跌额',
    `pct_chg`          double         DEFAULT NULL COMMENT '涨跌幅 （未复权）',
    `vol`              double         DEFAULT NULL COMMENT '成交量 （手）',
    `amount`           double         DEFAULT NULL COMMENT '成交额 （千元）',
    `volume_ratio`     double         DEFAULT NULL COMMENT '量比',
    `pe_ttm`           double         DEFAULT NULL COMMENT ' 市盈率（TTM,亏损的PE为空）',
    `pb`               double         DEFAULT NULL COMMENT ' 市净率（总市值/净资产）',
    `dv_ratio`		  double            DEFAULT NULL COMMENT ' 股息率 （%）',
    `total_share`	  double            DEFAULT NULL COMMENT '总股本 （万股）',
    `float_share`	  double            DEFAULT NULL COMMENT '流通股本 （万股）',
    `free_share`	  double            DEFAULT NULL COMMENT '自由流通股本 （万）',
    `total_mv`		  double            DEFAULT NULL COMMENT '总市值 （万元）',
    `circ_mv`		    double            DEFAULT NULL COMMENT '流通市值（万元）',
    `adj_factor`	  double            DEFAULT NULL COMMENT '复权因子',
    `ma_qfq_5`		  double            DEFAULT NULL COMMENT '简单移动平均-N=5',
    `ma_qfq_10`		  double            DEFAULT NULL COMMENT '简单移动平均-N=10',
    `ma_qfq_20`		  double            DEFAULT NULL COMMENT '简单移动平均-N=20',
    `ma_qfq_30`		  double            DEFAULT NULL COMMENT '简单移动平均-N=30',
    `ma_qfq_60`		  double            DEFAULT NULL COMMENT '简单移动平均-N=60',
    `ma_qfq_90`		  double            DEFAULT NULL COMMENT '简单移动平均-N=90',
    `ma_qfq_250`		double            DEFAULT NULL COMMENT '简单移动平均-N=250',
    `macd_qfq`		  double            DEFAULT NULL COMMENT 'MACD指标-CLOSE, SHORT=12, LONG=26, M=9',
    `macd_dea_qfq`	double            DEFAULT NULL COMMENT 'MACD指标-CLOSE, SHORT=12, LONG=26, M=9',
    `macd_dif_qfq`	double            DEFAULT NULL COMMENT 'MACD指标-CLOSE, SHORT=12, LONG=26, M=9',
    `kdj_qfq`		    double            DEFAULT NULL COMMENT 'KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3',
    `kdj_d_qfq`		  double            DEFAULT NULL COMMENT 'KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3',
    `kdj_k_qfq`		  double            DEFAULT NULL COMMENT 'KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    UNIQUE KEY `stk_factor_pro_ts_code_idx` (`ts_code`, `trade_date`) USING BTREE,
    UNIQUE KEY `stk_factor_pro_trade_date_idx` (`trade_date`, `ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='股票技术面因子(专业版)'
;
