-- stock.bak_daily definition

DROP TABLE IF EXISTS `bak_daily`;
CREATE TABLE `bak_daily`
(
    `ts_code`      varchar(16)        DEFAULT NULL COMMENT '股票代码',
    `trade_date`   int                DEFAULT NULL COMMENT '交易日期',
    `name`         varchar(64)        DEFAULT NULL COMMENT '股票名称',
    `pct_change`   double             DEFAULT NULL COMMENT '涨跌幅',
    `close`        double             DEFAULT NULL COMMENT '收盘价',
    `change`       double             DEFAULT NULL COMMENT '涨跌额',
    `open`         double             DEFAULT NULL COMMENT '开盘价',
    `high`         double             DEFAULT NULL COMMENT '最高价',
    `low`          double             DEFAULT NULL COMMENT '最低价',
    `pre_close`    double             DEFAULT NULL COMMENT '昨收价',
    `vol_ratio`    double             DEFAULT NULL COMMENT '量比',
    `turn_over`    double             DEFAULT NULL COMMENT '换手率',
    `swing`        double             DEFAULT NULL COMMENT '振幅',
    `vol`          double             DEFAULT NULL COMMENT '成交量',
    `amount`       double             DEFAULT NULL COMMENT '成交额',
    `selling`      double             DEFAULT NULL COMMENT '外盘',
    `buying`       double             DEFAULT NULL COMMENT '内盘',
    `total_share`  double             DEFAULT NULL COMMENT '总股本(万)',
    `float_share`  double             DEFAULT NULL COMMENT '流通股本(万)',
    `pe`           double             DEFAULT NULL COMMENT '市盈(动)',
    `industry`     varchar(32)        DEFAULT NULL COMMENT '所属行业',
    `area`         varchar(32)        DEFAULT NULL COMMENT '所属地域',
    `float_mv`     double             DEFAULT NULL COMMENT '流通市值',
    `total_mv`     double             DEFAULT NULL COMMENT '总市值',
    `avg_price`    double             DEFAULT NULL COMMENT '平均价',
    `strength`     double             DEFAULT NULL COMMENT '强弱度(%)',
    `activity`     double             DEFAULT NULL COMMENT '活跃度(%)',
    `avg_turnover` double             DEFAULT NULL COMMENT '笔换手',
    `attack`       double             DEFAULT NULL COMMENT '攻击波(%)',
    `interval_3`   double             DEFAULT NULL COMMENT '近3月涨幅',
    `interval_6`   double             DEFAULT NULL COMMENT '近6月涨幅',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    UNIQUE KEY `daily_ts_code_idx` (`ts_code`, `trade_date`) USING BTREE,
    UNIQUE KEY `daily_trade_date_idx` (`trade_date`, `ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='沪深股票-行情数据-备用行情'
;