-- stock.daily_p definition

DROP TABLE IF EXISTS `daily`;
CREATE TABLE `daily`
(
    `ts_code`      varchar(16)        DEFAULT NULL COMMENT '股票代码',
    `trade_date`   int                DEFAULT NULL COMMENT '交易日期',
    `open`         double             DEFAULT NULL COMMENT '开盘价',
    `high`         double             DEFAULT NULL COMMENT '最高价',
    `low`          double             DEFAULT NULL COMMENT '最低价',
    `close`        double             DEFAULT NULL COMMENT '收盘价',
    `pre_close`    double             DEFAULT NULL COMMENT '昨收价(前复权)',
    `change`       double             DEFAULT NULL COMMENT '涨跌额',
    `pct_chg`      double             DEFAULT NULL COMMENT '涨跌幅 （未复权）',
    `vol`          double             DEFAULT NULL COMMENT '成交量 （手）',
    `amount`       double             DEFAULT NULL COMMENT '成交额 （千元）',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    UNIQUE KEY `daily_ts_code_idx` (`ts_code`, `trade_date`) USING BTREE,
    UNIQUE KEY `daily_trade_date_idx` (`trade_date`, `ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='A股日线行情'
;
