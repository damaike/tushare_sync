-- 交易日历

-- limit: 20000
-- interval: 0
-- is_increasing: False

DROP TABLE IF EXISTS `trade_cal`;
CREATE TABLE `trade_cal`
(
    `exchange`      varchar(64)        DEFAULT NULL COMMENT '交易所 SSE上交所 SZSE深交所',
    `cal_date`      int                DEFAULT NULL COMMENT '日历日期',
    `is_open`       varchar(64)         DEFAULT NULL COMMENT '是否交易 0休市 1交易',
    `pretrade_date` int                DEFAULT NULL COMMENT '上一个交易日',
    `created_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     KEY `trade_cal_exchange` (`exchange`, `cal_date`) USING BTREE,
     KEY `trade_cal_cal_date` (`cal_date`, `exchange`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='交易日历';
