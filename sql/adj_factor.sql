-- adj_factor, 复权因子

-- limit: 10000
-- interval: 0.5
-- is_increasing: True

DROP TABLE IF EXISTS `adj_factor`;
CREATE TABLE `adj_factor` (
    `ts_code` varchar(64) COMMENT '股票代码',
    `trade_date` varchar(64) COMMENT '交易日期',
    `adj_factor` double COMMENT '复权因子',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY `adj_factor_ts_code` (`ts_code`, `trade_date`) USING BTREE,
    KEY `adj_factor_trade_date` (`trade_date`, `ts_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='复权因子';