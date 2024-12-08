-- 指数日线行情

-- limit: 10000
-- interval: 0.5
-- is_increasing: True

DROP TABLE IF EXISTS `index_daily`;
CREATE TABLE `index_daily` (
    `ts_code` varchar(64) COMMENT 'TS指数代码',
    `trade_date` varchar(64) COMMENT '交易日',
    `close` double COMMENT '收盘点位',
    `open` double COMMENT '开盘点位',
    `high` double COMMENT '最高点位',
    `low` double COMMENT '最低点位',
    `pre_close` double COMMENT '昨日收盘点',
    `change` double COMMENT '涨跌点',
    `pct_chg` double COMMENT '涨跌幅（%）',
    `vol` double COMMENT '成交量（手）',
    `amount` double COMMENT '成交额（千元）',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY `index_daily_ts_code` (`ts_code`, `trade_date`) USING BTREE,
    KEY `index_daily_trade_date` (`trade_date`, `ts_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数日线行情';