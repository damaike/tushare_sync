-- fund_nav, 基金净值

-- limit: 20000
-- interval: 0.5
-- is_increasing: True

-- api_name: fund_nav
-- date_column: nav_date
-- extra_params: {"market": "E", "status": "L"}
-- incremental: true

DROP TABLE IF EXISTS `fund_nav`;

CREATE TABLE `fund_nav` (
    `ts_code` varchar(64) COMMENT 'TS代码',
    `ann_date` varchar(64) COMMENT '公告日期',
    `nav_date` varchar(64) COMMENT '净值日期',
    `unit_nav` double COMMENT '单位净值',
    `accum_nav` double COMMENT '累计净值',
    `accum_div` double COMMENT '累计分红',
    `net_asset` double COMMENT '资产净值',
    `total_netasset` double COMMENT '合计资产净值',
    `adj_nav` double COMMENT '复权单位净值',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY `fund_nav_ts_code` (`ts_code`, `nav_date`) USING BTREE,
    KEY `fund_nav_nav_date` (`nav_date`, `ts_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金净值';