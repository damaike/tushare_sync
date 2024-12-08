-- fund_basic, 基金基本信息

-- limit: 20000
-- interval: 0
-- is_increasing: False
-- extra_params: {"market": "E", "status": "L"}

DROP TABLE IF EXISTS `fund_basic`;

CREATE TABLE `fund_basic` (
    `ts_code` varchar(64) COMMENT '基金代码',
    `name` varchar(64) COMMENT '简称',
    `management` varchar(256) COMMENT '管理人',
    `custodian` varchar(256) COMMENT '托管人',
    `fund_type` varchar(64) COMMENT '投资类型',
    `found_date` varchar(64) COMMENT '成立日期',
    `due_date` varchar(64) COMMENT '到期日期',
    `list_date` varchar(64) COMMENT '上市时间',
    `issue_date` varchar(64) COMMENT '发行日期',
    `delist_date` varchar(64) COMMENT '退市日期',
    `issue_amount` double COMMENT '发行份额(亿)',
    `m_fee` double COMMENT '管理费',
    `c_fee` double COMMENT '托管费',
    `duration_year` double COMMENT '存续期',
    `p_value` double COMMENT '面值',
    `min_amount` double COMMENT '起点金额(万元)',
    `exp_return` double COMMENT '预期收益率',
    `benchmark` varchar(1024) COMMENT '业绩比较基准',
    `status` varchar(64) COMMENT '存续状态D摘牌 I发行 L已上市',
    `invest_type` varchar(256) COMMENT '投资风格',
    `type` varchar(256) COMMENT '基金类型',
    `trustee` varchar(256) COMMENT '受托人',
    `purc_startdate` varchar(64) COMMENT '日常申购起始日',
    `redm_startdate` varchar(64) COMMENT '日常赎回起始日',
    `market` varchar(64) COMMENT 'E场内O场外',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    KEY `fund_basic_ts_code` (`ts_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='基金基本信息';

