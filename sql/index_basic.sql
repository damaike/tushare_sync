-- index_basic, 指数基本信息

DROP TABLE IF EXISTS `index_basic`;

CREATE TABLE `index_basic` (
    `ts_code` varchar(16) COMMENT 'TS代码',
    `name` varchar(128) COMMENT '简称',
    `fullname` varchar(128) COMMENT '指数全称',
    `market` varchar(64) COMMENT '市场',
    `publisher` varchar(64) COMMENT '发布方',
    `index_type` varchar(64) COMMENT '指数风格',
    `category` varchar(64) COMMENT '指数类别',
    `base_date` varchar(64) COMMENT '基期',
    `base_point` double COMMENT '基点',
    `list_date` varchar(64) COMMENT '发布日期',
    `weight_rule` varchar(64) COMMENT '加权方式',
    `desc` varchar(128) COMMENT '描述',
    `exp_date` varchar(64) COMMENT '终止日期',
    `created_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数基本信息';