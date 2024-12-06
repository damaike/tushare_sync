-- stock.name_change definition

DROP TABLE IF EXISTS `name_change`;
CREATE TABLE `name_change`
(
    `ts_code`       varchar(16)        DEFAULT NULL COMMENT 'TS代码',
    `name`          varchar(64)        DEFAULT NULL COMMENT '证券名称',
    `start_date`    int                DEFAULT NULL COMMENT '开始日期',
    `end_date`      int                DEFAULT NULL COMMENT '结束日期',
    `ann_date`      int                DEFAULT NULL COMMENT '公告日期',
    `change_reason` varchar(64)        DEFAULT NULL COMMENT '变更原因',
    `created_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time`  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     KEY `name_change_ts_code` (`ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='股票曾用名';