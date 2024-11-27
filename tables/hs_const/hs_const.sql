-- stock.hs_const definition

DROP TABLE IF EXISTS `hs_const`;
CREATE TABLE `hs_const`
(
    `ts_code`      varchar(16)        DEFAULT NULL COMMENT 'TS代码',
    `hs_type`      varchar(16)        DEFAULT NULL COMMENT '沪深港通类型SH沪SZ深',
    `in_date`      int                DEFAULT NULL COMMENT '纳入日期',
    `out_date`     int                DEFAULT NULL COMMENT '剔除日期',
    `is_new`       varchar(2)         DEFAULT NULL COMMENT '是否最新 1是 0否',
    `created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
     KEY `hs_const_ts_code` (`ts_code`) USING BTREE
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT ='沪深股通成份股';
