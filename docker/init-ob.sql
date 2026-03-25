-- OceanBase 初始化脚本
-- 创建用于存储 Kafka 数据质量检查问题数据的表

-- 创建数据库
CREATE DATABASE IF NOT EXISTS kafka_quality_check;
USE kafka_quality_check;

-- 创建正常数据存储表
CREATE TABLE IF NOT EXISTS valid_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    record_key VARCHAR(500) COMMENT '记录主键',
    record_timestamp VARCHAR(50) COMMENT '记录时间戳',
    database_name VARCHAR(100) COMMENT '数据库名',
    table_name VARCHAR(100) COMMENT '表名',
    opcode VARCHAR(10) COMMENT '操作类型：I=插入，U=更新，D=删除',
    data_type VARCHAR(50) COMMENT '数据类型：mysql, oracle, sqlserver',

    -- 业务字段
    personid BIGINT COMMENT '人员 ID',
    idcard VARCHAR(50) COMMENT '身份证号',
    name VARCHAR(100) COMMENT '姓名',
    sex VARCHAR(20) COMMENT '性别',
    age INT COMMENT '年龄',
    telephone VARCHAR(50) COMMENT '电话号码',
    bloodtype VARCHAR(20) COMMENT '血型',
    creditscore DOUBLE COMMENT '信用分',
    housingareas DOUBLE COMMENT '住房面积',

    -- 元数据
    log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '日志记录时间',
    raw_data JSON COMMENT '原始数据 (JSON 格式)',

    INDEX idx_record_key (record_key),
    INDEX idx_opcode (opcode),
    INDEX idx_table_name (table_name),
    INDEX idx_log_timestamp (log_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka 数据质量检查 - 正常数据表';

-- 创建问题数据存储表
CREATE TABLE IF NOT EXISTS invalid_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    record_key VARCHAR(500) COMMENT '记录主键',
    record_timestamp VARCHAR(50) COMMENT '记录时间戳',
    database_name VARCHAR(100) COMMENT '数据库名',
    table_name VARCHAR(100) COMMENT '表名',
    opcode VARCHAR(10) COMMENT '操作类型：I=插入，U=更新，D=删除',
    data_type VARCHAR(50) COMMENT '数据类型：mysql, oracle, sqlserver',

    -- 业务字段
    personid BIGINT COMMENT '人员 ID',
    idcard VARCHAR(50) COMMENT '身份证号',
    name VARCHAR(100) COMMENT '姓名',
    sex VARCHAR(20) COMMENT '性别',
    age INT COMMENT '年龄',
    telephone VARCHAR(50) COMMENT '电话号码',
    bloodtype VARCHAR(20) COMMENT '血型',
    creditscore DOUBLE COMMENT '信用分',
    housingareas DOUBLE COMMENT '住房面积',

    -- 质量检查结果
    failure_summary TEXT COMMENT '失败原因汇总',
    error_fields TEXT COMMENT '错误字段列表 (JSON 格式)',
    error_details JSON COMMENT '详细错误信息 (JSON 格式)',

    -- 元数据
    log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '日志记录时间',
    raw_data JSON COMMENT '原始数据 (JSON 格式)',

    INDEX idx_record_key (record_key),
    INDEX idx_opcode (opcode),
    INDEX idx_table_name (table_name),
    INDEX idx_log_timestamp (log_timestamp),
    INDEX idx_error_fields (error_fields(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka 数据质量检查 - 问题数据表';

-- 创建统计数据表
CREATE TABLE IF NOT EXISTS quality_stats (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    stat_date DATE COMMENT '统计日期',
    stat_hour INT COMMENT '统计小时 (0-23)',
    table_name VARCHAR(100) COMMENT '表名',
    total_count INT DEFAULT 0 COMMENT '总记录数',
    invalid_count INT DEFAULT 0 COMMENT '问题数据数',
    valid_count INT DEFAULT 0 COMMENT '正常数据数',
    invalid_rate DECIMAL(5,2) DEFAULT 0 COMMENT '问题数据率 (%)',

    -- 错误类型统计
    sex_error_count INT DEFAULT 0 COMMENT '性别错误数',
    age_error_count INT DEFAULT 0 COMMENT '年龄错误数',
    telephone_error_count INT DEFAULT 0 COMMENT '电话错误数',
    bloodtype_error_count INT DEFAULT 0 COMMENT '血型错误数',
    creditscore_error_count INT DEFAULT 0 COMMENT '信用分错误数',
    housingareas_error_count INT DEFAULT 0 COMMENT '住房面积错误数',

    created_time DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_date_hour_table (stat_date, stat_hour, table_name),
    INDEX idx_stat_date (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Kafka 数据质量检查 - 统计表';

-- 插入初始化统计配置
INSERT INTO quality_stats (stat_date, stat_hour, table_name, total_count, invalid_count, valid_count)
VALUES (CURDATE(), HOUR(NOW()), 'baseinfo', 0, 0, 0)
ON DUPLICATE KEY UPDATE updated_time = CURRENT_TIMESTAMP;
