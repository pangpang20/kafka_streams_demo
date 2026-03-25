package com.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * 表结构配置加载器
 *
 * 从 YAML 配置文件加载表结构定义
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class TableSchemaConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(TableSchemaConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/table-schema.yaml";

    private List<TableConfig> tables;

    /**
     * 表配置
     */
    public static class TableConfig {
        private String name;
        private String description;
        private List<FieldConfig> fields;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public List<FieldConfig> getFields() { return fields; }
        public void setFields(List<FieldConfig> fields) { this.fields = fields; }
    }

    /**
     * 字段配置
     */
    public static class FieldConfig {
        private String name;
        private String type;
        private String description;
        private boolean nullable;
        private boolean isPrimaryKey;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public boolean isNullable() { return nullable; }
        public void setNullable(boolean nullable) { this.nullable = nullable; }
        public boolean isPrimaryKey() { return isPrimaryKey; }
        public void setPrimaryKey(boolean primaryKey) { isPrimaryKey = primaryKey; }
    }

    /**
     * 默认构造函数，从默认路径加载配置
     */
    public TableSchemaConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    /**
     * 从指定路径加载配置
     *
     * @param configPath 配置文件路径
     */
    public TableSchemaConfigLoader(String configPath) {
        loadConfig(configPath);
    }

    /**
     * 加载配置文件
     *
     * @param configPath 配置文件路径
     */
    public void loadConfig(String configPath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                log.warn("配置文件不存在：{}, 尝试从 classpath 加载", configPath);
                if (getClass().getClassLoader().getResourceAsStream("table-schema.yaml") != null) {
                    TableSchemaConfigLoader loaded = mapper.readValue(
                        getClass().getClassLoader().getResourceAsStream("table-schema.yaml"),
                        TableSchemaConfigLoader.class
                    );
                    this.tables = loaded.tables;
                } else {
                    throw new RuntimeException("无法找到配置文件 table-schema.yaml");
                }
            } else {
                TableSchemaConfigLoader loaded = mapper.readValue(configFile, TableSchemaConfigLoader.class);
                this.tables = loaded.tables;
            }

            log.info("表结构配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载表结构配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载表结构配置失败", e);
        }
    }

    /**
     * 获取所有表配置
     */
    public List<TableConfig> getTables() {
        return tables;
    }

    /**
     * 根据表名获取表配置
     *
     * @param tableName 表名
     * @return 表配置，不存在返回 null
     */
    public TableConfig getTableByName(String tableName) {
        if (tables == null) {
            return null;
        }
        for (TableConfig table : tables) {
            if (table.getName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    /**
     * 生成建表 SQL
     *
     * @param tableName 表名
     * @param databaseName 数据库名
     * @return 建表 SQL 语句
     */
    public String generateCreateTableSQL(String tableName, String databaseName) {
        TableConfig table = getTableByName(tableName);
        if (table == null) {
            throw new RuntimeException("表配置不存在：" + tableName);
        }

        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(databaseName).append(".").append(tableName).append(" (\n");
        sql.append("  id BIGINT AUTO_INCREMENT PRIMARY KEY,\n");

        for (int i = 0; i < table.getFields().size(); i++) {
            FieldConfig field = table.getFields().get(i);
            sql.append("  ").append(field.getName()).append(" ").append(getSqlType(field.getType()));
            if (!field.isNullable()) {
                sql.append(" NOT NULL");
            }
            if (i < table.getFields().size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        sql.append("  record_key VARCHAR(500),\n");
        sql.append("  record_timestamp DATETIME,\n");
        sql.append("  opcode VARCHAR(10),\n");
        sql.append("  raw_data JSON,\n");
        sql.append("  log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP\n");
        sql.append(")");

        return sql.toString();
    }

    /**
     * 生成异议数据建表 SQL
     *
     * @param tableName 表名（带前缀）
     * @param databaseName 数据库名
     * @param invalidTablePrefix 前缀
     * @return 建表 SQL 语句
     */
    public String generateInvalidTableSQL(String tableName, String databaseName, String invalidTablePrefix) {
        TableConfig table = getTableByName(tableName);
        if (table == null) {
            throw new RuntimeException("表配置不存在：" + tableName);
        }

        String actualTableName = invalidTablePrefix + tableName;
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(databaseName).append(".").append(actualTableName).append(" (\n");
        sql.append("  id BIGINT AUTO_INCREMENT PRIMARY KEY,\n");

        for (int i = 0; i < table.getFields().size(); i++) {
            FieldConfig field = table.getFields().get(i);
            sql.append("  ").append(field.getName()).append(" ").append(getSqlType(field.getType()));
            if (!field.isNullable()) {
                sql.append(" NOT NULL");
            }
            if (i < table.getFields().size() - 1) {
                sql.append(",");
            }
            sql.append("\n");
        }

        sql.append("  record_key VARCHAR(500),\n");
        sql.append("  record_timestamp DATETIME,\n");
        sql.append("  opcode VARCHAR(10),\n");
        sql.append("  failure_summary TEXT,\n");
        sql.append("  error_fields TEXT,\n");
        sql.append("  error_details JSON,\n");
        sql.append("  raw_data JSON,\n");
        sql.append("  log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP\n");
        sql.append(")");

        return sql.toString();
    }

    /**
     * 将配置类型转换为 SQL 类型
     */
    private String getSqlType(String configType) {
        if (configType == null) {
            return "VARCHAR(255)";
        }
        switch (configType.toUpperCase()) {
            case "BIGINT":
                return "BIGINT";
            case "INT":
                return "INT";
            case "DOUBLE":
                return "DOUBLE";
            case "DECIMAL":
                return "DECIMAL(10,2)";
            case "TIMESTAMP":
            case "DATETIME":
                return "DATETIME";
            case "BYTES":
                return "BLOB";
            case "STRING":
            default:
                return "VARCHAR(255)";
        }
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("表结构配置");
        System.out.println("=====================================");
        if (tables != null) {
            for (TableConfig table : tables) {
                System.out.println("表名：" + table.getName());
                System.out.println("描述：" + table.getDescription());
                System.out.println("字段列表:");
                if (table.getFields() != null) {
                    for (FieldConfig field : table.getFields()) {
                        System.out.println("  - " + field.getName() + ": " + field.getType() +
                                (field.isPrimaryKey() ? " (主键)" : "") +
                                (field.isNullable() ? " (可空)" : " (非空)"));
                    }
                }
            }
        }
        System.out.println("=====================================");
    }
}
