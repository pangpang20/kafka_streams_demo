package com.kafka.producer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Producer 表结构配置加载器
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProducerTableConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ProducerTableConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/table-schema.yaml";

    private List<TableConfig> tables;

    public static class TableConfig {
        private String name;
        private String description;
        private String primaryKey;
        private String keyFields;
        private List<FieldConfig> fields;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getPrimaryKey() { return primaryKey; }
        public void setPrimaryKey(String primaryKey) { this.primaryKey = primaryKey; }
        public String getKeyFields() { return keyFields; }
        public void setKeyFields(String keyFields) { this.keyFields = keyFields; }
        public List<FieldConfig> getFields() { return fields; }
        public void setFields(List<FieldConfig> fields) { this.fields = fields; }
    }

    public static class FieldConfig {
        private String name;
        private String type;
        private String description;
        private String generator;
        private List<String> values;
        private Map<String, Object> valuesMap;  // for surnames/names
        private Integer min;
        private Integer max;
        private Double minDouble;
        private Double maxDouble;
        private String pattern;
        private String sequenceName;
        private Object value;  // for constant generator
        private Integer daysBack;  // for recent_timestamp
        private List<Object> badValues;
        private Integer badProbability;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public String getGenerator() { return generator; }
        public void setGenerator(String generator) { this.generator = generator; }
        public List<String> getValues() { return values; }
        public void setValues(List<String> values) { this.values = values; }
        public Map<String, Object> getValuesMap() { return valuesMap; }
        public void setValuesMap(Map<String, Object> valuesMap) { this.valuesMap = valuesMap; }
        public Integer getMin() { return min; }
        public void setMin(Integer min) { this.min = min; }
        public Integer getMax() { return max; }
        public void setMax(Integer max) { this.max = max; }
        public Double getMinDouble() { return minDouble; }
        public void setMinDouble(Double minDouble) { this.minDouble = minDouble; }
        public Double getMaxDouble() { return maxDouble; }
        public void setMaxDouble(Double maxDouble) { this.maxDouble = maxDouble; }
        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
        public String getSequenceName() { return sequenceName; }
        public void setSequenceName(String sequenceName) { this.sequenceName = sequenceName; }
        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
        public Integer getDaysBack() { return daysBack; }
        public void setDaysBack(Integer daysBack) { this.daysBack = daysBack; }
        public List<Object> getBadValues() { return badValues; }
        public void setBadValues(List<Object> badValues) { this.badValues = badValues; }
        public Integer getBadProbability() { return badProbability; }
        public void setBadProbability(Integer badProbability) { this.badProbability = badProbability; }
    }

    public ProducerTableConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    public ProducerTableConfigLoader(String configPath) {
        loadConfig(configPath);
    }

    public void loadConfig(String configPath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                throw new RuntimeException("配置文件不存在：" + configPath);
            }

            ProducerTableConfigLoader loaded = mapper.readValue(configFile, ProducerTableConfigLoader.class);
            this.tables = loaded.tables;

            log.info("表结构配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载表结构配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载表结构配置失败", e);
        }
    }

    public List<TableConfig> getTables() { return tables; }

    public TableConfig getTableByName(String tableName) {
        if (tables == null) return null;
        for (TableConfig table : tables) {
            if (table.getName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("表结构配置");
        System.out.println("=====================================");
        if (tables != null) {
            for (TableConfig table : tables) {
                System.out.println("表名：" + table.getName());
                System.out.println("描述：" + table.getDescription());
                System.out.println("主键：" + table.getPrimaryKey());
                System.out.println("Key 字段：" + table.getKeyFields());
                System.out.println("字段列表:");
                if (table.getFields() != null) {
                    for (FieldConfig field : table.getFields()) {
                        System.out.println("  - " + field.getName() + ": " + field.getType() +
                                " (generator: " + field.getGenerator() + ")");
                    }
                }
            }
        }
        System.out.println("=====================================");
    }
}
