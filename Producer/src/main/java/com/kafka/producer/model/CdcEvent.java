package com.kafka.producer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * CDC 数据变更事件模型
 *
 * 该类别表示数据库变更捕获（CDC）的数据结构，
 * 包含操作类型、表信息、字段数据等完整信息。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class CdcEvent {

    /**
     * 数据变更记录列表
     */
    @JsonProperty("data")
    private DataRecord[] data;

    /**
     * 单条数据变更记录
     */
    public static class DataRecord {
        /**
         * 主键字段名
         */
        @JsonProperty("keyfields")
        private String keyfields;

        /**
         * 数据位点（用于断点续传）
         */
        @JsonProperty("pos")
        private String pos;

        /**
         * 数据库名称
         */
        @JsonProperty("databasename")
        private String databasename;

        /**
         * 所有字段数据（包含 before 和 after）
         */
        @JsonProperty("allfields")
        private AllFields allfields;

        /**
         * 表名
         */
        @JsonProperty("tablename")
        private String tablename;

        /**
         * 操作类型：I=插入，D=删除，U=更新
         */
        @JsonProperty("opcode")
        private String opcode;

        /**
         * 数据库类型：mysql/oracle/sqlserver
         */
        @JsonProperty("type")
        private String type;

        /**
         * 数据操作时间戳
         */
        @JsonProperty("timestamp")
        private String timestamp;

        // Getters and Setters
        public String getKeyfields() { return keyfields; }
        public void setKeyfields(String keyfields) { this.keyfields = keyfields; }

        public String getPos() { return pos; }
        public void setPos(String pos) { this.pos = pos; }

        public String getDatabasename() { return databasename; }
        public void setDatabasename(String databasename) { this.databasename = databasename; }

        public AllFields getAllfields() { return allfields; }
        public void setAllfields(AllFields allfields) { this.allfields = allfields; }

        public String getTablename() { return tablename; }
        public void setTablename(String tablename) { this.tablename = tablename; }

        public String getOpcode() { return opcode; }
        public void setOpcode(String opcode) { this.opcode = opcode; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }

    /**
     * 字段数据容器，包含变更前后的数据
     */
    public static class AllFields {
        /**
         * 变更后的数据
         */
        @JsonProperty("after_data")
        private Map<String, FieldData> afterData;

        /**
         * 变更前的数据（更新操作时有值）
         */
        @JsonProperty("before_data")
        private Map<String, FieldData> beforeData;

        public Map<String, FieldData> getAfterData() { return afterData; }
        public void setAfterData(Map<String, FieldData> afterData) { this.afterData = afterData; }

        public Map<String, FieldData> getBeforeData() { return beforeData; }
        public void setBeforeData(Map<String, FieldData> beforeData) { this.beforeData = beforeData; }
    }

    /**
     * 单个字段数据，包含类型和值
     */
    public static class FieldData {
        /**
         * 字段类型（Kafka Connect 类型）
         */
        @JsonProperty("type")
        private String type;

        /**
         * 字段值
         */
        @JsonProperty("value")
        private Object value;

        public FieldData() {}

        public FieldData(String type, Object value) {
            this.type = type;
            this.value = value;
        }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
    }

    // Getters and Setters
    public DataRecord[] getData() { return data; }
    public void setData(DataRecord[] data) { this.data = data; }
}
