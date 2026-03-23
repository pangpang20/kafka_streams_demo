package com.kafka.consumer.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * CDC 数据变更事件模型
 *
 * 与 Producer 端模型保持一致，用于解析接收到的 CDC 数据。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class CdcEvent {

    @JsonProperty("data")
    private DataRecord[] data;

    public DataRecord[] getData() { return data; }
    public void setData(DataRecord[] data) { this.data = data; }

    /**
     * 单条数据变更记录
     */
    public static class DataRecord {
        @JsonProperty("keyfields")
        private String keyfields;

        @JsonProperty("pos")
        private String pos;

        @JsonProperty("databasename")
        private String databasename;

        @JsonProperty("allfields")
        private AllFields allfields;

        @JsonProperty("tablename")
        private String tablename;

        @JsonProperty("opcode")
        private String opcode;

        @JsonProperty("type")
        private String type;

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
     * 字段数据容器
     */
    public static class AllFields {
        @JsonProperty("after_data")
        private Map<String, FieldData> afterData;

        @JsonProperty("before_data")
        private Map<String, FieldData> beforeData;

        public Map<String, FieldData> getAfterData() { return afterData; }
        public void setAfterData(Map<String, FieldData> afterData) { this.afterData = afterData; }

        public Map<String, FieldData> getBeforeData() { return beforeData; }
        public void setBeforeData(Map<String, FieldData> beforeData) { this.beforeData = beforeData; }
    }

    /**
     * 单个字段数据
     */
    public static class FieldData {
        @JsonProperty("type")
        private String type;

        @JsonProperty("value")
        private Object value;

        public FieldData() {}

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
    }
}
