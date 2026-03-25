package com.kafka.consumer.model;

import java.util.HashMap;
import java.util.Map;

/**
 * CDC 事件模型
 *
 * 对应 Kafka 消息中的 CDC 数据格式
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class CdcEvent {

    private DataRecord[] data;

    public CdcEvent() {}

    public CdcEvent(DataRecord[] data) {
        this.data = data;
    }

    public DataRecord[] getData() {
        return data;
    }

    public void setData(DataRecord[] data) {
        this.data = data;
    }

    /**
     * CDC 数据记录
     */
    public static class DataRecord {
        private String databasename;
        private String tablename;
        private String opcode;
        private String type;
        private String timestamp;
        private String pos;
        private String keyfields;
        private AllFields allfields;

        public String getDatabasename() { return databasename; }
        public void setDatabasename(String databasename) { this.databasename = databasename; }

        public String getTablename() { return tablename; }
        public void setTablename(String tablename) { this.tablename = tablename; }

        public String getOpcode() { return opcode; }
        public void setOpcode(String opcode) { this.opcode = opcode; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

        public String getPos() { return pos; }
        public void setPos(String pos) { this.pos = pos; }

        public String getKeyfields() { return keyfields; }
        public void setKeyfields(String keyfields) { this.keyfields = keyfields; }

        public AllFields getAllfields() { return allfields; }
        public void setAllfields(AllFields allfields) { this.allfields = allfields; }
    }

    /**
     * 所有字段数据
     */
    public static class AllFields {
        private Map<String, FieldData> beforeData;
        private Map<String, FieldData> afterData;

        public Map<String, FieldData> getBeforeData() { return beforeData; }
        public void setBeforeData(Map<String, FieldData> beforeData) { this.beforeData = beforeData; }

        public Map<String, FieldData> getAfterData() { return afterData; }
        public void setAfterData(Map<String, FieldData> afterData) { this.afterData = afterData; }
    }

    /**
     * 字段数据
     */
    public static class FieldData {
        private String type;
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
}
