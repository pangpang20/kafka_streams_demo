package com.kafka.consumer.model;

/**
 * 处理结果
 *
 * 封装数据处理的结果
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProcessingResult {

    private boolean success;
    private String message;
    private String recordKey;
    private String recordTimestamp;
    private String databaseName;
    private String tableName;
    private String opcode;
    private CdcEvent.DataRecord record;
    private String rawJson;
    private ValidationSummary validationSummary;

    public ProcessingResult() {
        this.success = true;
    }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getRecordKey() { return recordKey; }
    public void setRecordKey(String recordKey) { this.recordKey = recordKey; }

    public String getRecordTimestamp() { return recordTimestamp; }
    public void setRecordTimestamp(String recordTimestamp) { this.recordTimestamp = recordTimestamp; }

    public String getDatabaseName() { return databaseName; }
    public void setDatabaseName(String databaseName) { this.databaseName = databaseName; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getOpcode() { return opcode; }
    public void setOpcode(String opcode) { this.opcode = opcode; }

    public CdcEvent.DataRecord getRecord() { return record; }
    public void setRecord(CdcEvent.DataRecord record) { this.record = record; }

    public String getRawJson() { return rawJson; }
    public void setRawJson(String rawJson) { this.rawJson = rawJson; }

    public ValidationSummary getValidationSummary() { return validationSummary; }
    public void setValidationSummary(ValidationSummary validationSummary) {
        this.validationSummary = validationSummary;
    }

    /**
     * 验证摘要
     */
    public static class ValidationSummary {
        private boolean valid;
        private String errorSummary;
        private String errorFields;
        private String errorDetails;

        public boolean isValid() { return valid; }
        public void setValid(boolean valid) { this.valid = valid; }

        public String getErrorSummary() { return errorSummary; }
        public void setErrorSummary(String errorSummary) { this.errorSummary = errorSummary; }

        public String getErrorFields() { return errorFields; }
        public void setErrorFields(String errorFields) { this.errorFields = errorFields; }

        public String getErrorDetails() { return errorDetails; }
        public void setErrorDetails(String errorDetails) { this.errorDetails = errorDetails; }
    }

    /**
     * 创建成功结果
     */
    public static ProcessingResult success(CdcEvent.DataRecord record, String recordKey, String rawJson) {
        ProcessingResult result = new ProcessingResult();
        result.setSuccess(true);
        result.setMessage("验证通过");
        result.setRecord(record);
        result.setRecordKey(recordKey);
        result.setRawJson(rawJson);

        if (record != null) {
            result.setDatabaseName(record.getDatabasename());
            result.setTableName(record.getTablename());
            result.setOpcode(record.getOpcode());
            result.setRecordTimestamp(record.getTimestamp());
        }

        ValidationSummary summary = new ValidationSummary();
        summary.setValid(true);
        result.setValidationSummary(summary);

        return result;
    }

    /**
     * 创建失败结果
     */
    public static ProcessingResult failure(String message, CdcEvent.DataRecord record,
                                           String recordKey, String rawJson,
                                           String errorSummary, String errorFields, String errorDetails) {
        ProcessingResult result = new ProcessingResult();
        result.setSuccess(false);
        result.setMessage(message);
        result.setRecord(record);
        result.setRecordKey(recordKey);
        result.setRawJson(rawJson);

        if (record != null) {
            result.setDatabaseName(record.getDatabasename());
            result.setTableName(record.getTablename());
            result.setOpcode(record.getOpcode());
            result.setRecordTimestamp(record.getTimestamp());
        }

        ValidationSummary summary = new ValidationSummary();
        summary.setValid(false);
        summary.setErrorSummary(errorSummary);
        summary.setErrorFields(errorFields);
        summary.setErrorDetails(errorDetails);
        result.setValidationSummary(summary);

        return result;
    }
}
