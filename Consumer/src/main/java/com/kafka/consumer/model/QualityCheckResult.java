package com.kafka.consumer.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据质量检查结果模型
 *
 * 用于记录和传递数据质量检查的结果，包括是否通过、错误信息列表等。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class QualityCheckResult {

    /**
     * 原始数据记录
     */
    private CdcEvent.DataRecord record;

    /**
     * 是否通过所有检查
     */
    private boolean passed;

    /**
     * 错误信息列表
     */
    private List<ValidationError> errors;

    /**
     * 检查时间
     */
    private String checkTime;

    /**
     * 记录键
     */
    private String recordKey;

    /**
     * 验证错误详情
     */
    public static class ValidationError {
        /**
         * 字段名
         */
        private String field;

        /**
         * 错误类型
         */
        private String errorType;

        /**
         * 错误消息
         */
        private String message;

        /**
         * 实际值
         */
        private Object actualValue;

        /**
         * 期望值/规则
         */
        private String expectedRule;

        public ValidationError() {}

        public ValidationError(String field, String errorType, String message, Object actualValue, String expectedRule) {
            this.field = field;
            this.errorType = errorType;
            this.message = message;
            this.actualValue = actualValue;
            this.expectedRule = expectedRule;
        }

        // Getters and Setters
        public String getField() { return field; }
        public void setField(String field) { this.field = field; }

        public String getErrorType() { return errorType; }
        public void setErrorType(String errorType) { this.errorType = errorType; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public Object getActualValue() { return actualValue; }
        public void setActualValue(Object actualValue) { this.actualValue = actualValue; }

        public String getExpectedRule() { return expectedRule; }
        public void setExpectedRule(String expectedRule) { this.expectedRule = expectedRule; }

        @Override
        public String toString() {
            return String.format("[%s] %s: %s (实际值：%s, 期望：%s)",
                    field, errorType, message, actualValue, expectedRule);
        }
    }

    public QualityCheckResult() {
        this.errors = new ArrayList<>();
        this.passed = true;
        this.checkTime = LocalDateTime.now().toString();
    }

    public QualityCheckResult(CdcEvent.DataRecord record, String recordKey) {
        this();
        this.record = record;
        this.recordKey = recordKey;
    }

    /**
     * 添加验证错误
     */
    public void addError(String field, String errorType, String message, Object actualValue, String expectedRule) {
        this.passed = false;
        this.errors.add(new ValidationError(field, errorType, message, actualValue, expectedRule));
    }

    /**
     * 添加错误（简化版）
     */
    public void addError(String field, String message) {
        this.passed = false;
        this.errors.add(new ValidationError(field, "VALIDATION_ERROR", message, null, null));
    }

    // Getters and Setters
    public CdcEvent.DataRecord getRecord() { return record; }
    public void setRecord(CdcEvent.DataRecord record) { this.record = record; }

    public boolean isPassed() { return passed; }
    public void setPassed(boolean passed) { this.passed = passed; }

    public List<ValidationError> getErrors() { return errors; }
    public void setErrors(List<ValidationError> errors) { this.errors = errors; }

    public String getCheckTime() { return checkTime; }
    public void setCheckTime(String checkTime) { this.checkTime = checkTime; }

    public String getRecordKey() { return recordKey; }
    public void setRecordKey(String recordKey) { this.recordKey = recordKey; }

    @Override
    public String toString() {
        return String.format("QualityCheckResult{passed=%s, errors=%d, key=%s}",
                passed, errors.size(), recordKey);
    }
}
