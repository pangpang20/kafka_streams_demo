package com.kafka.consumer.processor;

import com.kafka.consumer.model.CdcEvent;
import com.kafka.consumer.model.QualityCheckResult;

/**
 * 数据处理结果
 *
 * 封装数据处理后的结果，包括处理后的数据和状态信息。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProcessingResult {

    /**
     * 原始 CDC 记录
     */
    private CdcEvent.DataRecord record;

    /**
     * 记录键
     */
    private String recordKey;

    /**
     * 质量检查结果
     */
    private QualityCheckResult qualityResult;

    /**
     * 是否通过处理
     */
    private boolean success;

    /**
     * 处理消息
     */
    private String message;

    /**
     * 原始 JSON 数据
     */
    private String rawJson;

    public ProcessingResult() {
        this.success = true;
    }

    public ProcessingResult(CdcEvent.DataRecord record, String recordKey, QualityCheckResult qualityResult) {
        this.record = record;
        this.recordKey = recordKey;
        this.qualityResult = qualityResult;
        this.success = qualityResult != null && qualityResult.isPassed();
    }

    // Getters and Setters
    public CdcEvent.DataRecord getRecord() { return record; }
    public void setRecord(CdcEvent.DataRecord record) { this.record = record; }

    public String getRecordKey() { return recordKey; }
    public void setRecordKey(String recordKey) { this.recordKey = recordKey; }

    public QualityCheckResult getQualityResult() { return qualityResult; }
    public void setQualityResult(QualityCheckResult qualityResult) { this.qualityResult = qualityResult; }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getRawJson() { return rawJson; }
    public void setRawJson(String rawJson) { this.rawJson = rawJson; }
}
