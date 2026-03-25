package com.kafka.consumer.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.config.ValidationRuleConfigLoader;
import com.kafka.consumer.model.CdcEvent;
import com.kafka.consumer.model.ProcessingResult;
import com.kafka.consumer.validator.ConfigurableValidator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 可配置数据处理器
 *
 * 处理流程：认证 -> 解析 -> 验证
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableDataProcessor {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableDataProcessor.class);

    private final ObjectMapper objectMapper;
    private final String tableName;
    private final ConfigurableValidator validator;

    /**
     * 构造函数
     *
     * @param tableName 表名
     * @param ruleConfigLoader 验证规则配置加载器
     */
    public ConfigurableDataProcessor(String tableName, ValidationRuleConfigLoader ruleConfigLoader) {
        this.objectMapper = new ObjectMapper();
        this.tableName = tableName;
        this.validator = new ConfigurableValidator(ruleConfigLoader, tableName);
        log.info("ConfigurableDataProcessor 初始化完成，表名：{}", tableName);
    }

    /**
     * 处理单条消息
     *
     * @param key Kafka 消息 key
     * @param value Kafka 消息 value (JSON 字符串)
     * @return 处理结果
     */
    public ProcessingResult process(String key, String value) {
        try {
            // 1. 解析 CDC 数据
            CdcEvent event = parseCdcEvent(value);
            if (event == null || event.getData() == null || event.getData().length == 0) {
                return createFailureResult("CDC 数据解析失败", null, key, value,
                        "JSON 解析失败", "structure", null);
            }

            // 2. 获取第一条记录处理
            CdcEvent.DataRecord record = event.getData()[0];

            // 3. 认证检查
            if (!authenticate(record)) {
                return createFailureResult("认证失败", record, key, value,
                        "数据源认证未通过", "auth", null);
            }

            // 4. 检查是否是配置的表
            if (!tableName.equals(record.getTablename())) {
                log.debug("表名不匹配，跳过：{} != {}", tableName, record.getTablename());
                // 表名不匹配，跳过但不报错
                return null;
            }

            // 5. 结构检查
            if (record.getAllfields() == null || record.getAllfields().getAfterData() == null) {
                return createFailureResult("数据结构异常", record, key, value,
                        "缺少 allfields 或 after_data", "structure", null);
            }

            // 6. 提取字段值进行验证
            Map<String, CdcEvent.FieldData> afterData = record.getAllfields().getAfterData();
            Map<String, Object> fieldValues = extractFieldValues(afterData);

            // 7. 执行验证
            ConfigurableValidator.ValidationResult validationResult =
                    validator.validateFields(fieldValues);

            if (!validationResult.isValid()) {
                List<String> errors = validationResult.getErrors();
                String errorFields = extractErrorFields(errors);
                String errorSummary = validationResult.getErrorSummary();

                return createFailureResult("数据质量检查失败", record, key, value,
                        errorSummary, errorFields, errorSummary);
            }

            // 8. 验证通过
            return ProcessingResult.success(record, key, value);

        } catch (Exception e) {
            log.error("处理异常：{}", e.getMessage(), e);
            return createFailureResult("处理异常：" + e.getMessage(), null, key, value,
                    e.getMessage(), "system", null);
        }
    }

    /**
     * 解析 CDC 事件
     */
    private CdcEvent parseCdcEvent(String json) throws Exception {
        return objectMapper.readValue(json, CdcEvent.class);
    }

    /**
     * 认证检查
     */
    private boolean authenticate(CdcEvent.DataRecord record) {
        if (StringUtils.isBlank(record.getDatabasename())) {
            return false;
        }
        if (StringUtils.isBlank(record.getTablename())) {
            return false;
        }
        if (StringUtils.isBlank(record.getOpcode())) {
            return false;
        }

        String opcode = record.getOpcode();
        return "I".equals(opcode) || "U".equals(opcode) || "D".equals(opcode);
    }

    /**
     * 提取字段值
     */
    private Map<String, Object> extractFieldValues(Map<String, CdcEvent.FieldData> afterData) {
        Map<String, Object> result = new java.util.HashMap<>();
        for (Map.Entry<String, CdcEvent.FieldData> entry : afterData.entrySet()) {
            if (entry.getValue() != null && entry.getValue().getValue() != null) {
                result.put(entry.getKey(), entry.getValue().getValue());
            }
        }
        return result;
    }

    /**
     * 从错误列表中提取错误字段
     */
    private String extractErrorFields(List<String> errors) {
        List<String> fields = new ArrayList<>();
        for (String error : errors) {
            if (error.startsWith("[")) {
                int endIdx = error.indexOf("]");
                if (endIdx > 0) {
                    fields.add(error.substring(1, endIdx));
                }
            }
        }
        return String.join(",", fields);
    }

    /**
     * 创建失败结果
     */
    private ProcessingResult createFailureResult(String message, CdcEvent.DataRecord record,
                                                  String key, String rawJson,
                                                  String errorSummary, String errorFields,
                                                  String errorDetails) {
        return ProcessingResult.failure(message, record, key, rawJson,
                errorSummary, errorFields, errorDetails);
    }

    /**
     * 获取验证器
     */
    public ConfigurableValidator getValidator() {
        return validator;
    }
}
