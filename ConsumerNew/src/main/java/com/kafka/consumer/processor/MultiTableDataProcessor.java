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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 多表数据处理器
 *
 * 支持处理多个表的数据，根据消息中的 tablename 动态路由到对应的验证器
 * 表结构：从 table-schema.yaml 加载
 * 验证规则：从 validation-rules.yaml 加载（支持动态刷新）
 *
 * @author Kafka Demo
 * @version 2.0.0
 */
public class MultiTableDataProcessor {

    private static final Logger log = LoggerFactory.getLogger(MultiTableDataProcessor.class);

    private final ObjectMapper objectMapper;
    private final ValidationRuleConfigLoader ruleConfigLoader;

    // 表名 -> 验证器缓存（按需创建）
    private final Map<String, ConfigurableValidator> validatorCache = new ConcurrentHashMap<>();

    // 已知的表配置（从 schema 配置加载）
    private final Map<String, Boolean> knownTables = new ConcurrentHashMap<>();

    /**
     * 构造函数
     *
     * @param ruleConfigLoader 验证规则配置加载器
     */
    public MultiTableDataProcessor(ValidationRuleConfigLoader ruleConfigLoader) {
        this.objectMapper = new ObjectMapper();
        this.ruleConfigLoader = ruleConfigLoader;
        log.info("MultiTableDataProcessor 初始化完成，支持多表处理");
    }

    /**
     * 注册已知表（启动时调用）
     */
    public void registerTables(List<String> tableNames) {
        for (String tableName : tableNames) {
            knownTables.put(tableName, true);
            // 预创建验证器
            getValidator(tableName);
        }
        log.info("已注册 {} 个表：{}", knownTables.size(), String.join(",", knownTables.keySet()));
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

            // 4. 获取表名（动态识别）
            String tableName = record.getTablename();
            if (StringUtils.isBlank(tableName)) {
                return createFailureResult("认证失败", record, key, value,
                        "表名为空", "auth", null);
            }

            // 5. 检查是否是已知表（可选：如果希望只处理配置的表）
            // 如果不检查，可以自动处理任何表（只要有规则配置）
            if (!knownTables.containsKey(tableName)) {
                // 新表自动注册（按需处理）
                log.info("检测到新表：{}, 自动注册并创建验证器", tableName);
                knownTables.put(tableName, true);
            }

            // 6. 结构检查
            if (record.getAllfields() == null || record.getAllfields().getAfterData() == null) {
                return createFailureResult("数据结构异常", record, key, value,
                        "缺少 allfields 或 after_data", "structure", null);
            }

            // 7. 检查是否跳过验证（配置了 skip_validation: true 的表）
            if (ruleConfigLoader.shouldSkipValidation(tableName)) {
                log.info("[跳过验证] 表={}, 直接通过", tableName);
                return ProcessingResult.success(record, key, value);
            }

            // 8. 获取表对应的验证器（动态创建或从缓存获取）
            ConfigurableValidator validator = getValidator(tableName);

            // 9. 提取字段值进行验证
            Map<String, CdcEvent.FieldData> afterData = record.getAllfields().getAfterData();
            Map<String, Object> fieldValues = extractFieldValues(afterData);

            // 10. 执行验证
            ConfigurableValidator.ValidationResult validationResult =
                    validator.validateFields(fieldValues);

            if (!validationResult.isValid()) {
                List<String> errors = validationResult.getErrors();
                String errorFields = extractErrorFields(errors);
                String errorSummary = validationResult.getErrorSummary();

                return createFailureResult("数据质量检查失败", record, key, value,
                        errorSummary, errorFields, errorSummary);
            }

            // 11. 验证通过
            return ProcessingResult.success(record, key, value);

        } catch (Exception e) {
            log.error("处理异常：{}", e.getMessage(), e);
            return createFailureResult("处理异常：" + e.getMessage(), null, key, value,
                    e.getMessage(), "system", null);
        }
    }

    /**
     * 获取或创建表的验证器
     */
    private ConfigurableValidator getValidator(String tableName) {
        return validatorCache.computeIfAbsent(tableName, name -> {
            log.info("为表 [{}] 创建验证器", name);
            return new ConfigurableValidator(ruleConfigLoader, name);
        });
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
     * 获取所有已知的表
     */
    public List<String> getKnownTables() {
        return new ArrayList<>(knownTables.keySet());
    }

    /**
     * 获取验证器缓存大小
     */
    public int getValidatorCacheSize() {
        return validatorCache.size();
    }

    /**
     * 清除验证器缓存（用于重新加载）
     */
    public void clearValidatorCache() {
        validatorCache.clear();
        log.info("验证器缓存已清除，下次处理时会重新创建");
    }
}
