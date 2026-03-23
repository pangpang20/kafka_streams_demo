package com.kafka.consumer.processor;

import com.kafka.consumer.model.CdcEvent;
import com.kafka.consumer.model.QualityCheckResult;
import com.kafka.consumer.validator.DataValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 数据结构化处理器
 *
 * 实现结构化的数据处理流程，按照以下顺序处理：
 * 1. 登录认证 (Authentication) - 验证数据源合法性
 * 2. 字段解析 (Parsing) - 解析 JSON 数据为对象
 * 3. 质量检查 (Validation) - 执行数据质量验证规则
 *
 * 这种分层处理模式便于：
 * - 代码维护和扩展
 * - 问题定位和调试
 * - 新人理解和上手
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class DataProcessor {

    private static final Logger log = LoggerFactory.getLogger(DataProcessor.class);

    /**
     * JSON 解析器
     */
    private final ObjectMapper objectMapper;

    /**
     * 数据验证器
     */
    private final DataValidator validator;

    /**
     * 构造函数
     */
    public DataProcessor() {
        this.objectMapper = new ObjectMapper();
        this.validator = new DataValidator();
        log.info("DataProcessor 初始化完成");
    }

    /**
     * 处理单条数据
     *
     * 这是主要的数据处理方法，按照预定义的流程处理数据：
     * 1. 认证检查
     * 2. 字段解析
     * 3. 质量验证
     *
     * @param key Kafka 消息的键
     * @param jsonValue Kafka 消息的 JSON 值
     * @return 处理结果
     */
    public ProcessingResult process(String key, String jsonValue) {
        ProcessingResult result = new ProcessingResult();
        result.setRecordKey(key);

        try {
            // ============================================
            // 步骤 1: 登录认证 + 字段解析
            // ============================================
            // 将 JSON 字符串解析为 CDC 事件对象
            // 同时完成基本的数据认证（格式检查、必需字段检查）
            CdcEvent event = parseJsonToCdcEvent(jsonValue);

            if (event == null || event.getData() == null || event.getData().length == 0) {
                result.setSuccess(false);
                result.setMessage("数据解析失败：空数据或格式错误");
                log.warn("处理失败 [key={}]: {}", key, result.getMessage());
                return result;
            }

            // 获取第一条数据记录
            CdcEvent.DataRecord record = event.getData()[0];
            result.setRecord(record);

            // ============================================
            // 步骤 2: 数据质量检查
            // ============================================
            // 执行业务规则验证
            QualityCheckResult qualityResult = validator.validate(record, key);
            result.setQualityResult(qualityResult);

            // ============================================
            // 步骤 3: 记录处理结果
            // ============================================
            if (qualityResult.isPassed()) {
                result.setSuccess(true);
                result.setMessage("数据质量检查通过");
                log.debug("处理成功 [key={}, opcode={}]: 数据质量检查通过",
                        key, record.getOpcode());
            } else {
                result.setSuccess(false);
                result.setMessage(formatValidationErrors(qualityResult));
                log.info("处理失败 [key={}, opcode={}]: {}",
                        key, record.getOpcode(), result.getMessage());
            }

        } catch (Exception e) {
            // 捕获所有异常，避免单个消息处理失败影响整个流
            result.setSuccess(false);
            result.setMessage("处理异常：" + e.getMessage());
            log.error("处理异常 [key={}]: {}", key, e.getMessage(), e);
        }

        return result;
    }

    /**
     * 将 JSON 字符串解析为 CDC 事件对象
     *
     * @param json JSON 字符串
     * @return CDC 事件对象
     * @throws Exception 解析异常
     */
    private CdcEvent parseJsonToCdcEvent(String json) throws Exception {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        return objectMapper.readValue(json, CdcEvent.class);
    }

    /**
     * 格式化验证错误信息
     *
     * @param result 质量检查结果
     * @return 格式化的错误信息字符串
     */
    private String formatValidationErrors(QualityCheckResult result) {
        StringBuilder sb = new StringBuilder("数据质量检查失败：");

        for (QualityCheckResult.ValidationError error : result.getErrors()) {
            sb.append(String.format("[%s] %s; ", error.getField(), error.getMessage()));
        }

        return sb.toString();
    }

    /**
     * 获取验证器实例（用于扩展）
     *
     * @return DataValidator 实例
     */
    public DataValidator getValidator() {
        return validator;
    }
}
