package com.kafka.consumer.validator;

import com.kafka.consumer.config.ValidationRuleConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 可配置数据验证器
 *
 * 基于配置文件中的规则进行数据验证
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableValidator {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableValidator.class);

    private final ValidationRuleConfigLoader ruleConfigLoader;
    private final String tableName;

    // 预编译的正则表达式缓存
    private final java.util.Map<String, Pattern> patternCache = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 验证结果
     */
    public static class ValidationResult {
        private boolean valid;
        private List<String> errors;

        public ValidationResult() {
            this.valid = true;
            this.errors = new ArrayList<>();
        }

        public boolean isValid() { return valid; }
        public void setValid(boolean valid) { this.valid = valid; }
        public List<String> getErrors() { return errors; }
        public void setErrors(List<String> errors) { this.errors = errors; }

        public void addError(String error) {
            this.valid = false;
            this.errors.add(error);
        }

        public String getErrorSummary() {
            if (errors.isEmpty()) {
                return "";
            }
            return String.join("; ", errors);
        }
    }

    /**
     * 构造函数
     *
     * @param ruleConfigLoader 验证规则配置加载器
     * @param tableName 表名
     */
    public ConfigurableValidator(ValidationRuleConfigLoader ruleConfigLoader, String tableName) {
        this.ruleConfigLoader = ruleConfigLoader;
        this.tableName = tableName;
        log.info("ConfigurableValidator 初始化完成，表名：{}", tableName);
    }

    /**
     * 验证字段值
     *
     * @param fieldName 字段名
     * @param value 字段值
     * @return 验证结果
     */
    public ValidationResult validateField(String fieldName, Object value) {
        ValidationResult result = new ValidationResult();

        // 如果值为 null，跳过验证（允许空值）
        if (value == null || value.toString().trim().isEmpty()) {
            return result;
        }

        // 获取该字段的验证规则
        ValidationRuleConfigLoader.ValidationRule rule =
            ruleConfigLoader.getRuleByField(tableName, fieldName);

        if (rule == null) {
            // 没有配置规则，验证通过
            return result;
        }

        // 根据规则类型进行验证
        switch (rule.getType()) {
            case "enum":
                validateEnum(fieldName, value, rule, result);
                break;
            case "range":
                validateRange(fieldName, value, rule, result);
                break;
            case "regex":
                validateRegex(fieldName, value, rule, result);
                break;
            case "length":
                validateLength(fieldName, value, rule, result);
                break;
            default:
                log.warn("未知的验证规则类型：{}，字段：{}", rule.getType(), fieldName);
        }

        return result;
    }

    /**
     * 验证枚举值
     */
    private void validateEnum(String fieldName, Object value,
                              ValidationRuleConfigLoader.ValidationRule rule,
                              ValidationResult result) {
        String strValue = value.toString().trim();
        List<String> allowedValues = rule.getValues();

        if (allowedValues == null || allowedValues.isEmpty()) {
            log.warn("枚举规则配置错误，字段 {} 没有配置允许值", fieldName);
            return;
        }

        if (!allowedValues.contains(strValue)) {
            result.addError(String.format(
                "[%s] 值'%s'不在允许范围内，允许值：%s",
                fieldName, strValue, String.join(",", allowedValues)
            ));
        }
    }

    /**
     * 验证范围
     */
    private void validateRange(String fieldName, Object value,
                               ValidationRuleConfigLoader.ValidationRule rule,
                               ValidationResult result) {
        try {
            double numValue;
            if (value instanceof Number) {
                numValue = ((Number) value).doubleValue();
            } else {
                numValue = Double.parseDouble(value.toString());
            }

            Double minValue = rule.getMinValue();
            Double maxValue = rule.getMaxValue();

            if (minValue != null && numValue < minValue) {
                result.addError(String.format(
                    "[%s] 值%.2f 小于最小值%.2f",
                    fieldName, numValue, minValue
                ));
            }

            if (maxValue != null && numValue > maxValue) {
                result.addError(String.format(
                    "[%s] 值%.2f 大于最大值%.2f",
                    fieldName, numValue, maxValue
                ));
            }
        } catch (NumberFormatException e) {
            result.addError(String.format(
                "[%s] 值必须是数字类型：%s",
                fieldName, value
            ));
        }
    }

    /**
     * 验证正则表达式
     */
    private void validateRegex(String fieldName, Object value,
                               ValidationRuleConfigLoader.ValidationRule rule,
                               ValidationResult result) {
        String pattern = rule.getPattern();
        if (pattern == null || pattern.isEmpty()) {
            log.warn("正则规则配置错误，字段 {} 没有配置 pattern", fieldName);
            return;
        }

        // 从缓存获取或编译正则
        Pattern compiledPattern = patternCache.computeIfAbsent(
            fieldName + ":" + pattern, Pattern::compile
        );

        String strValue = value.toString().trim();
        if (!compiledPattern.matcher(strValue).matches()) {
            result.addError(String.format(
                "[%s] 值'%s'不符合格式要求：%s",
                fieldName, strValue, rule.getDescription()
            ));
        }
    }

    /**
     * 验证长度
     */
    private void validateLength(String fieldName, Object value,
                                ValidationRuleConfigLoader.ValidationRule rule,
                                ValidationResult result) {
        String strValue = value.toString();
        int length = strValue.length();

        Integer minLength = rule.getMinLength();
        Integer maxLength = rule.getMaxLength();

        if (minLength != null && length < minLength) {
            result.addError(String.format(
                "[%s] 长度%d 小于最小长度%d",
                fieldName, length, minLength
            ));
        }

        if (maxLength != null && length > maxLength) {
            result.addError(String.format(
                "[%s] 长度%d 大于最大长度%d",
                fieldName, length, maxLength
            ));
        }
    }

    /**
     * 验证多个字段
     *
     * @param fieldValues 字段名 -> 字段值的映射
     * @return 验证结果
     */
    public ValidationResult validateFields(java.util.Map<String, Object> fieldValues) {
        ValidationResult overallResult = new ValidationResult();

        for (java.util.Map.Entry<String, Object> entry : fieldValues.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            ValidationResult fieldResult = validateField(fieldName, value);
            if (!fieldResult.isValid()) {
                overallResult.getErrors().addAll(fieldResult.getErrors());
                overallResult.setValid(false);
            }
        }

        return overallResult;
    }

    /**
     * 获取所有启用的规则
     *
     * @return 验证规则列表
     */
    public List<ValidationRuleConfigLoader.ValidationRule> getAllRules() {
        return ruleConfigLoader.getEnabledRulesForTable(tableName);
    }

    /**
     * 打印验证规则
     */
    public void printRules() {
        System.out.println("=====================================");
        System.out.println("表 [" + tableName + "] 验证规则");
        System.out.println("=====================================");
        List<ValidationRuleConfigLoader.ValidationRule> rules = getAllRules();
        if (rules != null && !rules.isEmpty()) {
            for (ValidationRuleConfigLoader.ValidationRule rule : rules) {
                System.out.println(rule.getField() + " - " + rule.getType() +
                        ": " + rule.getDescription());
            }
        } else {
            System.out.println("未配置验证规则");
        }
        System.out.println("=====================================");
    }
}
