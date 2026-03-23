package com.kafka.consumer.validator;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.consumer.model.CdcEvent;
import com.kafka.consumer.model.QualityCheckResult;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * 数据质量验证器
 *
 * 负责对 CDC 数据进行质量检查，包括：
 * - 性别验证：只能是"男"或"女"
 * - 年龄验证：必须在 0-120 之间
 * - 电话验证：必须符合手机号格式
 * - 血型验证：只能是 A/B/O/AB
 * - 信用分验证：必须在 0-1000 之间
 * - 住房面积验证：必须非负
 *
 * 所有验证规则可在 ConsumerConfig 中配置。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class DataValidator {

    /**
     * 电话正则 Pattern（预编译提高性能）
     */
    private static final Pattern TELEPHONE_PATTERN = Pattern.compile(ConsumerConfig.TELEPHONE_REGEX);

    /**
     * 验证单条数据记录
     *
     * @param record CDC 数据记录
     * @param recordKey 记录键
     * @return 验证结果
     */
    public QualityCheckResult validate(CdcEvent.DataRecord record, String recordKey) {
        QualityCheckResult result = new QualityCheckResult(record, recordKey);

        // 1. 登录认证检查（模拟）
        if (!authenticate(record)) {
            result.addError("system", "认证失败", "认证未通过", null, null);
            return result;
        }

        // 2. 字段解析检查
        if (record.getAllfields() == null || record.getAllfields().getAfterData() == null) {
            result.addError("structure", "数据结构异常", "缺少 allfields 或 after_data", null, "必需字段");
            return result;
        }

        // 3. 字段质量检查
        validateSex(result);
        validateAge(result);
        validateTelephone(result);
        validateBloodType(result);
        validateCreditScore(result);
        validateHousingArea(result);

        return result;
    }

    /**
     * 模拟登录认证检查
     *
     * 在实际应用中，这里可以集成真实的认证系统，如：
     * - 检查数据源是否合法
     * - 验证发送者身份
     * - 检查数据签名
     *
     * @param record CDC 数据记录
     * @return 是否通过认证
     */
    private boolean authenticate(CdcEvent.DataRecord record) {
        // 检查必需的元数据字段
        if (StringUtils.isBlank(record.getDatabasename())) {
            return false;
        }
        if (StringUtils.isBlank(record.getTablename())) {
            return false;
        }
        if (StringUtils.isBlank(record.getOpcode())) {
            return false;
        }

        // 检查数据库类型是否合法
        String dbType = record.getType();
        if (!isValidDatabaseType(dbType)) {
            return false;
        }

        // 检查操作类型是否合法
        String opcode = record.getOpcode();
        return "I".equals(opcode) || "U".equals(opcode) || "D".equals(opcode);
    }

    /**
     * 验证数据库类型
     */
    private boolean isValidDatabaseType(String type) {
        return "mysql".equals(type) || "oracle".equals(type) || "sqlserver".equals(type);
    }

    /**
     * 验证性别字段
     *
     * 规则：性别只能是"男"或"女"
     * 常见错误数据：M/F、male/female、未知、空值、1/0 等
     */
    private void validateSex(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_SEX) {
            return;
        }

        Object sexValue = getFieldValue(result.getRecord(), "sex");
        if (sexValue == null) {
            // 允许为空
            return;
        }

        String sex = sexValue.toString().trim();
        if (StringUtils.isBlank(sex)) {
            result.addError("sex", "空值错误", "性别不能为空", sexValue, "男/女");
            return;
        }

        boolean isValid = false;
        for (String validSex : ConsumerConfig.VALID_SEX_VALUES) {
            if (validSex.equals(sex)) {
                isValid = true;
                break;
            }
        }

        if (!isValid) {
            result.addError("sex", "枚举值错误",
                    String.format("性别值'%s'不在允许范围内", sex),
                    sexValue,
                    String.join("/", ConsumerConfig.VALID_SEX_VALUES));
        }
    }

    /**
     * 验证年龄字段
     *
     * 规则：年龄必须在 0-120 之间
     * 常见错误数据：负数、超过 120、非数字等
     */
    private void validateAge(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_AGE) {
            return;
        }

        Object ageValue = getFieldValue(result.getRecord(), "age");
        if (ageValue == null) {
            // 允许为空
            return;
        }

        try {
            int age;
            if (ageValue instanceof Integer) {
                age = (Integer) ageValue;
            } else if (ageValue instanceof Number) {
                age = ((Number) ageValue).intValue();
            } else {
                age = Integer.parseInt(ageValue.toString());
            }

            if (age < ConsumerConfig.MIN_AGE) {
                result.addError("age", "范围错误",
                        String.format("年龄%d小于最小值%d", age, ConsumerConfig.MIN_AGE),
                        ageValue,
                        ">=" + ConsumerConfig.MIN_AGE);
            } else if (age > ConsumerConfig.MAX_AGE) {
                result.addError("age", "范围错误",
                        String.format("年龄%d大于最大值%d", age, ConsumerConfig.MAX_AGE),
                        ageValue,
                        "<=" + ConsumerConfig.MAX_AGE);
            }
        } catch (NumberFormatException e) {
            result.addError("age", "类型错误",
                    "年龄必须是数字",
                    ageValue,
                    "数字类型");
        }
    }

    /**
     * 验证电话号码字段
     *
     * 规则：必须符合中国手机号格式（1 开头，11 位数字）
     * 常见错误数据：长度不对、包含字母、非 1 开头等
     */
    private void validateTelephone(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_TELEPHONE) {
            return;
        }

        Object telValue = getFieldValue(result.getRecord(), "telephone");
        if (telValue == null) {
            // 允许为空
            return;
        }

        String telephone = telValue.toString().trim();
        if (StringUtils.isBlank(telephone)) {
            // 空值允许
            return;
        }

        if (!TELEPHONE_PATTERN.matcher(telephone).matches()) {
            result.addError("telephone", "格式错误",
                    String.format("电话号码'%s'不符合手机号格式", telephone),
                    telephone,
                    ConsumerConfig.TELEPHONE_REGEX);
        }
    }

    /**
     * 验证血型字段
     *
     * 规则：血型只能是 A、B、O、AB
     * 常见错误数据：A 型、B 型、Rh、C 等
     */
    private void validateBloodType(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_BLOOD_TYPE) {
            return;
        }

        Object bloodValue = getFieldValue(result.getRecord(), "bloodtype");
        if (bloodValue == null) {
            // 允许为空
            return;
        }

        String bloodType = bloodValue.toString().trim();
        if (StringUtils.isBlank(bloodType)) {
            // 空值允许
            return;
        }

        boolean isValid = false;
        for (String validType : ConsumerConfig.VALID_BLOOD_TYPES) {
            if (validType.equals(bloodType)) {
                isValid = true;
                break;
            }
        }

        if (!isValid) {
            result.addError("bloodtype", "枚举值错误",
                    String.format("血型'%s'不在允许范围内", bloodType),
                    bloodValue,
                    String.join("/", ConsumerConfig.VALID_BLOOD_TYPES));
        }
    }

    /**
     * 验证信用分字段
     *
     * 规则：信用分必须在 0-1000 之间
     * 常见错误数据：负数、超过 1000、非数字等
     */
    private void validateCreditScore(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_CREDIT_SCORE) {
            return;
        }

        Object scoreValue = getFieldValue(result.getRecord(), "creditscore");
        if (scoreValue == null) {
            // 允许为空
            return;
        }

        try {
            double score;
            if (scoreValue instanceof Double) {
                score = (Double) scoreValue;
            } else if (scoreValue instanceof Number) {
                score = ((Number) scoreValue).doubleValue();
            } else {
                score = Double.parseDouble(scoreValue.toString());
            }

            if (score < ConsumerConfig.MIN_CREDIT_SCORE) {
                result.addError("creditscore", "范围错误",
                        String.format("信用分%.2f 小于最小值%.2f", score, ConsumerConfig.MIN_CREDIT_SCORE),
                        scoreValue,
                        ">=" + ConsumerConfig.MIN_CREDIT_SCORE);
            } else if (score > ConsumerConfig.MAX_CREDIT_SCORE) {
                result.addError("creditscore", "范围错误",
                        String.format("信用分%.2f 大于最大值%.2f", score, ConsumerConfig.MAX_CREDIT_SCORE),
                        scoreValue,
                        "<=" + ConsumerConfig.MAX_CREDIT_SCORE);
            }
        } catch (NumberFormatException e) {
            result.addError("creditscore", "类型错误",
                    "信用分必须是数字",
                    scoreValue,
                    "数字类型");
        }
    }

    /**
     * 验证住房面积字段
     *
     * 规则：住房面积必须非负
     * 常见错误数据：负数
     */
    private void validateHousingArea(QualityCheckResult result) {
        if (!ConsumerConfig.CHECK_HOUSING_AREA) {
            return;
        }

        Object areaValue = getFieldValue(result.getRecord(), "housingareas");
        if (areaValue == null) {
            // 允许为空
            return;
        }

        try {
            double area;
            if (areaValue instanceof Double) {
                area = (Double) areaValue;
            } else if (areaValue instanceof Number) {
                area = ((Number) areaValue).doubleValue();
            } else {
                area = Double.parseDouble(areaValue.toString());
            }

            if (area < ConsumerConfig.MIN_HOUSING_AREA) {
                result.addError("housingareas", "范围错误",
                        String.format("住房面积%.2f 为负数", area),
                        areaValue,
                        ">=" + ConsumerConfig.MIN_HOUSING_AREA);
            }
        } catch (NumberFormatException e) {
            result.addError("housingareas", "类型错误",
                    "住房面积必须是数字",
                    areaValue,
                    "数字类型");
        }
    }

    /**
     * 获取字段值（辅助方法）
     *
     * @param record CDC 记录
     * @param fieldName 字段名
     * @return 字段值
     */
    private Object getFieldValue(CdcEvent.DataRecord record, String fieldName) {
        if (record.getAllfields() == null || record.getAllfields().getAfterData() == null) {
            return null;
        }

        CdcEvent.FieldData fieldData = record.getAllfields().getAfterData().get(fieldName);
        if (fieldData == null) {
            return null;
        }

        return fieldData.getValue();
    }
}
