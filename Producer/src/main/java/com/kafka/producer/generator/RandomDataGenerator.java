package com.kafka.producer.generator;

import com.kafka.producer.model.CdcEvent;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 随机数据生成器
 *
 * 用于生成模拟的 baseinfo 表数据，支持生成正常数据和测试用的问题数据。
 * 问题数据用于验证 Kafka Streams 数据质量检查功能。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class RandomDataGenerator {

    private static final Random RANDOM = new Random();
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 测试用姓氏
    private static final String[] SURNAMES = {
        "张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴",
        "徐", "孙", "朱", "马", "胡", "郭", "何", "高", "林", "罗"
    };

    // 测试用名字
    private static final String[] NAMES = {
        "伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋",
        "勇", "艳", "杰", "涛", "明", "超", "秀英", "刚", "平", "辉"
    };

    // 省份地址
    private static final String[] ADDRESSES = {
        "北京", "上海", "广东", "江苏", "浙江", "山东", "河南", "河北",
        "湖北", "湖南", "四川", "重庆", "福建", "安徽", "辽宁", "陕西"
    };

    // 血型
    private static final String[] BLOOD_TYPES = {"A", "B", "O", "AB"};

    // 身份证前缀（省份）
    private static final String[] ID_PREFIXES = {
        "110", "310", "440", "320", "330", "370", "410", "130",
        "420", "430", "510", "500", "350", "340", "210", "610"
    };

    /**
     * 生成随机的 baseinfo 表数据记录
     *
     * @param opcode 操作类型：I=插入，D=删除，U=更新
     * @param personId 人员 ID（更新和删除时需要）
     * @param existingData 已有数据（更新时作为 before_data）
     * @param includeBadData 是否包含问题数据（用于测试质量检查）
     * @return CDC 数据记录
     */
    public static CdcEvent.DataRecord generateBaseinfoRecord(
            String opcode,
            Long personId,
            Map<String, Object> existingData,
            boolean includeBadData) {

        CdcEvent.DataRecord record = new CdcEvent.DataRecord();

        // 基本信息
        record.setDatabasename("test");
        record.setTablename("baseinfo");
        record.setOpcode(opcode);
        record.setType("mysql");
        record.setTimestamp(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")));
        record.setPos("bin." + String.format("%06d", RANDOM.nextInt(9999)) + "," + System.nanoTime());
        record.setKeyfields("idcard");

        // 生成字段数据
        Map<String, CdcEvent.FieldData> afterData = new HashMap<>();
        Map<String, CdcEvent.FieldData> beforeData = new HashMap<>();

        // 生成或获取人员 ID
        long currentPersonId = (personId != null) ? personId : (RANDOM.nextInt(999999) + 1L);

        // 生成身份证号（作为主键）
        String idcard = existingData != null ? (String) existingData.get("idcard") : generateIdCard(currentPersonId);

        // 根据操作类型填充数据
        if ("I".equals(opcode)) {
            // 插入操作：只需要 after_data
            afterData = generateAfterData(currentPersonId, idcard, includeBadData);
        } else if ("U".equals(opcode)) {
            // 更新操作：需要 before_data 和 after_data
            beforeData = convertToFieldData(existingData);
            afterData = generateAfterDataWithChanges(currentPersonId, idcard, existingData, includeBadData);
        } else if ("D".equals(opcode)) {
            // 删除操作：只需要 before_data（这里放在 after_data 中方便处理）
            afterData = convertToFieldData(existingData);
        }

        // 构建 allfields
        CdcEvent.AllFields allFields = new CdcEvent.AllFields();
        allFields.setAfterData(afterData);
        allFields.setBeforeData(beforeData);

        record.setAllfields(allFields);

        return record;
    }

    /**
     * 生成 after_data 数据
     */
    private static Map<String, CdcEvent.FieldData> generateAfterData(
            long personId, String idcard, boolean includeBadData) {

        Map<String, CdcEvent.FieldData> data = new HashMap<>();

        // personid - 人员 ID
        data.put("personid", new CdcEvent.FieldData("null:INT64", personId));

        // idcard - 身份证号（主键）
        data.put("idcard", new CdcEvent.FieldData("null:STRING", idcard));

        // name - 姓名
        String name = generateName();
        data.put("name", new CdcEvent.FieldData("null:STRING", name));

        // sex - 性别（可能生成问题数据）
        String sex = includeBadData && RANDOM.nextInt(100) < 10
                ? getRandomBadSex()  // 10% 概率生成错误性别
                : (RANDOM.nextInt(2) == 0 ? "男" : "女");
        data.put("sex", new CdcEvent.FieldData("null:STRING", sex));

        // age - 年龄（可能生成问题数据）
        Integer age = includeBadData && RANDOM.nextInt(100) < 10
                ? getRandomBadAge()  // 10% 概率生成错误年龄
                : (RANDOM.nextInt(80) + 18);
        data.put("age", new CdcEvent.FieldData("null:INT32", age));

        // birthdate - 出生日期
        data.put("birthdate", new CdcEvent.FieldData("io.debezium.time.Timestamp:INT64",
                generateBirthdateString()));

        // birthplace - 出生地
        data.put("birthplace", new CdcEvent.FieldData("null:STRING",
                ADDRESSES[RANDOM.nextInt(ADDRESSES.length)]));

        // address - 现住址
        data.put("address", new CdcEvent.FieldData("null:STRING",
                ADDRESSES[RANDOM.nextInt(ADDRESSES.length)]));

        // telephone - 电话号码（可能生成问题数据）
        String telephone = includeBadData && RANDOM.nextInt(100) < 10
                ? generateBadPhone()  // 10% 概率生成错误电话
                : generatePhone();
        data.put("telephone", new CdcEvent.FieldData("null:STRING", telephone));

        // regdatetime - 注册时间
        data.put("regdatetime", new CdcEvent.FieldData("io.debezium.time.Timestamp:INT64",
                System.currentTimeMillis() - RANDOM.nextInt(31536000000L))); // 过去一年内

        // updatetime - 更新时间
        data.put("updatetime", new CdcEvent.FieldData("io.debezium.time.ZonedTimestamp:STRING",
                LocalDateTime.now().format(DATETIME_FORMATTER)));

        // citizen - 是否公民
        data.put("citizen", new CdcEvent.FieldData("org.apache.kafka.connect.data.Decimal:BYTES",
                new BigDecimal("1")));

        // creditscore - 信用分（可能生成问题数据）
        Double creditScore = includeBadData && RANDOM.nextInt(100) < 10
                ? generateBadCreditScore()  // 10% 概率生成错误信用分
                : (RANDOM.nextDouble() * 400 + 300); // 300-700
        data.put("creditscore", new CdcEvent.FieldData("null:FLOAT64", creditScore));

        // photo - 照片（二进制数据）
        data.put("photo", new CdcEvent.FieldData("null:BYTES", ""));

        // bloodtype - 血型（可能生成问题数据）
        String bloodType = includeBadData && RANDOM.nextInt(100) < 10
                ? getRandomBadBloodType()  // 10% 概率生成错误血型
                : BLOOD_TYPES[RANDOM.nextInt(BLOOD_TYPES.length)];
        data.put("bloodtype", new CdcEvent.FieldData("null:STRING", bloodType));

        // housingareas - 住房面积（可能生成问题数据）
        BigDecimal housingArea = includeBadData && RANDOM.nextInt(100) < 10
                ? new BigDecimal("-100")  // 10% 概率生成负数
                : new BigDecimal(RANDOM.nextInt(300) + 50);
        data.put("housingareas", new CdcEvent.FieldData("org.apache.kafka.connect.data.Decimal:BYTES",
                housingArea));

        return data;
    }

    /**
     * 基于已有数据生成变更后的数据（用于更新操作）
     */
    private static Map<String, CdcEvent.FieldData> generateAfterDataWithChanges(
            long personId, String idcard, Map<String, Object> existingData, boolean includeBadData) {

        // 复制已有数据
        Map<String, Object> updatedData = new HashMap<>(existingData);

        // 随机更新 1-3 个字段
        String[] updatableFields = {"name", "address", "telephone", "creditscore", "bloodtype"};
        int fieldsToUpdate = RANDOM.nextInt(3) + 1;

        for (int i = 0; i < fieldsToUpdate; i++) {
            String field = updatableFields[RANDOM.nextInt(updatableFields.length)];

            switch (field) {
                case "name":
                    updatedData.put("name", generateName());
                    break;
                case "address":
                    updatedData.put("address", ADDRESSES[RANDOM.nextInt(ADDRESSES.length)]);
                    break;
                case "telephone":
                    updatedData.put("telephone", generatePhone());
                    break;
                case "creditscore":
                    updatedData.put("creditscore", RANDOM.nextDouble() * 400 + 300);
                    break;
                case "bloodtype":
                    updatedData.put("bloodtype", BLOOD_TYPES[RANDOM.nextInt(BLOOD_TYPES.length)]);
                    break;
            }
        }

        // 更新时间
        updatedData.put("updatetime", LocalDateTime.now().format(DATETIME_FORMATTER));

        return convertToFieldData(updatedData);
    }

    /**
     * 将 Map 数据转换为 FieldData 格式
     */
    @SuppressWarnings("unchecked")
    private static Map<String, CdcEvent.FieldData> convertToFieldData(Map<String, Object> data) {
        Map<String, CdcEvent.FieldData> result = new HashMap<>();

        if (data == null || data.isEmpty()) {
            return result;
        }

        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String type = getTypeForField(key, value);
            result.put(key, new CdcEvent.FieldData(type, value));
        }

        return result;
    }

    /**
     * 根据字段名和值推断类型
     */
    private static String getTypeForField(String fieldName, Object value) {
        if (value == null) {
            return "null:STRING";
        }

        switch (fieldName) {
            case "personid":
                return "null:INT64";
            case "age":
                return "null:INT32";
            case "creditscore":
                return "null:FLOAT64";
            case "citizen":
            case "housingareas":
                return "org.apache.kafka.connect.data.Decimal:BYTES";
            case "birthdate":
            case "regdatetime":
                return "io.debezium.time.Timestamp:INT64";
            case "updatetime":
                return "io.debezium.time.ZonedTimestamp:STRING";
            case "photo":
                return "null:BYTES";
            default:
                return "null:STRING";
        }
    }

    // ==================== 辅助生成方法 ====================

    /**
     * 生成随机姓名
     */
    private static String generateName() {
        return SURNAMES[RANDOM.nextInt(SURNAMES.length)] +
               NAMES[RANDOM.nextInt(NAMES.length)];
    }

    /**
     * 生成身份证号
     */
    private static String generateIdCard(long personId) {
        String prefix = ID_PREFIXES[RANDOM.nextInt(ID_PREFIXES.length)];
        String year = String.format("%04d", 1970 + RANDOM.nextInt(50));
        String month = String.format("%02d", RANDOM.nextInt(12) + 1);
        String day = String.format("%02d", RANDOM.nextInt(28) + 1);
        String suffix = String.format("%04d", personId % 10000);
        return prefix + year + month + day + suffix;
    }

    /**
     * 生成出生日期字符串
     */
    private static String generateBirthdateString() {
        int year = 1970 + RANDOM.nextInt(50);
        int month = RANDOM.nextInt(12) + 1;
        int day = RANDOM.nextInt(28) + 1;
        return year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + " 00:00:00";
    }

    /**
     * 生成正常手机号
     */
    private static String generatePhone() {
        String[] prefixes = {"138", "139", "137", "136", "135", "188", "187", "182", "183"};
        return prefixes[RANDOM.nextInt(prefixes.length)] +
               String.format("%08d", RANDOM.nextInt(99999999));
    }

    /**
     * 生成问题手机号（长度不对或包含非法字符）
     */
    private static String generateBadPhone() {
        return RANDOM.nextBoolean()
                ? "138" + RANDOM.nextInt(999)  // 太短
                : "138abc" + RANDOM.nextInt(99999);  // 包含字母
    }

    /**
     * 生成问题性别
     */
    private static String getRandomBadSex() {
        String[] badSexes = {"M", "F", "未知", "X", "", "male", "female", "1", "0"};
        return badSexes[RANDOM.nextInt(badSexes.length)];
    }

    /**
     * 生成问题年龄
     */
    private static Integer getRandomBadAge() {
        return RANDOM.nextBoolean() ? -5 : 150 + RANDOM.nextInt(50);
    }

    /**
     * 生成问题血型
     */
    private static String getRandomBadBloodType() {
        String[] badBloodTypes = {"A型", "B 型", "C", "Rh", "", "Unknown"};
        return badBloodTypes[RANDOM.nextInt(badBloodTypes.length)];
    }

    /**
     * 生成问题信用分
     */
    private static Double generateBadCreditScore() {
        return RANDOM.nextBoolean() ? -100.0 : 1000.0 + RANDOM.nextDouble() * 500;
    }
}
