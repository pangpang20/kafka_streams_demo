package com.kafka.producer.generator;

import com.kafka.producer.config.ProducerTableConfigLoader;
import com.kafka.producer.config.ProducerAppConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可配置数据生成器
 *
 * 根据配置文件中的表结构定义生成数据
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableDataGenerator {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableDataGenerator.class);

    private final ProducerTableConfigLoader tableConfig;
    private final ProducerAppConfigLoader appConfig;
    private final Random random = new Random();
    private final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 当前表名（支持多表）
    private final String currentTableName;

    // 序列生成器
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    // 已生成的数据池（用于更新/删除操作）
    private final Map<String, Map<String, Object>> dataPool = new ConcurrentHashMap<>();

    // 常用数据
    private static final String[] SURNAMES = {"张", "王", "李", "赵", "刘", "陈", "杨", "黄", "周", "吴"};
    private static final String[] NAMES = {"伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "军", "洋"};
    private static final String[] ADDRESSES = {"北京", "上海", "广东", "江苏", "浙江", "山东", "河南", "河北", "湖北", "湖南"};
    private static final String[] BLOOD_TYPES = {"A", "B", "O", "AB"};
    private static final String[] ID_PREFIXES = {"110", "310", "440", "320", "330", "370", "410", "130"};
    private static final String[] PHONE_PREFIXES = {"138", "139", "137", "136", "135", "188", "187", "182", "183"};

    public ConfigurableDataGenerator(ProducerTableConfigLoader tableConfig, ProducerAppConfigLoader appConfig) {
        this(tableConfig, appConfig, null);
    }

    public ConfigurableDataGenerator(ProducerTableConfigLoader tableConfig, ProducerAppConfigLoader appConfig, String tableName) {
        this.tableConfig = tableConfig;
        this.appConfig = appConfig;
        this.currentTableName = tableName != null ? tableName : appConfig.getSource().getTableName();

        // 初始化序列
        if (tableConfig.getTables() != null) {
            for (ProducerTableConfigLoader.TableConfig table : tableConfig.getTables()) {
                if (table.getFields() != null) {
                    for (ProducerTableConfigLoader.FieldConfig field : table.getFields()) {
                        if ("sequence".equals(field.getGenerator()) && field.getSequenceName() != null) {
                            sequences.put(field.getSequenceName(), new AtomicLong(1));
                        }
                    }
                }
            }
        }

        log.info("ConfigurableDataGenerator 初始化完成，当前表：{}", currentTableName);
    }

    /**
     * 生成一条 CDC 记录
     */
    public Map<String, Object> generateRecord(String opcode, Map<String, Object> existingData) {
        ProducerTableConfigLoader.TableConfig table = tableConfig.getTableByName(currentTableName);

        if (table == null) {
            throw new RuntimeException("表配置不存在：" + currentTableName);
        }

        Map<String, Object> record = new HashMap<>();
        boolean includeBadData = random.nextInt(100) < appConfig.getDataQuality().getBadDataProbability();

        // 生成每个字段的值
        if (table.getFields() != null) {
            for (ProducerTableConfigLoader.FieldConfig field : table.getFields()) {
                Object value = generateFieldValue(field, existingData, includeBadData);
                record.put(field.getName(), value);
            }
        }

        // 如果是插入或更新，保存到数据池
        if ("I".equals(opcode) || "U".equals(opcode)) {
            String key = buildKey(table, record);
            dataPool.put(key, new HashMap<>(record));
        } else if ("D".equals(opcode) && existingData != null) {
            String key = buildKey(table, existingData);
            dataPool.remove(key);
        }

        return record;
    }

    /**
     * 生成字段值
     */
    @SuppressWarnings("unchecked")
    private Object generateFieldValue(ProducerTableConfigLoader.FieldConfig field,
                                       Map<String, Object> existingData,
                                       boolean includeBadData) {
        String generator = field.getGenerator();

        // 更新操作时，有一定概率保持原值
        if (existingData != null && existingData.containsKey(field.getName())) {
            if (random.nextInt(100) > 30) {  // 70% 概率保持原值
                return existingData.get(field.getName());
            }
        }

        // 是否生成问题数据
        boolean useBadValue = includeBadData &&
                field.getBadProbability() != null &&
                random.nextInt(100) < field.getBadProbability() &&
                field.getBadValues() != null &&
                !field.getBadValues().isEmpty();

        if (useBadValue) {
            return field.getBadValues().get(random.nextInt(field.getBadValues().size()));
        }

        switch (generator) {
            case "sequence":
                return generateSequence(field.getSequenceName());

            case "idcard":
                return generateIdCard();

            case "name":
                return generateName(field.getValuesMap());

            case "random":
            case "random_select":
                return generateRandomSelect(field.getValues());

            case "random_int":
                return generateRandomInt(field.getMin(), field.getMax());

            case "random_double":
                return generateRandomDouble(field.getMinDouble(), field.getMaxDouble());

            case "birthdate":
                return generateBirthdate();

            case "phone":
                return generatePhone();

            case "current_timestamp":
                return LocalDateTime.now().format(DATETIME_FORMATTER);

            case "recent_timestamp":
                return generateRecentTimestamp(field.getDaysBack());

            case "constant":
                return field.getValue();

            case "empty":
                return "";

            default:
                return null;
        }
    }

    private long generateSequence(String seqName) {
        return sequences.computeIfAbsent(seqName, k -> new AtomicLong(1)).getAndIncrement();
    }

    private String generateIdCard() {
        String prefix = ID_PREFIXES[random.nextInt(ID_PREFIXES.length)];
        String year = String.format("%04d", 1970 + random.nextInt(50));
        String month = String.format("%02d", random.nextInt(12) + 1);
        String day = String.format("%02d", random.nextInt(28) + 1);
        String suffix = String.format("%04d", random.nextInt(9999));
        return prefix + year + month + day + suffix;
    }

    private String generateName(Map<String, Object> valuesMap) {
        if (valuesMap != null) {
            List<String> surnames = (List<String>) valuesMap.get("surnames");
            List<String> names = (List<String>) valuesMap.get("names");
            if (surnames != null && names != null) {
                return surnames.get(random.nextInt(surnames.size())) +
                        names.get(random.nextInt(names.size()));
            }
        }
        return SURNAMES[random.nextInt(SURNAMES.length)] +
                NAMES[random.nextInt(NAMES.length)];
    }

    private String generateRandomSelect(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(random.nextInt(values.size()));
    }

    private int generateRandomInt(Integer min, Integer max) {
        if (min == null) min = 0;
        if (max == null) max = 100;
        return random.nextInt(max - min + 1) + min;
    }

    private double generateRandomDouble(Double min, Double max) {
        if (min == null) min = 0.0;
        if (max == null) max = 100.0;
        return random.nextDouble() * (max - min) + min;
    }

    private String generateBirthdate() {
        int year = 1970 + random.nextInt(50);
        int month = random.nextInt(12) + 1;
        int day = random.nextInt(28) + 1;
        return year + "-" + String.format("%02d", month) + "-" + String.format("%02d", day) + " 00:00:00";
    }

    private String generatePhone() {
        return PHONE_PREFIXES[random.nextInt(PHONE_PREFIXES.length)] +
                String.format("%08d", random.nextInt(99999999));
    }

    private String generateRecentTimestamp(Integer daysBack) {
        if (daysBack == null) daysBack = 365;
        long millis = System.currentTimeMillis() - random.nextInt(daysBack * 24 * 60 * 60 * 1000);
        return LocalDateTime.ofInstant(new Date(millis).toInstant(), java.time.ZoneId.systemDefault())
                .format(DATETIME_FORMATTER);
    }

    /**
     * 构建记录键
     */
    private String buildKey(ProducerTableConfigLoader.TableConfig table, Map<String, Object> record) {
        String keyField = table.getKeyFields();
        if (keyField != null && record.containsKey(keyField)) {
            return record.get(keyField).toString();
        }
        return UUID.randomUUID().toString();
    }

    /**
     * 获取一条已有数据（用于更新/删除）
     */
    public Map<String, Object> getExistingData() {
        if (dataPool.isEmpty()) {
            return null;
        }
        List<Map<String, Object>> values = new ArrayList<>(dataPool.values());
        return values.get(random.nextInt(values.size()));
    }

    /**
     * 获取数据池大小
     */
    public int getDataPoolSize() {
        return dataPool.size();
    }
}
