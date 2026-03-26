package com.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.ArrayList;

/**
 * 验证规则配置加载器（支持动态刷新）
 *
 * 监听配置文件变化，自动重新加载规则
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ValidationRuleConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ValidationRuleConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/validation-rules.yaml";
    private static final String DEFAULT_CONFIG_DIR = "src/main/resources/rules";

    private volatile List<TableRuleConfig> tables;
    private String configPath;
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    // 规则缓存（按表名缓存）
    private final Map<String, List<ValidationRule>> ruleCache = new HashMap<>();
    private final Map<String, ValidationRule> fieldRuleCache = new HashMap<>();

    // 文件监听
    private File configFile;
    private long lastModified;
    private volatile boolean isWatching = false;

    // 监听器列表
    private final List<Consumer<ValidationRuleConfigLoader>> listeners = new CopyOnWriteArrayList<>();

    public static class TableRuleConfig {
        private String name;
        private List<ValidationRule> rules;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public List<ValidationRule> getRules() { return rules; }
        public void setRules(List<ValidationRule> rules) { this.rules = rules; }
    }

    public static class ValidationRule {
        private String field;
        private String type;
        private String description;
        private boolean enabled;
        private java.util.List<String> values;
        private Double min_value;
        private Double max_value;
        private String pattern;
        private Integer min_length;
        private Integer max_length;

        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public java.util.List<String> getValues() { return values; }
        public void setValues(java.util.List<String> values) { this.values = values; }
        public Double getMinValue() { return min_value; }
        public void setMinValue(Double min_value) { this.min_value = min_value; }
        public Double getMaxValue() { return max_value; }
        public void setMaxValue(Double max_value) { this.max_value = max_value; }
        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
        public Integer getMinLength() { return min_length; }
        public void setMinLength(Integer min_length) { this.min_length = min_length; }
        public Integer getMaxLength() { return max_length; }
        public void setMaxLength(Integer max_length) { this.max_length = max_length; }
    }

    public ValidationRuleConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    public ValidationRuleConfigLoader(String configPath) {
        this.configPath = configPath;
        loadConfig(configPath);
    }

    /**
     * 加载配置文件（支持单文件或目录）
     */
    public synchronized void loadConfig(String configPath) {
        try {
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                log.warn("配置文件/目录不存在：{}, 尝试从 classpath 加载", configPath);
                var is = getClass().getClassLoader().getResourceAsStream("validation-rules.yaml");
                if (is != null) {
                    ValidationRuleConfigLoader loaded = mapper.readValue(is, ValidationRuleConfigLoader.class);
                    updateRules(loaded.tables);
                    this.configFile = new File(configPath);
                    this.lastModified = System.currentTimeMillis();
                    return;
                } else {
                    throw new RuntimeException("无法找到配置文件 validation-rules.yaml");
                }
            }

            this.configFile = configFile;

            if (configFile.isDirectory()) {
                // 从目录加载所有 YAML 文件
                loadFromDirectory(configFile);
            } else {
                // 从单文件加载
                ValidationRuleConfigLoader loaded = mapper.readValue(configFile, ValidationRuleConfigLoader.class);
                updateRules(loaded.tables);
                this.lastModified = configFile.lastModified();
            }

            this.configPath = configPath;
            log.info("验证规则配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载验证规则配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载验证规则配置失败", e);
        }
    }

    /**
     * 从目录加载所有 YAML 配置文件
     */
    private void loadFromDirectory(File dir) throws Exception {
        List<TableRuleConfig> allTables = new ArrayList<>();
        File[] yamlFiles = dir.listFiles((d, name) -> name.endsWith(".yaml") || name.endsWith(".yml"));

        if (yamlFiles == null || yamlFiles.length == 0) {
            log.warn("目录 {} 中没有找到 YAML 配置文件", dir.getAbsolutePath());
            updateRules(null);
            return;
        }

        for (File yamlFile : yamlFiles) {
            try {
                log.info("加载规则配置文件：{}", yamlFile.getName());
                ValidationRuleConfigLoader loaded = mapper.readValue(yamlFile, ValidationRuleConfigLoader.class);
                if (loaded.tables != null) {
                    allTables.addAll(loaded.tables);
                }
            } catch (Exception e) {
                log.error("加载规则配置文件 {} 失败：{}", yamlFile.getName(), e.getMessage());
            }
        }

        updateRules(allTables);
        this.lastModified = getLastModifiedInDirectory(dir);
        log.info("从目录加载规则完成：{} 个表配置", allTables.size());
    }

    /**
     * 获取目录中最新的文件修改时间
     */
    private long getLastModifiedInDirectory(File dir) {
        File[] yamlFiles = dir.listFiles((d, name) -> name.endsWith(".yaml") || name.endsWith(".yml"));
        long max = 0;
        if (yamlFiles != null) {
            for (File f : yamlFiles) {
                max = Math.max(max, f.lastModified());
            }
        }
        return max;
    }

    /**
     * 更新规则并刷新缓存
     */
    private void updateRules(List<TableRuleConfig> newTables) {
        this.tables = newTables;
        rebuildCache();
        log.info("验证规则已更新，共 {} 个表配置", tables != null ? tables.size() : 0);
    }

    /**
     * 重建缓存
     */
    private void rebuildCache() {
        ruleCache.clear();
        fieldRuleCache.clear();

        if (tables == null) return;

        for (TableRuleConfig table : tables) {
            String tableName = table.getName();
            List<ValidationRule> enabledRules = new java.util.ArrayList<>();

            if (table.getRules() != null) {
                for (ValidationRule rule : table.getRules()) {
                    if (rule.isEnabled()) {
                        enabledRules.add(rule);
                        fieldRuleCache.put(tableName + ":" + rule.getField(), rule);
                    }
                }
            }
            ruleCache.put(tableName, enabledRules);
        }
    }

    /**
     * 检查并刷新配置（如果文件已修改）
     */
    public void refreshIfNeeded() {
        if (configFile == null || !configFile.exists()) {
            return;
        }

        long currentModified = configFile.isDirectory()
            ? getLastModifiedInDirectory(configFile)
            : configFile.lastModified();

        if (currentModified > lastModified) {
            log.info("检测到配置文件变化，重新加载规则...");
            loadConfig(configPath);
            notifyListeners();
        }
    }

    /**
     * 启动文件监听（后台线程）
     */
    public void startWatching() {
        if (isWatching) {
            return;
        }
        isWatching = true;

        if (configFile == null) {
            configFile = new File(configPath);
        }

        final File fileToWatch = configFile;

        Thread watchThread = new Thread(() -> {
            log.info("开始监听配置文件变化：{}", fileToWatch.getAbsolutePath());
            while (isWatching) {
                try {
                    Thread.sleep(5000);  // 每 5 秒检查一次

                    if (!fileToWatch.exists()) {
                        continue;
                    }

                    long currentModified = fileToWatch.isDirectory()
                        ? getLastModifiedInDirectory(fileToWatch)
                        : fileToWatch.lastModified();

                    if (currentModified > lastModified) {
                        log.info("检测到配置文件变化，重新加载规则...");
                        loadConfig(configPath);
                        notifyListeners();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("检查配置文件变化失败", e);
                }
            }
            log.info("文件监听已停止");
        }, "RuleConfig-Watcher");
        watchThread.setDaemon(true);
        watchThread.start();
    }

    /**
     * 停止文件监听
     */
    public void stopWatching() {
        isWatching = false;
    }

    /**
     * 添加配置变化监听器
     */
    public void addChangeListener(Consumer<ValidationRuleConfigLoader> listener) {
        listeners.add(listener);
    }

    /**
     * 移除监听器
     */
    public void removeChangeListener(Consumer<ValidationRuleConfigLoader> listener) {
        listeners.remove(listener);
    }

    /**
     * 通知监听器
     */
    private void notifyListeners() {
        for (Consumer<ValidationRuleConfigLoader> listener : listeners) {
            try {
                listener.accept(this);
            } catch (Exception e) {
                log.error("通知监听器失败", e);
            }
        }
    }

    public List<TableRuleConfig> getTables() { return tables; }

    public List<ValidationRule> getRulesForTable(String tableName) {
        refreshIfNeeded();
        return ruleCache.get(tableName);
    }

    public List<ValidationRule> getEnabledRulesForTable(String tableName) {
        refreshIfNeeded();
        return ruleCache.get(tableName);
    }

    public ValidationRule getRuleByField(String tableName, String fieldName) {
        refreshIfNeeded();
        return fieldRuleCache.get(tableName + ":" + fieldName);
    }

    public java.util.Map<String, ValidationRule> getRuleMapForTable(String tableName) {
        refreshIfNeeded();
        java.util.Map<String, ValidationRule> ruleMap = new HashMap<>();
        List<ValidationRule> rules = getEnabledRulesForTable(tableName);
        if (rules != null) {
            for (ValidationRule rule : rules) {
                ruleMap.put(rule.getField(), rule);
            }
        }
        return ruleMap;
    }

    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("验证规则配置 (支持动态刷新)");
        System.out.println("=====================================");
        if (tables != null) {
            for (TableRuleConfig table : tables) {
                System.out.println("表名：" + table.getName());
                System.out.println("验证规则:");
                if (table.getRules() != null) {
                    for (ValidationRule rule : table.getRules()) {
                        String status = rule.isEnabled() ? "启用" : "禁用";
                        String ruleDetail = formatRuleDetail(rule);
                        System.out.println("  [" + status + "] " + rule.getField() +
                                " - " + rule.getType() + ": " + ruleDetail);
                    }
                }
            }
        }
        System.out.println("=====================================");
        System.out.println("配置文件：" + configPath);
        System.out.println("动态刷新：已启用 (每 5 秒检查一次)");
        System.out.println("=====================================");
    }

    private String formatRuleDetail(ValidationRule rule) {
        switch (rule.getType()) {
            case "enum":
                return "允许值：" + String.join(",", rule.getValues());
            case "range":
                return "范围：[" + rule.getMinValue() + ", " + rule.getMaxValue() + "]";
            case "regex":
                return "正则：" + rule.getPattern();
            case "length":
                return "长度：[" + rule.getMinLength() + ", " + rule.getMaxLength() + "]";
            default:
                return rule.getDescription();
        }
    }
}
