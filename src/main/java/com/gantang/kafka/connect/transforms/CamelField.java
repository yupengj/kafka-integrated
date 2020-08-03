package com.gantang.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * 数据库字段（material_num） 转成驼峰名称（materialNum）
 *
 * @param <R>
 */
public abstract class CamelField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String ID_FILE_NAME = "id";
    private static final String PURPOSE = "field replacement";
    private static final String ID_MAPPINGS = "mappingId";

    /**
     * key 是转换前的字段名称，value 是转换后的字段名称
     */
    private Cache<String, String> fileMappingCache;
    /**
     * key 是转换后的字段名称，value 是转换前的字段名称
     */
    private Cache<String, String> reverseFileMappingCache;
    /**
     * Schema 缓存
     */
    private Cache<Schema, Schema> schemaUpdateCache;
    /**
     * key 是表名称，如主题是 “ibom.mstdata.md_material” 表名是“md_material”，value 是主题的id字段名称
     */
    private Map<String, String> tableNameToIdField = new HashMap<>();

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef().define(ID_MAPPINGS, ConfigDef.Type.LIST, Collections.emptyList(), new ConfigDef.Validator() {
        @SuppressWarnings("unchecked")
        @Override
        public void ensureValid(String name, Object value) {
            parseRenameMappings((List<String>) value);
        }

        @Override
        public String toString() {
            return "list of colon-delimited pairs, e.g. <code>主题名:id字段,abc:xyz</code>";
        }
    }, ConfigDef.Importance.MEDIUM, "Field id mappings.");

    @Override
    public void configure(Map<String, ?> configs) {
        fileMappingCache = new SynchronizedCache<>(new LRUCache<>(512));
        reverseFileMappingCache = new SynchronizedCache<>(new LRUCache<>(512));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        tableNameToIdField = parseRenameMappings(config.getList(ID_MAPPINGS));
    }

    private static Map<String, String> parseRenameMappings(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for (String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ID_MAPPINGS, mappings, "Invalid id mapping: " + mapping);
            }
            m.put(parts[0], parts[1]);
        }
        return m;
    }

    private static Map<String, String> invert(Map<String, String> source) {
        final Map<String, String> m = new HashMap<>();
        for (Map.Entry<String, String> e : source.entrySet()) {
            m.put(e.getValue(), e.getKey());
        }
        return m;
    }

    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            // 没有 schema
            return applySchemaless(record);
        } else {
            // 有 schema
            return applyWithSchema(record);
        }
    }

    /**
     * 转换没有  Schema 的 record
     *
     * @param record record
     * @return 新的record
     */
    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final String tableName = tableName(record.topic());

        final Map<String, Object> updatedValue = new HashMap<>(value.size());
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            final String fieldName = entry.getKey();
            final Object fieldValue = entry.getValue();
            updatedValue.put(renamed(fieldName, tableName), fieldValue);
        }
        return newRecord(record, null, updatedValue);
    }

    /**
     * 根据主题名称获取表名称，采用最有一个“.”后的字符串为表名
     *
     * @param topicName topicName
     * @return 表名
     */
    private String tableName(String topicName) {
        final int index = topicName.lastIndexOf(".");
        return topicName.substring(index + 1);
    }

    /**
     * 根据字段名称转成驼峰命名格式
     *
     * @param fieldName 字段名称
     * @param tableName 字段值
     * @return 新的字段名称
     */
    private String renamed(String fieldName, String tableName) {
        // 从缓存中获取已转换过的名称
        final String mapping = fileMappingCache.get(fieldName);
        if (mapping != null && reverseFileMappingCache.get(mapping) != null) {
            return mapping;
        }

        // 如果是id字段直接返回 “id”
        if (isIdField(fieldName, tableName)) {
            fileMappingCache.put(fieldName, ID_FILE_NAME);
            // 如果是id字段，则用表名最为缓存的key
            reverseFileMappingCache.put(tableName, fieldName);
            return ID_FILE_NAME;
        }

        // 根据字段名称中的“_”转换驼峰命名格式
        final String lowerFieldName = fieldName.toLowerCase();
        StringBuilder stringBuffer = new StringBuilder(lowerFieldName.length());
        boolean flag = false;
        for (int index = 0; index < lowerFieldName.length(); index++) {
            char c = lowerFieldName.charAt(index);
            if (c == '_') {
                flag = true;
            } else {
                if (flag) {
                    c = Character.toUpperCase(c);
                    flag = false;
                }
                stringBuffer.append(c);
            }
        }
        final String newField = stringBuffer.toString();
        fileMappingCache.put(fieldName, newField);
        reverseFileMappingCache.put(newField, fieldName);
        return newField;
    }

    /**
     * 如果字段名和表名_ID相等（忽略大小写）则认为是id字段
     *
     * @param fieldName 字段名称
     * @param tableName 表名
     * @return true 是id字段
     */
    private boolean isIdField(String fieldName, String tableName) {
        if (tableNameToIdField.containsKey(tableName)) {
            return tableNameToIdField.get(tableName).equalsIgnoreCase(fieldName);
        }
        if (ID_FILE_NAME.equalsIgnoreCase(fieldName)) {
            return true;
        }
        final String idField = tableName + "_id";
        return fieldName.equalsIgnoreCase(idField);
    }


    /**
     * 转换有  Schema 的 record
     *
     * @param record record
     * @return 新的 record
     */
    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final String tableName = tableName(record.topic());

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), tableName);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(reverseRenamed(tableName, field.name()));
            updatedValue.put(field.name(), fieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    /**
     * 根据转换后的字段名称，获取转换前的字段名称，目的是为了根据转换前的字段取数据
     *
     * @param tableName 表名
     * @param fieldName 转换后的字段名
     * @return 转换前的字段名
     */
    private String reverseRenamed(String tableName, String fieldName) {
        if (ID_FILE_NAME.equals(fieldName)) {
            final String reverseName = reverseFileMappingCache.get(tableName);
            return reverseName == null ? fieldName : reverseName;
        }
        final String mapping = reverseFileMappingCache.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    /**
     * 复制修改 Schema
     *
     * @param schema    Schema
     * @param tableName 字段名称
     * @return 新的Schema
     */
    private Schema makeUpdatedSchema(Schema schema, String tableName) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(renamed(field.name(), tableName), field.schema());
        }
        return builder.build();
    }

    /**
     * 获得 Schema
     *
     * @param record record
     * @return Schema
     */
    protected abstract Schema operatingSchema(R record);

    /**
     * 获得 value
     *
     * @param record record
     * @return value
     */
    protected abstract Object operatingValue(R record);

    /**
     * 创建新的 record
     *
     * @param record        record
     * @param updatedSchema 新的 Schema
     * @param updatedValue  新的 value
     * @return 新的 record
     */
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    // 转换 key
    public static class Key<R extends ConnectRecord<R>> extends CamelField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    /**
     * 转换 value
     *
     * @param <R>
     */
    public static class Value<R extends ConnectRecord<R>> extends CamelField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
