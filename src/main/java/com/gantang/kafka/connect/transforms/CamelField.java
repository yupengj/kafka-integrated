package com.gantang.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CamelField<R extends ConnectRecord<R>> implements Transformation<R> {

    private final ConfigDef config = new ConfigDef();

    private static final String ID = "id";
    private static final String PURPOSE = "field replacement";

    private Cache<String, String> fileMappingCache;
    private Cache<String, String> reverseFileMappingCache;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return this.config;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        fileMappingCache = new SynchronizedCache<>(new LRUCache<>(512));
        reverseFileMappingCache = new SynchronizedCache<>(new LRUCache<>(512));
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public void close() {
    }

    private String tableName(R record) {
        final String topic = record.topic();
        final int index = topic.lastIndexOf(".");
        return topic.substring(index + 1);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final String tableName = tableName(record);
        final Map<String, Object> updatedValue = new HashMap<>(value.size());
        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldValue = e.getValue();
            updatedValue.put(renamed(fieldName, tableName), fieldValue);
        }
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final String tableName = tableName(record);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), tableName);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : updatedSchema.fields()) {
            final Object fieldValue = value.get(reverseRenamed(field.name()));
            updatedValue.put(field.name(), fieldValue);
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema, String tableName) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field : schema.fields()) {
            builder.field(renamed(field.name(), tableName), field.schema());
        }
        return builder.build();
    }

    private String renamed(String fieldName, String tableName) {
        final String mapping = fileMappingCache.get(fieldName);
        if (mapping != null && reverseFileMappingCache.get(mapping) != null) {
            return mapping;
        }

        if (isIdField(fieldName, tableName)) {
            fileMappingCache.put(fieldName, ID);
            reverseFileMappingCache.put(ID, fieldName);
            return ID;
        }

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

    private boolean isIdField(String fieldName, String tableName) {
        final String lowerCaseTableName = tableName.toLowerCase();
        final String lowerCaseFieldName = fieldName.toLowerCase();
        if (lowerCaseFieldName.startsWith(lowerCaseTableName)) {
            String[] idFields = {lowerCaseTableName + "_id", lowerCaseTableName + "_ID", lowerCaseTableName + "_Id", lowerCaseTableName + "_iD"};
            return Arrays.binarySearch(idFields, lowerCaseFieldName) != -1;
        }
        return false;
    }


    private String reverseRenamed(String fieldName) {
        final String mapping = reverseFileMappingCache.get(fieldName);
        return mapping == null ? fieldName : mapping;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

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
