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
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * 转换所有 schema name 等于 org.apache.kafka.connect.data.Timestamp 的字段，将值转换成 yyyy-MM-dd 日期格式
 *
 * @param <R>
 */
public class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String TIMESTAMP_TYPE = "org.apache.kafka.connect.data.Timestamp";
    private static final String PURPOSE = "converting all timestamp formats";
    private static final String FORMAT = "yyyy-MM-dd";
    private final SimpleDateFormat format = new SimpleDateFormat(FORMAT);

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(32));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return record;
        } else {
            // 只转换有 schema
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            for (Field field : schema.fields()) {
                if (TIMESTAMP_TYPE.equals(field.schema().name())) {
                    builder.field(field.name(), Schema.STRING_SCHEMA);
                } else {
                    builder.field(field.name(), field.schema());
                }
            }
            if (schema.isOptional()) {
                builder.optional();
            }
            if (schema.defaultValue() != null) {
                Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                builder.defaultValue(updatedDefaultValue);
            }
            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }
        Struct updatedValue = applyValueWithSchema(value, updatedSchema);
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (TIMESTAMP_TYPE.equals(field.schema().name())) {
                updatedFieldValue = convertTimestamp(value.get(field));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private Object convertTimestamp(Object timestamp) {
        Date date;
        if (timestamp instanceof Long) {
            date = Timestamp.toLogical(Timestamp.SCHEMA, (Long) timestamp);
        } else if (timestamp instanceof Date) {
            date = (Date) timestamp;
        } else {
            return timestamp;
        }
        synchronized (format) {
            return format.format(date);
        }
    }

    @Override
    public void close() {

    }

    private Schema operatingSchema(R record) {
        return record.valueSchema();
    }

    private Object operatingValue(R record) {
        return record.value();
    }

    private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
