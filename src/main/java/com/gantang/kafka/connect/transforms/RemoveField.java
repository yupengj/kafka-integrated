package com.gantang.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class RemoveField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Remove the specified field from a Struct when schema present, or a Map in the case of schemaless data.  " +
            "<p/>Use the concrete transformation type designed for the record value (<code>" + RemoveField.class.getName() + "</code>).";

    // field格式: tableName:[columnName1,columnName2]
    // 如: md_material:[material_num,material_name] // 大小写不敏感
    private static final String FIELD_CONFIG = "field";

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Field name to extract.");

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public R apply(R record) {
        return null;
    }

}
