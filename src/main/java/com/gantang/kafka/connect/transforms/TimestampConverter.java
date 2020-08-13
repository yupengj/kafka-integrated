package com.gantang.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class TimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    //org.apache.kafka.connect.data.Timestamp

    @Override
    public R apply(R record) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
