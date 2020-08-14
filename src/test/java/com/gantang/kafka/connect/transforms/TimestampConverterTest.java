package com.gantang.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 转换日期
 */
public class TimestampConverterTest {

    private TimestampConverter<SinkRecord> timestampConverter = new TimestampConverter<>();

    @Before
    public void before() {
        timestampConverter.configure(null);
    }

    @Test
    public void testFormat() {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        final Date date = Timestamp.toLogical(Timestamp.SCHEMA, 1597225795891L);
        final String format = simpleDateFormat.format(date);
        System.out.println(format);
    }

    @Test
    public void testSchema() {
        final SchemaBuilder int64 = SchemaBuilder.int64();
        int64.name("org.apache.kafka.connect.data.Timestamp").version(1);
        final Schema schema = SchemaBuilder.struct().field("MD_MATERIAL_ID", Schema.INT64_SCHEMA).field("MATERIAL_NUM", Schema.STRING_SCHEMA).field("IS_COLOR", Schema.BOOLEAN_SCHEMA)
                .field("TEST_ABC", Schema.FLOAT64_SCHEMA).field("CREATED_DATE", int64.build()).build();
        System.out.println(schema);
    }

    @Test
    public void apply() {
        final SchemaBuilder int64 = SchemaBuilder.int64();
        int64.name("org.apache.kafka.connect.data.Timestamp").version(1);
        final Schema schema = SchemaBuilder.struct().field("MD_MATERIAL_ID", Schema.INT64_SCHEMA).field("MATERIAL_NUM", Schema.STRING_SCHEMA).field("IS_COLOR", Schema.BOOLEAN_SCHEMA)
                .field("TEST_ABC", Schema.FLOAT64_SCHEMA).field("CREATED_DATE", int64.build()).field("UPDATED_DATE", int64.build()).build();

        final Struct value = new Struct(schema);
        value.put("MD_MATERIAL_ID", 1597225795891L);
        value.put("MATERIAL_NUM", "1234");
        value.put("IS_COLOR", true);
        value.put("TEST_ABC", 23.45d);
        value.put("CREATED_DATE", new Date(1597225795891L));
        value.put("UPDATED_DATE", new Date(1597225795891L));

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, schema, value, 0);
        final ConnectRecord apply = timestampConverter.apply(record);
        final Struct value1 = (Struct) apply.value();
        final Schema schema1 = apply.valueSchema();
        Assert.assertEquals(schema1.field("CREATED_DATE").schema(), Schema.STRING_SCHEMA);
        Assert.assertEquals(value1.get("CREATED_DATE"), "2020-08-12");
        Assert.assertEquals(value1.get("UPDATED_DATE"), "2020-08-12");
    }
}