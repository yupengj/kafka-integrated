package com.gantang.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CamelFieldTest {

    private CamelField<SinkRecord> xform = new CamelField.Value<>();

    @Before
    public void setUp() throws Exception {
        xform.configure(Collections.emptyMap());
    }

    @After
    public void tearDown() throws Exception {
        xform.close();
    }


    @Test
    public void schemaless() {
        final Map<String, Object> value = new HashMap<>();
        value.put("MD_MATERIAL_ID", 1234);
        value.put("MATERIAL_NUM", "1234");
        value.put("MATERIAL_NAME", "1234");
        value.put("IS_COLOR", true);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals(1234, updatedValue.get("id"));
        assertEquals("1234", updatedValue.get("materialNum"));
        assertEquals("1234", updatedValue.get("materialName"));
        assertEquals(true, updatedValue.get("isColor"));
    }

    @Test
    public void schemaless_cache() {
        final Map<String, Object> value = new HashMap<>();
        value.put("MD_MATERIAL_ID", 1234);
        value.put("MATERIAL_NUM", "1234");
        value.put("MATERIAL_NAME", "1234");
        value.put("IS_COLOR", true);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals(1234, updatedValue.get("id"));
        assertEquals("1234", updatedValue.get("materialNum"));
        assertEquals("1234", updatedValue.get("materialName"));
        assertEquals(true, updatedValue.get("isColor"));


        final Map<String, Object> value2 = new HashMap<>();
        value2.put("MD_MATERIAL_ID", 1234);
        value2.put("MATERIAL_NUM", "1234");
        value2.put("IS_COLOR", true);
        value2.put("TEST_ABC", 23.45d);

        final SinkRecord record2 = new SinkRecord(topic, 0, null, null, null, value2, 0);
        final SinkRecord transformedRecord2 = xform.apply(record2);

        final Map updatedValue2 = (Map) transformedRecord2.value();
        assertEquals(4, updatedValue2.size());
        assertEquals(1234, updatedValue2.get("id"));
        assertEquals("1234", updatedValue2.get("materialNum"));
        assertEquals(true, updatedValue2.get("isColor"));
        assertEquals(23.45, updatedValue2.get("testAbc"));
    }

    @Test
    public void withSchema() {
        final Schema schema = SchemaBuilder.struct().field("MD_MATERIAL_ID", Schema.INT64_SCHEMA).field("MATERIAL_NUM", Schema.STRING_SCHEMA).field("IS_COLOR", Schema.BOOLEAN_SCHEMA)
                .field("TEST_ABC", Schema.FLOAT64_SCHEMA).build();

        final Struct value = new Struct(schema);
        value.put("MD_MATERIAL_ID", 1234L);
        value.put("MATERIAL_NUM", "1234");
        value.put("IS_COLOR", true);
        value.put("TEST_ABC", 23.45d);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals(new Long(1234), updatedValue.getInt64("id"));
        assertEquals("1234", updatedValue.getString("materialNum"));
        assertEquals(true, updatedValue.getBoolean("isColor"));
        assertEquals(new Double(23.45d), updatedValue.getFloat64("testAbc"));
    }

}