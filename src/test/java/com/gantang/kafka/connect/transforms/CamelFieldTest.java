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

    private CamelField<SinkRecord> transformationValue = new CamelField.Value<>();
    private CamelField<SinkRecord> transformationKey = new CamelField.Key<>();
    private CamelField<SinkRecord> transformationValueConfig = new CamelField.Value<>();
    private CamelField<SinkRecord> transformationKeyConfig = new CamelField.Key<>();

    @Before
    public void setUp() throws Exception {
        transformationValue.configure(Collections.emptyMap());
        transformationKey.configure(Collections.emptyMap());
        transformationValueConfig.configure(Collections.singletonMap("mappingId", "bm_bommgmt_part_assembly_ebom:bm_bommgmt_part_assembly_id,bm_bommgmt_part_assembly_mbom:bm_bommgmt_part_assembly_id"));
        transformationKeyConfig.configure(Collections.singletonMap("mappingId", "bm_bommgmt_part_assembly_ebom:bm_bommgmt_part_assembly_id,bm_bommgmt_part_assembly_mbom:bm_bommgmt_part_assembly_id"));
    }

    @After
    public void tearDown() throws Exception {
        transformationValue.close();
        transformationKey.close();
    }

    @Test
    public void schemaless_value() {
        final Map<String, Object> value = new HashMap<>();
        value.put("MD_MATERIAL_ID", 1234);
        value.put("MATERIAL_NUM", "1234");
        value.put("MATERIAL_NAME", "1234");
        value.put("IS_COLOR", true);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = transformationValue.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(4, updatedValue.size());
        assertEquals(1234, updatedValue.get("id"));
        assertEquals("1234", updatedValue.get("materialNum"));
        assertEquals("1234", updatedValue.get("materialName"));
        assertEquals(true, updatedValue.get("isColor"));
    }

    @Test
    public void schemaless_cache_value() {
        final Map<String, Object> value = new HashMap<>();
        value.put("MD_MATERIAL_ID", 1234);
        value.put("MATERIAL_NUM", "1234");
        value.put("MATERIAL_NAME", "1234");
        value.put("IS_COLOR", true);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = transformationValue.apply(record);

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
        final SinkRecord transformedRecord2 = transformationValue.apply(record2);

        final Map updatedValue2 = (Map) transformedRecord2.value();
        assertEquals(4, updatedValue2.size());
        assertEquals(1234, updatedValue2.get("id"));
        assertEquals("1234", updatedValue2.get("materialNum"));
        assertEquals(true, updatedValue2.get("isColor"));
        assertEquals(23.45, updatedValue2.get("testAbc"));
    }

    @Test
    public void withSchema_value() {
        final Schema schema = SchemaBuilder.struct().field("MD_MATERIAL_ID", Schema.INT64_SCHEMA).field("MATERIAL_NUM", Schema.STRING_SCHEMA).field("IS_COLOR", Schema.BOOLEAN_SCHEMA)
                .field("TEST_ABC", Schema.FLOAT64_SCHEMA).build();

        final Struct value = new Struct(schema);
        value.put("MD_MATERIAL_ID", 1234L);
        value.put("MATERIAL_NUM", "1234");
        value.put("IS_COLOR", true);
        value.put("TEST_ABC", 23.45d);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = transformationValue.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(4, updatedValue.schema().fields().size());
        assertEquals(new Long(1234), updatedValue.getInt64("id"));
        assertEquals("1234", updatedValue.getString("materialNum"));
        assertEquals(true, updatedValue.getBoolean("isColor"));
        assertEquals(new Double(23.45d), updatedValue.getFloat64("testAbc"));
    }

    @Test
    public void schemaless_key() {
        final Map<String, Object> key = new HashMap<>();
        key.put("md_material_id", 1234);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, key, null, null, 0);
        final SinkRecord transformedRecord = transformationKey.apply(record);

        final Map updatedKey = (Map) transformedRecord.key();
        assertEquals(1, updatedKey.size());
        assertEquals(1234, updatedKey.get("id"));
    }

    @Test
    public void withSchema_key() {
        final Schema schema = SchemaBuilder.struct().field("md_material_id", Schema.INT64_SCHEMA).build();

        final Struct key = new Struct(schema);
        key.put("md_material_id", 1234L);

        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, schema, key, null, null, 0);
        final SinkRecord transformedRecord = transformationKey.apply(record);

        final Struct updatedKey = (Struct) transformedRecord.key();

        assertEquals(1, updatedKey.schema().fields().size());
        assertEquals(new Long(1234), updatedKey.getInt64("id"));
    }


    @Test
    public void withSchema_key_config(){
        final Schema schema = SchemaBuilder.struct().field("bm_bommgmt_part_assembly_id", Schema.INT64_SCHEMA).build();

        final Struct key = new Struct(schema);
        key.put("bm_bommgmt_part_assembly_id", 1234L);

        String topic = "ibom.bommgmt.bm_bommgmt_part_assembly_ebom";
        final SinkRecord record = new SinkRecord(topic, 0, schema, key, null, null, 0);
        final SinkRecord transformedRecord = transformationKeyConfig.apply(record);

        final Struct updatedKey = (Struct) transformedRecord.key();

        assertEquals(1, updatedKey.schema().fields().size());
        assertEquals(new Long(1234), updatedKey.getInt64("id"));
    }


}