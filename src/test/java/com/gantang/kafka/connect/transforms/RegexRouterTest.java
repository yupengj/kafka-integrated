package com.gantang.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RegexRouterTest {

    private RegexRouter<SinkRecord> regexRouter = new RegexRouter<>();

    @Before
    public void before() {
        Map<String, Object> props = new HashMap<>();
        props.put("regex", ".*\\.(.*)");
        props.put("replacement", "$1");
        regexRouter.configure(props);
    }

    @Test
    public void test() {
        String topic = "ibom.mstdata.md_material";
        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, null, 0);

        final ConnectRecord apply = regexRouter.apply(record);
        final String newTopic = apply.topic();
        Assert.assertEquals("md_material", newTopic);

        final int index = newTopic.lastIndexOf(".");
        final String tableName = newTopic.substring(index + 1);
        Assert.assertEquals("md_material", tableName);
    }
}
