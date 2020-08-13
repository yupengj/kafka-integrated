package com.gantang.kafka.connect.transforms;

import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * 转换日期
 */
public class TimestampConverterTest {

    private static final String PATTERN1 = "yyyy-MM-dd HH:mm:ss";
    private static final String PATTERN2= "yyyy-MM-dd";

    @Test
    public void testFormat() {
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN2);
        final Date date = Timestamp.toLogical(Timestamp.SCHEMA, 1597225795891L);
        final String format = simpleDateFormat.format(date);
        System.out.println(format);
    }

    @Test
    public void apply() {
    }

    @Test
    public void config() {
    }

    @Test
    public void configure() {
    }
}