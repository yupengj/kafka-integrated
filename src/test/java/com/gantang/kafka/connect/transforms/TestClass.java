package com.gantang.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;

public abstract class TestClass {

    private static Map<String, String> map = new HashMap<>();

    public static void main(String[] args) {
        new Thread(new TestClass1()).start();

        new Thread(new TestClass2()).start();
    }

    public void testPut(String i) {
        final String s = map.get(i);
        if (s == null) {
            map.put(i, i);
            System.out.println(Thread.currentThread().getName() + " 没有值");
        } else {
            System.out.println(Thread.currentThread().getName() + " 有值:" + s);
        }
    }

    public static class TestClass1 extends TestClass implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                super.testPut(i + "");
            }

        }
    }

    public static class TestClass2 extends TestClass implements Runnable {
        @Override
        public void run() {
            for (int i = 0; i < 10000; i++) {
                super.testPut(i + "");
            }
        }
    }
}
