package com.simple.mqtt;

public class ThreadConfig {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> System.out.println(e.getCause()));
    }
}
