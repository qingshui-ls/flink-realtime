package com.ecust.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPollExecutor;

    private ThreadPoolUtil() {

    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if (threadPollExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPollExecutor == null) {
                    threadPollExecutor = new ThreadPoolExecutor(4,
                            20,
                            300,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return threadPollExecutor;
    }
}
