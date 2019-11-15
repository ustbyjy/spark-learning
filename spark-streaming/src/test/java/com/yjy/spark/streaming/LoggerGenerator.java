package com.yjy.spark.streaming;

import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class);

    public static void main(String[] args) throws Exception {
        int index = 0;
        while (true) {
            TimeUnit.SECONDS.sleep(1);
            logger.info("current value is: " + index++);
        }
    }

}
