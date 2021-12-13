package com.upperleaf;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerShutdownHook extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerShutdownHook.class);
    private final KafkaConsumer<String, String> consumer;

    public ConsumerShutdownHook(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        logger.info("Shutdown Hook");
        consumer.wakeup();
    }
}
