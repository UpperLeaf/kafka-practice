package com.upperleaf;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SimpleStreamApplication {

    private static final String APPLICATION_NAME = "streams-application";
    private static final String BOOTSTRAP_SERVER = "ec2-kafka:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_COPY = "stream_log_copy";

    public void run() {
        Properties configs = new Properties();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        configs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(STREAM_LOG)
                .filter((key, value) -> value.length() > 5)
                .to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), configs);
        streams.start();
    }
}
