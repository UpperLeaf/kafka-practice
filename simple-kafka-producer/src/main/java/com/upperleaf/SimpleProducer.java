package com.upperleaf;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "ec2-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Custom Partitioner 를 사용하고 싶다면 아래와 같이 사용한다.
        //configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessage";

        //Producer Record Without Key
        ProducerRecord<String, String> recordWithoutKey = new ProducerRecord<>(TOPIC_NAME, messageValue);
        logger.info("{}", recordWithoutKey);

        //send 메서드는 결과로 Future 객체를 반환한다.
        RecordMetadata recordMetadata1 = producer.send(recordWithoutKey).get();
        logger.info("{}", recordMetadata1);

        //Producer Record With Key
        ProducerRecord<String, String> recordWithKey = new ProducerRecord<>(TOPIC_NAME, "messageKey", messageValue);
        logger.info("{}", recordWithKey);

        //만약 Future 객체를 Callback 을 통해 비동기로 처리하고 싶은 경우 Callback 객체를 전달한다.
        producer.send(recordWithKey, new ProducerCallback());

        producer.flush();
        producer.close();
    }
}
