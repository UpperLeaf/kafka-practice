package com.upperleaf;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "ec2-kafka:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {
        Properties configs = new Properties();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //명시적으로 Offset Commit을 수행하기 위해서 아래같은 옵션을 이용한다.
        //자동 Offset Commit(비명시 오프셋 커밋)은 데이터 중복 처리를 일으킬 수 있음.
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(List.of(TOPIC_NAME));

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
                //Record 별로 커밋한다.
                //createCommitByRecord(record);
            }

            //명시적으로 오프셋을 커밋한다. commitSync는 커밋을 동기적으로 실행하기 때문에, 성능에 영향을 미칠 수 있다.
            //데이터 처리시간에 비해 커밋 요청 및 응답시간이 많이 걸린다면 commitAsync() 메서드를 사용할 수 있다.
            consumer.commitSync();

            //비동기 커밋은 요청이 실패했을 경우 현재 처리중인 데이터의 순서를 보장할 수 없으며 데이터의 중복 처리가 발생할 수 있다.
            //consumer.commitAsync();
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> createCommitByRecord(ConsumerRecord<String, String> record) {
        return Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
    }
}
