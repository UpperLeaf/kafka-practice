package com.upperleaf;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaAdminClient {
    private static final Logger logger = LoggerFactory.getLogger(SimpleKafkaAdminClient.class);
    private static final String BOOTSTRAP_SERVERS = "ec2-kafka:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        AdminClient admin = AdminClient.create(configs);

        logger.info("== Get Broker Information");
        loggingNodesInfo(admin);
        loggingTopicInfo(admin);

        admin.close();
    }

    private static void loggingNodesInfo(AdminClient admin) throws ExecutionException, InterruptedException {
        for(Node node : admin.describeCluster().nodes().get()) {
            logger.info("node : {}", node);
            ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
            DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
            describeConfigs.all().get().forEach((broker, config) -> {
                config.entries().forEach(configEntry -> logger.info("{} = {}", configEntry.name(), configEntry.value()));
            });
        }
    }

    private static void loggingTopicInfo(AdminClient admin) throws ExecutionException, InterruptedException {
        Set<String> topics = admin.listTopics().names().get();
        admin.describeTopics(topics).all().get().forEach((topic, description) -> {
            logger.info("{} = {}", topic, description);
        });
    }
}
