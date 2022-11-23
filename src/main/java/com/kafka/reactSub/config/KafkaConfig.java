package com.kafka.reactSub.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class.getName());

    private String host="kafka-kafka-brokers.kafka:9092";

    private String topic="webfluxDemo";

    // private String groupId="testGroupId";

    @Bean
    public ReceiverOptions<String, Object> receiverOptions() {
        return ReceiverOptions.<String, Object>create(getConsumerProps())
                .subscription(Collections.singleton(topic))
                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
    }

    private Map<String, Object> getConsumerProps() {
        return new HashMap<String, Object>() {{
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
            put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
            put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }};
    }
}