package com.finbridge.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaConfig {

    public static final String INTEGRATION_TOPIC = "integration-events";
    public static final String DLT_TOPIC = "integration-events-dlt";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public NewTopic integrationTopic() {
        return TopicBuilder.name(INTEGRATION_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(DLT_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * I2: 소비 실패 메시지를 DLT로 라우팅하는 에러 핸들러.
     * 1초 간격 최대 3회 재시도 후 DLT_TOPIC 파티션 0으로 전송한다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            KafkaTemplate<String, String> kafkaTemplate) {

        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, "finbridge-group",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                )
        );

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // 파티션 무관하게 DLT 파티션 0으로 고정 라우팅
                (record, ex) -> new TopicPartition(DLT_TOPIC, 0)
        );

        // 1초 간격으로 3회 재시도 후 DLT 전송
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
                new FixedBackOff(1000L, 3));

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
