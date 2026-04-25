package com.finbridge.service;

import com.finbridge.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(topics = KafkaConfig.INTEGRATION_TOPIC,
                   groupId = "finbridge-group",
                   containerFactory = "kafkaListenerContainerFactory")
    public void consume(String message) {
        log.info("[KAFKA-CONSUMER] 메시지 수신: {}", message);

        String requestId = extractRequestId(message);

        // producer 쪽에서 protocol_results를 저장하므로 consumer는 로그만 남김
        log.info("[KAFKA-CONSUMER] requestId={} 처리 완료", requestId);
    }

    @KafkaListener(topics = KafkaConfig.DLT_TOPIC, groupId = "finbridge-dlt-group")
    public void consumeDlt(String message) {
        log.warn("[KAFKA-DLT] Dead Letter 메시지 수신: {}", message);

        try {
            String requestId = extractRequestId(message);
            log.warn("[KAFKA-DLT] requestId={} 메시지는 재시도 후 DLT로 이동했습니다.", requestId);

        } catch (Exception e) {
            log.error("[KAFKA-DLT] requestId 파싱 실패. 원본 메시지={}, error={}", message, e.getMessage());
        }
    }

    private String extractRequestId(String message) {
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("Kafka 메시지가 비어 있습니다.");
        }

        // limit 2로 payload 안의 구분자는 보존하되, requestId는 엄격하게 검증한다.
        String[] parts = message.split("\\|", 2);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Kafka 메시지 형식이 올바르지 않습니다: " + message);
        }
        String requestId = parts[0];
        if (requestId.isBlank()) {
            throw new IllegalArgumentException("Kafka 메시지에서 requestId를 파싱할 수 없습니다: " + message);
        }
        return requestId;
    }
}
