package com.finbridge.service;

import com.finbridge.config.KafkaConfig;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.ProtocolResultRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumerService {

    private final ProtocolResultRepository protocolResultRepository;

    @KafkaListener(topics = KafkaConfig.INTEGRATION_TOPIC, groupId = "finbridge-group")
    public void consume(String message) {
        log.info("[KAFKA-CONSUMER] 메시지 수신: {}", message);

        try {
            String requestId = message.split("\\|")[0];

            // 이미 producer 쪽에서 protocol_results를 저장하므로
            // consumer는 로그만 남기고 추가 처리 수행
            log.info("[KAFKA-CONSUMER] requestId={} 처리 완료", requestId);

        } catch (Exception e) {
            log.error("[KAFKA-CONSUMER] 처리 실패: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = KafkaConfig.DLT_TOPIC, groupId = "finbridge-dlt-group")
    public void consumeDlt(String message) {
        log.warn("[KAFKA-DLT] Dead Letter 메시지 수신: {}", message);

        try {
            String requestId = message.split("\\|")[0];

            ProtocolResult result = new ProtocolResult();
            result.setRequestId(requestId);
            result.setProtocol(ProtocolType.KAFKA);
            result.setStatus(ResultStatus.FAILED);
            result.setResponseCode("500");
            result.setResponseMessage("Dead Letter Queue로 이동됨");
            result.setExecutionTimeMs(0L);
            protocolResultRepository.save(result);

        } catch (Exception e) {
            log.error("[KAFKA-DLT] DLT 처리 실패: {}", e.getMessage());
        }
    }
}
