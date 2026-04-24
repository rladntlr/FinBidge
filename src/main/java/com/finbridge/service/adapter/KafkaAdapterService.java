package com.finbridge.service.adapter;

import com.finbridge.config.KafkaConfig;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaAdapterService implements ProtocolAdapter {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        long startTime = System.currentTimeMillis();
        log.info("[KAFKA] {} - 메시지 발행 시작", requestId);

        try {
            String message = requestId + "|" + payload.toString();

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(KafkaConfig.INTEGRATION_TOPIC, requestId, message);

            // 발행 완료 대기 (최대 5초)
            future.get(5, java.util.concurrent.TimeUnit.SECONDS);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[KAFKA] {} - 발행 성공 ({}ms)", requestId, executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "Kafka 메시지 발행 완료", executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[KAFKA] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                    e.getMessage(), executionTimeMs);
        }
    }
}
