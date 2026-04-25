package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.config.KafkaConfig;
import com.finbridge.model.dto.AdapterExecutionConfig;
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
    private final ObjectMapper objectMapper;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        return execute(requestId, payload, null);
    }

    @Override
    public ProtocolResultDTO execute(
            String requestId,
            Map<String, Object> payload,
            AdapterExecutionConfig config
    ) {
        long startTime = System.currentTimeMillis();
        String topic = endpointOrDefault(config, KafkaConfig.INTEGRATION_TOPIC);
        long timeoutMs = timeoutOrDefault(config, 5_000L);
        log.info("[KAFKA] {} - 메시지 발행 시작: topic={}, timeoutMs={}", requestId, topic, timeoutMs);

        try {
            String message = requestId + "|" + objectMapper.writeValueAsString(payload);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, requestId, message);

            future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[KAFKA] {} - 발행 성공 ({}ms, topic={})", requestId, executionTimeMs, topic);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "Kafka 메시지 발행 완료: " + topic, executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[KAFKA] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                    e.getMessage(), executionTimeMs);
        }
    }

    private String endpointOrDefault(AdapterExecutionConfig config, String defaultEndpoint) {
        return config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()
                ? config.getEndpoint()
                : defaultEndpoint;
    }

    private long timeoutOrDefault(AdapterExecutionConfig config, long defaultTimeoutMs) {
        return config != null && config.getTimeoutMs() != null && config.getTimeoutMs() > 0
                ? config.getTimeoutMs()
                : defaultTimeoutMs;
    }
}
