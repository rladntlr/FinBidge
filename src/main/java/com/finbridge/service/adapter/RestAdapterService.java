package com.finbridge.service.adapter;

import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class RestAdapterService implements ProtocolAdapter {

    private final WebClient.Builder webClientBuilder;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        long startTime = System.currentTimeMillis();
        log.info("[REST] {} - 외부 REST 시스템 호출 시작", requestId);

        try {
            Map<?, ?> response = webClientBuilder.build()
                    .post()
                    .uri("http://localhost:8080/internal/echo")
                    .bodyValue(payload)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .block();

            long executionTimeMs = System.currentTimeMillis() - startTime;
            String message = response != null ? (String) response.get("message") : "응답 없음";

            log.info("[REST] {} - 성공 ({}ms): {}", requestId, executionTimeMs, message);
            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200", message, executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[REST] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);
        }
    }
}
