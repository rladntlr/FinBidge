package com.finbridge.service.adapter;

import com.finbridge.model.dto.AdapterExecutionConfig;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.service.legacy.LegacyRestService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class RestAdapterService implements ProtocolAdapter {

    private final LegacyRestService legacyRestService;

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
        String endpoint = endpointOrDefault(config, "legacy-rest-service");
        log.info("[REST] {} - REST 레거시 처리 시작: endpoint={}", requestId, endpoint);

        try {
            Map<String, Object> response = legacyRestService.echo(payload);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            String message = response != null ? (String) response.get("message") : "응답 없음";

            log.info("[REST] {} - 성공 ({}ms, endpoint={}): {}", requestId, executionTimeMs, endpoint, message);
            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "REST 처리 완료[" + endpoint + "]: " + message, executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[REST] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);
        }
    }

    private String endpointOrDefault(AdapterExecutionConfig config, String defaultEndpoint) {
        return config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()
                ? config.getEndpoint()
                : defaultEndpoint;
    }
}
