package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.AdapterExecutionConfig;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.service.legacy.LegacySoapService;
import com.finbridge.soap.IntegrationSoapRequest;
import com.finbridge.soap.IntegrationSoapResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SoapAdapterService implements ProtocolAdapter {

    private final ObjectMapper objectMapper;
    private final LegacySoapService legacySoapService;

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
        String endpoint = endpointOrDefault(config, "legacy-soap-service");
        log.info("[SOAP] {} - 레거시 시스템 호출 시작: endpoint={}", requestId, endpoint);

        try {
            IntegrationSoapRequest request = new IntegrationSoapRequest();
            request.setRequestId(requestId);
            request.setPayload(objectMapper.writeValueAsString(payload));

            IntegrationSoapResponse response = legacySoapService.process(request);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[SOAP] {} - 성공 ({}ms, endpoint={}): {}", requestId, executionTimeMs, endpoint, response.getMessage());

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "SOAP 처리 완료[" + endpoint + "]: " + response.getMessage(), executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[SOAP] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);
        }
    }

    private String endpointOrDefault(AdapterExecutionConfig config, String defaultEndpoint) {
        return config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()
                ? config.getEndpoint()
                : defaultEndpoint;
    }
}
