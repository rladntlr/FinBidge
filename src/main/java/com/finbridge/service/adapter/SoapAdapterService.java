package com.finbridge.service.adapter;

import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.ProtocolResultRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SoapAdapterService implements ProtocolAdapter {

    private final ProtocolResultRepository protocolResultRepository;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        long startTime = System.currentTimeMillis();
        log.info("[SOAP] {} - 레거시 시스템 호출 시작", requestId);

        try {
            // Apache CXF SOAP 호출 모의 (100ms 네트워크 지연)
            Thread.sleep(100);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[SOAP] {} - 성공 ({}ms)", requestId, executionTimeMs);

            saveResult(requestId, ResultStatus.SUCCESS, "200",
                    "SOAP 레거시 시스템 처리 완료", executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "SOAP 레거시 시스템 처리 완료", executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[SOAP] {} - 실패: {}", requestId, e.getMessage());

            saveResult(requestId, ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                    e.getMessage(), executionTimeMs);
        }
    }

    private void saveResult(String requestId, ResultStatus status,
                            String code, String message, Long executionTimeMs) {
        ProtocolResult result = new ProtocolResult();
        result.setRequestId(requestId);
        result.setProtocol(ProtocolType.SOAP);
        result.setStatus(status);
        result.setResponseCode(code);
        result.setResponseMessage(message);
        result.setExecutionTimeMs(executionTimeMs);
        protocolResultRepository.save(result);
    }
}
