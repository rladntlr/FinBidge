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
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class BatchAdapterService implements ProtocolAdapter {

    private final ProtocolResultRepository protocolResultRepository;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        long startTime = System.currentTimeMillis();
        String batchId = "BATCH-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        log.info("[BATCH] {} - 배치 작업 제출 시작: {}", requestId, batchId);

        try {
            // Spring Batch Job 제출 모의 (50ms)
            Thread.sleep(50);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[BATCH] {} - 제출 성공: {} ({}ms)", requestId, batchId, executionTimeMs);

            saveResult(requestId, ResultStatus.SUCCESS, "200",
                    "배치 작업 제출 완료 (batchId: " + batchId + ")", executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "배치 작업 제출 완료 (batchId: " + batchId + ")", executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[BATCH] {} - 실패: {}", requestId, e.getMessage());

            saveResult(requestId, ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                    e.getMessage(), executionTimeMs);
        }
    }

    private void saveResult(String requestId, ResultStatus status,
                            String code, String message, Long executionTimeMs) {
        ProtocolResult result = new ProtocolResult();
        result.setRequestId(requestId);
        result.setProtocol(ProtocolType.BATCH);
        result.setStatus(status);
        result.setResponseCode(code);
        result.setResponseMessage(message);
        result.setExecutionTimeMs(executionTimeMs);
        protocolResultRepository.save(result);
    }
}
