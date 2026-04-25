package com.finbridge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.IntegrationRequestDTO;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.entity.IntegrationRequest;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.EventType;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.RequestStatus;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.ProtocolResultRepository;
import com.finbridge.repository.SystemLogRepository;
import com.finbridge.service.adapter.BatchAdapterService;
import com.finbridge.service.adapter.KafkaAdapterService;
import com.finbridge.service.adapter.RestAdapterService;
import com.finbridge.service.adapter.SftpAdapterService;
import com.finbridge.service.adapter.SoapAdapterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class IntegrationService {

    private final IntegrationRequestRepository integrationRequestRepository;
    private final ProtocolResultRepository protocolResultRepository;
    private final SystemLogRepository systemLogRepository;
    private final SoapAdapterService soapAdapterService;
    private final KafkaAdapterService kafkaAdapterService;
    private final SftpAdapterService sftpAdapterService;
    private final BatchAdapterService batchAdapterService;
    private final RestAdapterService restAdapterService;
    private final ObjectMapper objectMapper;
    private final ExecutorService integrationTaskExecutor;

    public IntegrationResponseDTO processIntegration(IntegrationRequestDTO request) {
        // [1] request_id 생성
        String requestId = UUID.randomUUID().toString();
        log.info("[INTEGRATION] 요청 시작: requestId={}, protocols={}", requestId, request.getProtocols());

        // [2] integration_requests 저장 (CREATED)
        IntegrationRequest entity = new IntegrationRequest();
        entity.setRequestId(requestId);
        entity.setProtocolsRequested(String.join(",", request.getProtocols()));
        entity.setStatus(RequestStatus.CREATED);
        try {
            entity.setPayload(objectMapper.writeValueAsString(request.getPayload()));
        } catch (Exception e) {
            entity.setPayload(request.getPayload().toString());
        }
        integrationRequestRepository.save(entity);

        // [3] SystemLog: INITIATED
        saveLog(requestId, null, EventType.INITIATED,
                "통합 요청 시작: " + request.getProtocols());

        // [4] 상태: PROCESSING
        entity.setStatus(RequestStatus.PROCESSING);
        integrationRequestRepository.save(entity);

        // [5] 프로토콜 병렬 실행
        Map<String, CompletableFuture<ProtocolResultDTO>> futures = new HashMap<>();

        if (request.getProtocols().contains("SOAP")) {
            futures.put("SOAP", CompletableFuture.supplyAsync(
                    () -> soapAdapterService.execute(requestId, request.getPayload()), integrationTaskExecutor));
        }
        if (request.getProtocols().contains("KAFKA")) {
            futures.put("KAFKA", CompletableFuture.supplyAsync(
                    () -> kafkaAdapterService.execute(requestId, request.getPayload()), integrationTaskExecutor));
        }
        if (request.getProtocols().contains("SFTP")) {
            futures.put("SFTP", CompletableFuture.supplyAsync(
                    () -> sftpAdapterService.execute(requestId, request.getPayload()), integrationTaskExecutor));
        }
        if (request.getProtocols().contains("BATCH")) {
            futures.put("BATCH", CompletableFuture.supplyAsync(
                    () -> batchAdapterService.execute(requestId, request.getPayload()), integrationTaskExecutor));
        }
        if (request.getProtocols().contains("REST")) {
            futures.put("REST", CompletableFuture.supplyAsync(
                    () -> restAdapterService.execute(requestId, request.getPayload()), integrationTaskExecutor));
        }

        // [6] 전체 완료 대기 (35초 timeout)
        Map<String, ProtocolResultDTO> results = new HashMap<>();

        try {
            CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                    .orTimeout(35, TimeUnit.SECONDS)
                    .join();

            for (var entry : futures.entrySet()) {
                results.put(entry.getKey(), normalizeResult(entry.getValue().getNow(null)));
            }
        } catch (CompletionException e) {
            log.warn("[INTEGRATION] {} - 일부 프로토콜 타임아웃 또는 실패", requestId);
            for (var entry : futures.entrySet()) {
                if (!entry.getValue().isDone()) {
                    results.put(entry.getKey(), new ProtocolResultDTO(
                            ResultStatus.TIMEOUT, "504", "35초 내 응답 없음", 35000L));
                } else {
                    try {
                        results.put(entry.getKey(), normalizeResult(entry.getValue().get()));
                    } catch (Exception ex) {
                        results.put(entry.getKey(), new ProtocolResultDTO(
                                ResultStatus.FAILED, "500", ex.getMessage(), 0L));
                    }
                }
            }
        }

        results.forEach((protocol, result) -> {
            ProtocolType protocolType = ProtocolType.valueOf(protocol);
            saveProtocolResult(requestId, protocolType, result);
            saveLog(requestId, protocolType, toEventType(result),
                    protocol + " 처리 결과: " + result.getStatus());
        });

        // [7-8] 전체 상태 판정
        OverallStatus overallStatus = determineOverallStatus(results);

        // [9] integration_requests 업데이트 (COMPLETED)
        entity.setStatus(RequestStatus.COMPLETED);
        entity.setOverallStatus(overallStatus);
        entity.setCompletedAt(LocalDateTime.now());
        integrationRequestRepository.save(entity);

        // [10] SystemLog: SUCCESS or FAILED
        EventType finalEvent = overallStatus == OverallStatus.ALL_FAILED
                ? EventType.FAILED : EventType.SUCCESS;
        saveLog(requestId, null, finalEvent,
                "통합 요청 완료: " + overallStatus);

        log.info("[INTEGRATION] {} 완료: {}", requestId, overallStatus);
        return new IntegrationResponseDTO(requestId, overallStatus, results, LocalDateTime.now());
    }

    public Optional<IntegrationResponseDTO> getStatus(String requestId) {
        Optional<IntegrationRequest> entityOptional = integrationRequestRepository.findByRequestId(requestId);
        if (entityOptional.isEmpty()) {
            return Optional.empty();
        }
        IntegrationRequest entity = entityOptional.get();

        List<ProtocolResult> protocolResults = protocolResultRepository.findByRequestId(requestId);
        Map<String, ProtocolResultDTO> resultMap = new HashMap<>();

        for (ProtocolResult r : protocolResults) {
            resultMap.put(r.getProtocol().name(), new ProtocolResultDTO(
                    r.getStatus(), r.getResponseCode(),
                    r.getResponseMessage(), r.getExecutionTimeMs()));
        }

        return Optional.of(new IntegrationResponseDTO(
                requestId, entity.getOverallStatus(), resultMap, entity.getCreatedAt()));
    }

    private OverallStatus determineOverallStatus(Map<String, ProtocolResultDTO> results) {
        if (results.isEmpty()) return OverallStatus.ALL_FAILED;

        long successCount = results.values().stream()
                .filter(r -> r.getStatus() == ResultStatus.SUCCESS)
                .count();

        if (successCount == results.size()) return OverallStatus.ALL_SUCCESS;
        if (successCount == 0) return OverallStatus.ALL_FAILED;
        return OverallStatus.PARTIAL_FAILURE;
    }

    private ProtocolResultDTO normalizeResult(ProtocolResultDTO result) {
        if (result != null) {
            return result;
        }
        return new ProtocolResultDTO(ResultStatus.FAILED, "500", "프로토콜 결과 없음", 0L);
    }

    private void saveProtocolResult(String requestId, ProtocolType protocol, ProtocolResultDTO dto) {
        ProtocolResult result = new ProtocolResult();
        result.setRequestId(requestId);
        result.setProtocol(protocol);
        result.setStatus(dto.getStatus());
        result.setResponseCode(dto.getResponseCode());
        result.setResponseMessage(dto.getResponseMessage());
        result.setExecutionTimeMs(dto.getExecutionTimeMs());
        protocolResultRepository.save(result);
    }

    private EventType toEventType(ProtocolResultDTO result) {
        return result.getStatus() == ResultStatus.SUCCESS ? EventType.SUCCESS : EventType.FAILED;
    }

    private void saveLog(String requestId, com.finbridge.model.enums.ProtocolType protocol,
                         EventType eventType, String detail) {
        SystemLog log = new SystemLog();
        log.setRequestId(requestId);
        log.setProtocol(protocol);
        log.setEventType(eventType);
        log.setEventDetail(detail);
        systemLogRepository.save(log);
    }
}
