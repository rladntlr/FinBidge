package com.finbridge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.IntegrationRequestDTO;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.LogResponseDTO;
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
import org.springframework.data.domain.AbstractPageRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

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
import java.util.stream.Collectors;

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
    private final TransactionTemplate transactionTemplate;

    /**
     * @Transactional: 요청 저장부터 완료 저장까지 하나의 트랜잭션으로 묶는다.
     * 주의: 어댑터 실행(최대 35초) 동안 DB 커넥션을 점유하므로
     * 고트래픽 환경에서는 커넥션 풀 고갈 가능성 있음.
     * (프로토타입 수준에서는 허용)
     */
    @Transactional
    public IntegrationResponseDTO processIntegration(IntegrationRequestDTO request) {
        // [1] request_id 생성
        String requestId = UUID.randomUUID().toString();
        log.info("[INTEGRATION] 요청 시작: requestId={}, protocols={}", requestId, request.getProtocols());

        // [2-4] integration_requests 저장 + 시작 로그 저장
        IntegrationRequest entity = createInitialRequest(requestId, request);

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

        // [7-8] 전체 상태 판정
        OverallStatus overallStatus = determineOverallStatus(results);

        // [9-10] 프로토콜 결과 + 완료 상태 저장
        CompletionTimes completionTimes = saveFinalResults(entity, results, overallStatus);

        log.info("[INTEGRATION] {} 완료: {}", requestId, overallStatus);
        return new IntegrationResponseDTO(
                requestId,
                overallStatus,
                results,
                completionTimes.createdAt(),
                completionTimes.completedAt()
        );
    }

    @Transactional(readOnly = true)
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
                requestId,
                entity.getOverallStatus(),
                resultMap,
                entity.getCreatedAt(),
                entity.getCompletedAt()
        ));
    }

    private IntegrationRequest createInitialRequest(String requestId, IntegrationRequestDTO request) {
        return transactionTemplate.execute(status -> {
            IntegrationRequest entity = new IntegrationRequest();
            entity.setRequestId(requestId);
            entity.setProtocolsRequested(String.join(",", request.getProtocols()));
            entity.setStatus(RequestStatus.CREATED);
            try {
                entity.setPayload(objectMapper.writeValueAsString(request.getPayload()));
            } catch (Exception e) {
                entity.setPayload(String.valueOf(request.getPayload()));
            }
            integrationRequestRepository.save(entity);

            saveLog(requestId, null, EventType.INITIATED,
                    "통합 요청 시작: " + request.getProtocols());

            entity.setStatus(RequestStatus.PROCESSING);
            integrationRequestRepository.save(entity);
            return entity;
        });
    }

    private CompletionTimes saveFinalResults(
            IntegrationRequest entity,
            Map<String, ProtocolResultDTO> results,
            OverallStatus overallStatus
    ) {
        return transactionTemplate.execute(status -> {
            String requestId = entity.getRequestId();

            results.forEach((protocol, result) -> {
                ProtocolType protocolType = ProtocolType.valueOf(protocol);
                saveProtocolResult(requestId, protocolType, result);
                saveLog(requestId, protocolType, toEventType(result),
                        protocol + " 처리 결과: " + result.getStatus());
            });

            entity.setStatus(RequestStatus.COMPLETED);
            entity.setOverallStatus(overallStatus);
            entity.setCompletedAt(LocalDateTime.now());
            integrationRequestRepository.save(entity);

            EventType finalEvent = overallStatus == OverallStatus.ALL_FAILED
                    ? EventType.FAILED : EventType.SUCCESS;
            saveLog(requestId, null, finalEvent,
                    "통합 요청 완료: " + overallStatus);

            return new CompletionTimes(entity.getCreatedAt(), entity.getCompletedAt());
        });
    }

    private record CompletionTimes(LocalDateTime createdAt, LocalDateTime completedAt) {}

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

    @Transactional(readOnly = true)
    public LogResponseDTO getLogs(ProtocolType protocolType, int limit, int offset) {
        Pageable pageable = new OffsetBasedPageRequest(
                offset, limit, Sort.by("timestamp").descending());

        Page<SystemLog> page = protocolType != null
                ? systemLogRepository.findByProtocol(protocolType, pageable)
                : systemLogRepository.findAll(pageable);

        List<LogResponseDTO.LogItem> logItems = page.getContent().stream()
                .map(l -> new LogResponseDTO.LogItem(
                        l.getId(),
                        l.getRequestId(),
                        l.getProtocol() != null ? l.getProtocol().name() : null,
                        l.getEventType().name(),
                        l.getEventDetail(),
                        l.getTimestamp()
                ))
                .collect(Collectors.toList());

        return new LogResponseDTO(page.getTotalElements(), limit, offset, logItems);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Offset-based pagination (서비스 내부에서만 사용)
    // ─────────────────────────────────────────────────────────────────────

    private static class OffsetBasedPageRequest extends AbstractPageRequest {

        private final long offset;
        private final Sort sort;

        private OffsetBasedPageRequest(long offset, int limit, Sort sort) {
            super((int) (offset / limit), limit);
            if (offset < 0) {
                throw new IllegalArgumentException("Offset must not be negative");
            }
            this.offset = offset;
            this.sort = sort;
        }

        @Override public long getOffset() { return offset; }
        @Override public Sort getSort()   { return sort; }

        @Override
        public Pageable next() {
            return new OffsetBasedPageRequest(offset + getPageSize(), getPageSize(), sort);
        }

        @Override
        public Pageable previous() {
            return hasPrevious()
                    ? new OffsetBasedPageRequest(offset - getPageSize(), getPageSize(), sort)
                    : this;
        }

        @Override
        public Pageable first() {
            return new OffsetBasedPageRequest(0, getPageSize(), sort);
        }

        @Override
        public Pageable withPage(int pageNumber) {
            return new OffsetBasedPageRequest((long) pageNumber * getPageSize(), getPageSize(), sort);
        }
    }
}
