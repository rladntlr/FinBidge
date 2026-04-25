package com.finbridge.service;

import com.finbridge.model.dto.LogResponseDTO;
import com.finbridge.model.dto.MonitoringSummaryDTO;
import com.finbridge.model.dto.ProtocolPerformanceDTO;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.RequestStatus;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.ProtocolResultRepository;
import com.finbridge.repository.SystemLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;

@Service
@RequiredArgsConstructor
public class MonitoringService {

    private static final int RECENT_LOG_LIMIT = 10;

    private final IntegrationRequestRepository integrationRequestRepository;
    private final ProtocolResultRepository protocolResultRepository;
    private final SystemLogRepository systemLogRepository;

    @Transactional(readOnly = true)
    public MonitoringSummaryDTO getSummary() {
        List<LogResponseDTO.LogItem> recentLogs = systemLogRepository
                .findAll(PageRequest.of(0, RECENT_LOG_LIMIT, Sort.by("timestamp").descending()))
                .getContent()
                .stream()
                .map(this::toLogItem)
                .toList();

        return new MonitoringSummaryDTO(
                integrationRequestRepository.count(),
                integrationRequestRepository.countByStatus(RequestStatus.PROCESSING),
                integrationRequestRepository.countByStatus(RequestStatus.COMPLETED),
                integrationRequestRepository.countByOverallStatus(OverallStatus.ALL_SUCCESS),
                integrationRequestRepository.countByOverallStatus(OverallStatus.PARTIAL_FAILURE),
                integrationRequestRepository.countByOverallStatus(OverallStatus.ALL_FAILED),
                systemLogRepository.count(),
                recentLogs
        );
    }

    @Transactional(readOnly = true)
    public List<ProtocolPerformanceDTO> getProtocolPerformance() {
        return Arrays.stream(ProtocolType.values())
                .map(this::toPerformanceDto)
                .toList();
    }

    private ProtocolPerformanceDTO toPerformanceDto(ProtocolType protocol) {
        long totalCount = protocolResultRepository.countByProtocol(protocol);
        long successCount = protocolResultRepository.countByProtocolAndStatus(protocol, ResultStatus.SUCCESS);
        long failedCount = protocolResultRepository.countByProtocolAndStatus(protocol, ResultStatus.FAILED);
        long timeoutCount = protocolResultRepository.countByProtocolAndStatus(protocol, ResultStatus.TIMEOUT);
        double successRate = totalCount == 0 ? 0.0 : (successCount * 100.0) / totalCount;
        Double avg = protocolResultRepository.averageExecutionTimeMsByProtocol(protocol);

        return new ProtocolPerformanceDTO(
                protocol.name(),
                totalCount,
                successCount,
                failedCount,
                timeoutCount,
                Math.round(successRate * 100.0) / 100.0,
                avg == null ? 0.0 : Math.round(avg * 100.0) / 100.0
        );
    }

    private LogResponseDTO.LogItem toLogItem(SystemLog log) {
        return new LogResponseDTO.LogItem(
                log.getId(),
                log.getRequestId(),
                log.getProtocol() != null ? log.getProtocol().name() : null,
                log.getEventType().name(),
                log.getEventDetail(),
                log.getTimestamp()
        );
    }
}
