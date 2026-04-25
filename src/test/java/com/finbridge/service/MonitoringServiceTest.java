package com.finbridge.service;

import com.finbridge.model.dto.MonitoringSummaryDTO;
import com.finbridge.model.dto.ProtocolPerformanceDTO;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.EventType;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.RequestStatus;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.ProtocolResultRepository;
import com.finbridge.repository.SystemLogRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MonitoringServiceTest {

    @Mock private IntegrationRequestRepository integrationRequestRepository;
    @Mock private ProtocolResultRepository protocolResultRepository;
    @Mock private SystemLogRepository systemLogRepository;

    @InjectMocks
    private MonitoringService monitoringService;

    @Test
    @DisplayName("요청 상태 집계와 최근 로그를 반환한다")
    void getSummary_returnsRequestCountsAndRecentLogs() {
        SystemLog log = new SystemLog();
        log.setId(1L);
        log.setRequestId("req-1");
        log.setProtocol(ProtocolType.SFTP);
        log.setEventType(EventType.SUCCESS);
        log.setEventDetail("SFTP 처리 결과: SUCCESS");
        log.setTimestamp(LocalDateTime.of(2026, 4, 26, 5, 0));

        when(integrationRequestRepository.count()).thenReturn(10L);
        when(integrationRequestRepository.countByStatus(RequestStatus.PROCESSING)).thenReturn(2L);
        when(integrationRequestRepository.countByStatus(RequestStatus.COMPLETED)).thenReturn(8L);
        when(integrationRequestRepository.countByOverallStatus(OverallStatus.ALL_SUCCESS)).thenReturn(6L);
        when(integrationRequestRepository.countByOverallStatus(OverallStatus.PARTIAL_FAILURE)).thenReturn(1L);
        when(integrationRequestRepository.countByOverallStatus(OverallStatus.ALL_FAILED)).thenReturn(1L);
        when(systemLogRepository.count()).thenReturn(30L);
        when(systemLogRepository.findAll(any(Pageable.class))).thenReturn(new PageImpl<>(List.of(log)));

        MonitoringSummaryDTO result = monitoringService.getSummary();

        assertThat(result.getTotalRequests()).isEqualTo(10L);
        assertThat(result.getProcessingRequests()).isEqualTo(2L);
        assertThat(result.getCompletedRequests()).isEqualTo(8L);
        assertThat(result.getAllSuccess()).isEqualTo(6L);
        assertThat(result.getPartialFailure()).isEqualTo(1L);
        assertThat(result.getAllFailed()).isEqualTo(1L);
        assertThat(result.getTotalLogs()).isEqualTo(30L);
        assertThat(result.getRecentLogs()).hasSize(1);
        assertThat(result.getRecentLogs().get(0).getProtocol()).isEqualTo("SFTP");
    }

    @Test
    @DisplayName("프로토콜별 성공률과 평균 실행시간을 계산한다")
    void getProtocolPerformance_returnsProtocolStats() {
        lenient().when(protocolResultRepository.countByProtocol(ProtocolType.REST)).thenReturn(4L);
        lenient().when(protocolResultRepository.countByProtocolAndStatus(ProtocolType.REST, ResultStatus.SUCCESS)).thenReturn(3L);
        lenient().when(protocolResultRepository.countByProtocolAndStatus(ProtocolType.REST, ResultStatus.FAILED)).thenReturn(1L);
        lenient().when(protocolResultRepository.countByProtocolAndStatus(ProtocolType.REST, ResultStatus.TIMEOUT)).thenReturn(0L);
        lenient().when(protocolResultRepository.averageExecutionTimeMsByProtocol(ProtocolType.REST))
                .thenReturn(25.555);

        List<ProtocolPerformanceDTO> result = monitoringService.getProtocolPerformance();

        ProtocolPerformanceDTO rest = result.stream()
                .filter(item -> item.getProtocol().equals("REST"))
                .findFirst()
                .orElseThrow();

        assertThat(rest.getTotalCount()).isEqualTo(4L);
        assertThat(rest.getSuccessCount()).isEqualTo(3L);
        assertThat(rest.getFailedCount()).isEqualTo(1L);
        assertThat(rest.getTimeoutCount()).isEqualTo(0L);
        assertThat(rest.getSuccessRate()).isEqualTo(75.0);
        assertThat(rest.getAverageExecutionTimeMs()).isEqualTo(25.56);

        ProtocolPerformanceDTO soap = result.stream()
                .filter(item -> item.getProtocol().equals("SOAP"))
                .findFirst()
                .orElseThrow();
        assertThat(soap.getTotalCount()).isEqualTo(0L);
        assertThat(soap.getSuccessRate()).isEqualTo(0.0);
        assertThat(soap.getAverageExecutionTimeMs()).isEqualTo(0.0);
    }
}
