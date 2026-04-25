package com.finbridge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.entity.IntegrationRequest;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.EventType;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.ProtocolResultRepository;
import com.finbridge.repository.SystemLogRepository;
import com.finbridge.service.adapter.BatchAdapterService;
import com.finbridge.service.adapter.KafkaAdapterService;
import com.finbridge.service.adapter.RestAdapterService;
import com.finbridge.service.adapter.SftpAdapterService;
import com.finbridge.service.adapter.SoapAdapterService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IntegrationServiceTest {

    @Mock private IntegrationRequestRepository integrationRequestRepository;
    @Mock private ProtocolResultRepository      protocolResultRepository;
    @Mock private SystemLogRepository           systemLogRepository;
    @Mock private SoapAdapterService            soapAdapterService;
    @Mock private KafkaAdapterService           kafkaAdapterService;
    @Mock private SftpAdapterService            sftpAdapterService;
    @Mock private BatchAdapterService           batchAdapterService;
    @Mock private RestAdapterService            restAdapterService;
    @Mock private ObjectMapper                  objectMapper;
    @Mock private ExecutorService               integrationTaskExecutor;

    @InjectMocks
    private IntegrationService integrationService;

    /** CompletableFuture.supplyAsync(supplier, executor)는 내부적으로
     *  executor.execute(task)를 호출한다. Runnable을 즉시 실행하면
     *  Future가 동기적으로 완료되어 allOf().join()이 바로 반환된다.
     *  getStatus 테스트는 executor를 사용하지 않으므로 lenient()로 선언한다. */
    @BeforeEach
    void makeExecutorSynchronous() {
        lenient().doAnswer(inv -> {
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(integrationTaskExecutor).execute(any(Runnable.class));
    }

    // =========================================================================
    // processIntegration
    // =========================================================================

    @Test
    @DisplayName("단일 프로토콜 REST 성공 → overallStatus = ALL_SUCCESS")
    void singleProtocolSuccess_returnsAllSuccess() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 100L));

        var request = buildRequest(List.of("REST"), Map.of("k", "v"));
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
        assertThat(response.getResults()).containsKey("REST");
        assertThat(response.getResults().get("REST").getStatus()).isEqualTo(ResultStatus.SUCCESS);
    }

    @Test
    @DisplayName("SOAP + KAFKA 모두 성공 → ALL_SUCCESS")
    void twoProtocolsBothSuccess_returnsAllSuccess() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ProtocolResultDTO success = new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 50L);
        when(soapAdapterService.execute(anyString(), any())).thenReturn(success);
        when(kafkaAdapterService.execute(anyString(), any())).thenReturn(success);

        var request = buildRequest(List.of("SOAP", "KAFKA"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
        assertThat(response.getResults()).containsKeys("SOAP", "KAFKA");
    }

    @Test
    @DisplayName("SOAP 성공, REST 실패 → PARTIAL_FAILURE")
    void oneSuccessOneFail_returnsPartialFailure() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(soapAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 80L));
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.FAILED, "500", "Error", 10L));

        var request = buildRequest(List.of("SOAP", "REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.PARTIAL_FAILURE);
    }

    @Test
    @DisplayName("모든 프로토콜 실패 → ALL_FAILED")
    void allProtocolsFail_returnsAllFailed() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ProtocolResultDTO fail = new ProtocolResultDTO(ResultStatus.FAILED, "500", "Error", 5L);
        when(soapAdapterService.execute(anyString(), any())).thenReturn(fail);
        when(kafkaAdapterService.execute(anyString(), any())).thenReturn(fail);

        var request = buildRequest(List.of("SOAP", "KAFKA"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_FAILED);
    }

    @Test
    @DisplayName("processIntegration이 반환하는 requestId는 UUID 형식이다")
    void processIntegration_requestIdIsUuid() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));

        var request = buildRequest(List.of("REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        // UUID 패턴: 8-4-4-4-12
        assertThat(response.getRequestId())
                .matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    @DisplayName("5개 프로토콜 전부 지정하면 results에 5개 키가 존재한다")
    void allFiveProtocols_resultHasFiveKeys() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        ProtocolResultDTO ok = new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 30L);
        when(soapAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(kafkaAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(sftpAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(batchAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(restAdapterService.execute(anyString(), any())).thenReturn(ok);

        var request = buildRequest(List.of("SOAP", "KAFKA", "SFTP", "BATCH", "REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        assertThat(response.getResults()).hasSize(5);
        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
    }

    @Test
    @DisplayName("프로토콜 결과 저장은 요청한 프로토콜별로 IntegrationService가 한 번씩 수행한다")
    void processIntegration_persistsOneProtocolResultPerRequestedProtocol() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(soapAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.FAILED, "500", "Error", 20L));

        var request = buildRequest(List.of("SOAP", "REST"), Map.of("k", "v"));
        integrationService.processIntegration(request);

        var resultCaptor = org.mockito.ArgumentCaptor.forClass(ProtocolResult.class);
        verify(protocolResultRepository, times(2)).save(resultCaptor.capture());

        assertThat(resultCaptor.getAllValues())
                .extracting(ProtocolResult::getProtocol)
                .containsExactlyInAnyOrder(ProtocolType.SOAP, ProtocolType.REST);
        assertThat(resultCaptor.getAllValues())
                .extracting(ProtocolResult::getStatus)
                .containsExactlyInAnyOrder(ResultStatus.SUCCESS, ResultStatus.FAILED);
    }

    @Test
    @DisplayName("통합 요청 로그는 시작, 프로토콜별 결과, 최종 완료 순서로 저장된다")
    void processIntegration_persistsSystemLogsForLifecycle() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));

        var request = buildRequest(List.of("REST"), Map.of());
        integrationService.processIntegration(request);

        var logCaptor = org.mockito.ArgumentCaptor.forClass(SystemLog.class);
        verify(systemLogRepository, times(3)).save(logCaptor.capture());

        List<SystemLog> logs = logCaptor.getAllValues();
        assertThat(logs).extracting(SystemLog::getEventType)
                .containsExactly(EventType.INITIATED, EventType.SUCCESS, EventType.SUCCESS);
        assertThat(logs.get(1).getProtocol()).isEqualTo(ProtocolType.REST);
        assertThat(logs.get(2).getProtocol()).isNull();
    }

    // =========================================================================
    // getStatus
    // =========================================================================

    @Test
    @DisplayName("존재하는 requestId → Optional에 DTO가 담겨 반환된다")
    void getStatus_foundRequest_returnsDtoInOptional() {
        IntegrationRequest entity = new IntegrationRequest();
        entity.setRequestId("req-1");
        entity.setOverallStatus(OverallStatus.ALL_SUCCESS);

        ProtocolResult pr = new ProtocolResult();
        pr.setProtocol(com.finbridge.model.enums.ProtocolType.REST);
        pr.setStatus(ResultStatus.SUCCESS);
        pr.setResponseCode("200");
        pr.setResponseMessage("OK");
        pr.setExecutionTimeMs(55L);

        when(integrationRequestRepository.findByRequestId("req-1"))
                .thenReturn(Optional.of(entity));
        when(protocolResultRepository.findByRequestId("req-1"))
                .thenReturn(List.of(pr));

        Optional<IntegrationResponseDTO> result = integrationService.getStatus("req-1");

        assertThat(result).isPresent();
        assertThat(result.get().getRequestId()).isEqualTo("req-1");
        assertThat(result.get().getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
        assertThat(result.get().getResults()).containsKey("REST");
    }

    @Test
    @DisplayName("존재하지 않는 requestId → Optional.empty()가 반환된다")
    void getStatus_notFoundRequest_returnsEmpty() {
        when(integrationRequestRepository.findByRequestId("no-such-id"))
                .thenReturn(Optional.empty());

        Optional<IntegrationResponseDTO> result = integrationService.getStatus("no-such-id");

        assertThat(result).isEmpty();
    }

    // =========================================================================
    // helpers
    // =========================================================================

    private com.finbridge.model.dto.IntegrationRequestDTO buildRequest(
            List<String> protocols, Map<String, Object> payload) {
        var dto = new com.finbridge.model.dto.IntegrationRequestDTO();
        dto.setProtocols(protocols);
        dto.setPayload(payload);
        return dto;
    }
}
