package com.finbridge.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.AdapterExecutionConfig;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.dto.RetryResponseDTO;
import com.finbridge.model.entity.InterfaceConfig;
import com.finbridge.model.entity.IntegrationRequest;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.EventType;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.InterfaceConfigRepository;
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
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IntegrationServiceTest {

    @Mock private IntegrationRequestRepository integrationRequestRepository;
    @Mock private InterfaceConfigRepository     interfaceConfigRepository;
    @Mock private ProtocolResultRepository      protocolResultRepository;
    @Mock private SystemLogRepository           systemLogRepository;
    @Mock private SoapAdapterService            soapAdapterService;
    @Mock private KafkaAdapterService           kafkaAdapterService;
    @Mock private SftpAdapterService            sftpAdapterService;
    @Mock private BatchAdapterService           batchAdapterService;
    @Mock private RestAdapterService            restAdapterService;
    @Mock private ObjectMapper                  objectMapper;
    @Mock private ExecutorService               integrationTaskExecutor;
    @Mock private TransactionTemplate           transactionTemplate;

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

        lenient().doAnswer(inv -> {
            TransactionCallback<?> callback = inv.getArgument(0);
            return callback.doInTransaction(null);
        }).when(transactionTemplate).execute(any());
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

    @Test
    @DisplayName("enabled=false인 프로토콜은 Adapter를 실행하지 않고 DISABLED 결과로 저장한다")
    void processIntegration_disabledProtocol_skipsAdapterAndPersistsDisabledResult() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        InterfaceConfig disabledRest = new InterfaceConfig();
        disabledRest.setProtocol(ProtocolType.REST);
        disabledRest.setEnabled(false);
        when(interfaceConfigRepository.findByProtocol(ProtocolType.REST))
                .thenReturn(List.of(disabledRest));

        var request = buildRequest(List.of("REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        verify(restAdapterService, never()).execute(anyString(), any());
        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_FAILED);
        assertThat(response.getResults()).containsKey("REST");
        assertThat(response.getResults().get("REST").getStatus()).isEqualTo(ResultStatus.FAILED);
        assertThat(response.getResults().get("REST").getResponseCode()).isEqualTo("DISABLED");
        assertThat(response.getResults().get("REST").getResponseMessage())
                .isEqualTo("비활성화된 인터페이스입니다.");

        var resultCaptor = org.mockito.ArgumentCaptor.forClass(ProtocolResult.class);
        verify(protocolResultRepository).save(resultCaptor.capture());
        assertThat(resultCaptor.getValue().getProtocol()).isEqualTo(ProtocolType.REST);
        assertThat(resultCaptor.getValue().getResponseCode()).isEqualTo("DISABLED");
    }

    @Test
    @DisplayName("프로토콜 설정이 없으면 기존처럼 Adapter를 실행한다")
    void processIntegration_noInterfaceConfig_runsAdapter() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(interfaceConfigRepository.findByProtocol(ProtocolType.REST)).thenReturn(List.of());
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));

        var request = buildRequest(List.of("REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        verify(restAdapterService).execute(anyString(), any());
        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
        assertThat(response.getResults().get("REST").getStatus()).isEqualTo(ResultStatus.SUCCESS);
    }

    @Test
    @DisplayName("enabled=true 설정이 있으면 Adapter에 endpoint/timeout 설정을 전달한다")
    void processIntegration_enabledConfig_passesConfigToAdapter() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");

        InterfaceConfig restConfig = new InterfaceConfig();
        restConfig.setProtocol(ProtocolType.REST);
        restConfig.setInterfaceName("REST Config");
        restConfig.setEndpoint("configured-rest-endpoint");
        restConfig.setTimeoutMs(1234);
        restConfig.setEnabled(true);
        when(interfaceConfigRepository.findByProtocol(ProtocolType.REST))
                .thenReturn(List.of(restConfig));
        when(restAdapterService.execute(anyString(), any(), any(AdapterExecutionConfig.class)))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));

        var request = buildRequest(List.of("REST"), Map.of());
        IntegrationResponseDTO response = integrationService.processIntegration(request);

        verify(restAdapterService, never()).execute(anyString(), any());

        var configCaptor = org.mockito.ArgumentCaptor.forClass(AdapterExecutionConfig.class);
        verify(restAdapterService).execute(anyString(), any(), configCaptor.capture());
        assertThat(configCaptor.getValue().getProtocol()).isEqualTo(ProtocolType.REST);
        assertThat(configCaptor.getValue().getInterfaceName()).isEqualTo("REST Config");
        assertThat(configCaptor.getValue().getEndpoint()).isEqualTo("configured-rest-endpoint");
        assertThat(configCaptor.getValue().getTimeoutMs()).isEqualTo(1234);
        assertThat(response.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
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
    // retryIntegration
    // =========================================================================

    @Test
    @DisplayName("명시한 프로토콜만 원본 payload로 새 requestId를 만들어 재처리한다")
    void retryIntegration_explicitProtocols_createsNewRequest() throws Exception {
        IntegrationRequest original = new IntegrationRequest();
        original.setRequestId("origin-1");
        original.setPayload("{\"k\":\"v\"}");

        when(integrationRequestRepository.findByRequestId("origin-1"))
                .thenReturn(Optional.of(original));
        when(objectMapper.readValue(eq("{\"k\":\"v\"}"), any(TypeReference.class)))
                .thenReturn(Map.of("k", "v"));
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"k\":\"v\"}");
        when(restAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 10L));

        Optional<RetryResponseDTO> result =
                integrationService.retryIntegration("origin-1", List.of("REST"));

        assertThat(result).isPresent();
        assertThat(result.get().getOriginalRequestId()).isEqualTo("origin-1");
        assertThat(result.get().getRetryRequestId()).isNotBlank();
        assertThat(result.get().getRetryRequestId()).isNotEqualTo("origin-1");
        assertThat(result.get().getRetryResponse().getResults()).containsOnlyKeys("REST");
    }

    @Test
    @DisplayName("프로토콜을 생략하면 원본 요청의 실패/타임아웃 프로토콜만 재처리한다")
    void retryIntegration_withoutProtocols_retriesOnlyFailedResults() throws Exception {
        IntegrationRequest original = new IntegrationRequest();
        original.setRequestId("origin-2");
        original.setPayload("{}");

        ProtocolResult restSuccess = protocolResult(ProtocolType.REST, ResultStatus.SUCCESS);
        ProtocolResult sftpFailed = protocolResult(ProtocolType.SFTP, ResultStatus.FAILED);
        ProtocolResult kafkaTimeout = protocolResult(ProtocolType.KAFKA, ResultStatus.TIMEOUT);

        when(integrationRequestRepository.findByRequestId("origin-2"))
                .thenReturn(Optional.of(original));
        when(protocolResultRepository.findByRequestId("origin-2"))
                .thenReturn(List.of(restSuccess, sftpFailed, kafkaTimeout));
        when(objectMapper.readValue(eq("{}"), any(TypeReference.class))).thenReturn(Map.of());
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(sftpAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 20L));
        when(kafkaAdapterService.execute(anyString(), any()))
                .thenReturn(new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 30L));

        Optional<RetryResponseDTO> result =
                integrationService.retryIntegration("origin-2", null);

        assertThat(result).isPresent();
        assertThat(result.get().getRetryResponse().getResults())
                .containsOnlyKeys("SFTP", "KAFKA");
    }

    @Test
    @DisplayName("원본 requestId가 없으면 Optional.empty()를 반환한다")
    void retryIntegration_originalNotFound_returnsEmpty() {
        when(integrationRequestRepository.findByRequestId("missing"))
                .thenReturn(Optional.empty());

        Optional<RetryResponseDTO> result =
                integrationService.retryIntegration("missing", List.of("REST"));

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("재처리할 실패 프로토콜이 없으면 예외를 던진다")
    void retryIntegration_noFailedProtocols_throwsException() {
        IntegrationRequest original = new IntegrationRequest();
        original.setRequestId("origin-3");
        original.setPayload("{}");

        when(integrationRequestRepository.findByRequestId("origin-3"))
                .thenReturn(Optional.of(original));
        when(protocolResultRepository.findByRequestId("origin-3"))
                .thenReturn(List.of(protocolResult(ProtocolType.REST, ResultStatus.SUCCESS)));

        assertThatThrownBy(() -> integrationService.retryIntegration("origin-3", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("재처리할 프로토콜이 없습니다.");
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

    private ProtocolResult protocolResult(ProtocolType protocol, ResultStatus status) {
        ProtocolResult result = new ProtocolResult();
        result.setProtocol(protocol);
        result.setStatus(status);
        return result;
    }
}
