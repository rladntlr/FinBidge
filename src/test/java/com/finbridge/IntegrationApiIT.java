package com.finbridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.ProtocolResultDTO;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 통합 테스트 (Integration Test)
 *
 * 전략: Spring 전체 컨텍스트 + H2 인메모리 DB + 외부 어댑터(MockBean)
 *   Controller → Service → Repository → H2 경로의 실제 통합을 검증한다.
 *   SOAP · Kafka · SFTP · Batch · REST 어댑터는 MockBean으로 대체한다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
class IntegrationApiIT {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    // ── 실제 Repository (H2에 실제로 저장/조회) ──────────────────────────
    @Autowired IntegrationRequestRepository integrationRequestRepository;
    @Autowired ProtocolResultRepository      protocolResultRepository;
    @Autowired SystemLogRepository           systemLogRepository;

    // ── 외부 시스템 어댑터 → MockitoBean으로 대체 ───────────────────────────
    @MockitoBean SoapAdapterService  soapAdapterService;
    @MockitoBean KafkaAdapterService kafkaAdapterService;
    @MockitoBean SftpAdapterService  sftpAdapterService;
    @MockitoBean BatchAdapterService batchAdapterService;
    @MockitoBean RestAdapterService  restAdapterService;

    // ─────────────────────────────────────────────────────────────────────
    // Setup / Cleanup
    // ─────────────────────────────────────────────────────────────────────

    @BeforeEach
    void cleanDatabase() {
        // 각 테스트가 독립적으로 실행되도록 매번 DB 초기화
        systemLogRepository.deleteAll();
        protocolResultRepository.deleteAll();
        integrationRequestRepository.deleteAll();
    }

    // ─────────────────────────────────────────────────────────────────────
    // POST /api/integrate
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[통합] POST → integration_requests와 protocol_results가 DB에 저장된다")
    void integrate_persistsRequestAndResultsToDb() throws Exception {
        when(soapAdapterService.execute(anyString(), any())).thenReturn(success());
        when(restAdapterService.execute(anyString(), any())).thenReturn(success());

        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"SOAP\",\"REST\"],\"payload\":{\"key\":\"value\"}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.overallStatus").value("ALL_SUCCESS"));

        // DB 검증: integration_requests 1건
        assertThat(integrationRequestRepository.count()).isEqualTo(1);
        var saved = integrationRequestRepository.findAll().get(0);
        assertThat(saved.getStatus().name()).isEqualTo("COMPLETED");
        assertThat(saved.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);

        // DB 검증: protocol_results 2건 (SOAP + REST)
        assertThat(protocolResultRepository.count()).isEqualTo(2);
        assertThat(protocolResultRepository.findAll())
                .extracting(r -> r.getProtocol())
                .containsExactlyInAnyOrder(ProtocolType.SOAP, ProtocolType.REST);
    }

    @Test
    @DisplayName("[통합] POST → GET /api/integrate/{requestId} → DB 데이터가 올바르게 조회된다")
    void integrate_thenGetStatus_returnsPersistedData() throws Exception {
        when(restAdapterService.execute(anyString(), any())).thenReturn(success());

        // POST로 요청 생성
        MvcResult postResult = mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"REST\"],\"payload\":{}}"))
                .andExpect(status().isOk())
                .andReturn();

        String responseBody = postResult.getResponse().getContentAsString();
        String requestId = objectMapper.readTree(responseBody).get("requestId").asText();

        // GET으로 동일 requestId 조회
        mockMvc.perform(get("/api/integrate/" + requestId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requestId").value(requestId))
                .andExpect(jsonPath("$.overallStatus").value("ALL_SUCCESS"))
                .andExpect(jsonPath("$.results.REST.status").value("SUCCESS"));
    }

    @Test
    @DisplayName("[통합] POST 후 GET /api/integrate/{nonExistentId} → 404")
    void getStatus_nonExistentId_returns404() throws Exception {
        mockMvc.perform(get("/api/integrate/no-such-id-xyz"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("[통합] 잘못된 프로토콜로 POST → 400, DB에 아무것도 저장되지 않는다")
    void integrate_invalidProtocol_nothingPersistedAndReturns400() throws Exception {
        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"INVALID\"],\"payload\":{}}"))
                .andExpect(status().isBadRequest());

        // 컨트롤러가 서비스를 호출하기 전에 리턴했으므로 DB는 비어 있어야 함
        assertThat(integrationRequestRepository.count()).isZero();
        assertThat(protocolResultRepository.count()).isZero();
        assertThat(systemLogRepository.count()).isZero();
    }

    @Test
    @DisplayName("[통합] 두 번 POST → 각각 독립적인 requestId, DB에 2건 저장")
    void twoIntegrations_haveIndependentRequestIds() throws Exception {
        when(restAdapterService.execute(anyString(), any())).thenReturn(success());

        String body = "{\"protocols\":[\"REST\"],\"payload\":{}}";

        MvcResult r1 = mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
        MvcResult r2 = mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();

        String id1 = objectMapper.readTree(r1.getResponse().getContentAsString()).get("requestId").asText();
        String id2 = objectMapper.readTree(r2.getResponse().getContentAsString()).get("requestId").asText();

        assertThat(id1).isNotEqualTo(id2);
        assertThat(integrationRequestRepository.count()).isEqualTo(2);
        assertThat(protocolResultRepository.count()).isEqualTo(2);
    }

    @Test
    @DisplayName("[통합] 5개 프로토콜 POST → protocol_results에 5건 저장, ALL_SUCCESS")
    void allFiveProtocols_savesAllFiveResults() throws Exception {
        ProtocolResultDTO ok = success();
        when(soapAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(kafkaAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(sftpAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(batchAdapterService.execute(anyString(), any())).thenReturn(ok);
        when(restAdapterService.execute(anyString(), any())).thenReturn(ok);

        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"SOAP\",\"KAFKA\",\"SFTP\",\"BATCH\",\"REST\"],"
                                + "\"payload\":{}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.overallStatus").value("ALL_SUCCESS"));

        assertThat(protocolResultRepository.count()).isEqualTo(5);
    }

    @Test
    @DisplayName("[통합] 어댑터 일부 실패 → DB에 PARTIAL_FAILURE로 저장된다")
    void partialFailure_persistedCorrectlyToDb() throws Exception {
        when(soapAdapterService.execute(anyString(), any())).thenReturn(success());
        when(restAdapterService.execute(anyString(), any())).thenReturn(failed());

        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"SOAP\",\"REST\"],\"payload\":{}}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.overallStatus").value("PARTIAL_FAILURE"));

        var request = integrationRequestRepository.findAll().get(0);
        assertThat(request.getOverallStatus()).isEqualTo(OverallStatus.PARTIAL_FAILURE);
    }

    // ─────────────────────────────────────────────────────────────────────
    // GET /api/logs
    // ─────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("[통합] POST 후 GET /api/logs → 시스템 로그가 DB에서 조회된다")
    void integrate_thenGetLogs_returnsPersistedLogs() throws Exception {
        when(restAdapterService.execute(anyString(), any())).thenReturn(success());

        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"REST\"],\"payload\":{}}"))
                .andExpect(status().isOk());

        // 로그는 INITIATED(1) + REST 결과(1) + 최종 SUCCESS(1) = 최소 3건
        mockMvc.perform(get("/api/logs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(org.hamcrest.Matchers.greaterThanOrEqualTo(3)))
                .andExpect(jsonPath("$.logs").isArray());
    }

    @Test
    @DisplayName("[통합] GET /api/logs?protocol=SOAP → SOAP 프로토콜 로그만 반환된다")
    void getLogs_withProtocolFilter_returnsOnlySoapLogs() throws Exception {
        when(soapAdapterService.execute(anyString(), any())).thenReturn(success());
        when(restAdapterService.execute(anyString(), any())).thenReturn(success());

        mockMvc.perform(post("/api/integrate")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"protocols\":[\"SOAP\",\"REST\"],\"payload\":{}}"))
                .andExpect(status().isOk());

        // protocol=SOAP 필터 → SOAP 결과 로그 1건만 반환
        mockMvc.perform(get("/api/logs").param("protocol", "SOAP"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.logs[0].protocol").value("SOAP"));
    }

    @Test
    @DisplayName("[통합] GET /api/logs?limit=0 → 400 (파라미터 검증이 전체 스택에서도 동작)")
    void getLogs_limitZero_returns400() throws Exception {
        mockMvc.perform(get("/api/logs").param("limit", "0"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("[통합] GET /api/logs?protocol=INVALID → 400 (전체 스택에서 enum 검증)")
    void getLogs_invalidProtocol_returns400() throws Exception {
        mockMvc.perform(get("/api/logs").param("protocol", "INVALID"))
                .andExpect(status().isBadRequest());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────

    private ProtocolResultDTO success() {
        return new ProtocolResultDTO(ResultStatus.SUCCESS, "200", "OK", 100L);
    }

    private ProtocolResultDTO failed() {
        return new ProtocolResultDTO(ResultStatus.FAILED, "500", "Error", 50L);
    }
}
