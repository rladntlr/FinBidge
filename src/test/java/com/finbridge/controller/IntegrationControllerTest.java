package com.finbridge.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.repository.SystemLogRepository;
import com.finbridge.service.IntegrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class IntegrationControllerTest {

    @Mock
    private IntegrationService integrationService;

    @Mock
    private SystemLogRepository systemLogRepository;

    @InjectMocks
    private IntegrationController controller;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(
                        new StringHttpMessageConverter(StandardCharsets.UTF_8),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    // =========================================================================
    // POST /api/integrate
    // =========================================================================

    @Nested
    @DisplayName("POST /api/integrate")
    class Integrate {

        @Test
        @DisplayName("protocols 필드가 null이면 400을 반환한다")
        void nullProtocols_returns400() throws Exception {
            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":null,\"payload\":{}}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("protocols 목록이 비어 있으면 400을 반환한다")
        void emptyProtocols_returns400() throws Exception {
            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[],\"payload\":{}}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("protocols에 빈 문자열이 포함되면 400을 반환한다")
        void blankProtocol_returns400() throws Exception {
            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"\"],\"payload\":{}}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("protocols 목록에 null 항목이 섞이면 400을 반환한다")
        void nullItemInList_returns400() throws Exception {
            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"REST\",null],\"payload\":{}}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("지원하지 않는 프로토콜이면 400을 반환한다")
        void unsupportedProtocol_returns400() throws Exception {
            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"GRPC\"],\"payload\":{}}"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("소문자 프로토콜은 대소문자 정규화 후 정상 처리되어 200을 반환한다")
        void lowercaseProtocol_normalizedAndReturns200() throws Exception {
            IntegrationResponseDTO dto = new IntegrationResponseDTO(
                    "id-1", OverallStatus.ALL_SUCCESS, Map.of(), LocalDateTime.now());
            when(integrationService.processIntegration(any())).thenReturn(dto);

            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"rest\"],\"payload\":{}}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.overallStatus").value("ALL_SUCCESS"));
        }

        @Test
        @DisplayName("유효한 protocols 목록이면 200과 requestId를 반환한다")
        void validProtocols_returns200WithRequestId() throws Exception {
            IntegrationResponseDTO dto = new IntegrationResponseDTO(
                    "id-2", OverallStatus.ALL_SUCCESS, Map.of(), LocalDateTime.now());
            when(integrationService.processIntegration(any())).thenReturn(dto);

            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"SOAP\",\"KAFKA\"],\"payload\":{\"key\":\"v\"}}"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.requestId").value("id-2"));
        }

        @Test
        @DisplayName("5개 프로토콜 모두 지정해도 200을 반환한다")
        void allFiveProtocols_returns200() throws Exception {
            IntegrationResponseDTO dto = new IntegrationResponseDTO(
                    "id-3", OverallStatus.ALL_SUCCESS, Map.of(), LocalDateTime.now());
            when(integrationService.processIntegration(any())).thenReturn(dto);

            mockMvc.perform(post("/api/integrate")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"protocols\":[\"SOAP\",\"KAFKA\",\"SFTP\",\"BATCH\",\"REST\"],"
                                    + "\"payload\":{}}"))
                    .andExpect(status().isOk());
        }
    }

    // =========================================================================
    // GET /api/integrate/{requestId}
    // =========================================================================

    @Nested
    @DisplayName("GET /api/integrate/{requestId}")
    class GetStatus {

        @Test
        @DisplayName("존재하는 requestId이면 200과 DTO를 반환한다")
        void existingId_returns200() throws Exception {
            IntegrationResponseDTO dto = new IntegrationResponseDTO(
                    "abc-123", OverallStatus.ALL_SUCCESS, Map.of(), LocalDateTime.now());
            when(integrationService.getStatus("abc-123")).thenReturn(Optional.of(dto));

            mockMvc.perform(get("/api/integrate/abc-123"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.requestId").value("abc-123"))
                    .andExpect(jsonPath("$.overallStatus").value("ALL_SUCCESS"));
        }

        @Test
        @DisplayName("존재하지 않는 requestId이면 404를 반환한다")
        void nonExistingId_returns404() throws Exception {
            when(integrationService.getStatus(anyString())).thenReturn(Optional.empty());

            mockMvc.perform(get("/api/integrate/does-not-exist"))
                    .andExpect(status().isNotFound());
        }
    }

    // =========================================================================
    // GET /api/logs
    // =========================================================================

    @Nested
    @DisplayName("GET /api/logs")
    class GetLogs {

        @Test
        @DisplayName("limit=0이면 400을 반환한다")
        void limitZero_returns400() throws Exception {
            mockMvc.perform(get("/api/logs").param("limit", "0"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("limit이 음수이면 400을 반환한다")
        void negativeLimit_returns400() throws Exception {
            mockMvc.perform(get("/api/logs").param("limit", "-5"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("offset이 음수이면 400을 반환한다")
        void negativeOffset_returns400() throws Exception {
            mockMvc.perform(get("/api/logs").param("offset", "-1"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("존재하지 않는 protocol 값이면 400을 반환한다")
        void invalidProtocol_returns400() throws Exception {
            mockMvc.perform(get("/api/logs").param("protocol", "NOTEXIST"))
                    .andExpect(status().isBadRequest());
        }

        @Test
        @DisplayName("파라미터 없이 호출하면 200과 빈 logs 배열을 반환한다")
        void defaultParams_returns200WithEmptyList() throws Exception {
            when(systemLogRepository.findAll(any(Pageable.class)))
                    .thenReturn(new PageImpl<>(List.of()));

            mockMvc.perform(get("/api/logs"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.total").value(0))
                    .andExpect(jsonPath("$.limit").value(50))
                    .andExpect(jsonPath("$.offset").value(0))
                    .andExpect(jsonPath("$.logs").isArray());
        }

        @Test
        @DisplayName("유효한 protocol 파라미터로 조회하면 200을 반환한다")
        void validProtocol_returns200() throws Exception {
            when(systemLogRepository.findByProtocol(eq(ProtocolType.SOAP), any(Pageable.class)))
                    .thenReturn(new PageImpl<>(List.of()));

            mockMvc.perform(get("/api/logs").param("protocol", "SOAP"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.total").value(0));
        }

        @Test
        @DisplayName("소문자 protocol 파라미터도 대소문자 정규화 후 200을 반환한다")
        void lowercaseProtocol_returns200() throws Exception {
            when(systemLogRepository.findByProtocol(eq(ProtocolType.KAFKA), any(Pageable.class)))
                    .thenReturn(new PageImpl<>(List.of()));

            mockMvc.perform(get("/api/logs").param("protocol", "kafka"))
                    .andExpect(status().isOk());
        }

        @Test
        @DisplayName("limit과 offset을 지정하면 응답에 그 값이 반영된다")
        void customLimitOffset_reflectedInResponse() throws Exception {
            when(systemLogRepository.findAll(any(Pageable.class)))
                    .thenReturn(new PageImpl<>(List.of()));

            mockMvc.perform(get("/api/logs").param("limit", "10").param("offset", "20"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.limit").value(10))
                    .andExpect(jsonPath("$.offset").value(20));
        }
    }
}
