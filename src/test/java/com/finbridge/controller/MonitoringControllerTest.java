package com.finbridge.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finbridge.model.dto.LogResponseDTO;
import com.finbridge.model.dto.MonitoringSummaryDTO;
import com.finbridge.model.dto.ProtocolPerformanceDTO;
import com.finbridge.service.MonitoringService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class MonitoringControllerTest {

    @Mock
    private MonitoringService monitoringService;

    @InjectMocks
    private MonitoringController controller;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(
                        new StringHttpMessageConverter(StandardCharsets.UTF_8),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    @DisplayName("GET /api/monitoring/summary는 모니터링 요약을 반환한다")
    void getSummary_returnsSummary() throws Exception {
        MonitoringSummaryDTO summary = new MonitoringSummaryDTO(
                10L,
                1L,
                9L,
                7L,
                1L,
                1L,
                30L,
                List.of(new LogResponseDTO.LogItem(
                        1L,
                        "req-1",
                        "REST",
                        "SUCCESS",
                        "REST 처리 결과: SUCCESS",
                        LocalDateTime.of(2026, 4, 26, 5, 0)
                ))
        );
        when(monitoringService.getSummary()).thenReturn(summary);

        mockMvc.perform(get("/api/monitoring/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalRequests").value(10))
                .andExpect(jsonPath("$.processingRequests").value(1))
                .andExpect(jsonPath("$.recentLogs[0].protocol").value("REST"));
    }

    @Test
    @DisplayName("GET /api/performance/protocols는 프로토콜별 성능 통계를 반환한다")
    void getProtocolPerformance_returnsStats() throws Exception {
        when(monitoringService.getProtocolPerformance())
                .thenReturn(List.of(new ProtocolPerformanceDTO(
                        "SFTP",
                        5L,
                        4L,
                        1L,
                        0L,
                        80.0,
                        123.45
                )));

        mockMvc.perform(get("/api/performance/protocols"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].protocol").value("SFTP"))
                .andExpect(jsonPath("$[0].successRate").value(80.0))
                .andExpect(jsonPath("$[0].averageExecutionTimeMs").value(123.45));
    }
}
