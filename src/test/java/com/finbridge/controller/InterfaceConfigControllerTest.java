package com.finbridge.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finbridge.model.dto.InterfaceConfigDTO;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.service.InterfaceConfigService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@ExtendWith(MockitoExtension.class)
class InterfaceConfigControllerTest {

    @Mock
    private InterfaceConfigService interfaceConfigService;

    @InjectMocks
    private InterfaceConfigController controller;

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

    @Nested
    @DisplayName("GET /api/interfaces")
    class GetInterfaces {

        @Test
        @DisplayName("등록된 인터페이스 목록을 조회한다")
        void getInterfaces_returnsList() throws Exception {
            when(interfaceConfigService.getInterfaces(null))
                    .thenReturn(List.of(buildDto(1L, ProtocolType.REST, "REST Interface")));

            mockMvc.perform(get("/api/interfaces"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].id").value(1))
                    .andExpect(jsonPath("$[0].protocol").value("REST"));
        }

        @Test
        @DisplayName("protocol query는 소문자도 허용한다")
        void getInterfaces_lowercaseProtocol_returnsList() throws Exception {
            when(interfaceConfigService.getInterfaces(ProtocolType.SFTP))
                    .thenReturn(List.of(buildDto(2L, ProtocolType.SFTP, "SFTP Interface")));

            mockMvc.perform(get("/api/interfaces").param("protocol", "sftp"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$[0].protocol").value("SFTP"));
        }

        @Test
        @DisplayName("잘못된 protocol query는 400을 반환한다")
        void getInterfaces_invalidProtocol_returns400() throws Exception {
            mockMvc.perform(get("/api/interfaces").param("protocol", "GRPC"))
                    .andExpect(status().isBadRequest());
        }
    }

    @Nested
    @DisplayName("POST /api/interfaces")
    class CreateInterface {

        @Test
        @DisplayName("인터페이스 설정을 등록한다")
        void createInterface_returnsCreatedConfig() throws Exception {
            when(interfaceConfigService.createInterface(any()))
                    .thenReturn(buildDto(10L, ProtocolType.KAFKA, "Kafka Topic"));

            mockMvc.perform(post("/api/interfaces")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {
                                      "protocol": "KAFKA",
                                      "interfaceName": "Kafka Topic",
                                      "endpoint": "integration-events",
                                      "enabled": true,
                                      "timeoutMs": 5000
                                    }
                                    """))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.id").value(10))
                    .andExpect(jsonPath("$.protocol").value("KAFKA"));
        }

        @Test
        @DisplayName("서비스 검증 실패는 400을 반환한다")
        void createInterface_validationFailure_returns400() throws Exception {
            when(interfaceConfigService.createInterface(any()))
                    .thenThrow(new IllegalArgumentException("endpoint는 필수입니다."));

            mockMvc.perform(post("/api/interfaces")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {
                                      "protocol": "REST",
                                      "interfaceName": "REST Interface",
                                      "endpoint": ""
                                    }
                                    """))
                    .andExpect(status().isBadRequest())
                    .andExpect(content().string("endpoint는 필수입니다."));
        }
    }

    @Nested
    @DisplayName("PUT /api/interfaces/{id}")
    class UpdateInterface {

        @Test
        @DisplayName("기존 인터페이스 설정을 수정한다")
        void updateInterface_returnsUpdatedConfig() throws Exception {
            when(interfaceConfigService.updateInterface(any(), any()))
                    .thenReturn(Optional.of(buildDto(1L, ProtocolType.REST, "REST Interface V2")));

            mockMvc.perform(put("/api/interfaces/1")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {
                                      "protocol": "REST",
                                      "interfaceName": "REST Interface V2",
                                      "endpoint": "legacy-rest-v2",
                                      "enabled": false,
                                      "timeoutMs": 10000
                                    }
                                    """))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.interfaceName").value("REST Interface V2"));
        }

        @Test
        @DisplayName("없는 id를 수정하면 404를 반환한다")
        void updateInterface_notFound_returns404() throws Exception {
            when(interfaceConfigService.updateInterface(any(), any()))
                    .thenReturn(Optional.empty());

            mockMvc.perform(put("/api/interfaces/99")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {
                                      "protocol": "REST",
                                      "interfaceName": "REST Interface",
                                      "endpoint": "legacy-rest-service"
                                    }
                                    """))
                    .andExpect(status().isNotFound());
        }
    }

    private InterfaceConfigDTO buildDto(Long id, ProtocolType protocol, String interfaceName) {
        return new InterfaceConfigDTO(
                id,
                protocol,
                interfaceName,
                "endpoint",
                true,
                35_000,
                "description",
                null,
                null
        );
    }
}
