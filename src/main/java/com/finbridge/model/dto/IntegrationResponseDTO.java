package com.finbridge.model.dto;

import com.finbridge.model.enums.OverallStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationResponseDTO {

    private String requestId;

    // ALL_SUCCESS, PARTIAL_FAILURE, ALL_FAILED
    private OverallStatus overallStatus;

    // 프로토콜별 실행 결과 (예: { "SOAP": {...}, "KAFKA": {...} })
    private Map<String, ProtocolResultDTO> results;

    private LocalDateTime timestamp;
}
