package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationRequestDTO {

    // 호출할 프로토콜 목록 (예: ["SOAP", "KAFKA", "SFTP", "BATCH"])
    private List<String> protocols;

    // 각 어댑터에 전달할 데이터
    private Map<String, Object> payload;
}
