package com.finbridge.model.dto;

import com.finbridge.model.enums.ResultStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProtocolResultDTO {

    private ResultStatus status;            // SUCCESS, FAILED, TIMEOUT, PENDING

    private String responseCode;            // HTTP 코드 또는 프로토콜 응답 코드

    private String responseMessage;         // 응답 메시지

    private Long executionTimeMs;           // 처리 시간 (밀리초)
}
