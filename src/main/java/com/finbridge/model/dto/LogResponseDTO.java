package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogResponseDTO {

    private Long total;         // 전체 로그 수

    private Integer limit;      // 페이지 크기

    private Integer offset;     // 시작 위치

    private List<LogItem> logs;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class LogItem {

        private Long id;

        private String requestId;

        private String protocol;    // SOAP, KAFKA, SFTP, BATCH (null 가능)

        private String eventType;   // INITIATED, SUCCESS, FAILED 등

        private String eventDetail;

        private LocalDateTime timestamp;
    }
}
