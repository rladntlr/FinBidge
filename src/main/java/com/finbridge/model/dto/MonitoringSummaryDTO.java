package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonitoringSummaryDTO {

    private Long totalRequests;

    private Long processingRequests;

    private Long completedRequests;

    private Long allSuccess;

    private Long partialFailure;

    private Long allFailed;

    private Long totalLogs;

    private List<LogResponseDTO.LogItem> recentLogs;
}
