package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProtocolPerformanceDTO {

    private String protocol;

    private Long totalCount;

    private Long successCount;

    private Long failedCount;

    private Long timeoutCount;

    private Double successRate;

    private Double averageExecutionTimeMs;
}
