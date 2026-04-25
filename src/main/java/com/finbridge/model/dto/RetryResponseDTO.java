package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RetryResponseDTO {

    private String originalRequestId;

    private String retryRequestId;

    private IntegrationResponseDTO retryResponse;
}
