package com.finbridge.model.dto;

import com.finbridge.model.enums.ProtocolType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InterfaceConfigDTO {

    private Long id;

    private ProtocolType protocol;

    private String interfaceName;

    private String endpoint;

    private Boolean enabled;

    private Integer timeoutMs;

    private String description;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;
}
