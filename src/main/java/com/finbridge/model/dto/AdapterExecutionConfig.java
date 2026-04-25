package com.finbridge.model.dto;

import com.finbridge.model.enums.ProtocolType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdapterExecutionConfig {

    private ProtocolType protocol;

    private String interfaceName;

    private String endpoint;

    private Integer timeoutMs;
}
