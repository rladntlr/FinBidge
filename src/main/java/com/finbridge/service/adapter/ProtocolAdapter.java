package com.finbridge.service.adapter;

import com.finbridge.model.dto.ProtocolResultDTO;

import java.util.Map;

public interface ProtocolAdapter {

    ProtocolResultDTO execute(String requestId, Map<String, Object> payload);
}
