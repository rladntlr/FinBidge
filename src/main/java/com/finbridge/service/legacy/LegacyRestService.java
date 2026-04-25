package com.finbridge.service.legacy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class LegacyRestService {

    public Map<String, Object> echo(Map<String, Object> payload) {
        log.info("[REST Legacy] 요청 처리: {}", payload);
        return Map.of(
                "status", "SUCCESS",
                "message", "REST 외부 시스템 처리 완료",
                "receivedPayload", payload
        );
    }
}
