package com.finbridge.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/internal")
@Slf4j
public class InternalEchoController {

    @PostMapping("/echo")
    public ResponseEntity<Map<String, Object>> echo(@RequestBody Map<String, Object> payload) {
        log.info("[REST Echo] 요청 수신: {}", payload);

        Map<String, Object> response = Map.of(
                "status", "SUCCESS",
                "message", "REST 외부 시스템 처리 완료",
                "receivedPayload", payload
        );

        return ResponseEntity.ok(response);
    }
}
