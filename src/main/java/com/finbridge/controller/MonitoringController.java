package com.finbridge.controller;

import com.finbridge.model.dto.MonitoringSummaryDTO;
import com.finbridge.model.dto.ProtocolPerformanceDTO;
import com.finbridge.service.MonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
@Slf4j
@RequiredArgsConstructor
public class MonitoringController {

    private final MonitoringService monitoringService;

    @GetMapping("/monitoring/summary")
    public ResponseEntity<MonitoringSummaryDTO> getSummary() {
        log.info("[API] GET /api/monitoring/summary");
        return ResponseEntity.ok(monitoringService.getSummary());
    }

    @GetMapping("/performance/protocols")
    public ResponseEntity<List<ProtocolPerformanceDTO>> getProtocolPerformance() {
        log.info("[API] GET /api/performance/protocols");
        return ResponseEntity.ok(monitoringService.getProtocolPerformance());
    }
}
