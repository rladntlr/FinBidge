package com.finbridge.controller;

import com.finbridge.model.dto.IntegrationRequestDTO;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.LogResponseDTO;
import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.repository.SystemLogRepository;
import com.finbridge.service.IntegrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Slf4j
@RequiredArgsConstructor
public class IntegrationController {

    private final IntegrationService integrationService;
    private final SystemLogRepository systemLogRepository;

    // POST /api/integrate
    @PostMapping("/integrate")
    public ResponseEntity<IntegrationResponseDTO> integrate(
            @RequestBody IntegrationRequestDTO request) {

        log.info("[API] POST /api/integrate - protocols={}", request.getProtocols());
        IntegrationResponseDTO response = integrationService.processIntegration(request);
        return ResponseEntity.ok(response);
    }

    // GET /api/integrate/{requestId}
    @GetMapping("/integrate/{requestId}")
    public ResponseEntity<IntegrationResponseDTO> getStatus(
            @PathVariable String requestId) {

        log.info("[API] GET /api/integrate/{}", requestId);
        IntegrationResponseDTO response = integrationService.getStatus(requestId);

        if (response == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(response);
    }

    // GET /api/logs?protocol=SOAP&limit=50&offset=0
    @GetMapping("/logs")
    public ResponseEntity<LogResponseDTO> getLogs(
            @RequestParam(required = false) String protocol,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {

        log.info("[API] GET /api/logs - protocol={}, limit={}, offset={}", protocol, limit, offset);

        List<SystemLog> logs;

        if (protocol != null && !protocol.isBlank()) {
            ProtocolType protocolType = ProtocolType.valueOf(protocol.toUpperCase());
            logs = systemLogRepository.findByProtocolOrderByTimestampDesc(protocolType);
        } else {
            logs = systemLogRepository.findAll(
                    PageRequest.of(offset / limit, limit,
                            org.springframework.data.domain.Sort.by("timestamp").descending())
            ).getContent();
        }

        long total = systemLogRepository.count();

        List<LogResponseDTO.LogItem> logItems = logs.stream()
                .skip(protocol != null ? offset : 0)
                .limit(limit)
                .map(l -> new LogResponseDTO.LogItem(
                        l.getId(),
                        l.getRequestId(),
                        l.getProtocol() != null ? l.getProtocol().name() : null,
                        l.getEventType().name(),
                        l.getEventDetail(),
                        l.getTimestamp()
                ))
                .collect(Collectors.toList());

        return ResponseEntity.ok(new LogResponseDTO(total, limit, offset, logItems));
    }
}
