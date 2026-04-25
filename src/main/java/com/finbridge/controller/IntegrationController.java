package com.finbridge.controller;

import com.finbridge.model.dto.IntegrationRequestDTO;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.dto.LogResponseDTO;
import com.finbridge.model.dto.RetryRequestDTO;
import com.finbridge.model.dto.RetryResponseDTO;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.service.IntegrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Slf4j
@RequiredArgsConstructor
public class IntegrationController {

    private static final Set<String> SUPPORTED_PROTOCOLS =
            Set.of("SOAP", "KAFKA", "SFTP", "BATCH", "REST");

    private final IntegrationService integrationService;

    // POST /api/integrate
    @PostMapping("/integrate")
    public ResponseEntity<?> integrate(@RequestBody IntegrationRequestDTO request) {
        List<String> normalizedProtocols = normalizeProtocols(request.getProtocols());
        if (normalizedProtocols == null) {
            return ResponseEntity.badRequest().body("protocols 목록이 비어 있습니다.");
        }
        if (normalizedProtocols.isEmpty()) {
            return ResponseEntity.badRequest().body("protocols에는 null 또는 빈 값이 포함될 수 없습니다.");
        }

        ResponseEntity<?> validationError = validateSupportedProtocols(normalizedProtocols);
        if (validationError != null) {
            return validationError;
        }
        request.setProtocols(normalizedProtocols);

        log.info("[API] POST /api/integrate - protocols={}", request.getProtocols());
        IntegrationResponseDTO response = integrationService.processIntegration(request);
        return ResponseEntity.ok(response);
    }

    // POST /api/integrate/{requestId}/retry
    @PostMapping("/integrate/{requestId}/retry")
    public ResponseEntity<?> retry(
            @PathVariable String requestId,
            @RequestBody(required = false) RetryRequestDTO request) {

        List<String> normalizedProtocols = null;
        if (request != null && request.getProtocols() != null && !request.getProtocols().isEmpty()) {
            normalizedProtocols = normalizeProtocols(request.getProtocols());
            if (normalizedProtocols == null || normalizedProtocols.isEmpty()) {
                return ResponseEntity.badRequest().body("protocols에는 null 또는 빈 값이 포함될 수 없습니다.");
            }

            ResponseEntity<?> validationError = validateSupportedProtocols(normalizedProtocols);
            if (validationError != null) {
                return validationError;
            }
        }

        log.info("[API] POST /api/integrate/{}/retry - protocols={}", requestId, normalizedProtocols);
        try {
            Optional<RetryResponseDTO> response =
                    integrationService.retryIntegration(requestId, normalizedProtocols);
            return response.map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    // GET /api/integrate/{requestId}
    @GetMapping("/integrate/{requestId}")
    public ResponseEntity<IntegrationResponseDTO> getStatus(@PathVariable String requestId) {
        log.info("[API] GET /api/integrate/{}", requestId);
        Optional<IntegrationResponseDTO> response = integrationService.getStatus(requestId);
        return response.map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    // GET /api/logs?protocol=SOAP&limit=50&offset=0
    @GetMapping("/logs")
    public ResponseEntity<LogResponseDTO> getLogs(
            @RequestParam(required = false) String protocol,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {

        log.info("[API] GET /api/logs - protocol={}, limit={}, offset={}", protocol, limit, offset);

        if (limit <= 0 || offset < 0) {
            return ResponseEntity.badRequest()
                    .body(new LogResponseDTO(0L, limit, offset, List.of()));
        }

        ProtocolType protocolType = null;
        if (protocol != null && !protocol.isBlank()) {
            try {
                protocolType = ProtocolType.valueOf(protocol.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest()
                        .body(new LogResponseDTO(0L, limit, offset, List.of()));
            }
        }

        return ResponseEntity.ok(integrationService.getLogs(protocolType, limit, offset));
    }

    private List<String> normalizeProtocols(List<String> protocols) {
        if (protocols == null || protocols.isEmpty()) {
            return null;
        }

        List<String> normalizedProtocols = protocols.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(p -> !p.isBlank())
                .map(p -> p.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
        if (normalizedProtocols.size() != protocols.size()) {
            return List.of();
        }
        return normalizedProtocols;
    }

    private ResponseEntity<?> validateSupportedProtocols(List<String> protocols) {
        List<String> invalid = protocols.stream()
                .filter(p -> !SUPPORTED_PROTOCOLS.contains(p))
                .collect(Collectors.toList());
        if (invalid.isEmpty()) {
            return null;
        }
        return ResponseEntity.badRequest()
                .body("지원하지 않는 프로토콜: " + invalid + ". 지원 목록: " + SUPPORTED_PROTOCOLS);
    }
}
