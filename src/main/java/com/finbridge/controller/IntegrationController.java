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
import org.springframework.data.domain.AbstractPageRequest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
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
    private final SystemLogRepository systemLogRepository;

    // POST /api/integrate
    @PostMapping("/integrate")
    public ResponseEntity<?> integrate(@RequestBody IntegrationRequestDTO request) {
        // P2: null/빈 목록 또는 지원하지 않는 프로토콜 → 400 Bad Request
        if (request.getProtocols() == null || request.getProtocols().isEmpty()) {
            return ResponseEntity.badRequest().body("protocols 목록이 비어 있습니다.");
        }

        List<String> normalizedProtocols = request.getProtocols().stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(p -> !p.isBlank())
                .map(p -> p.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
        if (normalizedProtocols.size() != request.getProtocols().size() || normalizedProtocols.isEmpty()) {
            return ResponseEntity.badRequest().body("protocols에는 null 또는 빈 값이 포함될 수 없습니다.");
        }

        List<String> invalid = normalizedProtocols.stream()
                .filter(p -> !SUPPORTED_PROTOCOLS.contains(p))
                .collect(Collectors.toList());
        if (!invalid.isEmpty()) {
            return ResponseEntity.badRequest()
                    .body("지원하지 않는 프로토콜: " + invalid + ". 지원 목록: " + SUPPORTED_PROTOCOLS);
        }
        // 대소문자 정규화 후 처리
        request.setProtocols(normalizedProtocols);

        log.info("[API] POST /api/integrate - protocols={}", request.getProtocols());
        IntegrationResponseDTO response = integrationService.processIntegration(request);
        return ResponseEntity.ok(response);
    }

    // GET /api/integrate/{requestId}
    @GetMapping("/integrate/{requestId}")
    public ResponseEntity<IntegrationResponseDTO> getStatus(
            @PathVariable String requestId) {

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

        Pageable pageable = new OffsetBasedPageRequest(offset, limit, Sort.by("timestamp").descending());
        Page<SystemLog> page;

        if (protocol != null && !protocol.isBlank()) {
            // I7: 잘못된 protocol 값은 500 대신 400 반환
            ProtocolType protocolType;
            try {
                protocolType = ProtocolType.valueOf(protocol.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return ResponseEntity.badRequest()
                        .body(new LogResponseDTO(0L, limit, offset, List.of()));
            }
            page = systemLogRepository.findByProtocol(protocolType, pageable);
        } else {
            page = systemLogRepository.findAll(pageable);
        }

        long total = page.getTotalElements();

        List<LogResponseDTO.LogItem> logItems = page.getContent().stream()
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

    private static class OffsetBasedPageRequest extends AbstractPageRequest {

        private final long offset;
        private final Sort sort;

        private OffsetBasedPageRequest(long offset, int limit, Sort sort) {
            super((int) (offset / limit), limit);
            if (offset < 0) {
                throw new IllegalArgumentException("Offset must not be negative");
            }
            this.offset = offset;
            this.sort = sort;
        }

        @Override
        public long getOffset() {
            return offset;
        }

        @Override
        public Sort getSort() {
            return sort;
        }

        @Override
        public Pageable next() {
            return new OffsetBasedPageRequest(offset + getPageSize(), getPageSize(), sort);
        }

        @Override
        public Pageable previous() {
            return hasPrevious()
                    ? new OffsetBasedPageRequest(offset - getPageSize(), getPageSize(), sort)
                    : this;
        }

        @Override
        public Pageable first() {
            return new OffsetBasedPageRequest(0, getPageSize(), sort);
        }

        @Override
        public Pageable withPage(int pageNumber) {
            return new OffsetBasedPageRequest((long) pageNumber * getPageSize(), getPageSize(), sort);
        }
    }
}
