package com.finbridge.controller;

import com.finbridge.model.dto.InterfaceConfigDTO;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.service.InterfaceConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

@RestController
@RequestMapping("/api/interfaces")
@Slf4j
@RequiredArgsConstructor
public class InterfaceConfigController {

    private final InterfaceConfigService interfaceConfigService;

    @GetMapping
    public ResponseEntity<?> getInterfaces(@RequestParam(required = false) String protocol) {
        ProtocolType protocolType;
        try {
            protocolType = parseProtocol(protocol);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        List<InterfaceConfigDTO> interfaces = interfaceConfigService.getInterfaces(protocolType);
        return ResponseEntity.ok(interfaces);
    }

    @PostMapping
    public ResponseEntity<?> createInterface(@RequestBody InterfaceConfigDTO request) {
        log.info("[API] POST /api/interfaces - protocol={}, name={}",
                request != null ? request.getProtocol() : null,
                request != null ? request.getInterfaceName() : null);

        try {
            return ResponseEntity.ok(interfaceConfigService.createInterface(request));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateInterface(
            @PathVariable Long id,
            @RequestBody InterfaceConfigDTO request) {
        log.info("[API] PUT /api/interfaces/{}", id);

        try {
            Optional<InterfaceConfigDTO> updated = interfaceConfigService.updateInterface(id, request);
            return updated.<ResponseEntity<?>>map(ResponseEntity::ok)
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    private ProtocolType parseProtocol(String protocol) {
        if (protocol == null || protocol.isBlank()) {
            return null;
        }
        try {
            return ProtocolType.valueOf(protocol.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("지원하지 않는 프로토콜: " + protocol);
        }
    }
}
