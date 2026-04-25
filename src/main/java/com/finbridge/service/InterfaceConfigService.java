package com.finbridge.service;

import com.finbridge.model.dto.InterfaceConfigDTO;
import com.finbridge.model.entity.InterfaceConfig;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.repository.InterfaceConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class InterfaceConfigService {

    private final InterfaceConfigRepository interfaceConfigRepository;

    @Transactional(readOnly = true)
    public List<InterfaceConfigDTO> getInterfaces(ProtocolType protocol) {
        List<InterfaceConfig> configs = protocol != null
                ? interfaceConfigRepository.findByProtocol(protocol)
                : interfaceConfigRepository.findAll(Sort.by("protocol").ascending()
                        .and(Sort.by("interfaceName").ascending()));

        return configs.stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional
    public InterfaceConfigDTO createInterface(InterfaceConfigDTO dto) {
        validate(dto);
        if (interfaceConfigRepository.existsByProtocolAndInterfaceName(
                dto.getProtocol(), dto.getInterfaceName().trim())) {
            throw new IllegalArgumentException("이미 등록된 인터페이스입니다.");
        }

        InterfaceConfig entity = new InterfaceConfig();
        apply(dto, entity);
        return toDto(interfaceConfigRepository.save(entity));
    }

    @Transactional
    public Optional<InterfaceConfigDTO> updateInterface(Long id, InterfaceConfigDTO dto) {
        validate(dto);
        return interfaceConfigRepository.findById(id)
                .map(entity -> {
                    Optional<InterfaceConfig> duplicate =
                            interfaceConfigRepository.findByProtocolAndInterfaceName(
                                    dto.getProtocol(), dto.getInterfaceName().trim());
                    if (duplicate.isPresent() && !duplicate.get().getId().equals(id)) {
                        throw new IllegalArgumentException("이미 등록된 인터페이스입니다.");
                    }

                    apply(dto, entity);
                    return toDto(interfaceConfigRepository.save(entity));
                });
    }

    private void validate(InterfaceConfigDTO dto) {
        if (dto == null) {
            throw new IllegalArgumentException("인터페이스 설정이 비어 있습니다.");
        }
        if (dto.getProtocol() == null) {
            throw new IllegalArgumentException("protocol은 필수입니다.");
        }
        if (dto.getInterfaceName() == null || dto.getInterfaceName().isBlank()) {
            throw new IllegalArgumentException("interfaceName은 필수입니다.");
        }
        if (dto.getEndpoint() == null || dto.getEndpoint().isBlank()) {
            throw new IllegalArgumentException("endpoint는 필수입니다.");
        }
        if (dto.getTimeoutMs() != null && dto.getTimeoutMs() <= 0) {
            throw new IllegalArgumentException("timeoutMs는 1 이상이어야 합니다.");
        }
    }

    private void apply(InterfaceConfigDTO dto, InterfaceConfig entity) {
        entity.setProtocol(dto.getProtocol());
        entity.setInterfaceName(dto.getInterfaceName().trim());
        entity.setEndpoint(dto.getEndpoint().trim());
        entity.setEnabled(dto.getEnabled() == null || dto.getEnabled());
        entity.setTimeoutMs(dto.getTimeoutMs() == null ? 35_000 : dto.getTimeoutMs());
        entity.setDescription(dto.getDescription());
    }

    private InterfaceConfigDTO toDto(InterfaceConfig entity) {
        return new InterfaceConfigDTO(
                entity.getId(),
                entity.getProtocol(),
                entity.getInterfaceName(),
                entity.getEndpoint(),
                entity.getEnabled(),
                entity.getTimeoutMs(),
                entity.getDescription(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }
}
