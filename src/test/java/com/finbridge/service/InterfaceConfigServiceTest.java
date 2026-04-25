package com.finbridge.service;

import com.finbridge.model.dto.InterfaceConfigDTO;
import com.finbridge.model.entity.InterfaceConfig;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.repository.InterfaceConfigRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InterfaceConfigServiceTest {

    @Mock
    private InterfaceConfigRepository interfaceConfigRepository;

    @InjectMocks
    private InterfaceConfigService interfaceConfigService;

    @Test
    @DisplayName("프로토콜 필터 없이 등록된 인터페이스 목록을 조회한다")
    void getInterfaces_withoutProtocol_returnsAll() {
        InterfaceConfig config = buildEntity(1L, ProtocolType.REST, "REST Interface", "legacy-rest-service");
        when(interfaceConfigRepository.findAll(any(org.springframework.data.domain.Sort.class)))
                .thenReturn(List.of(config));

        List<InterfaceConfigDTO> result = interfaceConfigService.getInterfaces(null);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getProtocol()).isEqualTo(ProtocolType.REST);
        assertThat(result.get(0).getInterfaceName()).isEqualTo("REST Interface");
    }

    @Test
    @DisplayName("프로토콜 필터가 있으면 해당 프로토콜만 조회한다")
    void getInterfaces_withProtocol_returnsFiltered() {
        InterfaceConfig config = buildEntity(1L, ProtocolType.SFTP, "SFTP Interface", "/upload");
        when(interfaceConfigRepository.findByProtocol(ProtocolType.SFTP))
                .thenReturn(List.of(config));

        List<InterfaceConfigDTO> result = interfaceConfigService.getInterfaces(ProtocolType.SFTP);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getEndpoint()).isEqualTo("/upload");
    }

    @Test
    @DisplayName("신규 인터페이스를 등록한다")
    void createInterface_savesConfig() {
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.KAFKA, "Kafka Topic", "integration-events",
                true, 5000, "Kafka topic", null, null);

        when(interfaceConfigRepository.existsByProtocolAndInterfaceName(ProtocolType.KAFKA, "Kafka Topic"))
                .thenReturn(false);
        when(interfaceConfigRepository.save(any(InterfaceConfig.class)))
                .thenAnswer(inv -> {
                    InterfaceConfig entity = inv.getArgument(0);
                    entity.setId(10L);
                    return entity;
                });

        InterfaceConfigDTO result = interfaceConfigService.createInterface(request);

        assertThat(result.getId()).isEqualTo(10L);
        assertThat(result.getProtocol()).isEqualTo(ProtocolType.KAFKA);
        assertThat(result.getTimeoutMs()).isEqualTo(5000);
        verify(interfaceConfigRepository).save(any(InterfaceConfig.class));
    }

    @Test
    @DisplayName("enabled와 timeoutMs가 비어 있으면 기본값을 적용한다")
    void createInterface_appliesDefaults() {
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.REST, "REST Interface", "legacy-rest-service",
                null, null, null, null, null);

        when(interfaceConfigRepository.save(any(InterfaceConfig.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        InterfaceConfigDTO result = interfaceConfigService.createInterface(request);

        assertThat(result.getEnabled()).isTrue();
        assertThat(result.getTimeoutMs()).isEqualTo(35_000);
    }

    @Test
    @DisplayName("같은 protocol/interfaceName 조합은 중복 등록할 수 없다")
    void createInterface_duplicate_throws() {
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.SOAP, "SOAP Interface", "legacy-soap-service",
                true, 35000, null, null, null);
        when(interfaceConfigRepository.existsByProtocolAndInterfaceName(ProtocolType.SOAP, "SOAP Interface"))
                .thenReturn(true);

        assertThatThrownBy(() -> interfaceConfigService.createInterface(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("이미 등록된 인터페이스");
    }

    @Test
    @DisplayName("timeoutMs가 0 이하이면 등록할 수 없다")
    void createInterface_invalidTimeout_throws() {
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.REST, "REST Interface", "legacy-rest-service",
                true, 0, null, null, null);

        assertThatThrownBy(() -> interfaceConfigService.createInterface(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeoutMs");
    }

    @Test
    @DisplayName("존재하는 인터페이스 설정을 수정한다")
    void updateInterface_updatesConfig() {
        InterfaceConfig existing = buildEntity(1L, ProtocolType.REST, "REST Interface", "legacy-rest-service");
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.REST, "REST Interface V2", "legacy-rest-v2",
                false, 10000, "updated", null, null);

        when(interfaceConfigRepository.findById(1L)).thenReturn(Optional.of(existing));
        when(interfaceConfigRepository.findByProtocolAndInterfaceName(ProtocolType.REST, "REST Interface V2"))
                .thenReturn(Optional.empty());
        when(interfaceConfigRepository.save(any(InterfaceConfig.class)))
                .thenAnswer(inv -> inv.getArgument(0));

        Optional<InterfaceConfigDTO> result = interfaceConfigService.updateInterface(1L, request);

        assertThat(result).isPresent();
        assertThat(result.get().getInterfaceName()).isEqualTo("REST Interface V2");
        assertThat(result.get().getEndpoint()).isEqualTo("legacy-rest-v2");
        assertThat(result.get().getEnabled()).isFalse();
    }

    @Test
    @DisplayName("없는 id를 수정하면 Optional.empty를 반환한다")
    void updateInterface_notFound_returnsEmpty() {
        InterfaceConfigDTO request = new InterfaceConfigDTO(
                null, ProtocolType.REST, "REST Interface", "legacy-rest-service",
                true, 35000, null, null, null);
        when(interfaceConfigRepository.findById(99L)).thenReturn(Optional.empty());

        Optional<InterfaceConfigDTO> result = interfaceConfigService.updateInterface(99L, request);

        assertThat(result).isEmpty();
    }

    private InterfaceConfig buildEntity(Long id, ProtocolType protocol, String name, String endpoint) {
        InterfaceConfig config = new InterfaceConfig();
        config.setId(id);
        config.setProtocol(protocol);
        config.setInterfaceName(name);
        config.setEndpoint(endpoint);
        config.setEnabled(true);
        config.setTimeoutMs(35_000);
        return config;
    }
}
