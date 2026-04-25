package com.finbridge.repository;

import com.finbridge.model.entity.InterfaceConfig;
import com.finbridge.model.enums.ProtocolType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface InterfaceConfigRepository extends JpaRepository<InterfaceConfig, Long> {

    List<InterfaceConfig> findByProtocol(ProtocolType protocol);

    Optional<InterfaceConfig> findByProtocolAndInterfaceName(ProtocolType protocol, String interfaceName);

    boolean existsByProtocolAndInterfaceName(ProtocolType protocol, String interfaceName);
}
