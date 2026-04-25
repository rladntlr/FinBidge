package com.finbridge.repository;

import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.ProtocolType;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long> {

    List<SystemLog> findByRequestId(String requestId);

    List<SystemLog> findByProtocolOrderByTimestampDesc(ProtocolType protocol);

    Page<SystemLog> findByProtocol(ProtocolType protocol, Pageable pageable);

    long countByProtocol(ProtocolType protocol);

    List<SystemLog> findByTimestampBetween(LocalDateTime start, LocalDateTime end);
}
