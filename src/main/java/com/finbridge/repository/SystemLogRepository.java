package com.finbridge.repository;

import com.finbridge.model.entity.SystemLog;
import com.finbridge.model.enums.ProtocolType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long> {

    List<SystemLog> findByRequestId(String requestId);

    List<SystemLog> findByProtocolOrderByTimestampDesc(ProtocolType protocol);

    List<SystemLog> findByTimestampBetween(LocalDateTime start, LocalDateTime end);
}
