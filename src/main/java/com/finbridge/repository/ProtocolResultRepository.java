package com.finbridge.repository;

import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProtocolResultRepository extends JpaRepository<ProtocolResult, Long> {

    List<ProtocolResult> findByRequestId(String requestId);

    List<ProtocolResult> findByProtocolAndStatus(ProtocolType protocol, ResultStatus status);

    long countByProtocol(ProtocolType protocol);

    long countByProtocolAndStatus(ProtocolType protocol, ResultStatus status);

    @Query("select avg(p.executionTimeMs) from ProtocolResult p where p.protocol = :protocol")
    Double averageExecutionTimeMsByProtocol(@Param("protocol") ProtocolType protocol);
}
