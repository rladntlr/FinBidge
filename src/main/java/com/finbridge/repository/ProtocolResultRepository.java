package com.finbridge.repository;

import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProtocolResultRepository extends JpaRepository<ProtocolResult, Long> {

    List<ProtocolResult> findByRequestId(String requestId);

    List<ProtocolResult> findByProtocolAndStatus(ProtocolType protocol, ResultStatus status);
}
