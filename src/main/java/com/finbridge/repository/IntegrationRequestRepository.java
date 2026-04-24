package com.finbridge.repository;

import com.finbridge.model.entity.IntegrationRequest;
import com.finbridge.model.enums.RequestStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface IntegrationRequestRepository extends JpaRepository<IntegrationRequest, Long> {

    Optional<IntegrationRequest> findByRequestId(String requestId);

    List<IntegrationRequest> findByStatus(RequestStatus status);
}
