package com.finbridge.model.entity;

import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.RequestStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "integration_requests")
@Getter
@Setter
@NoArgsConstructor
public class IntegrationRequest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", unique = true, nullable = false, length = 255)
    private String requestId;

    @Column(name = "protocols_requested", nullable = false, length = 100)
    private String protocolsRequested;                  // "SOAP,KAFKA,SFTP,BATCH"

    @Lob
    @Column(name = "payload", columnDefinition = "LONGTEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private RequestStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "overall_status", length = 20)
    private OverallStatus overallStatus;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;
}
