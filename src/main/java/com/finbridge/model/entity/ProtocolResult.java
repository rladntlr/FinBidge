package com.finbridge.model.entity;

import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "protocol_results")
@Getter
@Setter
@NoArgsConstructor
public class ProtocolResult {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", nullable = false, length = 255)
    private String requestId;

    @Enumerated(EnumType.STRING)
    @Column(name = "protocol", nullable = false, length = 50)
    private ProtocolType protocol;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private ResultStatus status;

    @Column(name = "response_code", length = 50)
    private String responseCode;

    @Lob
    @Column(name = "response_message", columnDefinition = "TEXT")
    private String responseMessage;

    @Column(name = "execution_time_ms")
    private Long executionTimeMs;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
