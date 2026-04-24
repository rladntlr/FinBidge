package com.finbridge.model.entity;

import com.finbridge.model.enums.EventType;
import com.finbridge.model.enums.ProtocolType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "system_logs")
@Getter
@Setter
@NoArgsConstructor
public class SystemLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", nullable = false, length = 255)
    private String requestId;

    @Enumerated(EnumType.STRING)
    @Column(name = "protocol", length = 50)
    private ProtocolType protocol;                      // null 허용 (전체 이벤트일 경우)

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 100)
    private EventType eventType;

    @Lob
    @Column(name = "event_detail", columnDefinition = "TEXT")
    private String eventDetail;

    @CreationTimestamp
    @Column(name = "timestamp", nullable = false, updatable = false)
    private LocalDateTime timestamp;

    @Column(name = "created_by", length = 100)
    private String createdBy;
}
