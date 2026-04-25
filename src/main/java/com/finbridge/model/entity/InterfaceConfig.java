package com.finbridge.model.entity;

import com.finbridge.model.enums.ProtocolType;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "interface_configs",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_interface_protocol_name",
                columnNames = {"protocol", "interface_name"}))
@Getter
@Setter
@NoArgsConstructor
public class InterfaceConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "protocol", nullable = false, length = 50)
    private ProtocolType protocol;

    @Column(name = "interface_name", nullable = false, length = 100)
    private String interfaceName;

    @Column(name = "endpoint", nullable = false, length = 255)
    private String endpoint;

    @Column(name = "enabled", nullable = false)
    private Boolean enabled = true;

    @Column(name = "timeout_ms", nullable = false)
    private Integer timeoutMs = 35_000;

    @Column(name = "description", length = 500)
    private String description;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
