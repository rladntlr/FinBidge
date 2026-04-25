CREATE TABLE interface_configs
(
    id             BIGINT AUTO_INCREMENT PRIMARY KEY,
    protocol       VARCHAR(50)  NOT NULL,
    interface_name VARCHAR(100) NOT NULL,
    endpoint       VARCHAR(255) NOT NULL,
    enabled        BOOLEAN      NOT NULL DEFAULT TRUE,
    timeout_ms     INT          NOT NULL DEFAULT 35000,
    description    VARCHAR(500),
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME     ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT uk_interface_protocol_name UNIQUE (protocol, interface_name)
);

CREATE INDEX idx_ic_protocol ON interface_configs (protocol);
CREATE INDEX idx_ic_enabled  ON interface_configs (enabled);

INSERT INTO interface_configs (protocol, interface_name, endpoint, enabled, timeout_ms, description)
VALUES
    ('SOAP', 'Default SOAP Legacy Interface', 'legacy-soap-service', TRUE, 35000, '로컬 레거시 SOAP 서비스 직접 호출'),
    ('KAFKA', 'Default Kafka Integration Topic', 'integration-events', TRUE, 5000, 'Kafka integration-events 토픽 발행'),
    ('SFTP', 'Default SFTP Upload Interface', '/upload', TRUE, 10000, 'Docker SFTP 서버 업로드 경로'),
    ('BATCH', 'Default Batch Job Interface', 'integrationJob', TRUE, 35000, 'Spring Batch integrationJob 실행'),
    ('REST', 'Default REST Legacy Interface', 'legacy-rest-service', TRUE, 35000, '로컬 레거시 REST 서비스 직접 호출');
