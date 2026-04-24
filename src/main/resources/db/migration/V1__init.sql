CREATE TABLE integration_requests
(
    id                   BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id           VARCHAR(255) NOT NULL UNIQUE,
    protocols_requested  VARCHAR(100) NOT NULL,
    payload              LONGTEXT,
    status               VARCHAR(20)  NOT NULL,
    overall_status       VARCHAR(20),
    created_at           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at         DATETIME
);

CREATE TABLE protocol_results
(
    id               BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id       VARCHAR(255) NOT NULL,
    protocol         VARCHAR(50)  NOT NULL,
    status           VARCHAR(20)  NOT NULL,
    response_code    VARCHAR(50),
    response_message TEXT,
    execution_time_ms BIGINT,
    created_at       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME     ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE system_logs
(
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    request_id   VARCHAR(255) NOT NULL,
    protocol     VARCHAR(50),
    event_type   VARCHAR(100) NOT NULL,
    event_detail TEXT,
    timestamp    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by   VARCHAR(100)
);
