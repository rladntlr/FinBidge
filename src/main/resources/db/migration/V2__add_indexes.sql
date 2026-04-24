-- integration_requests
CREATE INDEX idx_ir_request_id ON integration_requests (request_id);
CREATE INDEX idx_ir_status     ON integration_requests (status);

-- protocol_results
CREATE INDEX idx_pr_request_id      ON protocol_results (request_id);
CREATE INDEX idx_pr_protocol        ON protocol_results (protocol);
CREATE INDEX idx_pr_protocol_status ON protocol_results (protocol, status);

-- system_logs
CREATE INDEX idx_sl_request_id  ON system_logs (request_id);
CREATE INDEX idx_sl_timestamp   ON system_logs (timestamp);
CREATE INDEX idx_sl_protocol_ts ON system_logs (protocol, timestamp);
