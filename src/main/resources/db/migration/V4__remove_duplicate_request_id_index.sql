-- V1에서 request_id UNIQUE 선언 시 MySQL이 자동으로 인덱스를 생성했으므로
-- V2에서 추가한 idx_ir_request_id 는 중복 인덱스다. 삭제한다.
DROP INDEX idx_ir_request_id ON integration_requests;
