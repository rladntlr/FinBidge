# 아키텍처 설계: FinBridge 통합관리 플랫폼

**버전:** 2.0 (Kafka + MySQL + Flyway + 실제 구현)  
**상태:** 작성 중 (엔지니어 리뷰 준비)  
**마감:** 2026-04-27 자정

---

## 1. 시스템 개요

### 1.1 핵심 가치 제안
금융 IT 회사가 여러 외부 기관(금감원, 제3금융권, 파트너사)과 통신하는 **5개 프로토콜을 하나의 중앙화된 플랫폼에서 관리**하는 통합 인터페이스 시스템.

### 1.2 주요 기능 (MVP)
- **REST API 게이트웨이**: 모든 요청의 진입점 및 라우팅
- **SOAP 어댑터** (Apache CXF): 레거시 금융시스템 연동
- **Kafka 메시지큐**: 비동기 이벤트 기반 처리
- **SFTP 파일전송** (JSch): 배치 파일 송수신
- **Spring Batch**: 정시 자동화 작업
- **중앙 대시보드**: 5개 프로토콜 상태 모니터링, 로그 조회
- **시스템 로깅**: 모든 통합 이벤트 및 오류 기록

---

## 2. 데이터베이스 스키마

### 2.1 핵심 테이블 구조

#### `system_logs` (시스템 로그)
모든 프로토콜 요청과 처리 과정을 기록하는 감사 로그 테이블
```sql
CREATE TABLE system_logs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(255) NOT NULL,                  -- 통합 요청 ID (추적용)
  protocol VARCHAR(50),                              -- SOAP, KAFKA, SFTP, BATCH
  event_type VARCHAR(100),                           -- INITIATED, IN_PROGRESS, SUCCESS, FAILED
  event_detail TEXT,                                 -- 이벤트 상세 내용
  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,      -- 발생 시간
  created_by VARCHAR(100),                           -- 요청자
  
  INDEX idx_request_id (request_id),
  INDEX idx_protocol (protocol),
  INDEX idx_timestamp (timestamp),
  INDEX idx_event_type (event_type)
);
```

#### `protocol_results` (프로토콜 실행 결과)
각 프로토콜의 실행 결과를 저장
```sql
CREATE TABLE protocol_results (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(255) NOT NULL,                  -- 통합 요청 ID
  protocol VARCHAR(50) NOT NULL,                     -- SOAP, KAFKA, SFTP, BATCH
  status VARCHAR(20) NOT NULL,                       -- SUCCESS, FAILED, PENDING, TIMEOUT
  response_code VARCHAR(50),                         -- HTTP/프로토콜 응답 코드
  response_message TEXT,                             -- 응답 메시지
  execution_time_ms BIGINT,                          -- 밀리초 단위 실행 시간
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
  
  INDEX idx_request_id (request_id),
  INDEX idx_protocol (protocol),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at)
);
```

#### `integration_requests` (통합 요청 기록)
전체 통합 요청의 생명주기를 추적
```sql
CREATE TABLE integration_requests (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(255) UNIQUE NOT NULL,           -- 요청 ID (UUID)
  protocols_requested VARCHAR(100),                  -- 요청한 프로토콜 (예: SOAP,KAFKA,SFTP)
  payload LONGTEXT,                                  -- 전송할 데이터
  status VARCHAR(20),                                -- CREATED, PROCESSING, COMPLETED, FAILED
  overall_status VARCHAR(20),                        -- 전체 결과 (ALL_SUCCESS, PARTIAL_FAILURE)
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  completed_at DATETIME,
  
  INDEX idx_request_id (request_id),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at)
);
```

#### `protocol_configurations` (프로토콜 설정)
각 프로토콜의 동적 설정값 관리
```sql
CREATE TABLE protocol_configurations (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  protocol VARCHAR(50) NOT NULL,                     -- SOAP, KAFKA, SFTP, BATCH
  config_key VARCHAR(255) NOT NULL,                  -- 설정 키
  config_value TEXT,                                 -- 설정 값
  is_active BOOLEAN DEFAULT TRUE,                    -- 활성화 여부
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME ON UPDATE CURRENT_TIMESTAMP,
  
  UNIQUE KEY unique_protocol_key (protocol, config_key),
  INDEX idx_protocol (protocol),
  INDEX idx_is_active (is_active)
);
```

### 2.2 Flyway 마이그레이션 전략

| 파일 | 목적 |
|-----|------|
| **V1__init.sql** | 초기 스키마 (4개 테이블) |
| **V2__add_indexes.sql** | 성능 인덱스 추가 |
| **V3__add_protocol_config.sql** | 프로토콜 설정 테이블 (향후) |

### 2.3 데이터베이스 설계 원칙
- ✅ **request_id 추적**: 모든 통합 요청을 UUID 기반으로 추적
- ✅ **상태 전이**: CREATED → PROCESSING → COMPLETED/FAILED
- ✅ **성능 인덱싱**: 자주 조회되는 칼럼 (request_id, protocol, status, timestamp) 복합 인덱싱
- ✅ **감사 추적**: created_at, updated_at 자동 관리
- ✅ **프로토콜 격리**: 각 프로토콜의 결과를 독립적으로 저장해 개별 재시도 가능

---

## 3. REST API 구조

### 3.1 주요 엔드포인트

#### A. 통합 요청 시작 API
```
POST /api/integrate
```
**목적**: 5개 프로토콜 모두(또는 선택한 프로토콜)를 동시에 호출  
**요청 본문**:
```json
{
  "protocols": ["SOAP", "KAFKA", "SFTP", "BATCH"],
  "payload": {
    "amount": 1000000,
    "currency": "KRW",
    "targetSystem": "FINTECH_PARTNER"
  }
}
```
**응답**:
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "overallStatus": "PROCESSING",
  "results": {
    "SOAP": { "status": "SUCCESS", "message": "레거시시스템 연동 완료", "executionTimeMs": 145 },
    "KAFKA": { "status": "SUCCESS", "message": "메시지 발행 완료", "executionTimeMs": 23 },
    "SFTP": { "status": "PENDING", "message": "파일 업로드 대기중", "executionTimeMs": 0 },
    "BATCH": { "status": "SUCCESS", "batchId": "BATCH-xyz123", "executionTimeMs": 89 }
  },
  "timestamp": "2026-04-24T10:30:45Z"
}
```

#### B. 요청 상태 조회 API
```
GET /api/integrate/{requestId}
```
**목적**: 특정 통합 요청의 현재 상태 조회 (진행 상황 폴링)  
**응답**: 위와 동일한 구조로 현재 상태 반환

#### C. 시스템 로그 조회 API
```
GET /api/logs?protocol=SOAP&limit=50&offset=0
GET /api/logs?startTime=2026-04-24T00:00:00&endTime=2026-04-25T23:59:59
```
**목적**: 시스템 로그 및 처리 기록 조회  
**응답**:
```json
{
  "total": 150,
  "limit": 50,
  "offset": 0,
  "logs": [
    {
      "id": 1,
      "requestId": "550e8400-e29b-41d4-a716-446655440000",
      "protocol": "KAFKA",
      "eventType": "SUCCESS",
      "eventDetail": "토픽 integration-events에 메시지 발행됨",
      "timestamp": "2026-04-24T10:30:45"
    }
  ]
}
```

#### D. 대시보드 통계 API
```
GET /api/dashboard/stats?period=24h
```
**목적**: 각 프로토콜별 성공/실패율, 평균 응답시간, SLA 현황  
**응답**:
```json
{
  "period": "24h",
  "stats": {
    "SOAP": { "totalRequests": 150, "successCount": 145, "failureCount": 5, "successRate": 96.67, "avgExecutionTimeMs": 234 },
    "KAFKA": { "totalRequests": 320, "successCount": 318, "failureCount": 2, "successRate": 99.37, "avgExecutionTimeMs": 45 },
    "SFTP": { "totalRequests": 45, "successCount": 43, "failureCount": 2, "successRate": 95.56, "avgExecutionTimeMs": 567 },
    "BATCH": { "totalRequests": 12, "successCount": 12, "failureCount": 0, "successRate": 100, "avgExecutionTimeMs": 2340 }
  }
}
```

#### E. 부분 실패 복구 API (Rescue)
```
POST /api/rescue/{requestId}
```
**목적**: 실패한 프로토콜만 선택적으로 재처리  
**요청 본문**:
```json
{
  "protocolsToRetry": ["SFTP"],
  "retryStrategy": "EXPONENTIAL_BACKOFF"
}
```
**응답**:
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "rescueStatus": "COMPLETED",
  "retriedProtocols": ["SFTP"],
  "newResults": {
    "SFTP": { "status": "SUCCESS", "executionTimeMs": 1234 }
  }
}
```

### 3.2 API 응답 형식 (표준화)

#### 성공 응답 (200 OK)
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "SUCCESS",
  "data": {
    "message": "모든 프로토콜 처리 완료"
  },
  "timestamp": "2026-04-24T10:30:45Z"
}
```

#### 실패 응답 (400/500)
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "FAILED",
  "error": {
    "code": "PROTOCOL_TIMEOUT",
    "message": "SOAP 엔드포인트가 30초 이내에 응답하지 않음",
    "details": {
      "protocol": "SOAP",
      "expectedTimeMs": 30000,
      "actualTimeMs": 30145
    }
  },
  "timestamp": "2026-04-24T10:30:45Z"
}
```

### 3.3 API 설계 원칙
- ✅ **멱등성**: 모든 POST 요청은 request_id 기반으로 중복 요청 자동 감지
- ✅ **비동기 처리**: Kafka 메시지 발행은 즉시 반환, 실제 처리는 백그라운드에서
- ✅ **부분 실패 복구**: Rescue API로 실패한 프로토콜만 재시도 가능
- ✅ **타임아웃 정책**: 프로토콜별 configurable timeout (SOAP 30초, Kafka 5초, SFTP 60초, Batch 5분)

---

## 4. 프로토콜 통합 아키텍처

### 4.1 프로토콜별 구현 전략

#### 1) SOAP (Apache CXF) - 레거시시스템 연동
```
입력: SOAP 페이로드 XML
출력: protocol_results에 결과 저장
타임아웃: 30초
재시도: 지수 백오프 (1초 → 2초 → 4초)

처리 흐름:
  SoapAdapterService.callLegacySystem(payload)
    └─ Apache CXF로 WSDL 기반 레거시 시스템 호출
    └─ 응답 파싱 및 protocol_results 저장
    └─ 실패 시 system_logs에 ERROR 이벤트 기록
```

#### 2) Kafka (Message Queue) - 비동기 이벤트 처리
```
입력: 메시지 페이로드
출력: integration-events 토픽에 발행
타임아웃: 5초
재시도: 3회 (Kafka Producer 기본값)
Dead Letter: integration-events-dlt 토픽

처리 흐름:
  KafkaService.publishMessage(messageId, payload)
    └─ KafkaTemplate.send(integration-events, ...)
    └─ 즉시 반환 (비동기, 발행자 측에서 대기 없음)
    └─ 별도 @KafkaListener에서 비동기 처리
    └─ 소비자가 처리 완료 후 protocol_results 저장
```

#### 3) SFTP (JSch) - 파일 전송
```
입력: 파일명, 파일 바이트 배열
출력: /tmp/{filename}에 저장 (로컬 SFTP 시뮬레이션)
타임아웃: 60초
재시도: 2회 (연결 끊김 대비)

처리 흐름:
  SftpAdapterService.uploadFile(filename, data)
    └─ JSch로 SFTP 서버 연결
    └─ 파일 업로드 (프로토콜 수행)
    └─ protocol_results에 SUCCESS 저장
    └─ 실패 시 자동 재시도 또는 Rescue API로 재처리
```

#### 4) Spring Batch - 정시 자동화 작업
```
입력: Batch 페이로드
출력: BATCH-{UUID} 형태 배치 ID 즉시 반환
타임아웃: 5분 (기본)
스케줄링: Cron 표현식으로 정시 실행 가능

처리 흐름:
  BatchAdapterService.submitBatchJob(payload)
    └─ Spring Batch Job 제출
    └─ 배치 ID 즉시 반환 (비동기)
    └─ 백그라운드 스레드에서 실제 처리
    └─ protocol_results에 최종 상태 저장 (SUCCESS/FAILED)
```

#### 5) REST API (통합 게이트웨이)
```
입력: HTTP 요청 (모든 프로토콜 지정)
출력: JSON 응답 (모든 프로토콜 결과 집계)
타임아웃: 35초 (모든 프로토콜 완료 대기)

처리 흐름:
  IntegrationController.integrate(IntegrationRequest)
    └─ request_id 생성 (UUID)
    └─ integration_requests 저장 (상태: CREATED)
    └─ 5개 프로토콜 병렬 호출 (CompletableFuture)
       ├─ SOAP 호출 (30초 타임아웃)
       ├─ Kafka 발행 (5초)
       ├─ SFTP 업로드 (60초)
       └─ Batch 제출 (5초)
    └─ 모든 결과 수집 (CompletableFuture.allOf)
    └─ protocol_results 저장
    └─ integration_requests 상태 업데이트 (COMPLETED/PARTIAL_FAILURE)
    └─ JSON 응답 반환
```

### 4.2 프로토콜 통합 흐름 (시퀀스)

```
클라이언트 (웹 또는 API 클라이언트)
   │
   │ POST /api/integrate (페이로드)
   ▼
IntegrationController
   │
   ├─ [1] request_id 생성 (UUID)
   ├─ [2] integration_requests 테이블 INSERT (상태: CREATED)
   │
   ├─ [3] 병렬 실행 시작 (CompletableFuture.allOf)
   │  │
   │  ├─► SoapAdapterService.callLegacySystem()
   │  │     └─ protocol_results INSERT (SOAP 결과)
   │  │
   │  ├─► KafkaService.publishMessage()
   │  │     ├─ Kafka 토픽에 메시지 발행 (즉시 반환)
   │  │     └─ @KafkaListener에서 비동기 처리
   │  │         └─ protocol_results INSERT (KAFKA 결과)
   │  │
   │  ├─► SftpAdapterService.uploadFile()
   │  │     └─ protocol_results INSERT (SFTP 결과)
   │  │
   │  └─► BatchAdapterService.submitBatchJob()
   │       └─ protocol_results INSERT (BATCH 결과)
   │
   ├─ [4] 모든 결과 수집 (최대 35초 대기)
   ├─ [5] integration_requests UPDATE (상태: COMPLETED 또는 PARTIAL_FAILURE)
   │
   ▼
응답 반환 (모든 프로토콜 결과 집계 JSON)
```

### 4.3 에러 처리 및 복구 (Rescue) 전략

#### 실제 사례: SFTP 타임아웃, 나머지는 성공

**첫 번째 요청 응답** (부분 실패):
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "overallStatus": "PARTIAL_FAILURE",
  "results": {
    "SOAP": { "status": "SUCCESS", "executionTimeMs": 145 },
    "KAFKA": { "status": "SUCCESS", "executionTimeMs": 23 },
    "SFTP": { "status": "TIMEOUT", "executionTimeMs": 60145 },
    "BATCH": { "status": "SUCCESS", "executionTimeMs": 89 }
  }
}
```

**Rescue 요청** (실패한 프로토콜만 재처리):
```json
POST /api/rescue/550e8400-e29b-41d4-a716-446655440000
{
  "protocolsToRetry": ["SFTP"],
  "retryStrategy": "EXPONENTIAL_BACKOFF"
}
```

**Rescue 응답** (재처리 결과):
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "rescueStatus": "COMPLETED",
  "retriedProtocols": ["SFTP"],
  "newResults": {
    "SFTP": { "status": "SUCCESS", "executionTimeMs": 1234 }
  }
}
```

#### 에러 분류 및 처리 정책
| 에러 유형 | HTTP 코드 | 재시도 가능 | 설명 |
|----------|----------|---------|------|
| **TIMEOUT** | 504 | ✅ Yes | 프로토콜이 지정된 시간 내 응답하지 않음 |
| **CONNECTION_REFUSED** | 503 | ✅ Yes | 대상 서버에 연결 불가 |
| **INVALID_PAYLOAD** | 400 | ❌ No | 요청 데이터 포맷 오류 (재시도 무의미) |
| **UNAUTHORIZED** | 401 | ❌ No | 인증 실패 (자격증명 문제) |
| **PROTOCOL_ERROR** | 500 | ✅ Yes | 프로토콜 자체 오류 (서버 상태 회복 대기) |
| **INTERNAL_ERROR** | 500 | ✅ Yes | 시스템 내부 오류 |

---

## 5. 시스템 통합 흐름 다이어그램

### 5.1 전체 아키텍처 (컴포넌트 다이어그램)

```
┌─────────────────────────────────────────────────────────────┐
│         클라이언트 (웹 대시보드 / API 클라이언트)               │
│         - 중앙 통합관리 UI                                    │
│         - 프로토콜 선택 및 요청 제출                          │
└────────────────────┬────────────────────────────────────────┘
                     │ HTTP REST API
                     ▼
┌─────────────────────────────────────────────────────────────┐
│      REST API Gateway (IntegrationController)                │
│  - 요청 검증 및 request_id 생성                              │
│  - 5개 프로토콜 병렬 실행 (CompletableFuture)                │
│  - 결과 집계 및 응답 포장                                    │
└────────┬─────────────┬──────────┬─────────────┬──────────────┘
         │             │          │             │
    [SOAP]         [Kafka]      [SFTP]       [Batch]
         │             │          │             │
    ┌────▼────┐   ┌────▼────┐ ┌──▼────┐  ┌────▼──────┐
    │CXF      │   │Producer │ │JSch   │  │Spring     │
    │Adapter  │   │Consumer │ │Adapter│  │Batch Job  │
    │Service  │   │@Listener│ │Service   │Config     │
    └────┬────┘   └────┬────┘ └───┬───┘  └────┬──────┘
         │             │          │             │
         │        Kafka Topic      │             │
         │     (integration-       │             │
         │      events, DLT)       │             │
         │                         │             │
         └─────────────┼───────────┼─────────────┘
                       │
                    ┌──▼────────────┐
                    │ MySQL Database │
                    ├─ system_logs
                    ├─ protocol_results
                    ├─ integration_requests
                    └─ protocol_configurations
```

### 5.2 데이터 흐름 (순서도)

```
[1] 클라이언트 요청
    POST /api/integrate
    { protocols: [SOAP, KAFKA, SFTP, BATCH], payload: {...} }
         ↓
[2] IntegrationController 진입
    - request_id 생성 (UUID)
    - integration_requests 테이블 INSERT (상태: CREATED)
         ↓
[3] 병렬 어댑터 호출 (CompletableFuture 동시 실행)
    ├─ SoapAdapterService: SOAP 호출 → protocol_results 저장
    ├─ KafkaService: 메시지 발행 → 즉시 반환 → 소비자가 비동기 처리
    ├─ SftpAdapterService: 파일 업로드 → protocol_results 저장
    └─ BatchAdapterService: 배치 제출 → protocol_results 저장
         ↓
[4] 모든 결과 수집 (최대 35초 대기)
    - CompletableFuture.allOf() 또는 timeout 도달
         ↓
[5] 통합 상태 업데이트
    - integration_requests UPDATE (상태: COMPLETED 또는 PARTIAL_FAILURE)
    - system_logs INSERT (전체 처리 이벤트 기록)
         ↓
[6] 응답 반환
    - 클라이언트에게 JSON 응답 (모든 프로토콜 결과 포함)
```

---

## 6. 성능 및 확장성 고려사항

### 6.1 병렬 처리 및 성능 최적화
- **CompletableFuture 병렬 실행**: 5개 프로토콜을 동시에 처리 (최대 35초 대기)
  - 각 프로토콜은 독립적인 ThreadPool 사용
  - 하나의 타임아웃이 다른 프로토콜 대기 시간에 영향 없음

- **Kafka 비동기 처리**: 메시지 발행은 즉시 반환 (5초 타임아웃)
  - 소비자 측에서 별도 스레드로 비동기 처리
  - protocol_results 저장도 비동기로 수행

- **ThreadPool 전략**: 
  - ForkJoinPool (기본) 또는 Custom ExecutorService
  - 스레드 풀 크기: CPU 코어 수 + 대기 스레드 (동적 조정 가능)

### 6.2 데이터베이스 성능 최적화
| 전략 | 목적 | 대상 |
|-----|------|-----|
| **복합 인덱스** | 쿼리 성능 | (request_id, protocol), (protocol, status), (timestamp) |
| **Pagination** | 메모리 관리 | 로그 조회 시 limit/offset 강제 |
| **파티셔닝** | 대규모 데이터 | 월별 또는 분기별 (향후) |
| **아카이빙** | 공간 절약 | 6개월 이상 로그는 archive 테이블로 이동 |
| **조인 최소화** | 쿼리 속도 | protocol_results 쿼리는 JOIN 최소화 |

### 6.3 확장 계획 (MVP 이후)
- **모니터링**: Prometheus/Grafana (프로토콜별 실시간 메트릭)
- **로깅 수집**: ELK Stack (Elasticsearch + Logstash + Kibana)
- **분산 추적**: Jaeger (request_id 기반 end-to-end 추적)
- **캐싱**: Redis (자주 조회되는 설정값 캐싱)

---

## 7. 보안 고려사항

### 7.1 인증 및 인가
- **API 인증**: JWT 토큰 기반 (현재 모의, 향후 구현)
  - 모든 API 요청에 Authorization 헤더 필수
  
- **프로토콜별 인증 자격증명**: 
  - SOAP: WSDL 서버 인증 (username/password)
  - SFTP: SSH 공개키 또는 비밀번호
  - Kafka: SASL/SSL (현재 PLAINTEXT, 향후 보안화)

### 7.2 데이터 보호
- **민감 정보 암호화**: 
  - payload 컬럼: 민감 정보는 encrypted로 저장 (선택사항)
  - 로그: 실제 금액/개인정보는 마스킹

- **감사 추적 로그**:
  - 모든 통합 요청과 프로토콜 호출을 system_logs에 기록
  - 실패 원인 및 복구 이력도 함께 기록

### 7.3 에러 정보 보안
- **외부 응답 (클라이언트에게)**:
  - Stack trace 제외, 상태 코드와 간단한 메시지만 반환
  - 시스템 내부 구조 정보 노출 금지

- **내부 로그**:
  - system_logs에는 모든 상세 정보 포함 (엔지니어 디버깅용)
  - 로그 접근 권한은 개발/운영팀으로 제한

---

## 8. 엔지니어 리뷰 항목 (Plan-Eng-Review)

### 8.1 데이터베이스 아키텍처 확정

**[결정 필요]**
1. **테이블 정규화 전략**
   - 현재: system_logs와 protocol_results 분리 (정규화)
   - 검토: 조인 비용 vs 쿼리 단순성, 데이터 중복

2. **대규모 로그 관리**
   - 파티셔닝: 월별 또는 분기별 (partition by created_at)?
   - 로그 보관 기간: 6개월? 1년? 무제한?
   - archive 테이블 필요성?

3. **동시성 제어**
   - 같은 request_id 중복 요청 방지: 유니크 제약만으로 충분?
   - 낙관적 잠금 (version column) 필요 여부
   - Pessimistic lock 사용 시나리오?

### 8.2 REST API 계약 확정

**[결정 필요]**
1. **Pagination 정책**
   - limit/offset 방식 vs cursor-based pagination?
   - 기본 페이지 크기: 50? 100?

2. **Filtering & Sorting**
   - 로그 조회: protocol, event_type, date range, status 모두 필요?
   - 정렬: 시간순? 프로토콜순?

3. **응답 시간 SLA**
   - 통합 요청 p99 latency 목표: 2초? 5초?
   - 로그 조회 응답 시간: 1초?

### 8.3 에러 처리 정책 확정

**[결정 필요]**
1. **Retry 정책**
   - Exponential backoff 계수: 2배? 1.5배?
   - 최대 재시도 횟수: 3회? 5회?

2. **Circuit Breaker**
   - Resilience4j 도입 여부?
   - 임계값: 5분 동안 50% 이상 실패?

3. **Timeout 값 조정**
   - 현재: SOAP 30s, Kafka 5s, SFTP 60s, Batch 5m
   - 조정 필요?

### 8.4 모니터링 기준 정의

**[결정 필요]**
1. **Alert 임계값**
   - 프로토콜별 에러율 > 5%?
   - 평균 응답시간 > 1초?

2. **SLA 정의**
   - 월간 가용성 목표: 99.5%? 99.9%?
   - 프로토콜별 목표가 다른가?

3. **메트릭 수집 간격**
   - 1분 또는 5분?
   - 보유 기간: 30일? 90일?

### 8.5 기술 검증 (확정됨)
- ✅ **ORM**: Spring Data JPA (Hibernate)
- ✅ **쿼리**: Spring Data named queries + custom @Query
- ✅ **DB 마이그레이션**: Flyway (버전 관리)
- ✅ **빌드**: Gradle
- ✅ **메시지큐**: Kafka (Docker Compose)
- ✅ **프로토콜 구현**: Apache CXF (SOAP), JSch (SFTP), Spring Batch

---
