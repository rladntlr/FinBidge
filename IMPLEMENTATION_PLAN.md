# FinBridge 실행 계획: 2일 MVP

**기준:** 1순위 100% 완성  
**마감:** 2026-04-27 자정 (48시간)  
**모드:** 포트폴리오 (프로덕션 완벽도 불필요)

---

## 0. 프로토콜별 실제 금융 IT 사용 예시

FinBridge는 금융사가 여러 외부 시스템과 통신할 때 사용하는 프로토콜들을 하나의 게이트웨이로 통합 관리하는 미들웨어다.

### SOAP — 레거시 코어뱅킹 연동
은행의 핵심 시스템(계좌조회, 이체처리 등)은 2000년대에 구축돼 대부분 SOAP 기반이다.
예를 들어 핀테크 앱이 "A은행 잔액 조회"를 하면, A은행 코어뱅킹에 XML 메시지를 보내고 XML 응답을 받는다.
금융결제원 공동망도 SOAP 방식이다.
- **FinBridge 구현:** 앱 내부 `SoapEndpoint`에 `WebServiceTemplate`으로 XML 요청/응답

### Kafka — 실시간 이상거래 탐지 (FDS)
카드 결제가 발생하는 순간 거래 이벤트를 Kafka 토픽에 발행 → FDS(금융사기탐지시스템)가 구독해서
수백ms 안에 이상 여부 판단. 이체 한도 초과, 새벽 해외 결제 등의 패턴을 실시간 감지한다.
배치로 하면 이미 돈이 빠져나간 후라 Kafka를 쓴다.
- **FinBridge 구현:** `KafkaTemplate.send()` → `integration-events` 토픽 발행 → `KafkaConsumerService` 소비

### SFTP — 금감원 감독 보고
금융사는 금감원에 정기적으로 보고 파일을 제출해야 한다.
매일 밤 11시에 "오늘 전체 거래 내역 CSV" 또는 "분기 리스크 현황 파일"을 금감원 SFTP 서버에 올린다.
실시간이 아닌 대용량 파일 전송이라 SFTP가 적합하다.
- **FinBridge 구현:** JSch로 `atmoz/sftp` 컨테이너(port 2222)에 실제 SSH 연결 후 파일 업로드

### Batch — 야간 정산 (EOD)
은행 영업 마감 후 일괄 처리한다. 오늘 하루 발생한 이자 계산, 연체 고객 상태 변경,
카드 청구서 생성 등이 새벽 2~4시에 Spring Batch Job으로 돌아간다.
건별로 실시간 처리하면 부하가 너무 크기 때문이다.
- **FinBridge 구현:** `JobLauncher.run()` → `integrationJob` (Reader → Processor → Writer) 실제 실행

### REST — 오픈뱅킹 / 핀테크 연동
금융결제원 오픈뱅킹 플랫폼이나 토스·카카오페이 같은 핀테크와 연동할 때 쓴다.
예를 들어 토스가 "내 모든 은행 잔액 한눈에 보기"를 제공할 때, 각 은행의 오픈뱅킹 REST API를 호출한다.
- **FinBridge 구현:** `WebClient`로 내부 `/internal/echo` 엔드포인트 호출 (외부 REST API 연동 구조 시연)

---

## 1. 최종 패키지 구조

```
finbridge-portfolio/
├── src/main/java/com/finbridge/
│   ├── FinbridgeApplication.java           # Spring Boot 진입점
│   │
│   ├── controller/
│   │   ├── IntegrationController.java      # REST API 엔드포인트
│   │   ├── HomeController.java             # Thymeleaf 대시보드 (2순위)
│   │   └── InternalEchoController.java     # REST 어댑터 테스트용 내부 엔드포인트
│   │
│   ├── service/
│   │   ├── IntegrationService.java         # 통합 오케스트레이션 (메인 로직)
│   │   ├── adapter/
│   │   │   ├── ProtocolAdapter.java        # 인터페이스
│   │   │   ├── SoapAdapterService.java     # SOAP 실제 구현 (WebServiceTemplate)
│   │   │   ├── KafkaAdapterService.java    # Kafka 실제 구현 (KafkaTemplate)
│   │   │   ├── SftpAdapterService.java     # SFTP 실제 구현 (JSch)
│   │   │   ├── BatchAdapterService.java    # Batch 실제 구현 (JobLauncher)
│   │   │   └── RestAdapterService.java     # REST 실제 구현 (WebClient)
│   │   └── KafkaConsumerService.java       # Kafka 소비자 (비동기 처리)
│   │
│   ├── model/
│   │   ├── entity/
│   │   │   ├── IntegrationRequest.java
│   │   │   ├── ProtocolResult.java
│   │   │   └── SystemLog.java
│   │   │
│   │   └── dto/
│   │       ├── IntegrationRequestDTO.java
│   │       ├── IntegrationResponseDTO.java
│   │       ├── ProtocolResultDTO.java
│   │       └── LogResponseDTO.java
│   │
│   ├── repository/
│   │   ├── IntegrationRequestRepository.java
│   │   ├── ProtocolResultRepository.java
│   │   └── SystemLogRepository.java
│   │
│   ├── config/
│   │   ├── KafkaConfig.java                # Kafka 설정 (토픽 생성)
│   │   ├── AppConfig.java                  # ObjectMapper Bean
│   │   ├── WebConfig.java                  # CORS 설정
│   │   ├── SoapConfig.java                 # WebServiceTemplate Bean
│   │   └── BatchJobConfig.java             # Spring Batch Job/Step 정의
│   │
│   └── soap/
│       └── SoapEndpoint.java               # 내부 SOAP 엔드포인트 (@Endpoint)
│
├── src/main/resources/
│   ├── application.yml                     # Spring 설정
│   ├── wsdl/
│   │   └── integration.wsdl                # SOAP 계약 정의
│   ├── db/migration/
│   │   ├── V1__init.sql                    # 초기 스키마
│   │   └── V2__add_indexes.sql             # 인덱스
│   ├── templates/
│   │   └── index.html                      # 대시보드 (2순위)
│   └── static/
│       └── style.css                       # 스타일 (2순위)
│
├── src/test/java/
│   └── com/finbridge/
│       ├── IntegrationTest.java            # E2E 테스트 (1순위)
│       └── adapter/
│           └── AdapterTests.java           # 어댑터 단위 테스트
│
├── build.gradle                            # 의존성 (✅ 기존)
├── docker-compose.yml                      # MySQL + Kafka + SFTP
└── ARCHITECTURE.md                         # 상세 설계 (✅ 기존)
```

**주요 결정:**
- `ProtocolAdapter` 인터페이스로 5개 어댑터 통일 (DRY) — **REST 추가로 5개**
- 모든 어댑터 실제 구현 (mock 없음)
  - SOAP: 앱 내부 `@Endpoint` + `WebServiceTemplate` 자기 자신 호출
  - SFTP: docker-compose `atmoz/sftp` 컨테이너 + JSch 실제 업로드
  - Batch: Spring Batch `JobLauncher` + Job/Step 실제 실행
  - REST: `WebClient` + 내부 `/internal/echo` 엔드포인트 호출
- Entity와 DTO 명확히 분리
- HomeController는 2순위 (templates 제거 시에도 API만 동작)

---

## 2. Entity / DTO 목록

### Entity (JPA `@Entity`)

#### `IntegrationRequest` — 통합 요청 기록
```java
@Entity
@Table(name = "integration_requests")
public class IntegrationRequest {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(unique = true, nullable = false)
    private String requestId;                    // UUID (추적용)
    
    @Column(nullable = false)
    private String protocolsRequested;           // "SOAP,KAFKA,SFTP,BATCH"
    
    @Lob
    private String payload;                      // JSON 페이로드
    
    @Enumerated(EnumType.STRING)
    private RequestStatus status;                // CREATED, PROCESSING, COMPLETED, FAILED
    
    @Enumerated(EnumType.STRING)
    private OverallStatus overallStatus;         // ALL_SUCCESS, PARTIAL_FAILURE, ALL_FAILED
    
    @CreationTimestamp
    private LocalDateTime createdAt;
    
    private LocalDateTime completedAt;
}

public enum RequestStatus { CREATED, PROCESSING, COMPLETED, FAILED }
public enum OverallStatus { ALL_SUCCESS, PARTIAL_FAILURE, ALL_FAILED }
```

#### `ProtocolResult` — 프로토콜 실행 결과
```java
@Entity
@Table(name = "protocol_results")
public class ProtocolResult {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String requestId;                    // IntegrationRequest의 requestId
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ProtocolType protocol;               // SOAP, KAFKA, SFTP, BATCH
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ResultStatus status;                 // SUCCESS, FAILED, TIMEOUT, PENDING
    
    private String responseCode;                 // HTTP 코드 또는 프로토콜 코드
    
    private String responseMessage;              // 응답 메시지
    
    private Long executionTimeMs;                // 밀리초 단위
    
    @CreationTimestamp
    private LocalDateTime createdAt;
    
    @UpdateTimestamp
    private LocalDateTime updatedAt;
}

public enum ProtocolType { SOAP, KAFKA, SFTP, BATCH }
public enum ResultStatus { SUCCESS, FAILED, TIMEOUT, PENDING }
```

#### `SystemLog` — 시스템 감사 로그
```java
@Entity
@Table(name = "system_logs")
public class SystemLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String requestId;                    // 추적용
    
    @Enumerated(EnumType.STRING)
    private ProtocolType protocol;               // SOAP, KAFKA, SFTP, BATCH (선택)
    
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventType eventType;                 // INITIATED, IN_PROGRESS, SUCCESS, FAILED
    
    @Lob
    private String eventDetail;                  // 이벤트 상세
    
    @CreationTimestamp
    private LocalDateTime timestamp;
    
    private String createdBy;                    // 요청자 (선택)
}

public enum EventType { INITIATED, IN_PROGRESS, SUCCESS, FAILED }
```

### DTO (데이터 전송 객체)

#### `IntegrationRequestDTO` — 클라이언트 요청
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationRequestDTO {
    private List<String> protocols;             // ["SOAP", "KAFKA", "SFTP", "BATCH"]
    
    private Map<String, Object> payload;        // 전송할 데이터
}
```

#### `IntegrationResponseDTO` — API 응답
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationResponseDTO {
    private String requestId;                   // UUID
    
    private OverallStatus overallStatus;        // ALL_SUCCESS, PARTIAL_FAILURE, ALL_FAILED
    
    private Map<String, ProtocolResultDTO> results;  // 프로토콜별 결과
    
    private LocalDateTime timestamp;
}
```

#### `ProtocolResultDTO` — 프로토콜 결과
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProtocolResultDTO {
    private ResultStatus status;                // SUCCESS, FAILED, TIMEOUT
    
    private String responseCode;
    
    private String responseMessage;
    
    private Long executionTimeMs;
}
```

#### `LogResponseDTO` — 로그 조회 응답
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogResponseDTO {
    private Long total;
    
    private Integer limit;
    
    private Integer offset;
    
    private List<LogItemDTO> logs;
}

@Data
public class LogItemDTO {
    private Long id;
    private String requestId;
    private String protocol;
    private String eventType;
    private String eventDetail;
    private LocalDateTime timestamp;
}
```

---

## 3. Repository 목록

```java
// 1. IntegrationRequestRepository
public interface IntegrationRequestRepository extends JpaRepository<IntegrationRequest, Long> {
    IntegrationRequest findByRequestId(String requestId);
    List<IntegrationRequest> findByStatus(RequestStatus status);
}

// 2. ProtocolResultRepository
public interface ProtocolResultRepository extends JpaRepository<ProtocolResult, Long> {
    List<ProtocolResult> findByRequestId(String requestId);
    List<ProtocolResult> findByProtocolAndStatus(ProtocolType protocol, ResultStatus status);
}

// 3. SystemLogRepository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long> {
    List<SystemLog> findByRequestId(String requestId);
    List<SystemLog> findByProtocolOrderByTimestampDesc(ProtocolType protocol);
    List<SystemLog> findByTimestampBetween(LocalDateTime start, LocalDateTime end);
}
```

---

## 4. Controller API 목록 (1순위)

### `IntegrationController`

```
[1순위 API]
POST /api/integrate
  요청: { protocols: ["SOAP", "KAFKA", "SFTP", "BATCH"], payload: {...} }
  응답: { requestId, overallStatus, results: {...}, timestamp }
  처리: IntegrationService.processIntegration() 호출

GET /api/integrate/{requestId}
  응답: 위와 동일 (현재 상태 반환)
  처리: IntegrationService.getStatus() 호출

GET /api/logs?protocol=SOAP&limit=50&offset=0
  응답: { total, limit, offset, logs: [...] }
  처리: SystemLogRepository.findByProtocol() + pagination

[2순위 API]
POST /api/rescue/{requestId}
  요청: { protocolsToRetry: ["SFTP"], retryStrategy: "EXPONENTIAL_BACKOFF" }
  응답: { requestId, rescueStatus, retriedProtocols, newResults }
  처리: IntegrationService.rescueFailedProtocols() 호출

GET /api/dashboard/stats?period=24h
  응답: { period, stats: { SOAP: {...}, KAFKA: {...}, ... } }
  처리: ProtocolResultRepository 쿼리 집계

[3순위 API]
GET /api/dashboard/stats (위와 동일)
```

---

## 5. Service 호출 흐름

### 전체 흐름 다이어그램

```
POST /api/integrate (클라이언트)
  │
  ▼
IntegrationController.integrate()
  │
  ├─ [1] request_id 생성 (UUID)
  ├─ [2] IntegrationRequest INSERT (상태: CREATED)
  ├─ [3] SystemLog INSERT (INITIATED)
  │
  ▼
IntegrationService.processIntegration()
  │
  ├─ [4] integration_requests 상태 업데이트 (PROCESSING)
  │
  ├─ [5] 병렬 실행 (CompletableFuture.allOf)
  │  │
  │  ├─► SoapAdapterService.callSoapEndpoint()
  │  │     └─ protocol_results INSERT (SOAP 결과)
  │  │
  │  ├─► KafkaAdapterService.publishMessage()
  │  │     ├─ Kafka 토픽에 메시지 발행
  │  │     └─ KafkaConsumerService가 비동기로 처리
  │  │         └─ protocol_results INSERT (KAFKA 결과)
  │  │
  │  ├─► SftpAdapterService.uploadFile()
  │  │     └─ protocol_results INSERT (SFTP 결과)
  │  │
  │  └─► BatchAdapterService.submitJob()
  │       └─ protocol_results INSERT (BATCH 결과)
  │
  ├─ [6] 모든 결과 수집 (CompletableFuture.allOf, timeout: 35초)
  ├─ [7] ProtocolResult 조회 및 집계
  ├─ [8] 전체 상태 판정 (ALL_SUCCESS / PARTIAL_FAILURE / ALL_FAILED)
  ├─ [9] integration_requests 상태 업데이트 (COMPLETED)
  ├─ [10] SystemLog INSERT (SUCCESS 또는 FAILED)
  │
  ▼
응답 반환 (IntegrationResponseDTO)
```

### 주요 Service 클래스

#### `IntegrationService` (메인 오케스트레이션)
```java
@Service
@Slf4j
public class IntegrationService {
    
    public IntegrationResponseDTO processIntegration(IntegrationRequestDTO request) {
        // [1] request_id 생성
        String requestId = UUID.randomUUID().toString();
        
        // [2] integration_requests 저장
        IntegrationRequest entity = new IntegrationRequest();
        entity.setRequestId(requestId);
        entity.setProtocolsRequested(String.join(",", request.getProtocols()));
        entity.setPayload(ObjectMapper.writeValueAsString(request.getPayload()));
        entity.setStatus(RequestStatus.CREATED);
        integrationRequestRepository.save(entity);
        
        // [3] SystemLog: INITIATED
        systemLogService.logInitiated(requestId, request.getProtocols());
        
        // [4] 상태 업데이트: PROCESSING
        entity.setStatus(RequestStatus.PROCESSING);
        integrationRequestRepository.save(entity);
        
        // [5] 병렬 실행
        Map<String, CompletableFuture<ProtocolResultDTO>> futures = new HashMap<>();
        
        if (request.getProtocols().contains("SOAP")) {
            futures.put("SOAP", CompletableFuture.supplyAsync(() -> 
                soapAdapterService.execute(requestId, request.getPayload())));
        }
        
        if (request.getProtocols().contains("KAFKA")) {
            futures.put("KAFKA", CompletableFuture.supplyAsync(() -> 
                kafkaAdapterService.execute(requestId, request.getPayload())));
        }
        
        if (request.getProtocols().contains("SFTP")) {
            futures.put("SFTP", CompletableFuture.supplyAsync(() -> 
                sftpAdapterService.execute(requestId, request.getPayload())));
        }
        
        if (request.getProtocols().contains("BATCH")) {
            futures.put("BATCH", CompletableFuture.supplyAsync(() -> 
                batchAdapterService.execute(requestId, request.getPayload())));
        }
        
        // [6] 모든 결과 대기 (35초 timeout)
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
            futures.values().toArray(new CompletableFuture[0]));
        
        Map<String, ProtocolResultDTO> results = new HashMap<>();
        try {
            allOf.orTimeout(35, TimeUnit.SECONDS).join();
            
            for (var entry : futures.entrySet()) {
                results.put(entry.getKey(), entry.getValue().getNow(null));
            }
        } catch (CompletionException e) {
            // 타임아웃 또는 예외 처리
            for (var entry : futures.entrySet()) {
                if (!entry.getValue().isDone()) {
                    results.put(entry.getKey(), new ProtocolResultDTO(
                        ResultStatus.TIMEOUT, "504", "Timeout after 35s", 35000L));
                }
            }
        }
        
        // [7-8] 전체 상태 판정
        OverallStatus overallStatus = determineOverallStatus(results);
        
        // [9] integration_requests 상태 업데이트
        entity.setStatus(RequestStatus.COMPLETED);
        entity.setOverallStatus(overallStatus);
        entity.setCompletedAt(LocalDateTime.now());
        integrationRequestRepository.save(entity);
        
        // [10] SystemLog: SUCCESS
        systemLogService.logSuccess(requestId, overallStatus);
        
        // 응답
        return new IntegrationResponseDTO(requestId, overallStatus, results, LocalDateTime.now());
    }
    
    public IntegrationResponseDTO getStatus(String requestId) {
        IntegrationRequest entity = integrationRequestRepository.findByRequestId(requestId);
        if (entity == null) return null;
        
        List<ProtocolResult> results = protocolResultRepository.findByRequestId(requestId);
        Map<String, ProtocolResultDTO> resultMap = new HashMap<>();
        
        for (ProtocolResult r : results) {
            resultMap.put(r.getProtocol().name(), new ProtocolResultDTO(
                r.getStatus(), r.getResponseCode(), r.getResponseMessage(), 
                r.getExecutionTimeMs()));
        }
        
        return new IntegrationResponseDTO(requestId, entity.getOverallStatus(), 
            resultMap, entity.getCreatedAt());
    }
    
    private OverallStatus determineOverallStatus(Map<String, ProtocolResultDTO> results) {
        long successCount = results.values().stream()
            .filter(r -> r.getStatus() == ResultStatus.SUCCESS)
            .count();
        
        if (successCount == results.size()) return OverallStatus.ALL_SUCCESS;
        if (successCount == 0) return OverallStatus.ALL_FAILED;
        return OverallStatus.PARTIAL_FAILURE;
    }
}
```

#### `ProtocolAdapter` (인터페이스)
```java
public interface ProtocolAdapter {
    ProtocolResultDTO execute(String requestId, Map<String, Object> payload);
}
```

#### `SoapAdapterService` — 실제 구현
- 앱 내부 `SoapEndpoint` (`@Endpoint`)에 `WebServiceTemplate`으로 XML 요청/응답
- `SoapConfig`에서 `WebServiceTemplate` Bean 등록
- `integration.wsdl` 계약 기반 마샬링

#### `SftpAdapterService` — 실제 구현
- JSch 라이브러리로 SSH 세션 연결
- docker-compose의 `atmoz/sftp` 컨테이너(port 2222)에 파일 업로드
- 업로드 경로: `/upload/finbridge-{requestId}.json`

#### `BatchAdapterService` — 실제 구현
- Spring Batch `JobLauncher.run(integrationJob, params)` 호출
- `BatchJobConfig`에 Job/Step/ItemReader/ItemProcessor/ItemWriter 정의
- payload를 읽어 처리 후 결과 반환

#### `RestAdapterService` — 신규 실제 구현
- `WebClient`로 앱 내부 `/internal/echo` 엔드포인트 호출
- `InternalEchoController`가 payload를 그대로 반환 (실제 외부 REST API 연동 구조 시연)

(KafkaAdapterService는 기존 실제 구현 유지 — KafkaTemplate.send())

#### `KafkaConsumerService` (비동기 Kafka 소비자)
```java
@Service
@Slf4j
public class KafkaConsumerService {
    
    @KafkaListener(topics = "integration-events", groupId = "finbridge-group")
    public void consumeMessage(String message) {
        try {
            log.info("Kafka 메시지 수신: {}", message);
            
            // 메시지 처리
            String[] parts = message.split("\\|");
            String messageId = parts[0];
            String payload = parts[1];
            
            // protocol_results 저장
            ProtocolResult result = new ProtocolResult();
            result.setRequestId(messageId);
            result.setProtocol(ProtocolType.KAFKA);
            result.setStatus(ResultStatus.SUCCESS);
            result.setResponseCode("200");
            result.setResponseMessage("Kafka 메시지 처리 완료");
            result.setExecutionTimeMs(100L);
            protocolResultRepository.save(result);
            
        } catch (Exception e) {
            log.error("Kafka 처리 실패: {}", e.getMessage());
        }
    }
}
```

---

## 6. 개발 순서 (2일 48시간)

### 일정표

| 순서 | 작업 | 예상 시간 | 커밋 단위 |
|------|------|---------|---------|
| **Day 1 (1순위)** | | | |
| 1 | DB 스키마 생성 (Flyway V1, V2) | 30분 | `feat: init database schema` |
| 2 | Entity 클래스 작성 (3개) | 45분 | `feat: add JPA entities` |
| 3 | DTO 클래스 작성 (4개) | 30분 | `feat: add DTOs` |
| 4 | Repository 작성 (3개) | 30분 | `feat: add repositories` |
| 5 | ProtocolAdapter 인터페이스 | 15분 | `feat: add protocol adapter interface` |
| 6 | SoapAdapterService 구현 | 45분 | `feat: implement SOAP adapter` |
| 7 | KafkaAdapterService + Consumer | 60분 | `feat: implement Kafka adapter` |
| 8 | SftpAdapterService 구현 | 45분 | `feat: implement SFTP adapter` |
| 9 | BatchAdapterService 구현 | 45분 | `feat: implement Batch adapter` |
| 10 | IntegrationService (메인 로직) | 90분 | `feat: implement integration service` |
| 11 | IntegrationController (POST /api/integrate) | 60분 | `feat: add integration controller` |
| 12 | IntegrationController (GET /api/integrate/{id}, logs) | 45분 | `feat: add status and logs endpoints` |
| 13 | 기본 E2E 테스트 | 60분 | `test: add integration tests` |
| **Day 1 소계** | | **10시간** | - |
| | | | |
| **Day 2 (2순위 + 버퍼)** | | | |
| 14 | HomeController + Thymeleaf templates | 60분 | `feat: add dashboard UI` |
| 15 | Rescue API 구현 | 60분 | `feat: add rescue API` |
| 16 | 통합 테스트 강화 | 45분 | `test: add more integration tests` |
| 17 | 버그 픽스 + 안정화 | 90분 | `fix: stability improvements` |
| 18 | 최종 검증 + README 작성 | 45분 | `docs: add README` |
| **Day 2 소계** | | **5시간** | - |
| **총합** | | **15시간** | - |

**여유:** 33시간 (예상의 2.2배 여유) → 2순위 충분히 완성 가능

---

## 7. 각 단계별 커밋 단위 (최소 원자성)

### Day 1 커밋 순서

```bash
# [1] DB 스키마
git commit -m "feat: init database schema with Flyway
- V1__init.sql: system_logs, protocol_results, integration_requests
- V2__add_indexes.sql: 성능 인덱스"

# [2] Entity
git commit -m "feat: add JPA entities
- IntegrationRequest, ProtocolResult, SystemLog
- Enums: RequestStatus, ResultStatus, ProtocolType, EventType"

# [3] DTO
git commit -m "feat: add DTOs
- IntegrationRequestDTO, IntegrationResponseDTO
- ProtocolResultDTO, LogResponseDTO"

# [4] Repository
git commit -m "feat: add repositories
- IntegrationRequestRepository, ProtocolResultRepository, SystemLogRepository"

# [5] Adapter 인터페이스
git commit -m "feat: add protocol adapter interface
- ProtocolAdapter with execute() method"

# [6] SOAP 어댑터
git commit -m "feat: implement SOAP adapter
- SoapAdapterService with 100ms mock delay
- Auto-save protocol_results"

# [7] Kafka 어댑터
git commit -m "feat: implement Kafka adapter
- KafkaAdapterService (producer)
- KafkaConsumerService (listener)
- Auto-save protocol_results"

# [8] SFTP 어댑터
git commit -m "feat: implement SFTP adapter
- SftpAdapterService with file upload simulation
- 500ms mock delay for network"

# [9] Batch 어댑터
git commit -m "feat: implement Batch adapter
- BatchAdapterService with job submission
- 50ms mock delay"

# [10] IntegrationService (메인)
git commit -m "feat: implement integration service
- processIntegration(): parallel execution, timeout, error handling
- getStatus(): request status query
- determineOverallStatus(): result aggregation"

# [11] Controller - 통합 요청
git commit -m "feat: add integration controller - POST endpoint
- POST /api/integrate
- request_id generation, parallel protocol invocation
- integrated response with all protocol results"

# [12] Controller - 조회 & 로그
git commit -m "feat: add integration controller - query endpoints
- GET /api/integrate/{requestId}: status query
- GET /api/logs: system log query with pagination"

# [13] 통합 테스트
git commit -m "test: add integration tests
- IntegrationTest.testFullIntegrationFlow()
- Verify all 4 protocols execute and save results
- Check status query and logs retrieval"
```

### Day 2 커밋 순서

```bash
# [14] 대시보드 UI
git commit -m "feat: add Thymeleaf dashboard
- HomeController with GET /
- index.html: request form, result display, recent logs
- style.css: basic styling
- Auto-refresh logs every 5s"

# [15] Rescue API
git commit -m "feat: add rescue API
- POST /api/rescue/{requestId}
- Retry failed protocols with exponential backoff
- Update protocol_results with new results"

# [16] 테스트 강화
git commit -m "test: add comprehensive tests
- AdapterTests for each protocol
- Timeout scenarios
- Partial failure handling"

# [17] 버그 픽스
git commit -m "fix: stability improvements
- Handle concurrent requests (optimistic locking)
- Better error messages
- Connection pool tuning"

# [18] 문서
git commit -m "docs: add README
- Quick start (gradle bootRun)
- API documentation
- Architecture overview"
```

---

## 8. 1순위 구현 체크리스트 (Day 1)

```
[ ] DB 스키마 (Flyway)
  [ ] IntegrationRequest 테이블
  [ ] ProtocolResult 테이블
  [ ] SystemLog 테이블
  [ ] 인덱스 생성

[ ] Entity (JPA)
  [ ] IntegrationRequest
  [ ] ProtocolResult
  [ ] SystemLog
  [ ] 모든 Enum (RequestStatus, ResultStatus, ProtocolType, EventType)

[ ] DTO
  [ ] IntegrationRequestDTO
  [ ] IntegrationResponseDTO
  [ ] ProtocolResultDTO
  [ ] LogResponseDTO

[ ] Repository
  [ ] IntegrationRequestRepository
  [ ] ProtocolResultRepository (with findByRequestId)
  [ ] SystemLogRepository (with findByProtocol, findByTimestampBetween)

[ ] 어댑터 (전부 실제 구현)
  [ ] ProtocolAdapter (인터페이스)
  [ ] SoapAdapterService (WebServiceTemplate → 내부 SoapEndpoint 호출)
  [ ] SoapEndpoint (@Endpoint, WSDL 기반)
  [ ] SoapConfig (WebServiceTemplate Bean)
  [ ] KafkaAdapterService (KafkaTemplate.send() — 기존 유지)
  [ ] KafkaConsumerService (@KafkaListener — 기존 유지)
  [ ] SftpAdapterService (JSch → atmoz/sftp 컨테이너 실제 업로드)
  [ ] BatchAdapterService (JobLauncher → Spring Batch Job 실제 실행)
  [ ] BatchJobConfig (Job/Step/Reader/Processor/Writer 정의)
  [ ] RestAdapterService (WebClient → /internal/echo 호출)
  [ ] InternalEchoController (/internal/echo 엔드포인트)

[ ] IntegrationService
  [ ] processIntegration() - 병렬 실행, timeout, 상태 관리
  [ ] getStatus() - 요청 상태 조회
  [ ] determineOverallStatus() - 전체 상태 판정

[ ] IntegrationController
  [ ] POST /api/integrate - 통합 요청 시작
  [ ] GET /api/integrate/{requestId} - 상태 조회
  [ ] GET /api/logs - 로그 조회 (pagination)

[ ] 테스트
  [ ] IntegrationTest.testFullIntegrationFlow()
  [ ] 모든 4개 프로토콜 실행 검증
  [ ] 상태 조회 검증
  [ ] 로그 저장 검증
```

---

## 9. 2순위 체크리스트 (Day 2, 선택)

```
[ ] Thymeleaf 대시보드
  [ ] HomeController (GET /)
  [ ] index.html - 요청 폼 + 결과 표시 + 최근 로그
  [ ] style.css - 기본 스타일
  [ ] 5초마다 자동 새로고침

[ ] Rescue API
  [ ] POST /api/rescue/{requestId}
  [ ] 실패한 프로토콜만 선택적 재처리
  [ ] exponential backoff (1s, 2s, 4s)

[ ] 추가 테스트
  [ ] AdapterTests (각 프로토콜)
  [ ] Timeout 시나리오
  [ ] 부분 실패 처리
```

---

## 10. 제외 항목

| 항목 | 이유 |
|------|------|
| Circuit Breaker (Resilience4j) | 복잡도 추가, MVP 불필요 |
| Redis 캐싱 | 단일 인스턴스 테스트에 불필요 |
| 배포 파이프라인 (GitHub Actions) | 포트폴리오 범위 밖 |
| 복잡한 인증/인가 | JWT 구현 미필요 |
| Protocol_configurations 테이블 | 2순위 (시간 부족 시 제거) |
| Dashboard/stats API | 3순위 (시간 부족 시 제거) |
| 구조화 로깅 (JSON) | 단순 텍스트 로그 충분 |
| Swagger API 문서 | 수동 API 문서로 충분 |

---

## 마지막 확인

**1순위 완성 기준:**
- ✅ REST API 게이트웨이 (POST /api/integrate, GET /status, GET /logs)
- ✅ 5개 프로토콜 병렬 호출 (SOAP, Kafka, SFTP, Batch, REST)
- ✅ 모든 어댑터 실제 구현 (mock 없음)
- ✅ docker-compose: MySQL + Kafka(KRaft) + SFTP(atmoz/sftp)
- ✅ request_id 추적 + DB 저장
- ✅ 통합 E2E 테스트

**권장:** 이 계획대로 진행하면 **Day 1 끝에 1순위 100% 완성**, **Day 2에 2순위 완성 + 버그 픽스 가능**.

---
