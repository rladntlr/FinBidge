# 개발 가이드: 금융 IT 인터페이스 통합관리 시스템

**버전:** 1.0  
**작성일:** 2026-04-24  
**마감:** 2026-04-27 자정

---

## 빠른 시작 (5분)

```bash
# 1. 프로젝트 클론 및 빌드
mvn clean install

# 2. RabbitMQ 실행 (Docker)
docker-compose up -d

# 3. 애플리케이션 실행
mvn spring-boot:run

# 4. 브라우저에서 접속
http://localhost:8080
```

---

## 프로젝트 구조

```
finbridge-portfolio/
├── pom.xml                              # Maven 의존성
│
├── src/main/java/com/finbridge/
│   ├── FinbridgeApplication.java        # Spring Boot 진입점
│   │
│   ├── controller/
│   │   └── IntegrationController.java   # REST API 엔드포인트
│   │
│   ├── service/
│   │   ├── IntegrationService.java      # 통합 오케스트레이션
│   │   ├── SoapAdapterService.java      # SOAP 모의 어댑터
│   │   ├── SftpAdapterService.java      # SFTP 모의 어댑터
│   │   ├── BatchAdapterService.java     # Batch 모의 어댑터
│   │   └── MqService.java               # RabbitMQ 실제 구현
│   │
│   ├── model/
│   │   ├── IntegrationRequest.java      # 요청 DTO
│   │   ├── IntegrationResponse.java     # 응답 DTO
│   │   ├── ProtocolResult.java          # 개별 프로토콜 결과
│   │   └── SystemLog.java               # 로그 Entity
│   │
│   ├── config/
│   │   ├── RabbitMqConfig.java          # RabbitMQ 설정
│   │   └── WebConfig.java               # Web 설정
│   │
│   └── repository/
│       └── SystemLogRepository.java     # 로그 조회 저장소
│
├── src/main/resources/
│   ├── application.yml                  # Spring 설정
│   ├── templates/
│   │   ├── index.html                   # 메인 UI
│   │   └── logs.html                    # 로그 조회 UI
│   └── static/
│       └── style.css                    # 간단한 스타일
│
├── src/test/java/com/finbridge/
│   └── IntegrationTest.java             # End-to-End 통합 테스트
│
└── docker-compose.yml                   # RabbitMQ 구성
```

---

## 핵심 구현 순서

### 1단계: 프로젝트 초기화 (30분)

**pom.xml 의존성:**
```xml
<!-- Spring Boot -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-thymeleaf</artifactId>
</dependency>

<!-- Database -->
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- Logging -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-logging</artifactId>
</dependency>

<!-- Test -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-test</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.springframework.amqp</groupId>
    <artifactId>spring-rabbit-test</artifactId>
    <scope>test</scope>
</dependency>
```

**application.yml:**
```yaml
spring:
  application:
    name: finbridge-portfolio
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
  jpa:
    hibernate:
      ddl-auto: create-drop
    show-sql: false
  h2:
    console:
      enabled: true
  datasource:
    url: jdbc:h2:mem:testdb
    driverClassName: org.h2.Driver

server:
  port: 8080
  servlet:
    context-path: /

logging:
  level:
    com.finbridge: DEBUG
    org.springframework: INFO
```

---

### 2단계: REST API + 컨트롤러 (1시간)

**IntegrationController.java:**
```java
@RestController
@RequestMapping("/api")
@Slf4j
public class IntegrationController {

    @Autowired
    private IntegrationService integrationService;

    /**
     * 통합 요청 처리
     * POST /api/integrate
     */
    @PostMapping("/integrate")
    public ResponseEntity<IntegrationResponse> integrate(
            @RequestBody IntegrationRequest request) {
        log.info("Received integration request: {}", request);
        IntegrationResponse response = integrationService.processIntegration(request);
        return ResponseEntity.ok(response);
    }

    /**
     * 프로토콜 상태 조회
     * GET /api/status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("soap", "available");
        status.put("sftp", "available");
        status.put("mq", "connected");
        status.put("batch", "idle");
        status.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(status);
    }

    /**
     * 로그 조회
     * GET /api/logs
     */
    @GetMapping("/logs")
    public ResponseEntity<List<?>> getLogs() {
        return ResponseEntity.ok(integrationService.getLogs());
    }
}
```

**IntegrationRequest.java:**
```java
@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntegrationRequest {
    private String requestId;
    private String protocol;  // SOAP, SFTP, MQ, BATCH
    private String payload;
    private long timestamp = System.currentTimeMillis();
}
```

**IntegrationResponse.java:**
```java
@Data
@Builder
public class IntegrationResponse {
    private String requestId;
    private String status;  // SUCCESS, FAILURE
    private Map<String, ProtocolResult> results;  // 각 프로토콜별 결과
    private long processingTime;
    private long timestamp;
}
```

**ProtocolResult.java:**
```java
@Data
@Builder
public class ProtocolResult {
    private String protocol;
    private String status;  // SUCCESS, FAILURE, SKIPPED
    private String message;
    private Object data;
    private long executionTime;
}
```

---

### 3단계: IntegrationService (오케스트레이션) (1.5시간)

**IntegrationService.java:**
```java
@Service
@Slf4j
public class IntegrationService {

    @Autowired
    private SoapAdapterService soapAdapter;
    
    @Autowired
    private SftpAdapterService sftpAdapter;
    
    @Autowired
    private MqService mqService;
    
    @Autowired
    private BatchAdapterService batchAdapter;
    
    @Autowired
    private SystemLogRepository logRepository;

    /**
     * 메인 통합 프로세스
     * 1개의 요청 → 5개 프로토콜 모두 호출 → 1개의 응답
     */
    public IntegrationResponse processIntegration(IntegrationRequest request) {
        long startTime = System.currentTimeMillis();
        String requestId = request.getRequestId() != null ? 
            request.getRequestId() : UUID.randomUUID().toString();

        Map<String, ProtocolResult> results = new HashMap<>();

        try {
            // 1. SOAP 호출
            log.info("Processing SOAP protocol");
            ProtocolResult soapResult = callSoapAdapter(request, requestId);
            results.put("SOAP", soapResult);

            // 2. MQ 호출 (실제)
            log.info("Publishing to MQ");
            ProtocolResult mqResult = callMqService(request, requestId);
            results.put("MQ", mqResult);

            // 3. SFTP 호출
            log.info("Processing SFTP protocol");
            ProtocolResult sftpResult = callSftpAdapter(request, requestId);
            results.put("SFTP", sftpResult);

            // 4. Batch 호출
            log.info("Processing Batch protocol");
            ProtocolResult batchResult = callBatchAdapter(request, requestId);
            results.put("BATCH", batchResult);

            // 5. 로그 저장
            saveLog(requestId, "INTEGRATION_COMPLETED", "All protocols processed");

            long processingTime = System.currentTimeMillis() - startTime;

            return IntegrationResponse.builder()
                    .requestId(requestId)
                    .status("SUCCESS")
                    .results(results)
                    .processingTime(processingTime)
                    .timestamp(System.currentTimeMillis())
                    .build();

        } catch (Exception e) {
            log.error("Integration failed for request: {}", requestId, e);
            saveLog(requestId, "INTEGRATION_FAILED", e.getMessage());

            long processingTime = System.currentTimeMillis() - startTime;
            return IntegrationResponse.builder()
                    .requestId(requestId)
                    .status("FAILURE")
                    .results(results)
                    .processingTime(processingTime)
                    .timestamp(System.currentTimeMillis())
                    .build();
        }
    }

    private ProtocolResult callSoapAdapter(IntegrationRequest request, String requestId) {
        long start = System.currentTimeMillis();
        try {
            String response = soapAdapter.callLegacySystem(request.getPayload());
            return ProtocolResult.builder()
                    .protocol("SOAP")
                    .status("SUCCESS")
                    .message("Legacy system called successfully")
                    .data(response)
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        } catch (Exception e) {
            return ProtocolResult.builder()
                    .protocol("SOAP")
                    .status("FAILURE")
                    .message(e.getMessage())
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        }
    }

    private ProtocolResult callMqService(IntegrationRequest request, String requestId) {
        long start = System.currentTimeMillis();
        try {
            mqService.publishMessage(requestId, request.getPayload());
            return ProtocolResult.builder()
                    .protocol("MQ")
                    .status("SUCCESS")
                    .message("Message published to queue")
                    .data("Message ID: " + requestId)
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        } catch (Exception e) {
            return ProtocolResult.builder()
                    .protocol("MQ")
                    .status("FAILURE")
                    .message(e.getMessage())
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        }
    }

    private ProtocolResult callSftpAdapter(IntegrationRequest request, String requestId) {
        long start = System.currentTimeMillis();
        try {
            boolean success = sftpAdapter.uploadFile(requestId + ".txt", 
                request.getPayload().getBytes());
            return ProtocolResult.builder()
                    .protocol("SFTP")
                    .status(success ? "SUCCESS" : "FAILURE")
                    .message(success ? "File uploaded" : "Upload failed")
                    .data("File: " + requestId + ".txt")
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        } catch (Exception e) {
            return ProtocolResult.builder()
                    .protocol("SFTP")
                    .status("FAILURE")
                    .message(e.getMessage())
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        }
    }

    private ProtocolResult callBatchAdapter(IntegrationRequest request, String requestId) {
        long start = System.currentTimeMillis();
        try {
            String batchId = batchAdapter.submitBatchJob(request.getPayload());
            return ProtocolResult.builder()
                    .protocol("BATCH")
                    .status("SUCCESS")
                    .message("Batch job submitted")
                    .data("Batch ID: " + batchId)
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        } catch (Exception e) {
            return ProtocolResult.builder()
                    .protocol("BATCH")
                    .status("FAILURE")
                    .message(e.getMessage())
                    .executionTime(System.currentTimeMillis() - start)
                    .build();
        }
    }

    private void saveLog(String requestId, String event, String details) {
        SystemLog log = SystemLog.builder()
                .requestId(requestId)
                .event(event)
                .details(details)
                .timestamp(new Date())
                .build();
        logRepository.save(log);
    }

    public List<?> getLogs() {
        return logRepository.findAll();
    }
}
```

---

### 4단계: 프로토콜 어댑터 구현 (2시간)

**SoapAdapterService.java (모의):**
```java
@Service
@Slf4j
public class SoapAdapterService {

    public String callLegacySystem(String payload) throws Exception {
        log.info("SOAP: Calling legacy system with payload: {}", payload);
        Thread.sleep(100);  // 네트워크 지연 시뮬레이션
        return "SOAP Response: Processed [" + payload + "]";
    }
}
```

**SftpAdapterService.java (모의):**
```java
@Service
@Slf4j
public class SftpAdapterService {

    public boolean uploadFile(String filename, byte[] data) {
        log.info("SFTP: Uploading file: {} (size: {} bytes)", filename, data.length);
        // 실제로 파일을 저장 (로컬 임시 디렉토리)
        try {
            java.nio.file.Files.write(
                java.nio.file.Paths.get("/tmp/" + filename),
                data
            );
            log.info("SFTP: File saved to /tmp/{}", filename);
            return true;
        } catch (Exception e) {
            log.error("SFTP: Upload failed", e);
            return false;
        }
    }
}
```

**BatchAdapterService.java (모의):**
```java
@Service
@Slf4j
public class BatchAdapterService {

    public String submitBatchJob(String payload) {
        String batchId = "BATCH-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("BATCH: Job submitted with ID: {} Payload: {}", batchId, payload);
        return batchId;
    }
}
```

**MqService.java (실제 RabbitMQ):**
```java
@Service
@Slf4j
public class MqService {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void publishMessage(String messageId, String payload) {
        log.info("MQ: Publishing message ID: {}", messageId);
        rabbitTemplate.convertAndSend("integration-queue", 
            messageId + "|" + payload);
    }

    @RabbitListener(queues = "integration-queue")
    public void consumeMessage(String message) {
        log.info("MQ: Consumed message: {}", message);
    }
}
```

---

### 5단계: RabbitMQ 설정 (30분)

**RabbitMqConfig.java:**
```java
@Configuration
public class RabbitMqConfig {

    public static final String INTEGRATION_QUEUE = "integration-queue";
    public static final String INTEGRATION_EXCHANGE = "integration-exchange";
    public static final String ROUTING_KEY = "integration-key";

    @Bean
    public Queue integrationQueue() {
        return new Queue(INTEGRATION_QUEUE, true);
    }

    @Bean
    public DirectExchange integrationExchange() {
        return new DirectExchange(INTEGRATION_EXCHANGE, true, false);
    }

    @Bean
    public Binding binding(Queue queue, DirectExchange exchange) {
        return BindingBuilder.bind(queue)
                .to(exchange)
                .with(ROUTING_KEY);
    }
}
```

**docker-compose.yml:**
```yaml
version: '3'
services:
  rabbitmq:
    image: rabbitmq:3.12-management
    container_name: finbridge-rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    healthcheck:
      test: rabbitmq-diagnostics -q ping
      interval: 30s
      timeout: 10s
      retries: 5
```

---

### 6단계: 웹 UI (1.5시간)

**templates/index.html:**
```html
<!DOCTYPE html>
<html>
<head>
    <title>금융 IT 인터페이스 통합관리</title>
    <style>
        body { font-family: Arial; margin: 20px; }
        .container { max-width: 800px; margin: 0 auto; }
        .form-group { margin: 15px 0; }
        label { display: block; font-weight: bold; margin-bottom: 5px; }
        input, textarea { width: 100%; padding: 8px; }
        button { padding: 10px 20px; background: #007bff; color: white; border: none; cursor: pointer; }
        button:hover { background: #0056b3; }
        .result { margin-top: 20px; border: 1px solid #ddd; padding: 15px; background: #f5f5f5; }
        .status { padding: 10px; margin: 10px 0; border-radius: 4px; }
        .success { background: #d4edda; }
        .failure { background: #f8d7da; }
        h2 { color: #333; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #f0f0f0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🏦 금융 IT 인터페이스 통합관리 시스템</h1>
        
        <h2>통합 요청</h2>
        <form id="integrationForm">
            <div class="form-group">
                <label>요청 ID (선택사항)</label>
                <input type="text" id="requestId" placeholder="자동 생성됨">
            </div>
            
            <div class="form-group">
                <label>프로토콜 (선택사항, 기본값: 모두)</label>
                <input type="text" id="protocol" placeholder="SOAP, MQ, SFTP, BATCH">
            </div>
            
            <div class="form-group">
                <label>페이로드</label>
                <textarea id="payload" rows="4" placeholder="처리할 데이터 입력" required></textarea>
            </div>
            
            <button type="submit">🚀 요청 전송</button>
        </form>

        <div id="result" class="result" style="display:none;">
            <h3>응답 결과</h3>
            <div id="resultContent"></div>
        </div>

        <h2>시스템 상태</h2>
        <div id="status"></div>

        <h2>최근 로그</h2>
        <button onclick="refreshLogs()">새로고침</button>
        <div id="logs"></div>
    </div>

    <script>
        document.getElementById('integrationForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const request = {
                requestId: document.getElementById('requestId').value || 'AUTO-' + Date.now(),
                protocol: document.getElementById('protocol').value || 'ALL',
                payload: document.getElementById('payload').value,
                timestamp: Date.now()
            };

            try {
                const response = await fetch('/api/integrate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(request)
                });
                
                const data = await response.json();
                displayResult(data);
            } catch (error) {
                console.error('Error:', error);
                alert('요청 실패: ' + error.message);
            }
        });

        async function displayResult(data) {
            const html = `
                <p><strong>요청 ID:</strong> ${data.requestId}</p>
                <p><strong>상태:</strong> <span class="status ${data.status.toLowerCase()}">${data.status}</span></p>
                <p><strong>처리 시간:</strong> ${data.processingTime}ms</p>
                <h4>프로토콜별 결과</h4>
                <table>
                    <tr>
                        <th>프로토콜</th>
                        <th>상태</th>
                        <th>메시지</th>
                        <th>실행 시간</th>
                    </tr>
                    ${Object.entries(data.results).map(([protocol, result]) => `
                        <tr>
                            <td>${protocol}</td>
                            <td><span class="status ${result.status.toLowerCase()}">${result.status}</span></td>
                            <td>${result.message}</td>
                            <td>${result.executionTime}ms</td>
                        </tr>
                    `).join('')}
                </table>
            `;
            
            document.getElementById('resultContent').innerHTML = html;
            document.getElementById('result').style.display = 'block';
            
            refreshLogs();
        }

        async function loadStatus() {
            try {
                const response = await fetch('/api/status');
                const status = await response.json();
                const html = Object.entries(status).map(([key, value]) => 
                    `<p><strong>${key}:</strong> ${JSON.stringify(value)}</p>`
                ).join('');
                document.getElementById('status').innerHTML = html;
            } catch (error) {
                console.error('Error loading status:', error);
            }
        }

        async function refreshLogs() {
            try {
                const response = await fetch('/api/logs');
                const logs = await response.json();
                if (logs.length === 0) {
                    document.getElementById('logs').innerHTML = '<p>로그 없음</p>';
                    return;
                }
                
                const html = `
                    <table>
                        <tr>
                            <th>시간</th>
                            <th>요청 ID</th>
                            <th>이벤트</th>
                            <th>상세</th>
                        </tr>
                        ${logs.slice(-10).reverse().map(log => `
                            <tr>
                                <td>${new Date(log.timestamp).toLocaleString()}</td>
                                <td>${log.requestId}</td>
                                <td>${log.event}</td>
                                <td>${log.details}</td>
                            </tr>
                        `).join('')}
                    </table>
                `;
                document.getElementById('logs').innerHTML = html;
            } catch (error) {
                console.error('Error loading logs:', error);
            }
        }

        // 페이지 로드 시
        loadStatus();
        refreshLogs();
        setInterval(loadStatus, 5000);
    </script>
</body>
</html>
```

---

### 7단계: 통합 테스트 (1시간)

**IntegrationTest.java:**
```java
@SpringBootTest
@AutoConfigureMockMvc
class IntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private IntegrationService integrationService;

    @Test
    void testFullIntegrationFlow() throws Exception {
        // Given
        IntegrationRequest request = IntegrationRequest.builder()
                .requestId("TEST-001")
                .protocol("ALL")
                .payload("Test payment data")
                .build();

        // When & Then
        mockMvc.perform(post("/api/integrate")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new ObjectMapper().writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.results.SOAP.status").value("SUCCESS"))
                .andExpect(jsonPath("$.results.MQ.status").value("SUCCESS"))
                .andExpect(jsonPath("$.results.SFTP.status").value("SUCCESS"))
                .andExpect(jsonPath("$.results.BATCH.status").value("SUCCESS"));
    }

    @Test
    void testStatusEndpoint() throws Exception {
        mockMvc.perform(get("/api/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.soap").value("available"))
                .andExpect(jsonPath("$.mq").value("connected"));
    }
}
```

---

## 필수 엔티티/모델

**SystemLog.java (Entity):**
```java
@Entity
@Table(name = "system_logs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SystemLog {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    private String requestId;
    private String event;
    private String details;
    private Date timestamp;
}
```

**SystemLogRepository.java:**
```java
@Repository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long> {
}
```

---

## 체크리스트

- [ ] Maven 프로젝트 생성
- [ ] pom.xml 의존성 추가
- [ ] application.yml 설정
- [ ] RestController + 기본 엔드포인트
- [ ] IntegrationService 구현
- [ ] 5개 프로토콜 어댑터 구현
- [ ] RabbitMQ 설정 + Docker Compose
- [ ] 웹 UI (Thymeleaf)
- [ ] 통합 테스트
- [ ] 로그 저장소
- [ ] README 작성
- [ ] 빌드 및 테스트

---

## 실행 명령어

```bash
# 빌드
mvn clean install

# RabbitMQ 실행
docker-compose up -d

# 애플리케이션 시작
mvn spring-boot:run

# 테스트 실행
mvn test

# 웹 접속
http://localhost:8080
```

---

## 예상 결과

브라우저에서:
1. 페이로드 입력 → "🚀 요청 전송"
2. 응답 받음 (5개 프로토콜 모두 SUCCESS)
3. 로그에 기록됨
4. 상태 표시

이것이 **48시간 안에 제출 가능한 완전한 프로토타입**입니다.

