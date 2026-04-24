# 개발 가이드: 금융 IT 인터페이스 통합관리 시스템

**버전:** 2.0 (Kafka + MySQL + Flyway)  
**작성일:** 2026-04-24  
**마감:** 2026-04-27 자정

---

## 빠른 시작 (5분)

```bash
# 1. 프로젝트 빌드
gradle clean build

# 2. MySQL + Kafka 실행 (Docker)
docker-compose up -d

# 3. Flyway 마이그레이션 자동 실행 (Spring Boot 시작 시)
gradle bootRun

# 4. 브라우저에서 접속
http://localhost:8080
```

---

## 프로젝트 구조

```
finbridge-portfolio/
├── build.gradle                         # Gradle 의존성
├── docker-compose.yml                   # MySQL + Kafka
│
├── src/main/java/com/finbridge/
│   ├── FinbridgeApplication.java        # Spring Boot 진입점
│   │
│   ├── controller/
│   │   └── IntegrationController.java   # REST API 엔드포인트
│   │
│   ├── service/
│   │   ├── IntegrationService.java      # 통합 오케스트레이션
│   │   ├── SoapAdapterService.java      # SOAP (Apache CXF)
│   │   ├── SftpAdapterService.java      # SFTP (JSch)
│   │   ├── BatchAdapterService.java     # Batch (Spring Batch)
│   │   └── KafkaService.java            # Kafka 메시지 처리
│   │
│   ├── model/
│   │   ├── IntegrationRequest.java
│   │   ├── IntegrationResponse.java
│   │   ├── ProtocolResult.java
│   │   └── SystemLog.java
│   │
│   ├── config/
│   │   ├── KafkaConfig.java             # Kafka 설정
│   │   ├── BatchConfig.java             # Spring Batch 설정
│   │   └── WebConfig.java
│   │
│   └── repository/
│       └── SystemLogRepository.java
│
├── src/main/resources/
│   ├── application.yml                  # Spring 설정
│   ├── db/migration/                    # Flyway 마이그레이션
│   │   ├── V1__init.sql
│   │   └── V2__add_indexes.sql
│   ├── templates/
│   │   └── index.html
│   └── static/
│       └── style.css
│
├── src/test/java/
│   └── IntegrationTest.java
│
└── docker-compose.yml
```

---

## build.gradle 의존성

```gradle
dependencies {
    // Spring Boot
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    implementation 'org.springframework.boot:spring-boot-starter-thymeleaf'
    
    // Kafka
    implementation 'org.springframework.kafka:spring-kafka'
    
    // Spring Batch
    implementation 'org.springframework.boot:spring-boot-starter-batch'
    
    // MySQL + Flyway
    runtimeOnly 'com.mysql:mysql-connector-j'
    implementation 'org.flywaydb:flyway-core'
    implementation 'org.flywaydb:flyway-mysql'
    
    // SOAP (Apache CXF)
    implementation 'org.apache.cxf:cxf-spring-boot-starter-jaxws:4.0.0'
    
    // SFTP (JSch)
    implementation 'com.jcraft:jsch:0.1.55'
    
    // Lombok
    compileOnly 'org.projectlombok:lombok'
    annotationProcessor 'org.projectlombok:lombok'
    
    // Test
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.kafka:spring-kafka-test'
}
```

---

## application.yml 설정

```yaml
spring:
  application:
    name: finbridge-portfolio
  
  # MySQL 연결
  datasource:
    url: jdbc:mysql://localhost:3306/finbridge
    username: root
    password: root
    driver-class-name: com.mysql.cj.jdbc.Driver
  
  # JPA 설정
  jpa:
    hibernate:
      ddl-auto: validate  # Flyway가 관리하므로 validate만 사용
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.MySQL8Dialect
  
  # Kafka 설정
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
    consumer:
      bootstrap-servers: localhost:9092
      group-id: finbridge-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      auto-offset-reset: earliest

server:
  port: 8080
  servlet:
    context-path: /

logging:
  level:
    com.finbridge: DEBUG
    org.springframework: INFO
    org.apache.kafka: WARN
```

---

## docker-compose.yml

```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: finbridge-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: finbridge
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: finbridge-kafka
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'PLAINTEXT:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka:9092,CONTROLLER://kafka:29093'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
      CLUSTER_ID: 'MkQkSWJUTHW0NjB3ZEdWdQ'
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    ports:
      - "9092:9092"
    volumes:
      - kafka_data:/tmp/kraft-combined-logs
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions.sh", "--bootstrap-server=localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  mysql_data:
  kafka_data:
```

---

## Flyway 마이그레이션

### V1__init.sql (초기 스키마)

```sql
CREATE TABLE system_logs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(255) NOT NULL,
  event VARCHAR(255) NOT NULL,
  details TEXT,
  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_request_id (request_id),
  INDEX idx_timestamp (timestamp)
);

CREATE TABLE protocol_results (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  request_id VARCHAR(255) NOT NULL,
  protocol VARCHAR(50) NOT NULL,
  status VARCHAR(20) NOT NULL,
  message TEXT,
  execution_time BIGINT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_request_id (request_id),
  INDEX idx_protocol (protocol)
);
```

### V2__add_indexes.sql (성능 인덱스)

```sql
CREATE INDEX idx_logs_event ON system_logs(event);
CREATE INDEX idx_protocol_status ON protocol_results(status);
```

---

## 핵심 구현체

### KafkaConfig.java

```java
@Configuration
public class KafkaConfig {
    
    public static final String INTEGRATION_TOPIC = "integration-events";
    public static final String DLT_TOPIC = "integration-events-dlt";
    
    @Bean
    public NewTopic integrationTopic() {
        return TopicBuilder.name(INTEGRATION_TOPIC)
            .partitions(3)
            .replicas(1)
            .build();
    }
    
    @Bean
    public NewTopic dltTopic() {
        return TopicBuilder.name(DLT_TOPIC)
            .partitions(1)
            .replicas(1)
            .build();
    }
}
```

### SoapAdapterService.java (Apache CXF)

```java
@Service
@Slf4j
public class SoapAdapterService {
    
    public String callLegacySystem(String payload) throws Exception {
        log.info("SOAP: Calling legacy system with payload: {}", payload);
        // Apache CXF로 SOAP 호출
        // 실제 WSDL 기반 웹서비스 호출
        Thread.sleep(100);  // 네트워크 지연 시뮬레이션
        return "SOAP Response: Processed [" + payload + "]";
    }
}
```

### SftpAdapterService.java (JSch)

```java
@Service
@Slf4j
public class SftpAdapterService {
    
    public boolean uploadFile(String filename, byte[] data) {
        log.info("SFTP: Uploading file: {} (size: {} bytes)", filename, data.length);
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

### KafkaService.java

```java
@Service
@Slf4j
public class KafkaService {
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    public void publishMessage(String messageId, String payload) {
        log.info("Kafka: Publishing message ID: {}", messageId);
        kafkaTemplate.send(KafkaConfig.INTEGRATION_TOPIC, 
            messageId, messageId + "|" + payload);
    }
    
    @KafkaListener(topics = KafkaConfig.INTEGRATION_TOPIC)
    public void consumeMessage(String message) {
        log.info("Kafka: Consumed message: {}", message);
    }
}
```

### BatchAdapterService.java (Spring Batch)

```java
@Service
@Slf4j
public class BatchAdapterService {
    
    public String submitBatchJob(String payload) {
        String batchId = "BATCH-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Batch: Job submitted with ID: {} Payload: {}", batchId, payload);
        return batchId;
    }
}
```

---

## 통합 테스트

```java
@SpringBootTest
@AutoConfigureMockMvc
class IntegrationTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void testFullIntegrationFlow() throws Exception {
        IntegrationRequest request = IntegrationRequest.builder()
            .requestId("TEST-001")
            .protocol("ALL")
            .payload("Test payment data")
            .build();
        
        mockMvc.perform(post("/api/integrate")
            .contentType(MediaType.APPLICATION_JSON)
            .content(new ObjectMapper().writeValueAsString(request)))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("SUCCESS"))
            .andExpect(jsonPath("$.results.SOAP.status").value("SUCCESS"))
            .andExpect(jsonPath("$.results.KAFKA.status").value("SUCCESS"))
            .andExpect(jsonPath("$.results.SFTP.status").value("SUCCESS"))
            .andExpect(jsonPath("$.results.BATCH.status").value("SUCCESS"));
    }
}
```

---

## 실행 체크리스트

- [ ] Docker Desktop 실행
- [ ] `docker-compose up -d` 실행
- [ ] `gradle clean build` 실행
- [ ] `gradle bootRun` 실행
- [ ] MySQL 마이그레이션 확인 (로그에 Flyway 메시지)
- [ ] Kafka 토픽 생성 확인
- [ ] `http://localhost:8080` 접속
- [ ] 요청 폼에서 "요청 전송" 버튼 클릭
- [ ] 5개 프로토콜 모두 SUCCESS 표시 확인
- [ ] 로그 탭에서 처리 기록 확인

---

## 주요 변경사항

| 항목 | 이전 | 현재 |
|------|------|------|
| **메시지 큐** | RabbitMQ | Kafka |
| **DB** | H2 (인메모리) | MySQL (Docker) |
| **마이그레이션** | 없음 | Flyway |
| **SOAP** | Mock | Apache CXF (실제) |
| **SFTP** | Mock | JSch (실제) |
| **Batch** | Mock | Spring Batch (실제) |
| **빌드** | Maven | Gradle |

---

## 트러블슈팅

**Kafka 연결 실패:**
```bash
docker logs finbridge-kafka
# 또는
docker-compose logs kafka
```

**MySQL 접속 실패:**
```bash
docker exec finbridge-mysql mysql -uroot -proot -D finbridge -e "SELECT 1;"
```

**Flyway 마이그레이션 에러:**
- MySQL 접속 확인
- `db/migration/` 파일 권한 확인
- `application.yml`의 datasource 설정 확인
