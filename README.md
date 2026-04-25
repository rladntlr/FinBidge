# FinBridge

금융 IT 인터페이스 통합관리 시스템입니다.

FinBridge는 하나의 API 요청으로 SOAP, Kafka, SFTP, Batch, REST 프로토콜을 병렬 실행하고, 프로토콜별 결과와 전체 처리 상태를 통합해서 반환합니다. 금융권에서 여러 연계 방식이 동시에 운영되는 상황을 로컬에서 재현하고, 요청 ID 기준으로 결과와 로그를 추적할 수 있게 만든 포트폴리오 프로젝트입니다.

## 바이브코딩 기록

이 프로젝트는 바이브코딩 방식으로 만들었습니다.

직접 코드를 한 줄씩 작성하기보다, 요구사항 정의, 리뷰, 수정 방향 결정, 테스트 검증을 AI에게 요청하면서 전체 구현을 완성했습니다. 단순히 “코드를 생성해줘”가 아니라, 초기 기획, 설계 리뷰, 코드 리뷰, 단위 테스트, 통합 테스트, Docker E2E 검증, QA, 문서화까지 AI와 대화하면서 반복 개선한 프로젝트입니다.

## 주요 기능

- `POST /api/integrate`로 다중 프로토콜 병렬 실행
- SOAP, Kafka, SFTP, Batch, REST 어댑터 지원
- 프로토콜별 실행 결과 통합 반환
- 전체 상태 계산
  - `ALL_SUCCESS`
  - `PARTIAL_FAILURE`
  - `ALL_FAILED`
- `GET /api/integrate/{requestId}`로 요청 상태 조회
- `GET /api/logs`로 시스템 로그 조회
- protocol 필터와 limit/offset 페이지네이션 지원
- MySQL 기반 요청, 결과, 로그 저장
- Flyway 기반 DB 마이그레이션
- Spring Batch 메타데이터 테이블 Flyway 관리
- Kafka DLT 처리 구성
- SFTP `StrictHostKeyChecking=yes`와 고정 host key 구성
- 브라우저에서 확인 가능한 정적 웹 콘솔 제공

## 기술 스택

| 영역 | 기술 |
| --- | --- |
| Language | Java 17 |
| Framework | Spring Boot 3.4.4 |
| API | Spring MVC |
| Database | MySQL 8.0 |
| ORM | Spring Data JPA |
| Migration | Flyway |
| Messaging | Kafka KRaft |
| Batch | Spring Batch |
| SOAP | Spring Web Services |
| SFTP | JSch, `com.github.mwiede:jsch` |
| Test | JUnit 5, Mockito, H2, Spring Batch Test, Spring Kafka Test |
| Infra | Docker Compose |
| UI | Static HTML, CSS, JavaScript |

## 아키텍처 요약

```text
Client / Web Console
  |
  v
IntegrationController
  |
  | protocol validation
  v
IntegrationService
  |
  | requestId 생성
  | IntegrationRequest 저장
  | SystemLog INITIATED 저장
  |
  | CompletableFuture + integrationTaskExecutor
  v
+-------------------+--------------------+--------------------+--------------------+------------------+
| SoapAdapterService | KafkaAdapterService | SftpAdapterService  | BatchAdapterService | RestAdapterService |
+-------------------+--------------------+--------------------+--------------------+------------------+
  |
  v
IntegrationService
  |
  | ProtocolResult 중앙 저장
  | SystemLog 중앙 저장
  | overallStatus 계산
  v
IntegrationResponseDTO
```

어댑터는 DB에 직접 결과를 저장하지 않습니다. 각 어댑터는 `ProtocolResultDTO`만 반환하고, 최종 저장 책임은 `IntegrationService`가 가집니다. timeout 이후 늦게 완료된 어댑터가 DB 상태를 다시 오염시키는 문제를 줄이기 위한 설계입니다.

`processIntegration()` 전체에는 `@Transactional`을 사용하지 않습니다. 요청 생성과 최종 결과 저장 구간만 `TransactionTemplate`으로 짧게 트랜잭션 처리합니다. 외부 프로토콜 실행 중에는 DB 커넥션을 오래 붙잡지 않습니다.

## 프로젝트 문서

| 문서 | 설명 |
| --- | --- |
| [PORTFOLIO_PLAN.md](PORTFOLIO_PLAN.md) | 프로젝트 기획서 |
| [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) | 개발자용 내부 개발문서 |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 상세 아키텍처 문서 |
| [docker/sftp/README.md](docker/sftp/README.md) | SFTP host key 설명 |

## 로컬 실행 방법

### 1. Docker 서비스 실행

```bash
docker compose up -d
```

실행되는 서비스:

| 서비스 | 주소 |
| --- | --- |
| MySQL | `localhost:3307` |
| Kafka | `localhost:9092` |
| SFTP | `localhost:2222` |

상태 확인:

```bash
docker ps
```

### 2. SFTP known_hosts 등록

SFTP는 `StrictHostKeyChecking=yes`로 동작합니다. 처음 실행하는 환경에서는 로컬 `known_hosts`에 SFTP host key를 등록해야 합니다.

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
```

Docker Compose는 로컬 데모용 SFTP host key를 `docker/sftp/host_keys/`에 고정해 둡니다. 그래서 일반적인 `docker compose down -v` 이후에도 host key가 바뀌지 않습니다.

단, `docker/sftp/host_keys/` 파일을 직접 교체한 경우에는 위 명령으로 `known_hosts`를 다시 등록해야 합니다.

SFTP 업로드 디렉터리 권한은 `docker/sftp/init.d/fix-upload-permissions.sh`가 컨테이너 시작 시 자동 보정합니다.

### 3. 애플리케이션 실행

```bash
./gradlew bootRun
```

애플리케이션은 기본적으로 `http://localhost:8080`에서 실행됩니다.

앱 시작 시 Flyway가 MySQL에 도메인 테이블과 Spring Batch 메타데이터 테이블을 생성 또는 검증합니다. JPA는 `ddl-auto=validate`로 동작하므로, 스키마 생성은 Flyway가 담당합니다.

### 4. 웹 콘솔 접속

브라우저에서 접속합니다.

```text
http://localhost:8080
```

웹 콘솔에서 할 수 있는 일:

- 프로토콜 선택
- 샘플 요청 실행
- 5개 프로토콜 전체 요청 실행
- 응답 결과 확인
- requestId 복사
- 로그 조회
- protocol 필터 조회

## 환경변수 설정

로컬 데모는 기본값으로 바로 실행할 수 있습니다. 운영 또는 다른 환경에서는 아래 값을 환경변수로 바꿔서 실행합니다.

| 환경변수 | 기본값 | 설명 |
| --- | --- | --- |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:8080` | API 호출을 허용할 origin 목록. 여러 개는 쉼표로 구분 |
| `SFTP_HOST` | `localhost` | SFTP host |
| `SFTP_PORT` | `2222` | SFTP port |
| `SFTP_USERNAME` | `finbridge` | SFTP username |
| `SFTP_PASSWORD` | `finbridge123` | SFTP password |
| `SFTP_UPLOAD_DIR` | `/upload` | SFTP 업로드 디렉터리 |

예시:

```bash
CORS_ALLOWED_ORIGINS=http://localhost:8080,http://localhost:3000 \
SFTP_PASSWORD=finbridge123 \
./gradlew bootRun
```

## API 사용 예시

### POST /api/integrate

5개 프로토콜을 모두 실행합니다.

```bash
curl -s -X POST 'http://localhost:8080/api/integrate' \
  -H 'Content-Type: application/json' \
  -d '{
    "protocols": ["SOAP", "KAFKA", "SFTP", "BATCH", "REST"],
    "payload": {
      "customerId": "demo-customer",
      "amount": 12000,
      "currency": "KRW"
    }
  }'
```

응답 예시:

```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "overallStatus": "ALL_SUCCESS",
  "results": {
    "SOAP": {
      "status": "SUCCESS",
      "responseCode": "200",
      "responseMessage": "레거시 시스템 처리 완료: 550e8400-e29b-41d4-a716-446655440000",
      "executionTimeMs": 12
    },
    "KAFKA": {
      "status": "SUCCESS",
      "responseCode": "200",
      "responseMessage": "Kafka 메시지 발행 완료",
      "executionTimeMs": 41
    },
    "SFTP": {
      "status": "SUCCESS",
      "responseCode": "200",
      "responseMessage": "파일 업로드 완료: /upload/finbridge-550e8400-e29b-41d4-a716-446655440000.json",
      "executionTimeMs": 120
    },
    "BATCH": {
      "status": "SUCCESS",
      "responseCode": "200",
      "responseMessage": "배치 작업 완료 (jobId: 1)",
      "executionTimeMs": 85
    },
    "REST": {
      "status": "SUCCESS",
      "responseCode": "200",
      "responseMessage": "REST 외부 시스템 처리 완료",
      "executionTimeMs": 5
    }
  },
  "createdAt": "2026-04-26T04:20:10",
  "completedAt": "2026-04-26T04:20:11"
}
```

응답 필드:

| 필드 | 설명 |
| --- | --- |
| `requestId` | 통합 요청 추적 ID |
| `overallStatus` | 전체 처리 결과. `ALL_SUCCESS`, `PARTIAL_FAILURE`, `ALL_FAILED` |
| `results` | 프로토콜별 실행 결과 |
| `createdAt` | 요청 생성 시각 |
| `completedAt` | 요청된 모든 프로토콜 결과가 확정된 시각 |

지원 프로토콜:

```text
SOAP, KAFKA, SFTP, BATCH, REST
```

소문자 요청도 정규화됩니다.

```json
{
  "protocols": ["rest", "sftp"],
  "payload": {
    "customerId": "demo-customer"
  }
}
```

잘못된 protocol, 빈 protocol 목록, null 또는 blank protocol은 400 Bad Request로 처리됩니다.

### GET /api/integrate/{requestId}

특정 요청의 저장된 처리 결과를 조회합니다.

```bash
curl -s 'http://localhost:8080/api/integrate/550e8400-e29b-41d4-a716-446655440000'
```

존재하지 않는 `requestId`는 404 Not Found를 반환합니다.

### GET /api/logs

전체 로그 조회:

```bash
curl -s 'http://localhost:8080/api/logs?limit=10&offset=0'
```

프로토콜 필터 조회:

```bash
curl -s 'http://localhost:8080/api/logs?protocol=SFTP&limit=10&offset=0'
```

응답 예시:

```json
{
  "total": 3,
  "limit": 10,
  "offset": 0,
  "logs": [
    {
      "id": 1,
      "requestId": "550e8400-e29b-41d4-a716-446655440000",
      "protocol": "SFTP",
      "eventType": "SUCCESS",
      "eventDetail": "SFTP 처리 결과: SUCCESS",
      "timestamp": "2026-04-26T04:20:11"
    }
  ]
}
```

`limit <= 0`, `offset < 0`, 잘못된 protocol query는 400 Bad Request로 처리됩니다.

## 로컬 데모 계정 정보

Docker Compose 기준 로컬 데모 계정입니다.

| 서비스 | 값 |
| --- | --- |
| MySQL user | `root` |
| MySQL password | `root` |
| MySQL database | `finbridge` |
| MySQL port | `3307` |
| Kafka bootstrap server | `localhost:9092` |
| SFTP user | `finbridge` |
| SFTP password | `finbridge123` |
| SFTP port | `2222` |
| SFTP upload dir | `/upload` |

## 테스트 실행 방법

### 기본 테스트

단위 테스트와 H2 기반 Spring 통합 테스트를 실행합니다.

```bash
./gradlew test
```

### Docker E2E 테스트

실제 Docker MySQL, Kafka, SFTP와 Flyway, Spring Batch 경로를 검증합니다.

전제:

```bash
docker compose up -d
```

SFTP `known_hosts` 등록도 완료되어 있어야 합니다.

실행:

```bash
RUN_DOCKER_E2E=true ./gradlew test --tests '*RealDockerE2EIT'
```

이 테스트는 기본 `./gradlew test`에는 포함되지 않습니다.

### 패키징 검증

```bash
./gradlew bootJar
```

## 장애 대응 메모

### SFTP: HostKey has been changed

SFTP host key가 바뀌었거나 `known_hosts`에 예전 key가 남아 있는 경우입니다.

```bash
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
```

현재는 Docker SFTP host key가 고정되어 있으므로, host key 파일을 교체하지 않았다면 매번 반복할 필요는 없습니다.

### Kafka 연결 실패

Spring Boot 앱은 host 기준으로 `localhost:9092`에 연결합니다. `docker-compose.yml`은 host listener와 Docker 내부 listener를 분리합니다.

```text
host app -> localhost:9092
docker internal -> kafka:29092
```

Kafka listener 설정을 바꿀 때 이 분리를 유지해야 합니다.

### Batch metadata table 오류

Spring Batch는 `BATCH_*` 메타데이터 테이블이 필요합니다. 이 프로젝트는 Flyway `V3__create_spring_batch_metadata_tables.sql`에서 해당 테이블을 생성합니다.

MySQL 볼륨을 지우고 다시 시작하려면:

```bash
docker compose down -v
docker compose up -d
./gradlew bootRun
```

## 운영 전 주의사항

이 프로젝트는 로컬 데모와 포트폴리오 검증을 목표로 합니다. 운영 환경에 반영하려면 아래 항목을 추가로 설계해야 합니다.

| 항목 | 현재 상태 | 운영 전 권장 |
| --- | --- | --- |
| 인증/권한 | 없음 | Spring Security 기반 인증/인가 추가 |
| CORS | `CORS_ALLOWED_ORIGINS`로 설정 가능 | 실제 프론트엔드 도메인만 허용 |
| SFTP 비밀번호 | 환경변수 override 가능, 기본값 있음 | Secret Manager 또는 배포 환경변수로 강제 |
| SOAP/REST 외부 연계 | 내부 legacy service 직접 호출 | 외부 endpoint, 인증, timeout, 장애 격리 설계 |
| 알림 | 없음 | 실패/timeout 기준 Slack, Email, SMS 알림 |
| 재처리 | 없음 | 실패 프로토콜 단위 retry API 추가 |
| 관측성 | 로그 중심 | metric, tracing, dashboard 추가 |
| 부하 제어 | fixed thread pool 10 | 요청량 기준 queue, rate limit, rejection policy 검토 |
