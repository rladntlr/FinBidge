# FinBridge

금융 IT 환경에서 함께 운영되는 REST API, SOAP, Kafka(MQ), Batch, SFTP 인터페이스를 하나의 화면과 API 흐름으로 실행, 추적, 재처리하는 통합관리 시스템입니다.

하나의 `requestId` 기준으로 프로토콜별 실행 결과, 전체 상태, 로그, 성능 지표를 확인할 수 있도록 구성했습니다.

## 바이브코딩 기록

이 프로젝트는 바이브코딩 방식으로 만들었습니다. 코드를 한 줄씩 직접 작성하기보다, 요구사항 정의, 설계 리뷰, 코드 구현, 테스트 검증, QA, 문서화를 AI에게 요청하고 결과를 검토하면서 완성했습니다.

## 주요 기능

- 다중 프로토콜 병렬 실행: SOAP, Kafka, SFTP, Batch, REST
- 인터페이스 등록/설정 관리: `enabled`, `endpoint`, `timeoutMs`
- `enabled=false` 프로토콜은 Adapter 실행 없이 `DISABLED` 결과 처리
- 요청 상태 조회와 시스템 로그 조회
- 실패/타임아웃 프로토콜 재처리
- 모니터링 요약과 프로토콜별 성능관리
- MySQL, Kafka, SFTP, Batch를 Docker 기반으로 로컬 검증
- SFTP `StrictHostKeyChecking=yes`와 고정 host key 구성
- 브라우저 기반 정적 웹 콘솔 제공

## 기술 스택

| 영역 | 기술 |
| --- | --- |
| Language | Java 17 |
| Framework | Spring Boot 3.4.4 |
| Database | MySQL 8.0, Spring Data JPA |
| Migration | Flyway |
| Messaging | Kafka KRaft |
| Batch | Spring Batch |
| SOAP | Spring Web Services |
| SFTP | JSch |
| Test | JUnit 5, Mockito, H2, Spring Kafka Test, Spring Batch Test |
| Infra | Docker Compose |
| UI | Static HTML, CSS, JavaScript |

## 아키텍처 요약

```text
Client / Web Console
  -> IntegrationController
  -> IntegrationService
     -> InterfaceConfig 조회(enabled, endpoint, timeoutMs)
     -> CompletableFuture + integrationTaskExecutor 병렬 실행
        -> SOAP / Kafka / SFTP / Batch / REST Adapter
     -> ProtocolResult 중앙 저장
     -> SystemLog 중앙 저장
     -> overallStatus 계산
  -> IntegrationResponseDTO
```

Adapter는 DB에 직접 결과를 저장하지 않습니다. 각 Adapter는 `ProtocolResultDTO`만 반환하고, 최종 저장과 로그 기록은 `IntegrationService`가 중앙에서 처리합니다.

`processIntegration()` 전체에는 `@Transactional`을 사용하지 않습니다. 요청 생성과 최종 결과 저장 구간만 `TransactionTemplate`으로 짧게 처리해 외부 연동 중 DB 커넥션을 오래 점유하지 않도록 설계했습니다.

## 프로젝트 문서

| 문서 | 설명 |
| --- | --- |
| [PORTFOLIO_PLAN.md](PORTFOLIO_PLAN.md) | 프로젝트 기획서 |
| [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) | 개발자용 내부 개발문서 |
| [docker/sftp/README.md](docker/sftp/README.md) | SFTP host key 설명 |

## 로컬 실행

### 1. Docker 서비스 실행

```bash
docker compose up -d
```

| 서비스 | 주소 |
| --- | --- |
| MySQL | `localhost:3307` |
| Kafka | `localhost:9092` |
| SFTP | `localhost:2222` |

### 2. SFTP known_hosts 등록

SFTP는 `StrictHostKeyChecking=yes`로 동작합니다. 처음 실행하는 환경에서는 host key를 등록합니다.

macOS/Linux:

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
```

Windows PowerShell에서는 직접 한 번 접속해 fingerprint를 등록해도 됩니다.

```powershell
sftp -P 2222 finbridge@localhost
```

처음 접속하면 아래 질문이 나오며, `yes`를 입력하면 Windows 사용자 홈의 `known_hosts`에 등록됩니다.

```text
Are you sure you want to continue connecting (yes/no/[fingerprint])?
```

비밀번호는 로컬 기본값 `finbridge123`입니다. 접속 확인 후 `sftp>` 프롬프트에서 `exit`로 나오면 됩니다.

Docker Compose는 로컬 데모용 SFTP host key를 `docker/sftp/host_keys/`에 고정해 둡니다.

기본 포트는 `2222`입니다. 로컬에서 포트 충돌 때문에 `22022:22`처럼 바꿨다면 위 명령의 포트도 `2222` 대신 `22022`를 사용하고, 애플리케이션 실행 시 `SFTP_PORT=22022`도 함께 설정해야 합니다.

SFTP 컨테이너가 시작 직후 종료되면 `docker/sftp/init.d/fix-upload-permissions.sh` 줄바꿈이 CRLF인지 확인하세요. Docker init script는 LF여야 합니다. 이 저장소는 `.gitattributes`로 `*.sh`와 `docker/**` 파일을 LF로 고정합니다.

```bash
file docker/sftp/init.d/fix-upload-permissions.sh
```

`with CRLF line terminators`가 보이면 LF로 변환한 뒤 다시 실행하세요.

### 3. 애플리케이션 실행

```bash
./gradlew bootRun
```

웹 콘솔:

```text
http://localhost:8080
```

웹 콘솔에서 실행, 결과 조회, 인터페이스 등록/설정, 모니터링, 재처리, 로그, 성능관리를 확인할 수 있습니다.

## 환경변수

| 환경변수 | 기본값 | 설명 |
| --- | --- | --- |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:8080` | 허용 origin |
| `SFTP_HOST` | `localhost` | SFTP host |
| `SFTP_PORT` | `2222` | SFTP port |
| `SFTP_USERNAME` | `finbridge` | SFTP username |
| `SFTP_PASSWORD` | `finbridge123` | SFTP password |
| `SFTP_UPLOAD_DIR` | `/upload` | SFTP 업로드 디렉터리 |

## API 빠른 테스트

### 통합 실행

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

주요 응답 필드:

| 필드 | 설명 |
| --- | --- |
| `requestId` | 통합 요청 추적 ID |
| `overallStatus` | `ALL_SUCCESS`, `PARTIAL_FAILURE`, `ALL_FAILED` |
| `results` | 프로토콜별 실행 결과 |
| `createdAt` | 요청 생성 시각 |
| `completedAt` | 모든 프로토콜 결과 확정 시각 |

### 상태 조회

```bash
curl -s 'http://localhost:8080/api/integrate/{requestId}'
```

### 실패 프로토콜 재처리

```bash
curl -s -X POST 'http://localhost:8080/api/integrate/{requestId}/retry'
```

특정 프로토콜만 재처리:

```bash
curl -s -X POST 'http://localhost:8080/api/integrate/{requestId}/retry' \
  -H 'Content-Type: application/json' \
  -d '{"protocols":["SFTP","KAFKA"]}'
```

### 인터페이스 설정

```bash
curl -s 'http://localhost:8080/api/interfaces'
curl -s 'http://localhost:8080/api/interfaces?protocol=SFTP'
```

`enabled=false`로 설정된 프로토콜은 Adapter를 실행하지 않고 `responseCode=DISABLED` 결과로 저장됩니다.

### 로그/모니터링/성능

```bash
curl -s 'http://localhost:8080/api/logs?limit=10&offset=0'
curl -s 'http://localhost:8080/api/monitoring/summary'
curl -s 'http://localhost:8080/api/performance/protocols'
```

## 로컬 데모 계정

| 서비스 | 값 |
| --- | --- |
| MySQL user/password | `root` / `root` |
| MySQL database | `finbridge` |
| Kafka bootstrap server | `localhost:9092` |
| SFTP user/password | `finbridge` / `finbridge123` |
| SFTP upload dir | `/upload` |

## 테스트

```bash
./gradlew test
```

Docker E2E 테스트:

```bash
RUN_DOCKER_E2E=true ./gradlew test --tests '*RealDockerE2EIT'
```

패키징:

```bash
./gradlew bootJar
```

## 로컬 데이터 초기화

로컬 데모 중 쌓인 요청, 결과, 시스템 로그, Batch 메타데이터를 모두 초기화하려면 Docker volume을 삭제하고 다시 실행합니다.

```bash
docker compose down -v
docker compose up -d
./gradlew bootRun
```

이 명령은 MySQL 데이터까지 함께 삭제합니다. 제출/시연 전 깨끗한 상태로 다시 확인할 때 사용하세요.

## 운영 전 주의사항

이 프로젝트는 로컬 데모와 포트폴리오 검증을 목표로 합니다.

| 항목 | 현재 상태 | 운영 전 권장 |
| --- | --- | --- |
| 인증/권한 | 없음 | Spring Security 기반 인증/인가 |
| SFTP 비밀번호 | 기본값 제공 | Secret Manager 또는 배포 환경변수 사용 |
| SOAP/REST | 내부 legacy service 호출 | 실제 외부 endpoint, 인증, timeout 적용 |
| FTP | SFTP 기준 구현 | FTP가 필수이면 별도 Adapter 추가 |
| 재처리 | API 구현 | 이력 테이블, 사유, 작업자, 횟수 제한 추가 |
| 관측성 | 로그, 요약 통계 | Micrometer, tracing, Grafana, p95/p99 지표 |

자세한 설계와 개선 포인트는 [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)를 참고하세요.
