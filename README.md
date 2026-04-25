# FinBridge

금융 IT 인터페이스 통합관리 시스템입니다.

`POST /api/integrate` 요청을 받으면 SOAP, Kafka, SFTP, Batch, REST 어댑터를 병렬 실행하고,
프로토콜별 결과와 전체 상태를 통합해 반환합니다.

## 기술 스택

- Java 17
- Spring Boot 3.4.4
- Spring MVC, Spring Data JPA
- MySQL 8.0
- Flyway
- Kafka KRaft
- Spring Batch
- Spring Web Services
- JSch SFTP
- Docker Compose

## 로컬 실행

### 1. Docker 서비스 실행

```bash
docker compose up -d
```

실행되는 서비스:

- MySQL: `localhost:3307`
- Kafka: `localhost:9092`
- SFTP: `localhost:2222`

상태 확인:

```bash
docker ps
```

### 2. SFTP known_hosts 등록

SFTP는 `StrictHostKeyChecking=yes`로 동작합니다.
Docker Compose는 로컬 데모용 SFTP host key를 `docker/sftp/host_keys/`에 고정해 둡니다.
처음 실행하는 환경에서는 한 번만 로컬 `known_hosts`에 등록하면 됩니다.

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
ssh-keygen -R "[localhost]:2222" -f "$HOME/.ssh/known_hosts"
ssh-keyscan -T 10 -p 2222 localhost >> "$HOME/.ssh/known_hosts"
chmod 600 "$HOME/.ssh/known_hosts"
```

`docker compose down -v`로 컨테이너와 볼륨을 지워도 SFTP host key는 repo의 고정 파일을 사용하므로 바뀌지 않습니다.
단, `docker/sftp/host_keys/` 파일을 직접 교체한 경우에는 위 명령으로 `known_hosts`를 다시 등록해야 합니다.

SFTP 업로드 디렉터리 권한은 `docker/sftp/init.d/fix-upload-permissions.sh`가 컨테이너 시작 시 자동 보정합니다.

### 3. 애플리케이션 실행

```bash
./gradlew bootRun
```

애플리케이션은 기본적으로 `http://localhost:8080`에서 실행됩니다.
Flyway는 앱 시작 시 MySQL에 도메인 테이블과 Spring Batch 메타데이터 테이블을 검증/생성합니다.

## API Smoke Test

### 통합 요청

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

정상 응답은 `overallStatus`가 `ALL_SUCCESS`이고, `results`에 5개 프로토콜 결과가 포함됩니다.

### 상태 조회

```bash
curl -s 'http://localhost:8080/api/integrate/{requestId}'
```

### 로그 조회

```bash
curl -s 'http://localhost:8080/api/logs?limit=10&offset=0'
curl -s 'http://localhost:8080/api/logs?protocol=SFTP&limit=10&offset=0'
```

## 테스트

### 기본 테스트

유닛 테스트와 H2 기반 Spring 통합 테스트를 실행합니다.

```bash
./gradlew test
```

### Docker E2E 테스트

실제 Docker MySQL, Kafka, SFTP와 Flyway, Spring Batch를 사용하는 E2E 테스트입니다.
Docker 서비스가 실행 중이고 SFTP `known_hosts`가 등록된 상태에서 실행합니다.

```bash
RUN_DOCKER_E2E=true ./gradlew test --tests '*RealDockerE2EIT'
```

이 테스트는 기본 `./gradlew test`에서는 실행되지 않습니다.

### 패키징 검증

```bash
./gradlew bootJar
```

## 주요 엔드포인트

| Method | Path | 설명 |
| --- | --- | --- |
| POST | `/api/integrate` | 프로토콜 병렬 통합 실행 |
| GET | `/api/integrate/{requestId}` | 통합 요청 상태 조회 |
| GET | `/api/logs` | 시스템 로그 조회 |

## 로컬 계정 정보

Docker Compose 기준 로컬 데모 계정입니다.

| 서비스 | 값 |
| --- | --- |
| MySQL user | `root` |
| MySQL password | `root` |
| MySQL database | `finbridge` |
| SFTP user | `finbridge` |
| SFTP password | `finbridge123` |
| SFTP upload dir | `/upload` |
