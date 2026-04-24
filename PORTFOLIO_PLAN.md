# 포트폴리오 프로젝트: 금융 IT 인터페이스 통합관리 시스템

**작성일:** 2026-04-24  
**상태:** DRAFT  
**모드:** 포트폴리오 (백엔드 엔지니어 면접)  
**제출 마감:** 2026-04-27 자정 (48시간)

---

## 1. 프로젝트 개요

### 문제 정의
금융사는 여러 프로토콜(REST, SOAP, MQ, Batch, SFTP)을 사용해 다양한 외부 시스템과 통신해야 한다. 각 프로토콜마다 별도의 연결/처리 로직을 구현하면:
- 코드 중복 증가
- 통합 지점 관리 어려움
- 모니터링과 로깅이 산재됨

**이 프로젝트의 목표:** 5개 프로토콜을 하나의 통합 플랫폼에서 관리하고, 단일 API로 요청/응답을 처리하는 시스템을 만든다.

### 핵심 기능
- **REST API 인터페이스** → 클라이언트 요청 진입점
- **SOAP 어댑터** → 레거시 시스템 연결 (모의)
- **MQ 처리** → 비동기 메시지 큐 (RabbitMQ/ActiveMQ)
- **Batch 작업** → 정시 작업 (Spring Batch)
- **SFTP 파일 전송** → 파일 업로드/다운로드 (모의)
- **통합 대시보드** → 각 프로토콜 상태, 로그, 성능 모니터링

---

## 2. 아키텍처 전략

### 핵심 설계 원칙

```
┌─────────────────────────────────────────────────────┐
│         REST API Gateway (Spring Boot)              │
│  (모든 요청의 진입점, 라우팅, 인증)                 │
└──────────┬──────────────────────────────────────────┘
           │
    ┌──────┴────────┬─────────┬─────────┬──────────┐
    │               │         │         │          │
┌───▼──┐      ┌────▼──┐  ┌──▼──┐  ┌───▼──┐  ┌───▼──┐
│SOAP  │      │SFTP   │  │ MQ  │  │Batch │  │ Log  │
│모의  │      │모의   │  │실제 │  │ 모의  │  │      │
└──────┘      └───────┘  └─────┘  └──────┘  └──────┘
```

### 각 프로토콜의 구현 전략

| 프로토콜 | 구현 방식 | 목적 |
|---------|---------|------|
| **REST API** | 실제 구현 | 메인 인터페이스, CRUD |
| **SOAP** | 모의 서버 | 레거시 시스템 시뮬레이션 |
| **MQ** | 실제 구현 (RabbitMQ) | 비동기 작업 처리 |
| **Batch** | 모의 구현 | 정시 작업 스케줄링 |
| **SFTP** | 모의 서버 | 파일 전송 시뮬레이션 |

**핵심 아이디어:** 
- 실제로 구현할 것: REST API, MQ (이 둘이 통합의 핵심)
- 모의로 대체할 것: SOAP, SFTP, Batch (프로토콜 호출 흐름만 증명)
- 결과: 5개 모두 "동작"하지만, 개발 시간 크게 단축

---

## 3. 구현 계획

### Phase 1: 프로젝트 구조 (2시간)
```
finbridge-portfolio/
├── pom.xml                          # Maven 의존성
├── src/main/java/com/finbridge/
│   ├── controller/                  # REST API 엔드포인트
│   │   └── IntegrationController.java
│   ├── service/                     # 비즈니스 로직
│   │   ├── SoapAdapterService.java  # SOAP 모의
│   │   ├── MqService.java           # MQ 실제 구현
│   │   ├── SftpAdapterService.java  # SFTP 모의
│   │   └── BatchService.java        # Batch 모의
│   ├── model/                       # DTO, Entity
│   │   ├── IntegrationRequest.java
│   │   └── IntegrationResponse.java
│   ├── config/                      # 설정
│   │   ├── RabbitMqConfig.java      # MQ 설정
│   │   └── WebConfig.java
│   └── Application.java             # Spring Boot 메인
├── src/test/java/                   # 통합 테스트
└── README.md
```

### Phase 2: REST API 기본 구조 (4시간)
- Spring Boot 프로젝트 초기화
- IntegrationController 구현
  - `POST /api/integrate` → 통합 요청
  - `GET /api/status` → 프로토콜 상태 조회
  - `GET /api/logs` → 처리 로그 조회
- 요청/응답 모델 정의
- 기본 에러 핸들링

### Phase 3: MQ 통합 (6시간)
- RabbitMQ 로컬 구성 (Docker)
- RabbitMqConfig 작성
- 메시지 publish/subscribe 구현
- 데드레터 큐(DLQ) 처리
- 통합 테스트

### Phase 4: SOAP/SFTP/Batch 모의 구현 (8시간)
각 서비스마다:
- Mock 클래스 구현 (실제 외부 호출 없음)
- 요청 수신 → 응답 반환 로직
- 로깅 추가

**SOAP 모의:**
```java
// SoapAdapterService.java
public SoapResponse callLegacySystem(SoapRequest request) {
    // 실제 SOAP 호출 대신 모의 응답 반환
    return SoapResponse.builder()
        .status("SUCCESS")
        .data("Mock legacy system response")
        .build();
}
```

**MQ 실제:**
```java
// MqService.java
@Service
public class MqService {
    @Autowired
    private RabbitTemplate rabbitTemplate;
    
    public void sendMessage(String message) {
        rabbitTemplate.convertAndSend("integration-queue", message);
    }
}
```

**SFTP 모의:**
```java
// SftpAdapterService.java
public boolean uploadFile(String filename, byte[] data) {
    // 실제 SFTP 대신 로컬 파일로 저장
    return true;  // 성공 모의
}
```

### Phase 5: 통합 테스트 (4시간)
- 시나리오: "요청 1개 → 5개 프로토콜 모두 호출 → 응답 반환"
- MockMvc로 REST API 테스트
- 각 프로토콜이 호출되었는지 검증

### Phase 6: 대시보드 UI + 문서 (6시간)
- **간단한 웹 UI** (Thymeleaf/HTML)
  - 요청 폼 (5개 프로토콜 선택 가능)
  - 응답 결과 표시
  - 로그 조회
  - 프로토콜별 상태 표시
- README.md 작성 (실행 방법, 아키텍처 설명)
- API 문서 (Swagger/Springdoc 자동 생성)

---

## 4. 기술 스택

| 레이어 | 선택 | 이유 |
|-------|------|------|
| **프레임워크** | Spring Boot 3.x | 금융사 표준, 빠른 개발 |
| **메시지 큐** | RabbitMQ | 가장 널리 사용, Docker로 쉽게 실행 |
| **배치** | Spring Batch | Spring 생태계, 문서 풍부 |
| **테스트** | JUnit 5 + Mockito | 표준 |
| **빌드** | Maven | 금융사 선호 |
| **UI** | Thymeleaf + Bootstrap | 간단하고 빠름 |
| **DB** | H2 (인메모리) | 별도 DB 없이 실행 가능 |

---

## 5. 성공 기준

✅ **반드시 완료**
- [ ] REST API 동작 (최소 3개 엔드포인트)
- [ ] 5개 프로토콜 모두 호출 가능 (모의/실제 혼합)
- [ ] 통합 테스트 1개 이상 (end-to-end 흐름 증명)
- [ ] 웹 UI에서 실제로 동작 확인 가능
- [ ] README + 개발 문서 완성

✅ **있으면 좋은 것**
- [ ] Swagger API 문서
- [ ] 에러 처리 상세 구현
- [ ] 로깅 구조화 (JSON)
- [ ] Docker Compose (RabbitMQ 포함)

---

## 6. 면접에서의 설명 포인트

**"왜 모의를 썼나?"**
→ "48시간 안에 5개 프로토콜을 모두 보여주려면, 핵심 능력(통합 아키텍처)과 필수 구현(REST, MQ)에 집중했습니다. 모의는 금융업계의 표준 테스트 패턴입니다."

**"프로덕션으로 만들려면?"**
→ "SOAP → Apache CXF 라이브러리, SFTP → JSch 라이브러리, Batch → 실제 DB 연결. 구조는 그대로입니다."

**"가장 어려웠던 부분?"**
→ "5개 프로토콜을 하나의 요청 흐름에 통합하면서 각각의 에러를 처리하는 부분. 각 어댑터는 독립적이지만, 전체 흐름은 원자성(atomicity)을 유지해야 했습니다."

---

## 7. 제출 구성

```
finbridge-portfolio/
├── README.md                    (실행 방법, 아키텍처)
├── ARCHITECTURE.md              (이 문서)
├── pom.xml + 소스코드
├── docker-compose.yml           (RabbitMQ 포함)
└── 스크린샷 (UI 동작 확인)
```

---

## 다음 단계

1. **지금 (4/24 오후):** 이 아키텍처 문서 검토 + 프로젝트 구조 생성
2. **오늘 밤:** REST API + MQ 구현 완료
3. **내일 오전:** SOAP/SFTP/Batch 모의 구현
4. **내일 오후:** 통합 테스트 + UI 완성
5. **내일 저녁:** 문서 정리 + 제출 준비

**타임라인: 32-36시간 (여유 12-16시간)**

---

## 열린 질문

- [ ] RabbitMQ를 Docker로 실행할까, 로컬 설치할까? → Docker Compose 추천
- [ ] UI를 React로 할까, Thymeleaf로 할까? → Thymeleaf (빠름)
- [ ] DB가 필요할까? → H2 인메모리로 충분
- [ ] SOAP 모의는 실제 WSDL을 쓸까? → Mock 클래스로 충분

---

