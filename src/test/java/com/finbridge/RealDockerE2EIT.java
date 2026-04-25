package com.finbridge;

import com.finbridge.model.dto.IntegrationRequestDTO;
import com.finbridge.model.dto.IntegrationResponseDTO;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.OverallStatus;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.RequestStatus;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.IntegrationRequestRepository;
import com.finbridge.repository.ProtocolResultRepository;
import com.finbridge.repository.SystemLogRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real Docker E2E test.
 *
 * Run explicitly after local Docker services are up:
 * RUN_DOCKER_E2E=true ./gradlew test --tests '*RealDockerE2EIT'
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("e2e")
@EnabledIfEnvironmentVariable(named = "RUN_DOCKER_E2E", matches = "true")
class RealDockerE2EIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private IntegrationRequestRepository integrationRequestRepository;

    @Autowired
    private ProtocolResultRepository protocolResultRepository;

    @Autowired
    private SystemLogRepository systemLogRepository;

    @Test
    @DisplayName("[E2E] Docker MySQL/Kafka/SFTP/Batch/Flyway 경로로 5개 프로토콜이 성공한다")
    void allProtocols_executeThroughRealDockerServices() {
        IntegrationRequestDTO request = new IntegrationRequestDTO(
                List.of("SOAP", "KAFKA", "SFTP", "BATCH", "REST"),
                Map.of(
                        "customerId", "e2e-customer-001",
                        "amount", 12000,
                        "currency", "KRW"
                )
        );

        ResponseEntity<IntegrationResponseDTO> response = restTemplate.postForEntity(
                "/api/integrate",
                request,
                IntegrationResponseDTO.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();

        IntegrationResponseDTO body = response.getBody();
        assertThat(body.getRequestId()).isNotBlank();
        assertThat(body.getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);
        assertThat(body.getResults()).containsOnlyKeys("SOAP", "KAFKA", "SFTP", "BATCH", "REST");
        assertThat(body.getResults().values())
                .allSatisfy(result -> assertThat(result.getStatus()).isEqualTo(ResultStatus.SUCCESS));
        assertThat(body.getResults().get("SFTP").getResponseMessage())
                .contains("/upload/finbridge-" + body.getRequestId() + ".json");

        var savedRequest = integrationRequestRepository.findByRequestId(body.getRequestId());
        assertThat(savedRequest).isPresent();
        assertThat(savedRequest.get().getStatus()).isEqualTo(RequestStatus.COMPLETED);
        assertThat(savedRequest.get().getOverallStatus()).isEqualTo(OverallStatus.ALL_SUCCESS);

        List<ProtocolResult> savedResults = protocolResultRepository.findByRequestId(body.getRequestId());
        assertThat(savedResults).hasSize(5);
        assertThat(savedResults)
                .extracting(ProtocolResult::getStatus)
                .containsOnly(ResultStatus.SUCCESS);

        Set<ProtocolType> savedProtocols = savedResults.stream()
                .map(ProtocolResult::getProtocol)
                .collect(Collectors.toSet());
        assertThat(savedProtocols).containsExactlyInAnyOrder(
                ProtocolType.SOAP,
                ProtocolType.KAFKA,
                ProtocolType.SFTP,
                ProtocolType.BATCH,
                ProtocolType.REST
        );

        assertThat(systemLogRepository.findByRequestId(body.getRequestId()))
                .hasSizeGreaterThanOrEqualTo(7);
    }

    @Test
    @DisplayName("[E2E] 잘못된 프로토콜은 실제 MySQL에도 request를 남기지 않고 400을 반환한다")
    void invalidProtocol_returnsBadRequestWithoutPersistingRequest() {
        long requestCountBefore = integrationRequestRepository.count();

        IntegrationRequestDTO request = new IntegrationRequestDTO(
                List.of("INVALID"),
                Map.of("source", "e2e")
        );

        ResponseEntity<String> response = restTemplate.exchange(
                "/api/integrate",
                HttpMethod.POST,
                new HttpEntity<>(request),
                String.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(integrationRequestRepository.count()).isEqualTo(requestCountBefore);
    }
}
