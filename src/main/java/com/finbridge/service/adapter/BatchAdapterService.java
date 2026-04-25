package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.AdapterExecutionConfig;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class BatchAdapterService implements ProtocolAdapter {

    private final JobLauncher jobLauncher;
    private final Job integrationJob;
    private final ObjectMapper objectMapper;
    private final Object batchLaunchLock = new Object();

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        return execute(requestId, payload, null);
    }

    @Override
    public ProtocolResultDTO execute(
            String requestId,
            Map<String, Object> payload,
            AdapterExecutionConfig config
    ) {
        long startTime = System.currentTimeMillis();
        String configuredJobName = endpointOrDefault(config, integrationJob.getName());
        log.info("[BATCH] {} - 배치 작업 실행 시작: configuredJobName={}", requestId, configuredJobName);

        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("requestId", requestId)
                    .addString("payload", objectMapper.writeValueAsString(payload))
                    .addString("configuredJobName", configuredJobName)
                    .addString("interfaceName", interfaceNameOrDefault(config))
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution execution;
            synchronized (batchLaunchLock) {
                execution = jobLauncher.run(integrationJob, params);
            }

            long executionTimeMs = System.currentTimeMillis() - startTime;
            BatchStatus batchStatus = execution.getStatus();

            if (batchStatus == BatchStatus.COMPLETED) {
                log.info("[BATCH] {} - 완료 ({}ms)", requestId, executionTimeMs);
                return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                        "배치 작업 완료[" + configuredJobName + "] (jobId: " + execution.getJobId() + ")",
                        executionTimeMs);
            } else {
                log.warn("[BATCH] {} - 비정상 종료: {}", requestId, batchStatus);
                return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                        "배치 상태: " + batchStatus, executionTimeMs);
            }

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[BATCH] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);
        }
    }

    private String endpointOrDefault(AdapterExecutionConfig config, String defaultEndpoint) {
        if (config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()) {
            return config.getEndpoint();
        }
        return defaultEndpoint != null && !defaultEndpoint.isBlank()
                ? defaultEndpoint
                : "integrationJob";
    }

    private String interfaceNameOrDefault(AdapterExecutionConfig config) {
        return config != null && config.getInterfaceName() != null
                ? config.getInterfaceName()
                : "default";
    }
}
