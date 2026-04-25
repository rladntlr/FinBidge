package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.enums.ResultStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchAdapterServiceTest {

    @Mock private JobLauncher jobLauncher;
    @Mock private Job integrationJob;
    @Mock private ObjectMapper objectMapper;

    @InjectMocks
    private BatchAdapterService batchAdapterService;

    @Test
    @DisplayName("Batch 실행 시 requestId, payload, timestamp JobParameters를 전달한다")
    void execute_passesExpectedJobParameters() throws Exception {
        when(objectMapper.writeValueAsString(Map.of("account", "A-1")))
                .thenReturn("{\"account\":\"A-1\"}");
        JobExecution execution = new JobExecution(42L);
        execution.setStatus(BatchStatus.COMPLETED);
        when(jobLauncher.run(eq(integrationJob), any(JobParameters.class))).thenReturn(execution);

        var result = batchAdapterService.execute("req-1", Map.of("account", "A-1"));

        ArgumentCaptor<JobParameters> paramsCaptor = ArgumentCaptor.forClass(JobParameters.class);
        verify(jobLauncher).run(eq(integrationJob), paramsCaptor.capture());
        JobParameters params = paramsCaptor.getValue();
        assertThat(params.getString("requestId")).isEqualTo("req-1");
        assertThat(params.getString("payload")).isEqualTo("{\"account\":\"A-1\"}");
        assertThat(params.getLong("timestamp")).isNotNull();
        assertThat(result.getStatus()).isEqualTo(ResultStatus.SUCCESS);
    }

    @Test
    @DisplayName("Batch 상태가 COMPLETED가 아니면 FAILED 결과를 반환한다")
    void execute_nonCompletedStatusReturnsFailed() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        JobExecution execution = new JobExecution(43L);
        execution.setStatus(BatchStatus.FAILED);
        when(jobLauncher.run(eq(integrationJob), any(JobParameters.class))).thenReturn(execution);

        var result = batchAdapterService.execute("req-1", Map.of());

        assertThat(result.getStatus()).isEqualTo(ResultStatus.FAILED);
        assertThat(result.getResponseMessage()).contains("FAILED");
    }
}
