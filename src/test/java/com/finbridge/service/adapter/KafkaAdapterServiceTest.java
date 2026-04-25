package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.config.KafkaConfig;
import com.finbridge.model.enums.ResultStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaAdapterServiceTest {

    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private ObjectMapper objectMapper;
    @Mock private SendResult<String, String> sendResult;

    @InjectMocks
    private KafkaAdapterService kafkaAdapterService;

    @Test
    @DisplayName("Kafka 메시지는 requestId와 JSON payload를 파이프로 구분해 발행한다")
    void execute_sendsRequestIdAndJsonPayload() throws Exception {
        when(objectMapper.writeValueAsString(Map.of("amount", 1000)))
                .thenReturn("{\"amount\":1000}");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        var result = kafkaAdapterService.execute("req-1", Map.of("amount", 1000));

        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaConfig.INTEGRATION_TOPIC), eq("req-1"), messageCaptor.capture());
        assertThat(messageCaptor.getValue()).isEqualTo("req-1|{\"amount\":1000}");
        assertThat(result.getStatus()).isEqualTo(ResultStatus.SUCCESS);
    }

    @Test
    @DisplayName("Kafka 발행 중 예외가 발생하면 FAILED 결과를 반환한다")
    void execute_sendFailureReturnsFailed() throws Exception {
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

        var result = kafkaAdapterService.execute("req-1", Map.of());

        assertThat(result.getStatus()).isEqualTo(ResultStatus.FAILED);
        assertThat(result.getResponseCode()).isEqualTo("500");
    }
}
