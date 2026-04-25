package com.finbridge.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class IntegrationExecutorConfigTest {

    @Test
    @DisplayName("integrationTaskExecutor는 디버깅 가능한 thread name을 사용한다")
    void integrationTaskExecutor_usesNamedThreads() throws Exception {
        var executor = new IntegrationExecutorConfig().integrationTaskExecutor();
        try {
            String threadName = executor.submit(() -> Thread.currentThread().getName())
                    .get(1, TimeUnit.SECONDS);

            assertThat(threadName).startsWith("integration-adapter-");
        } finally {
            executor.shutdownNow();
        }
    }
}
