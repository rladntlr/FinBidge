package com.finbridge.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class IntegrationExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService integrationTaskExecutor() {
        AtomicInteger threadNumber = new AtomicInteger(1);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("integration-adapter-" + threadNumber.getAndIncrement());
            return thread;
        };
        return Executors.newFixedThreadPool(10, threadFactory);
    }
}
