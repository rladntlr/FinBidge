package com.finbridge.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@Configuration
@Slf4j
public class BatchJobConfig {

    // payload 항목 하나를 읽어 처리하는 단순 Job
    // requestId/payload 는 JobParameters 로 전달받아 로그에 기록

    @Bean
    @StepScope
    public ListItemReader<String> integrationItemReader() {
        return new ListItemReader<>(List.of("integration-payload"));
    }

    @Bean
    public ItemProcessor<String, String> integrationItemProcessor() {
        return item -> {
            log.info("[BATCH Processor] 페이로드 처리 중: {}", item);
            return "processed-" + item;
        };
    }

    @Bean
    public ItemWriter<String> integrationItemWriter() {
        return items -> items.forEach(item ->
                log.info("[BATCH Writer] 처리 완료 항목 기록: {}", item));
    }

    @Bean
    public Step integrationStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                ListItemReader<String> integrationItemReader,
                                ItemProcessor<String, String> integrationItemProcessor,
                                ItemWriter<String> integrationItemWriter) {
        return new StepBuilder("integrationStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(integrationItemReader)
                .processor(integrationItemProcessor)
                .writer(integrationItemWriter)
                .build();
    }

    @Bean
    public Job integrationJob(JobRepository jobRepository, Step integrationStep) {
        return new JobBuilder("integrationJob", jobRepository)
                .start(integrationStep)
                .build();
    }

}
