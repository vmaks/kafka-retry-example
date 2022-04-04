package com.maks.kafkaretry;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class TreadPoolTaskSchedulerConfig {

    // TODO:
    @Bean
    public ThreadPoolTaskScheduler threadPoolTaskScheduler() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(4);
        threadPoolTaskScheduler.setThreadNamePrefix("KafkaListenerThreadPoolTaskScheduler");
        return threadPoolTaskScheduler;
    }
}
