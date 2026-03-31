package com.butler.aeorder.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

@Configuration
@EnableScheduling
public class AppConfig {

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder,
                                     @Value("${butler.http.timeout-ms:5000}") long timeoutMs) {
        Duration timeout = Duration.ofMillis(timeoutMs);
        return builder
                .connectTimeout(timeout)
                .readTimeout(timeout)
                .build();
    }
}
