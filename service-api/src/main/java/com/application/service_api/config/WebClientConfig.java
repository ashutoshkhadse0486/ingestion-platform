package com.application.service_api.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public WebClient airflowWebClient(@Value("${airflow.base-url}") String baseUrl, @Value("${airflow.username}") String username, @Value("${airflow.password}") String password) {
        // Instantiate the builder
        return WebClient.builder().baseUrl(baseUrl).defaultHeaders(headers -> headers.setBasicAuth(username, password)).build();
    }

    @Bean
    public WebClient.Builder webClientBuilder() {
        return WebClient.builder();
    }
}