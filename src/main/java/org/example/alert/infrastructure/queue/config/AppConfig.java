package org.example.alert.infrastructure.queue.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Register Java 8 time module for Instant serialization
        mapper.findAndRegisterModules();

        // Pretty print for debugging
        // mapper.enable(SerializationFeature.INDENT_OUTPUT);

        return mapper;
    }
}
