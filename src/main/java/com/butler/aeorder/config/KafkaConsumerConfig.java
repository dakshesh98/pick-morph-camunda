package com.butler.aeorder.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka consumer configuration for DTO-consuming listeners.
 * Provides a dedicated listener container factory with StringDeserializer;
 * listeners handle JSON-to-DTO conversion via ObjectMapper themselves,
 * which avoids type-header issues with Spring's JsonDeserializer.
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * ConsumerFactory using StringDeserializer for both key and value.
     * Listeners parse the JSON string into DTOs via ObjectMapper.
     */
    @Bean
    public ConsumerFactory<String, Object> jsonConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));

        // Use StringDeserializer for both key and value.
        // The listeners handle JSON→DTO conversion via ObjectMapper themselves,
        // which avoids type-header issues and gives full control over error handling.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Listener container factory that uses JSON deserialization.
     * Reference this factory in @KafkaListener via:
     *   containerFactory = "jsonKafkaListenerContainerFactory"
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> jsonKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> jsonConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(jsonConsumerFactory);
        // Skip up to 3 retries, then give up on the bad record and move on
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2L)));
        return factory;
    }
}
