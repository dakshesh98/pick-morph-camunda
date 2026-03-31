package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Generic event envelope for all Kafka messages published and consumed by this service.
 * Wraps the business payload with routing and tracing metadata.
 *
 * <p>P = Payload type (e.g. SrmsPickListResponse, TransactionUpdate, AePickListRequest)
 *
 * <p>On the publish side, {@code OutboxService} builds this envelope automatically.
 * On the consume side, deserialize with {@code TypeReference<KafkaEventEnvelope<MyPayload>>}.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class KafkaEventEnvelope<P> {

    private String sourceService;
    private String timestamp;
    private String messageId;
    private String entityId;
    private String name;
    private P payload;
    private Context context;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Context {
        private String executionId;
    }
}
