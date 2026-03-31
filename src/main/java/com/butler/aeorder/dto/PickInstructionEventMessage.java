package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;

/**
 * Envelope DTO for messages consumed from the "pick-instruction.requests" Kafka topic.
 * The actual pick instruction data is in {@link #payload}; the envelope carries routing
 * and tracing metadata.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PickInstructionEventMessage {

    private String sourceService;
    private String timestamp;
    private String messageId;
    private String entityId;
    private String name;
    private PickInstructionRequestMessage payload;
    private Context context;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Context {
        private String executionId;
    }
}
