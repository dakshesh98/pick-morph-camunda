package com.butler.aeorder.dto.ae;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Envelope DTO for messages consumed from the <tenant>.pick-list.response Kafka topic.
 * Published by SRMS after processing a pick-list request from AE.
 *
 * Example:
 * {
 *   "source_service": "srms",
 *   "timestamp": "2026-03-24T13:00:00Z",
 *   "message_id": "d84cef01-...",
 *   "entity_id": "Kellanova_order999977132",
 *   "name": "pick_list_response",
 *   "payload": { ...PickListResponse fields... },
 *   "context": { "execution_id": "0" }
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PickListResponseEvent {

    @JsonProperty("source_service")
    private String sourceService;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("message_id")
    private String messageId;

    @JsonProperty("entity_id")
    private String entityId;

    @JsonProperty("name")
    private String name;

    @JsonProperty("payload")
    private PickListResponse payload;

    @JsonProperty("context")
    private Context context;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Context {

        @JsonProperty("execution_id")
        private String executionId;
    }
}
