package com.temporallearn.spring_temporal.dto.ae;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * AE contract DTO for <tenant>.pick-list.response topic.
 * Consumed by AA (this service) from AE after a pick-list.request is processed.
 * Handles both SUCCESS and FAILURE responses (nullable fields for failure case).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PickListResponse {

    @JsonProperty("externalServiceRequestId")
    private String externalServiceRequestId;

    /** AE-internal order ID. Null on FAILURE. */
    @JsonProperty("serviceRequestId")
    private Long serviceRequestId;

    /** AE-internal execution ID. Null on FAILURE. */
    @JsonProperty("executionId")
    private String executionId;

    /** "SUCCESS" or "FAILURE" */
    @JsonProperty("status")
    private String status;

    /** Error code on FAILURE (e.g. VALIDATION_FAILED). Null on SUCCESS. */
    @JsonProperty("errorCode")
    private String errorCode;

    @JsonProperty("message")
    private String message;

    /** Populated only on FAILURE with per-field validation errors. */
    @JsonProperty("errors")
    private List<FieldError> errors;

    @JsonProperty("timestamp")
    private String timestamp;

    /** Per-orderline service request IDs returned on SUCCESS. */
    @JsonProperty("serviceRequests")
    private List<ServiceRequestRef> serviceRequests;

    // ── Nested types ────────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldError {

        @JsonProperty("field")
        private String field;

        @JsonProperty("code")
        private String code;

        @JsonProperty("message")
        private String message;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceRequestRef {

        @JsonProperty("externalServiceRequestId")
        private String externalServiceRequestId;

        @JsonProperty("serviceRequestId")
        private Long serviceRequestId;
    }

    // ── Convenience helpers ─────────────────────────────────────────────────

    public boolean isSuccess() {
        return "SUCCESS".equalsIgnoreCase(status);
    }
}
