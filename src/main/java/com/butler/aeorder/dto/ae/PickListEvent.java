package com.butler.aeorder.dto.ae;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * AE contract DTO for <tenant>.pick-list.events topic.
 * Consumed by AA (this service) to track pick state transitions from AE.
 *
 * Two event_type values (in headers):
 *   - "update"           — order state / data changed
 *   - "pick_transaction" — an item was physically picked on bot
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PickListEvent {

    /** Kafka message headers — contains event_type. */
    @JsonProperty("headers")
    private Headers headers;

    @JsonProperty("value")
    private Value value;

    // ── Headers ─────────────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Headers {

        @JsonProperty("event_type")
        private String eventType;
    }

    // ── Envelope value ──────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Value {

        @JsonProperty("name")
        private String name;

        @JsonProperty("payload")
        private Payload payload;
    }

    // ── Payload (Order) ─────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Payload {

        @JsonProperty("id")
        private Long id;

        @JsonProperty("externalServiceRequestId")
        private String externalServiceRequestId;

        /** Order-level state: created, cancellation_locked, fullfillable, complete */
        @JsonProperty("state")
        private String state;

        @JsonProperty("type")
        private String type;

        @JsonProperty("fulfillmentArea")
        private List<String> fulfillmentArea;

        @JsonProperty("attributes")
        private PayloadAttributes attributes;

        @JsonProperty("serviceRequests")
        private List<ServiceRequest> serviceRequests;

        /**
         * Order-level actuals — populated by AE on pick-list.events updates.
         * Stored as Object (Map/List) to avoid brittle nested DTOs.
         */
        @JsonProperty("actuals")
        private Object actuals;

        /**
         * Order-level expectations — may be updated by AE on pick-list.events.
         */
        @JsonProperty("expectations")
        private Object expectations;

        @JsonProperty("createdOn")
        private String createdOn;

        @JsonProperty("updatedOn")
        private String updatedOn;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PayloadAttributes {

        @JsonProperty("event_type")
        private String eventType;

        @JsonProperty("sub_state")
        private String subState;

        @JsonProperty("cust_identity")
        private String custIdentity;

        @JsonProperty("destination")
        private String destination;

        @JsonProperty("flow_name")
        private String flowName;
    }

    // ── Service request (order line) within event ───────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceRequest {

        @JsonProperty("id")
        private Long id;

        @JsonProperty("externalServiceRequestId")
        private String externalServiceRequestId;

        @JsonProperty("type")
        private String type;

        /** PICK_LINE-level state */
        @JsonProperty("state")
        private String state;

        @JsonProperty("status")
        private String status;

        @JsonProperty("attributes")
        private ServiceRequestAttributes attributes;

        /**
         * Per-pick transactions for this service request.
         * Each entry represents one physical pick/drop/scan transaction.
         */
        @JsonProperty("transactions")
        private List<Transaction> transactions;

        /**
         * Service-request-level actuals — what AE actually processed for this PICK_LINE.
         * Stored as Object (Map/List) to avoid brittle nested DTOs.
         */
        @JsonProperty("actuals")
        private Object actuals;

        /**
         * Service-request-level expectations — may be updated by AE events.
         */
        @JsonProperty("expectations")
        private Object expectations;

        @JsonProperty("receivedOn")
        private String receivedOn;

        @JsonProperty("updatedOn")
        private String updatedOn;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceRequestAttributes {

        @JsonProperty("sub_state")
        private String subState;

        @JsonProperty("orderType")
        private String orderType;

        @JsonProperty("simple_priority")
        private String simplePriority;
    }

    // ── Transaction ──────────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Transaction {

        @JsonProperty("transactionId")
        private String transactionId;

        /**
         * State of the transaction (e.g. "complete", "in_progress").
         * Confluent/AE sends this as "state" (not "transactionState").
         */
        @JsonProperty("state")
        private String transactionState;

        /**
         * Type of the transaction container (e.g. "VIRTUAL", "PHYSICAL").
         * Confluent/AE sends this as "type" (not "transactionType").
         */
        @JsonProperty("type")
        private String transactionType;

        @JsonProperty("containerAttributes")
        private ContainerAttributes containerAttributes;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContainerAttributes {

        @JsonProperty("internal_order_id")
        private Long internalOrderId;

        @JsonProperty("lpn_id")
        private Long lpnId;

        @JsonProperty("qty_to_be_picked")
        private int qtyToBePicked;

        @JsonProperty("qty_picked")
        private int qtyPicked;

        /** created, loaded, complete, unloaded */
        @JsonProperty("status")
        private String status;

        @JsonProperty("bot_id")
        private String botId;

        @JsonProperty("pps_id")
        private String ppsId;
    }
}
