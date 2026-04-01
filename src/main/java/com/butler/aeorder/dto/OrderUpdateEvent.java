package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Outbound event published to &lt;tenant&gt;.order_update.events (consumed by Butler Core).
 *
 * Published for every pick-list event received from AE — both event_type=update
 * and event_type=pick_transaction — so Butler Core can call
 * make_and_send_order_related_notifications for its business orders.
 *
 * Field sources:
 *   order_id / orderline_id   — PickInstruction (Camunda process variable)
 *   pick_instruction_id       — PickInstruction.pickInstructionId
 *   transaction_id            — same as pick_instruction_id (PickInstruction.pickInstructionId)
 *   state / sub_state         — PickListEvent.payload (AE event)
 *   actuals                   — PickListEvent.payload.serviceRequests[i].actuals (per SR)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderUpdateEvent {

    @JsonProperty("pick_instruction_id")
    private String pickInstructionId;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("orderlineId")
    private String orderlineId;

    @JsonProperty("state")
    private String state;

    @JsonProperty("subState")
    private String subState;

    @JsonProperty("transaction")
    private Object transaction;
}
