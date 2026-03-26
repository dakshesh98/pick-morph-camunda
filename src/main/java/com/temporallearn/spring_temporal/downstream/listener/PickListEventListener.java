package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


/**
 * Step 3 — Kafka listener for AE item picking event messages.
 *
 * Consumes messages from gor.pick-list.events published by AE as items are picked
 * throughout the pick lifecycle.
 *
 * pick_transaction events:
 *   Correlates the Camunda ItemPickingEventMessage with pickListEventJson.
 *   ae_order update + transaction_status + outbox writes are all performed atomically
 *   inside SendItemPickedEventDelegate → PickInstructionService.processPickTransaction().
 *
 * update events:
 *   Updates ae_order selectively (state/sub_state/actuals/expectations/attributes),
 *   then correlates the Camunda signal with the resolved command (UPDATE or COMPLETE).
 *
 * State mapping:
 *   event_type = "pick_transaction"                          → command = UPDATE (or COMPLETE if released)
 *   event_type = "update", derivedOrderStatus = "released"  → command = COMPLETE
 *   event_type = "update", any other derivedOrderStatus     → command = UPDATE
 */
@Service
@Slf4j
public class PickListEventListener {

    private final RuntimeService runtimeService;
    private final ObjectMapper objectMapper;
    private final PickInstructionService pickInstructionService;

    public PickListEventListener(RuntimeService runtimeService,
                                 ObjectMapper objectMapper,
                                 PickInstructionService pickInstructionService) {
        this.runtimeService = runtimeService;
        this.objectMapper = objectMapper;
        this.pickInstructionService = pickInstructionService;
    }

    @KafkaListener(
            topics = "${kafka.topic.pick-list-events}",
            groupId = "pick-list-events-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickListEvent event;
        try {
            event = objectMapper.readValue(payload, PickListEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickListEvent from Kafka message: {}", e.getMessage());
            return;
        }

        String eventType = resolveEventType(event);
        PickListEvent.Payload eventPayload = event.getValue() != null ? event.getValue().getPayload() : null;

        if (eventPayload == null) {
            log.error("PickListEvent has no payload — dropping message");
            return;
        }

        String pickInstructionId = eventPayload.getExternalServiceRequestId();
        String orderState = eventPayload.getState();
        String subState = eventPayload.getAttributes() != null
                ? eventPayload.getAttributes().getSubState() : null;

        log.info("Received pick-list event from AE | pickInstructionId: {} | event_type: {} | state: {} | sub_state: {}",
                pickInstructionId, eventType, orderState, subState);

        if ("pick_transaction".equalsIgnoreCase(eventType)) {
            // ae_order update is deferred to the delegate so all DB writes are atomic.
            // Only the raw event JSON is passed as a process variable.
            log.info("pick_transaction event | pickInstructionId: {}", pickInstructionId);
            correlatePickTransaction(payload, pickInstructionId);
        } else {
            // update events: ae_order updated here (no transaction_status involved)
            String derivedOrderStatus = pickInstructionService.updateAeOrderFromEvent(event);
            String command = resolveCommand(derivedOrderStatus);
            log.info("AE update event | pickInstructionId: {} | command: {} | state: {} | sub_state: {} | derivedStatus: {}",
                    pickInstructionId, command, orderState, subState, derivedOrderStatus);
            correlateUpdateEvent(command, eventType, subState, pickInstructionId, payload);
        }
    }

    // ── State mapping ────────────────────────────────────────────────────────

    /** Resolves the Camunda command: COMPLETE when derived order status is released, UPDATE otherwise. */
    private String resolveCommand(String derivedOrderStatus) {
        return "released".equals(derivedOrderStatus) ? "COMPLETE" : "UPDATE";
    }

    // ── Camunda correlation ──────────────────────────────────────────────────

    /**
     * Correlates a pick_transaction event.
     * Passes only pickListEventJson — ae_order update and all DB writes happen atomically
     * inside SendItemPickedEventDelegate via processPickTransaction().
     */
    private void correlatePickTransaction(String rawEventJson, String pickInstructionId) {
        try {
            runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickInstructionId", pickInstructionId)
                    .setVariable("command", "UPDATE")
                    .setVariable("aeEventType", "pick_transaction")
                    .setVariable("pickListEventJson", rawEventJson.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .correlate();
            log.info("ItemPickingEventMessage correlated for pick_transaction | pickInstructionId: {}", pickInstructionId);
        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance waiting for pickInstructionId: {} — pick_transaction event dropped", pickInstructionId);
        } catch (Exception e) {
            log.error("Failed to correlate pick_transaction for pickInstructionId: {} | error: {}", pickInstructionId, e.getMessage(), e);
        }
    }

    /** Correlates a non-pick_transaction (update) event — passes raw event JSON so the delegate can enqueue order updates. */
    private void correlateUpdateEvent(String command, String eventType, String subState, String pickInstructionId, String rawEventJson) {
        try {
            runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickInstructionId", pickInstructionId)
                    .setVariable("command", command)
                    .setVariable("aeEventType", eventType)
                    .setVariable("failureReason", subState)
                    .setVariable("pickListEventJson", rawEventJson.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                    .correlate();
            log.info("ItemPickingEventMessage correlated for update event | pickInstructionId: {} | command: {}", pickInstructionId, command);
        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance waiting for pickInstructionId: {} — update event dropped", pickInstructionId);
        } catch (Exception e) {
            log.error("Failed to correlate update event for pickInstructionId: {} | error: {}", pickInstructionId, e.getMessage(), e);
        }
    }

    // ── Extraction helpers ───────────────────────────────────────────────────

    /** Resolves event_type: headers.event_type first, then payload.attributes.event_type fallback. */
    private String resolveEventType(PickListEvent event) {
        if (event.getHeaders() != null && event.getHeaders().getEventType() != null) {
            return event.getHeaders().getEventType();
        }
        if (event.getValue() != null
                && event.getValue().getPayload() != null
                && event.getValue().getPayload().getAttributes() != null) {
            return event.getValue().getPayload().getAttributes().getEventType();
        }
        return "update";
    }

}
