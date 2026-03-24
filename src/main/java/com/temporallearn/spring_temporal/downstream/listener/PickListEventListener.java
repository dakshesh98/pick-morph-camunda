package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Step 3 — Kafka listener for AE item picking event messages.
 *
 * Consumes messages from gor.pick-list.events published by AE as items are picked
 * throughout the pick lifecycle. On every event:
 *
 *   1. Updates ae_order.payload selectively (state/sub_state/actuals/expectations/attributes
 *      at both the parent Payload level and per-serviceRequest level).
 *
 *   2. Maps AE state transitions to internal commands and correlates the Camunda
 *      ItemPickingEventMessage signal. For pick_transaction events, the full event JSON
 *      is also passed as a process variable so SendItemPickedEventDelegate can iterate
 *      all service requests.
 *
 * State mapping:
 *   event_type = "pick_transaction"              → command = UPDATE,   status = IN_PROGRESS
 *   event_type = "update", state = "complete"    → command = COMPLETE, status = COMPLETED
 *   event_type = "update", any other state       → command = UPDATE,   status = IN_PROGRESS
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

        String pickId = eventPayload.getExternalServiceRequestId();
        String orderState = eventPayload.getState();
        String subState = eventPayload.getAttributes() != null
                ? eventPayload.getAttributes().getSubState() : null;

        log.info("Received pick-list event from AE | pickId: {} | event_type: {} | state: {} | sub_state: {}",
                pickId, eventType, orderState, subState);

        // Step 1 — update ae_order selectively (DB write before Camunda correlation)
        pickInstructionService.updateAeOrderFromEvent(event);

        // Step 2 — correlate Camunda signal
        // transactionUpdatesJson (JSON array) is only built and passed for pick_transaction events;
        // other event types only need command + aeEventType to drive the gateway.
        if ("pick_transaction".equalsIgnoreCase(eventType)) {
            List<TransactionUpdate> updates = buildPickTransactionUpdates(pickId, eventPayload);
            log.info("pick_transaction event | pickId: {} | transactions: {}", pickId, updates.size());
            correlatePickTransaction(updates, payload, pickId);
        } else {
            String command = resolveCommand(eventType, orderState, subState);
            log.info("AE update event | pickId: {} | command: {} | state: {} | sub_state: {}", pickId, command, orderState, subState);
            correlateUpdateEvent(command, eventType, subState, pickId);
        }
    }

    // ── State mapping ────────────────────────────────────────────────────────

    /**
     * Builds one TransactionUpdate per (serviceRequest × transaction) for pick_transaction events.
     * Each transaction needs its own idempotency check and ItemPickedEvent publish.
     */
    private List<TransactionUpdate> buildPickTransactionUpdates(String pickId, PickListEvent.Payload payload) {
        List<TransactionUpdate> updates = new ArrayList<>();
        if (payload.getServiceRequests() == null) return updates;
        for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
            if (sr.getTransactions() == null) continue;
            for (PickListEvent.Transaction tx : sr.getTransactions()) {
                updates.add(TransactionUpdate.builder()
                        .pickId(pickId)
                        .transactionId(tx.getTransactionId())
                        .command("UPDATE")
                        .transactionType("PICK")
                        .processedQty(tx.getContainerAttributes() != null
                                ? tx.getContainerAttributes().getQtyPicked() : 0)
                        .status("IN_PROGRESS")
                        .timestamp(Instant.now().toString())
                        .build());
            }
        }
        return updates;
    }

    /** Resolves the Camunda command for non-pick_transaction (update) events. */
    private String resolveCommand(String eventType, String orderState, String subState) {
        if ("update".equalsIgnoreCase(eventType)
                && "complete".equalsIgnoreCase(orderState)
                && "complete".equalsIgnoreCase(subState)) {
            return "COMPLETE";
        }
        return "UPDATE";
    }

    // ── Camunda correlation ──────────────────────────────────────────────────

    /** Correlates a pick_transaction event — includes transactionUpdatesJson (array) and pickListEventJson. */
    private void correlatePickTransaction(List<TransactionUpdate> updates, String rawEventJson, String pickId) {
        try {
            String transactionUpdatesJson = objectMapper.writeValueAsString(updates);
            runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickId", pickId)
                    .setVariable("command", "UPDATE")
                    .setVariable("aeEventType", "pick_transaction")
                    .setVariable("transactionUpdatesJson", transactionUpdatesJson)
                    .setVariable("pickListEventJson", rawEventJson)
                    .correlate();
            log.info("ItemPickingEventMessage correlated for pick_transaction | pickId: {} | transactions: {}",
                    pickId, updates.size());
        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance waiting for pickId: {} — pick_transaction event dropped", pickId);
        } catch (Exception e) {
            log.error("Failed to correlate pick_transaction for pickId: {} | error: {}", pickId, e.getMessage(), e);
        }
    }

    /** Correlates a non-pick_transaction (update) event — only command + aeEventType, no transactionUpdateJson. */
    private void correlateUpdateEvent(String command, String eventType, String subState, String pickId) {
        try {
            runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickId", pickId)
                    .setVariable("command", command)
                    .setVariable("aeEventType", eventType)
                    .setVariable("failureReason", subState)
                    .correlate();
            log.info("ItemPickingEventMessage correlated for update event | pickId: {} | command: {}", pickId, command);
        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance waiting for pickId: {} — update event dropped", pickId);
        } catch (Exception e) {
            log.error("Failed to correlate update event for pickId: {} | error: {}", pickId, e.getMessage(), e);
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
