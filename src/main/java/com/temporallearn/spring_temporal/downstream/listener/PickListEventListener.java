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

        // Step 2 — map to internal TransactionUpdate and correlate Camunda signal
        TransactionUpdate transactionUpdate = mapToTransactionUpdate(eventType, orderState, subState, pickId, eventPayload);

        log.info("Mapped AE event to TransactionUpdate | pickId: {} | command: {} | status: {} | transactionId: {}",
                pickId, transactionUpdate.getCommand(), transactionUpdate.getStatus(), transactionUpdate.getTransactionId());

        correlateCamundaMessage(transactionUpdate, event, eventType, payload);
    }

    // ── State mapping ────────────────────────────────────────────────────────

    private TransactionUpdate mapToTransactionUpdate(String eventType,
                                                     String orderState,
                                                     String subState,
                                                     String pickId,
                                                     PickListEvent.Payload payload) {
        String command;
        String status;

        if ("pick_transaction".equalsIgnoreCase(eventType)) {
            command = "UPDATE";
            status = "IN_PROGRESS";
        } else if ("update".equalsIgnoreCase(eventType)
                && "complete".equalsIgnoreCase(orderState)
                && "complete".equalsIgnoreCase(subState)) {
            command = "COMPLETE";
            status = "COMPLETED";
        } else {
            command = "UPDATE";
            status = "IN_PROGRESS";
        }

        String transactionId = extractTransactionId(payload, pickId);
        int processedQty = extractQtyPicked(payload);

        return TransactionUpdate.builder()
                .pickId(pickId)
                .transactionId(transactionId)
                .command(command)
                .transactionType("PICK")
                .processedQty(processedQty)
                .status(status)
                .timestamp(Instant.now().toString())
                .additionalInfo(subState)
                .build();
    }

    // ── Camunda correlation ──────────────────────────────────────────────────

    private void correlateCamundaMessage(TransactionUpdate transactionUpdate,
                                         PickListEvent event,
                                         String eventType,
                                         String rawEventJson) {
        try {
            String transactionUpdateJson = objectMapper.writeValueAsString(transactionUpdate);
            String command = transactionUpdate.getCommand() != null
                    ? transactionUpdate.getCommand().toUpperCase()
                    : "UPDATE";

            var correlation = runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickId", transactionUpdate.getPickId())
                    .setVariable("command", command)
                    .setVariable("txStatus", transactionUpdate.getStatus())
                    .setVariable("transactionUpdateJson", transactionUpdateJson)
                    .setVariable("transactionId", transactionUpdate.getTransactionId())
                    .setVariable("failureReason", transactionUpdate.getAdditionalInfo());

            // For pick_transaction events pass the full event JSON so the delegate can
            // iterate all serviceRequests and publish one ItemPickedEvent per transaction
            if ("pick_transaction".equalsIgnoreCase(eventType)) {
                correlation.setVariable("pickListEventJson", rawEventJson);
            }

            correlation.correlate();

            log.info("ItemPickingEventMessage correlated successfully for pickId: {} | command: {} | status: {}",
                    transactionUpdate.getPickId(), command, transactionUpdate.getStatus());

        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance found waiting for ItemPickingEventMessage for pickId: {}. " +
                    "AE item picking event dropped. transactionId: {}",
                    transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        } catch (Exception e) {
            log.error("Failed to correlate ItemPickingEventMessage for pickId: {} | error: {}",
                    transactionUpdate.getPickId(), e.getMessage(), e);
        }
    }

    // ── Extraction helpers ───────────────────────────────────────────────────

    /**
     * Resolves event_type: headers.event_type first, then payload.attributes.event_type fallback.
     */
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

    /**
     * Extracts transactionId from serviceRequests[0].transactions[0].
     * Falls back to pickId if absent.
     */
    private String extractTransactionId(PickListEvent.Payload payload, String fallback) {
        PickListEvent.Transaction tx = getFirstTransaction(payload);
        if (tx != null && tx.getTransactionId() != null && !tx.getTransactionId().isEmpty()) {
            return tx.getTransactionId();
        }
        return fallback;
    }

    /**
     * Extracts qty_picked from serviceRequests[0].transactions[0].containerAttributes.
     */
    private int extractQtyPicked(PickListEvent.Payload payload) {
        PickListEvent.Transaction tx = getFirstTransaction(payload);
        if (tx != null && tx.getContainerAttributes() != null) {
            return tx.getContainerAttributes().getQtyPicked();
        }
        return 0;
    }

    /**
     * Returns serviceRequests[0].transactions[0], or null if not present.
     */
    private PickListEvent.Transaction getFirstTransaction(PickListEvent.Payload payload) {
        List<PickListEvent.ServiceRequest> serviceRequests = payload.getServiceRequests();
        if (serviceRequests == null || serviceRequests.isEmpty()) {
            return null;
        }
        List<PickListEvent.Transaction> transactions = serviceRequests.get(0).getTransactions();
        if (transactions == null || transactions.isEmpty()) {
            return null;
        }
        return transactions.get(0);
    }
}
