package com.butler.aeorder.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.ae.PickListEvent;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Kafka listener for AE pick-list.events.
 *
 * Receives all pick-list events (any event_type) and correlates
 * {@code ItemPickingEventMessage} on the Camunda process instance.
 * All business logic — ae_order update, transaction dedup, outbox dispatch —
 * is handled inside {@link com.butler.aeorder.delegates.ProcessPickListEventDelegate}
 * via {@link com.butler.aeorder.service.PickInstructionService#processPickListEvent}.
 *
 * command is initialised to UPDATE; the delegate overrides it to COMPLETE
 * when the derived order status turns "released".
 */
@Service
@Slf4j
public class PickListEventListener {

    private final RuntimeService runtimeService;
    private final ObjectMapper objectMapper;

    public PickListEventListener(RuntimeService runtimeService, ObjectMapper objectMapper) {
        this.runtimeService = runtimeService;
        this.objectMapper = objectMapper;
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

        PickListEvent.Payload eventPayload = event.getPayload();
        if (eventPayload == null) {
            log.error("PickListEvent has no payload — dropping message");
            return;
        }

        String pickInstructionId = eventPayload.getExternalServiceRequestId();
        String eventType = resolveEventType(event);
        String orderState = eventPayload.getState();
        String subState   = eventPayload.getAttributes() != null ? eventPayload.getAttributes().getSubState() : null;

        log.info("Received pick-list event from AE | pickInstructionId: {} | event_type: {} | state: {} | sub_state: {}",
                pickInstructionId, eventType, orderState, subState);

        correlateEvent(pickInstructionId, payload);
    }

    private void correlateEvent(String pickInstructionId, String rawEventJson) {
        try {
            runtimeService.createMessageCorrelation("ItemPickingEventMessage")
                    .processInstanceVariableEquals("pickInstructionId", pickInstructionId)
                    .setVariable("command", "UPDATE")
                    .setVariable("pickListEventJson", rawEventJson.getBytes(StandardCharsets.UTF_8))
                    .correlate();
            log.info("ItemPickingEventMessage correlated | pickInstructionId: {}", pickInstructionId);
        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance waiting for pickInstructionId: {} — event dropped", pickInstructionId);
        } catch (Exception e) {
            log.error("Failed to correlate event for pickInstructionId: {} | error: {}", pickInstructionId, e.getMessage(), e);
        }
    }

    /** Resolves event_type: context first, then payload.attributes fallback. */
    private String resolveEventType(PickListEvent event) {
        if (event.getContext() != null && event.getContext().getEventType() != null) {
            return event.getContext().getEventType();
        }
        if (event.getPayload() != null && event.getPayload().getAttributes() != null) {
            return event.getPayload().getAttributes().getEventType();
        }
        return "unknown";
    }
}
