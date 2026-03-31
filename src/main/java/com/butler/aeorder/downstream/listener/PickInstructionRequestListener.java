package com.butler.aeorder.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.PickInstructionEventMessage;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.service.PickInstructionProcessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka listener for pick instruction events published by butler_server.
 *
 * Consumes messages from "pick-instruction.requests" and starts a Camunda process.
 * The process is responsible for constructing the AE order payload, persisting it
 * to PostgreSQL, and publishing it to "pick-list.requests".
 *
 * Field semantics:
 *   id            → pick_instruction_id = AE externalServiceRequestId (workflow/order PK)
 *   order_id      → normal/main order ID
 *   orderline_id  → normal/main orderline ID
 *   tpid          → product type ID (integer) — gRPC use only, NOT the product SKU
 */
@Slf4j
@Service
public class PickInstructionRequestListener {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private PickInstructionProcessService processService;

    @KafkaListener(
            topics = "${kafka.topics.pick-instructions-request}",
            groupId = "pick-instruction-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickInstructionEventMessage event;
        try {
            event = objectMapper.readValue(payload, PickInstructionEventMessage.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickInstructionEventMessage: {}", e.getMessage());
            return;
        }

        PickInstructionRequestMessage msg = event.getPayload();

        if (msg.getId() == null || msg.getId().isBlank()) {
            log.error("Received pick-instruction.requests message with null/blank id — dropping");
            return;
        }
        if (msg.getOrderId() == null || msg.getOrderId().isBlank()) {
            log.error("Received pick-instruction.requests with null/blank orderId for pickId: {} — dropping", msg.getId());
            return;
        }
        if (msg.getOrderlineId() == null || msg.getOrderlineId().isBlank()) {
            log.error("Received pick-instruction.requests with null/blank orderlineId for pickId: {} — dropping", msg.getId());
            return;
        }
        if (msg.getQty() <= 0) {
            log.error("Received pick-instruction.requests with invalid qty: {} for pickId: {} — dropping", msg.getQty(), msg.getId());
            return;
        }
        if (msg.getSlotId() == null || msg.getSlotId().isBlank()) {
            log.error("Received pick-instruction.requests with null/blank slotId for pickId: {} — dropping", msg.getId());
            return;
        }

        log.info("Received pick-instruction | pickId: {} | orderId: {} | tpid: {} | ppsId: {}",
                msg.getId(), msg.getOrderId(), msg.getTpid(), msg.getPpsId());

        processService.startProcess(msg);
    }
}
