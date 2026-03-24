package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.service.PickInstructionProcessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Step 1 — Kafka listener for inbound pick instructions.
 *
 * Consumes messages from gor.pick-instruction.events and starts a Camunda
 * "pickInstructionProcess" workflow for each instruction.
 *
 * The REST endpoint POST /Order/pick_instruction remains active for backward
 * compatibility and testing. Both paths call the same idempotent
 * {@link PickInstructionProcessService#startProcess(PickInstruction)}.
 */
@Service
@Slf4j
public class PickInstructionListener {

    private final PickInstructionProcessService pickInstructionProcessService;
    private final ObjectMapper objectMapper;

    public PickInstructionListener(PickInstructionProcessService pickInstructionProcessService,
                                   ObjectMapper objectMapper) {
        this.pickInstructionProcessService = pickInstructionProcessService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${kafka.topic.pick-instructions}",
            groupId = "pick-instructions-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickInstruction instruction;
        try {
            instruction = objectMapper.readValue(payload, PickInstruction.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickInstruction from Kafka message: {}", e.getMessage());
            return;
        }

        log.info("Received pick instruction via Kafka for pickInstructionId: {}", instruction.getPickInstructionId());

        try {
            pickInstructionProcessService.startProcess(instruction);
        } catch (Exception e) {
            log.error("Failed to start Camunda process for pickInstructionId: {} via Kafka trigger: {}",
                    instruction.getPickInstructionId(), e.getMessage(), e);
            // Re-throw to trigger Kafka consumer retry / dead-letter handling
            throw new RuntimeException("Failed to start process for pickInstructionId: " + instruction.getPickInstructionId(), e);
        }
    }
}
