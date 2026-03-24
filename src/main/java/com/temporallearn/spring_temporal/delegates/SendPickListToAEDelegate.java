package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.ae.PickListRequest;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import com.temporallearn.spring_temporal.service.PickListRequestMapper;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Step 2 — Reads the PickInstruction from the "instructionJson" process variable,
 * then atomically persists ae_order and enqueues an outbox event for pick-list.requests.
 *
 * Transactional Outbox guarantee (single DB transaction):
 *   persistAeOrder() writes ae_order + outbox_event atomically.
 *   The OutboxRelayService publishes the PENDING outbox row to gor.pick-list.requests.
 *   If the process crashes after DB commit, the relay re-publishes on next poll.
 *
 * On Camunda RETRY: ae_order write is skipped (idempotent), outbox row is re-inserted
 * so the message is re-published to AE.
 */
@Component
@Slf4j
public class SendPickListToAEDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final PickListRequestMapper pickListRequestMapper;
    private final ObjectMapper objectMapper;

    public SendPickListToAEDelegate(PickInstructionService pickInstructionService,
                                    PickListRequestMapper pickListRequestMapper,
                                    ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.pickListRequestMapper = pickListRequestMapper;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String instructionJson = (String) execution.getVariable("instructionJson");
        PickInstruction instruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        String pickInstructionId = instruction.getPickInstructionId();

        log.info("SendPickListToAEDelegate executing for pickInstructionId: {}", pickInstructionId);

        // Step 1 — build PickListRequest (pure mapping, no I/O)
        PickListRequest request = pickListRequestMapper.toPickListRequest(instruction);

        // Step 2 — atomically persist ae_order + enqueue outbox event (throws on failure → Camunda retries)
        // OutboxRelayService publishes the PENDING row to gor.pick-list.requests asynchronously.
        pickInstructionService.persistAeOrder(pickInstructionId, request);
    }
}
