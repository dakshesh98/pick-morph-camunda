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
 * persists it to ae_order (DB-first commit gate), then sends a PickListRequest to
 * AE via gor.pick-list.requests.
 *
 * DB-first ordering guarantee:
 *   1. persistAeOrder()       — writes ae_order to postgres; throws on failure → Camunda retries
 *   2. sendPicklistOrderToAE() — publishes to Kafka; only reached after DB commit
 *
 * Also invoked on RETRY — persistAeOrder() is idempotent (skips if record already exists).
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
        String pickId = instruction.getPickId();

        log.info("SendPickListToAEDelegate executing for pickId: {}", pickId);

        // Step 1 — build PickListRequest (pure mapping, no I/O)
        PickListRequest request = pickListRequestMapper.toPickListRequest(instruction);

        // Step 2 — DB commit gate: persist ae_order FIRST (throws on failure → no Kafka send)
        pickInstructionService.persistAeOrder(pickId, request);

        // Step 3 — DB committed; now safe to publish to AE
        pickInstructionService.sendPicklistOrderToAE(pickId, request);
    }
}
