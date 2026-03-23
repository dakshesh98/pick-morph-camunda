package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Step 2 — Reads the PickInstruction from the "instructionJson" process variable
 * and sends a PickListRequest to AE via gor.pick-list.requests.
 *
 * Also invoked on RETRY to re-send the picklist after a retriable failure.
 */
@Component
@Slf4j
public class SendPickListToAEDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public SendPickListToAEDelegate(PickInstructionService pickInstructionService,
                                    ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String instructionJson = (String) execution.getVariable("instructionJson");
        PickInstruction instruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        log.info("SendPickListToAEDelegate executing for pickId: {}", instruction.getPickId());
        pickInstructionService.sendPicklistOrderToAE(instruction);
    }
}
