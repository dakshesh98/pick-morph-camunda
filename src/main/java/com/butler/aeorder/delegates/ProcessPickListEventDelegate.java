package com.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.PickInstruction;
import com.butler.aeorder.dto.ae.PickListEvent;
import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Unified delegate that processes every pick-list event regardless of event_type.
 *
 * Replaces SendItemPickedEventDelegate. Delegates all business logic to
 * {@link PickInstructionService#processPickListEvent}, which:
 *   1. Updates ae_order.
 *   2. For every transaction whose container status is new (dedup check):
 *        loaded / complete  → ItemPickedEvent (danglingArea = "bot")
 *        unloaded           → ItemPickedEvent (danglingArea = null)
 *        created            → OrderUpdateEvent (transaction data as actuals)
 *   3. Enqueues an SR-level OrderUpdateEvent.
 *
 * Sets command = COMPLETE when derived order status is "released".
 */
@Component
@Slf4j
public class ProcessPickListEventDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public ProcessPickListEventDelegate(PickInstructionService pickInstructionService,
                                        ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickInstructionId = (String) execution.getVariable("pickInstructionId");
        String instructionJson   = (String) execution.getVariable("instructionJson");

        Object rawEvent = execution.getVariable("pickListEventJson");
        String pickListEventJson = rawEvent instanceof byte[]
                ? new String((byte[]) rawEvent, StandardCharsets.UTF_8)
                : (String) rawEvent;

        PickInstruction pickInstruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        PickListEvent   pickListEvent   = objectMapper.readValue(pickListEventJson, PickListEvent.class);

        log.info("ProcessPickListEventDelegate executing for pickInstructionId: {}", pickInstructionId);

        String orderStatus = pickInstructionService.processPickListEvent(
                pickInstructionId, pickListEvent, pickInstruction);

        if ("released".equals(orderStatus)) {
            log.info("Order released for pickInstructionId: {} — triggering workflow completion", pickInstructionId);
            execution.setVariable("command", "COMPLETE");
        }

        // txStatus drives txFailedGateway; txComplete drives updateResultGateway
        execution.setVariable("txStatus", "SUCCESS");
        execution.setVariable("txComplete", false);
    }
}
