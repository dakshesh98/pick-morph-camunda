package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import com.temporallearn.spring_temporal.service.PickInstructionService.ProcessResult;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Processes every item picking event correlated by PickListEventListener.
 *
 * pick_transaction events (aeEventType = "pick_transaction"):
 *   Calls processPickTransaction() which first pre-checks ALL transactionIds for duplicates.
 *   If ANY txId is already SUCCESS in transaction_status, the entire event is rejected:
 *     - No ae_order update.
 *     - No item_picked events enqueued.
 *     - txComplete stays false → BPMN loops back to waitForTransactionUpdate to wait
 *       for the next valid pick_transaction event.
 *   If all txIds are new, atomically in ONE DB transaction:
 *     1. Updates ae_order with actuals from the event.
 *     2. For every (serviceRequest × transaction) pair:
 *        a. Persists transaction_status.
 *        b. Builds the ItemPickedEvent payload.
 *        c. Writes an outbox_event row — OutboxRelayService publishes to item_picked.events.
 *
 * Update/complete events (aeEventType = "update"):
 *   No transaction processing. txComplete = false — commandGateway routes COMPLETE to markCompleteCmd.
 */
@Component
@Slf4j
public class SendItemPickedEventDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public SendItemPickedEventDelegate(PickInstructionService pickInstructionService,
                                       ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickInstructionId      = (String) execution.getVariable("pickInstructionId");
        String aeEventType = (String) execution.getVariable("aeEventType");

        log.info("SendItemPickedEventDelegate executing for pickInstructionId: {}, aeEventType: {}", pickInstructionId, aeEventType);

        String instructionJson = (String) execution.getVariable("instructionJson");
        Object rawPickListEventVar = execution.getVariable("pickListEventJson");
        String pickListEventJson = rawPickListEventVar instanceof byte[]
                ? new String((byte[]) rawPickListEventVar, java.nio.charset.StandardCharsets.UTF_8)
                : (String) rawPickListEventVar;

        PickInstruction pickInstruction = objectMapper.readValue(instructionJson, PickInstruction.class);
        PickListEvent   pickListEvent   = objectMapper.readValue(pickListEventJson, PickListEvent.class);

        if ("pick_transaction".equalsIgnoreCase(aeEventType)) {
            // Pre-checks all txIds; rejects entire event if any duplicate found.
            ProcessResult result = pickInstructionService.processPickTransaction(pickInstructionId, pickListEvent, pickInstruction);
            if (!result.processed()) {
                log.info("pick_transaction rejected (duplicate txId) for pickInstructionId: {} — " +
                         "looping back to waitForTransactionUpdate", pickInstructionId);
            } else if ("released".equals(result.orderStatus())) {
                log.info("pick_transaction resulted in released status for pickInstructionId: {} — triggering workflow completion", pickInstructionId);
                execution.setVariable("command", "COMPLETE");
            }
        }

        // Publish OrderUpdateEvent to order_update.events for both update and pick_transaction events
        pickInstructionService.enqueueOrderUpdate(pickInstructionId, pickInstruction, pickListEvent);

        // txStatus drives txFailedGateway: SUCCESS = normal path, FAILED = terminal failure path
        execution.setVariable("txStatus", "SUCCESS");
        // txComplete always false — completion is driven by command=COMPLETE via commandGateway
        execution.setVariable("txComplete", false);
    }
}
