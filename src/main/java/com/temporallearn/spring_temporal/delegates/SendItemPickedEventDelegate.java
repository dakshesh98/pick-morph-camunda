package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.service.PickInstructionService;
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

        if ("pick_transaction".equalsIgnoreCase(aeEventType)) {
            String instructionJson  = (String) execution.getVariable("instructionJson");
            String pickListEventJson = (String) execution.getVariable("pickListEventJson");

            PickInstruction pickInstruction = objectMapper.readValue(instructionJson, PickInstruction.class);
            PickListEvent   pickListEvent   = objectMapper.readValue(pickListEventJson, PickListEvent.class);

            // Pre-checks all txIds; rejects entire event if any duplicate found.
            // Returns false → BPMN loops back to waitForTransactionUpdate via default UPDATE path.
            boolean processed = pickInstructionService.processPickTransaction(pickInstructionId, pickListEvent, pickInstruction);
            if (!processed) {
                log.info("pick_transaction rejected (duplicate txId) for pickInstructionId: {} — " +
                         "looping back to waitForTransactionUpdate", pickInstructionId);
            }
        }

        // txComplete always false — completion is driven by command=COMPLETE via commandGateway
        execution.setVariable("txComplete", false);
    }
}
