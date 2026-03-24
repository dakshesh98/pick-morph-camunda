package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Processes every item picking event correlated by PickListEventListener.
 *
 * pick_transaction events (aeEventType = "pick_transaction"):
 *   Iterates ALL (serviceRequest × transaction) pairs from transactionUpdatesJson.
 *   Per transaction:
 *     1. Validate and persist to transaction_status DB (idempotency gate).
 *     2. If duplicate — skip only that transaction (continue loop).
 *     3. Otherwise — publish one ItemPickedEvent for that transaction.
 *   Sets txComplete = false — loop back to wait for next event.
 *
 * Update/complete events (aeEventType = "update"):
 *   - Skip transaction_status write and item picked publish entirely.
 *   - Set txComplete = false — commandGateway routes COMPLETE events to markCompleteCmd.
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
        String pickId = (String) execution.getVariable("pickId");
        String aeEventType = (String) execution.getVariable("aeEventType");
        boolean isPickTransaction = "pick_transaction".equalsIgnoreCase(aeEventType);

        log.info("SendItemPickedEventDelegate executing for pickId: {}, aeEventType: {}", pickId, aeEventType);

        if (isPickTransaction) {
            String transactionUpdatesJson = (String) execution.getVariable("transactionUpdatesJson");
            String instructionJson = (String) execution.getVariable("instructionJson");
            String pickListEventJson = (String) execution.getVariable("pickListEventJson");

            List<TransactionUpdate> updates = objectMapper.readValue(
                    transactionUpdatesJson, new TypeReference<List<TransactionUpdate>>() {});
            PickInstruction pickInstruction = objectMapper.readValue(instructionJson, PickInstruction.class);
            PickListEvent pickListEvent = objectMapper.readValue(pickListEventJson, PickListEvent.class);

            for (TransactionUpdate tu : updates) {
                // Step 1 — idempotency check + write to transaction_status DB (per transaction)
                Optional<Boolean> result = pickInstructionService.validateAndPersist(pickId, tu);
                if (result.isEmpty()) {
                    log.info("Duplicate transaction {} skipped for pickId: {} — continuing to next", tu.getTransactionId(), pickId);
                    continue;
                }
                // Step 2 — DB committed; publish one ItemPickedEvent for this transaction
                pickInstructionService.publishItemPickedEvent(pickId, pickInstruction, pickListEvent, tu.getTransactionId());
            }
        }

        // txComplete always false here — completion is driven by command=COMPLETE via commandGateway
        execution.setVariable("txComplete", false);
    }
}
