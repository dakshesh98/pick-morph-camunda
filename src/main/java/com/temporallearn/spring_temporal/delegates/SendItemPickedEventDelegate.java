package com.temporallearn.spring_temporal.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Processes every item picking event received from AE in three ordered steps:
 *
 *   1. Validate and persist — idempotency check + write to transaction_status DB.
 *      If the transaction is a duplicate, stop here (do not publish to Kafka).
 *
 *   2. Publish item picked event — only after DB commit, publish TransactionUpdate
 *      to gor.item-picked-events (consumed by Butler Core).
 *
 *   3. Set outcome process variables:
 *      - txComplete (Boolean)      — true if the pick is now fully complete.
 *      - finalStatus ("COMPLETED") — set only when txComplete == true.
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
        String transactionUpdateJson = (String) execution.getVariable("transactionUpdateJson");

        TransactionUpdate transactionUpdate = objectMapper.readValue(transactionUpdateJson, TransactionUpdate.class);

        log.info("SendItemPickedEventDelegate executing for pickId: {}, transactionId: {}",
                pickId, transactionUpdate.getTransactionId());

        // Step 1 — validate and persist to DB (commit gate)
        Optional<Boolean> result = pickInstructionService.validateAndPersist(pickId, transactionUpdate);

        if (result.isEmpty()) {
            // Duplicate transaction — skip Kafka publish, treat as in-progress
            log.info("Duplicate transaction skipped for pickId: {}. Waiting for next event.", pickId);
            execution.setVariable("txComplete", false);
            return;
        }

        // Step 2 — DB committed, now safe to publish to Kafka
        pickInstructionService.sendItemPickedEvent(transactionUpdate);

        // Step 3 — set outcome variables
        boolean txComplete = result.get();
        log.info("Item picked event published for pickId: {}, txComplete: {}", pickId, txComplete);

        execution.setVariable("txComplete", txComplete);

        if (txComplete) {
            execution.setVariable("finalStatus", "COMPLETED");
        }
    }
}
