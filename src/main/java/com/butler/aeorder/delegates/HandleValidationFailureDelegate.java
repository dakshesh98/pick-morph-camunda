package com.butler.aeorder.delegates;

import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Atomically publishes a failure response to "pick-instruction.response" and
 * deletes the AE order from PostgreSQL in a single transaction.
 *
 * Replaces the previous two-step sequence of PublishPickInstructionResponseDelegate
 * (failure) → TerminateAeOrderDelegate, which ran in separate transactions.
 *
 * Called exclusively on the BPMN validation-failure path (validationSuccess == false).
 */
@Component
@Slf4j
public class HandleValidationFailureDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public HandleValidationFailureDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickId      = (String) execution.getVariable("pickId");
        String status      = (String) execution.getVariable("validationStatus");
        String orderId     = (String) execution.getVariable("validationOrderId");
        String orderlineId = (String) execution.getVariable("validationOrderlineId");
        String message     = (String) execution.getVariable("validationMessage");
        String errorCode   = (String) execution.getVariable("validationErrorCode");
        String errorsJson  = (String) execution.getVariable("validationErrors");
        pickInstructionService.terminateWithFailureResponse(pickId, status, orderId, orderlineId, message, errorCode, errorsJson);
        log.info("Failure response queued and AE order deleted atomically | pickId: {}", pickId);
    }
}
