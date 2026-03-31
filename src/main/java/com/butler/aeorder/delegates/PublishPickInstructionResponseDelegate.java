package com.butler.aeorder.delegates;

import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Publishes the validation outcome to "pick-instruction.response" so that
 * butler_server is notified whether the AE order was accepted or rejected.
 * Uses the transactional outbox via PickInstructionService.
 *
 * Reads process variables:
 *   pickId                - pick instruction ID
 *   validationStatus      - "SUCCESS" or "FAILURE" from SRMS
 *   validationOrderId     - serviceRequestId from SRMS (String, null on FAILURE)
 *   validationOrderlineId - serviceRequestId from serviceRequests[0] (String, SUCCESS only)
 *   validationMessage     - message from SRMS response
 *   validationErrorCode   - errorCode from SRMS response (FAILURE only)
 *   validationErrors      - JSON-serialized errors array (FAILURE only)
 */
@Component
@Slf4j
public class PublishPickInstructionResponseDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public PublishPickInstructionResponseDelegate(PickInstructionService pickInstructionService) {
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

        pickInstructionService.publishPickInstructionResponse(pickId, status, orderId, orderlineId, message, errorCode, errorsJson);
        log.info("Queued pick-instruction.response via outbox | pickId: {} | status: {}", pickId, status);
    }
}
