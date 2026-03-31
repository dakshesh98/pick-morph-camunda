package com.butler.aeorder.delegates;

import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Terminal cleanup delegate — always invoked at the end of every process path.
 * Reads "pickInstructionId", "finalStatus", and "finalFailureReason" from process variables,
 * then delegates to PickInstructionService.onWorkflowComplete() for status
 * validation against Butler Core and audit event publication to Kafka.
 */
@Component
@Slf4j
public class OnWorkflowCompleteDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;

    public OnWorkflowCompleteDelegate(PickInstructionService pickInstructionService) {
        this.pickInstructionService = pickInstructionService;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String pickInstructionId = (String) execution.getVariable("pickInstructionId");
        String finalStatus = (String) execution.getVariable("finalStatus");
        String failureReason = (String) execution.getVariable("finalFailureReason");

        log.info("OnWorkflowCompleteDelegate executing for pickInstructionId: {}, finalStatus: {}",
                pickInstructionId, finalStatus);

        pickInstructionService.onWorkflowComplete(pickInstructionId, finalStatus, failureReason);
    }
}
