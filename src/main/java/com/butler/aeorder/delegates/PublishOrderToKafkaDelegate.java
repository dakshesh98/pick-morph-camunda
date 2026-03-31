package com.butler.aeorder.delegates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.service.PickInstructionService;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

/**
 * Deserializes PickInstructionRequestMessage from the "instructionJson" process variable,
 * builds the AE order payload, persists it to PostgreSQL, and publishes to "pick-list.requests".
 */
@Component
@Slf4j
public class PublishOrderToKafkaDelegate implements JavaDelegate {

    private final PickInstructionService pickInstructionService;
    private final ObjectMapper objectMapper;

    public PublishOrderToKafkaDelegate(PickInstructionService pickInstructionService,
                                       ObjectMapper objectMapper) {
        this.pickInstructionService = pickInstructionService;
        this.objectMapper = objectMapper;
    }

    @Override
    public void execute(DelegateExecution execution) throws Exception {
        String instructionJson = (String) execution.getVariable("instructionJson");
        PickInstructionRequestMessage msg = objectMapper.readValue(instructionJson, PickInstructionRequestMessage.class);
        log.info("PublishOrderToKafkaDelegate executing for pickId: {}", msg.getId());
        pickInstructionService.publishPickListRequest(msg);
    }
}
