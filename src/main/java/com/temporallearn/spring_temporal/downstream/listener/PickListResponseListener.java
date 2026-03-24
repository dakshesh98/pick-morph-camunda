package com.temporallearn.spring_temporal.downstream.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.ae.PickListResponse;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.MismatchingMessageCorrelationException;
import org.camunda.bpm.engine.RuntimeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Step 3 — Kafka listener for AE pick-list response messages.
 *
 * Consumes messages from gor.pick-list.response published by AE after
 * processing a pick-list request sent in Step 2.
 *
 * Maps AE response → PickListResponseMessage Camunda signal:
 *   status == "SUCCESS" → validationSuccess = true  (proceed to wait for item picking events)
 *   status == "FAILURE" → validationSuccess = false (fail the workflow)
 */
@Service
@Slf4j
public class PickListResponseListener {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${kafka.topic.pick-list-response}",
            groupId = "pick-list-response-consumer-group",
            containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void listen(@Payload String payload) {
        PickListResponse response;
        try {
            response = objectMapper.readValue(payload, PickListResponse.class);
        } catch (Exception e) {
            log.error("Failed to deserialize PickListResponse from Kafka message: {}", e.getMessage());
            return;
        }

        boolean success = response.isSuccess();

        log.info("Received pick-list response from AE | externalServiceRequestId: {} | status: {} | success: {}",
                response.getExternalServiceRequestId(),
                response.getStatus(),
                success);

        if (!success) {
            log.warn("AE returned FAILURE for pickInstructionId: {} | errorCode: {} | message: {}",
                    response.getExternalServiceRequestId(),
                    response.getErrorCode(),
                    response.getMessage());
        }

        try {
            runtimeService.createMessageCorrelation("PickListResponseMessage")
                    .processInstanceVariableEquals("pickInstructionId", response.getExternalServiceRequestId())
                    .setVariable("validationSuccess", success)
                    .setVariable("validationResult", response.getExternalServiceRequestId())
                    .correlate();

            log.info("PickListResponseMessage correlated successfully for pickInstructionId: {} | success: {}",
                    response.getExternalServiceRequestId(), success);

        } catch (MismatchingMessageCorrelationException e) {
            log.error("No process instance found waiting for PickListResponseMessage for pickInstructionId: {}. " +
                    "Pick-list response dropped.", response.getExternalServiceRequestId());
        } catch (Exception e) {
            log.error("Failed to correlate ValidationResultMessage for pickInstructionId: {} | error: {}",
                    response.getExternalServiceRequestId(), e.getMessage(), e);
        }
    }
}
