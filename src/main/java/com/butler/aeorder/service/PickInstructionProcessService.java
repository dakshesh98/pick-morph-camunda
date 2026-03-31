package com.butler.aeorder.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.PickInstruction;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import org.camunda.bpm.engine.ProcessEngineException;
import lombok.extern.slf4j.Slf4j;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Camunda process management service.
 * Replaces PickInstructionWorkflowStarter — starts and terminates
 * the "pickInstructionProcess" Camunda BPM process.
 */
@Service
@Slf4j
public class PickInstructionProcessService {

    private final RuntimeService runtimeService;
    private final ObjectMapper objectMapper;

    public PickInstructionProcessService(RuntimeService runtimeService,
                                          ObjectMapper objectMapper) {
        this.runtimeService = runtimeService;
        this.objectMapper = objectMapper;
    }

    /**
     * Start a new Camunda process instance for the given PickInstruction.
     * Idempotent: if a process with the same business key is already running,
     * the start is skipped and a warning is logged.
     */
    public void startProcess(PickInstruction instruction) {
        String businessKey = "Order_workflow_" + instruction.getPickInstructionId();

        // Idempotency check
        long existing = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(businessKey)
                .count();
        if (existing > 0) {
            log.warn("Process already running for pickInstructionId: {}", instruction.getPickInstructionId());
            return;
        }

        try {
            String instructionJson = objectMapper.writeValueAsString(instruction);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickInstructionId", instruction.getPickInstructionId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");

            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickInstructionId: {}", instruction.getPickInstructionId());
        } catch (Exception e) {
            log.error("Failed to start Camunda process for pickInstructionId: {}", instruction.getPickInstructionId(), e);
            throw new RuntimeException("Failed to start process for pickInstructionId: " + instruction.getPickInstructionId(), e);
        }
    }

    /**
     * Start a new Camunda process instance from a PickInstructionRequestMessage (Kafka-triggered).
     * Idempotent: duplicate starts for the same pickId are silently ignored.
     */
    public void startProcess(PickInstructionRequestMessage msg) {
        String businessKey = "Order_workflow_" + msg.getId();
        try {
            String instructionJson = objectMapper.writeValueAsString(msg);
            Map<String, Object> vars = new HashMap<>();
            vars.put("pickId", msg.getId());
            vars.put("pickInstructionId", msg.getId());
            vars.put("orderId", msg.getOrderId());
            vars.put("instructionJson", instructionJson);
            vars.put("finalStatus", "UNKNOWN");
            runtimeService.startProcessInstanceByKey("pickInstructionProcess", businessKey, vars);
            log.info("Started Camunda process for pickId: {}", msg.getId());
        } catch (ProcessEngineException e) {
            String message = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
            if (message.contains("unique") || message.contains("duplicate") || message.contains("already exists")) {
                log.warn("Process already running for pickId: {} — duplicate start suppressed", msg.getId());
                return;
            }
            log.error("Failed to start Camunda process for pickId: {}", msg.getId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + msg.getId(), e);
        } catch (Exception e) {
            log.error("Failed to start Camunda process for pickId: {}", msg.getId(), e);
            throw new RuntimeException("Failed to start process for pickId: " + msg.getId(), e);
        }
    }

    /**
     * Terminate the running process instance for the given pickInstructionId, if one exists.
     */
    public void terminateProcess(String pickInstructionId) {
        String businessKey = "Order_workflow_" + pickInstructionId;
        ProcessInstance pi = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(businessKey)
                .singleResult();
        if (pi != null) {
            runtimeService.deleteProcessInstance(pi.getId(), "Manually terminated");
            log.info("Terminated Camunda process for pickInstructionId: {}", pickInstructionId);
        } else {
            log.warn("No running process found to terminate for pickInstructionId: {}", pickInstructionId);
        }
    }
}
