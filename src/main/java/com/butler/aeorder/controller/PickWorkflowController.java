package com.butler.aeorder.controller;

import com.butler.aeorder.dto.PickInstruction;
import com.butler.aeorder.service.PickInstructionProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/Order")
public class PickWorkflowController {

    private static final Logger log = LoggerFactory.getLogger(PickWorkflowController.class);

    private final PickInstructionProcessService processService;

    public PickWorkflowController(PickInstructionProcessService processService) {
        this.processService = processService;
    }

    /**
     * Start a new Camunda process instance for the given PickInstruction.
     * The process will publish a PickListRequest to <tenant>.pick-list.requests
     * and wait for AE to respond on <tenant>.pick-list.response.
     *
     * Idempotent: duplicate starts for the same pickInstructionId are silently ignored.
     *
     * Example: POST /Order/pick_instruction
     * Body: {"pickInstructionId": "Pick5", "item": "SKU-001", ...}
     */
    @PostMapping("/pick_instruction")
    public ResponseEntity<String> startPickInstruction(@RequestBody PickInstruction pickInstruction) {
        processService.startProcess(pickInstruction);
        log.info("Pick instruction process started for pickInstructionId: {}", pickInstruction.getPickInstructionId());
        return ResponseEntity.ok("Pick instruction process started for pickInstructionId: "
                + pickInstruction.getPickInstructionId());
    }

    /**
     * Terminate a stuck/running process instance.
     * Example: DELETE /Order/terminate/Pick5
     */
    @DeleteMapping("/terminate/{pickInstructionId}")
    public ResponseEntity<String> terminateWorkflow(@PathVariable String pickInstructionId) {
        processService.terminateProcess(pickInstructionId);
        log.info("Process terminate requested for pickInstructionId: {}", pickInstructionId);
        return ResponseEntity.ok("Process terminated for pickInstructionId: " + pickInstructionId);
    }
}
