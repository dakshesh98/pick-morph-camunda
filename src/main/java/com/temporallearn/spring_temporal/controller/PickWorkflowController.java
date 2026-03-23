package com.temporallearn.spring_temporal.controller;

import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.service.PickInstructionProcessService;
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
     * Idempotent: duplicate starts for the same pickId are silently ignored.
     *
     * Example: POST /Order/pick_instruction
     * Body: {"pickId": "Pick5", "item": "SKU-001", ...}
     */
    @PostMapping("/pick_instruction")
    public ResponseEntity<String> startPickInstruction(@RequestBody PickInstruction pickInstruction) {
        processService.startProcess(pickInstruction);
        log.info("Pick instruction process started for pickId: {}", pickInstruction.getPickId());
        return ResponseEntity.ok("Pick instruction process started for pickId: "
                + pickInstruction.getPickId());
    }

    /**
     * Terminate a stuck/running process instance.
     * Example: DELETE /Order/terminate/Pick5
     */
    @DeleteMapping("/terminate/{pickId}")
    public ResponseEntity<String> terminateWorkflow(@PathVariable String pickId) {
        processService.terminateProcess(pickId);
        log.info("Process terminate requested for pickId: {}", pickId);
        return ResponseEntity.ok("Process terminated for pickId: " + pickId);
    }
}
