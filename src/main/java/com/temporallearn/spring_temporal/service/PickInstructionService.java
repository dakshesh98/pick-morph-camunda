package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.ae.PickListRequest;
import com.temporallearn.spring_temporal.grpc.ButlerCoreGrpcClient;
import com.temporallearn.spring_temporal.model.TransactionStatus;
import com.temporallearn.spring_temporal.repository.TransactionStatusRepository;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Business logic service extracted from PickActivitiesImpl.
 * Used by Camunda JavaDelegate classes to perform all domain operations.
 */
@Service
@Slf4j
public class PickInstructionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ButlerCoreGrpcClient butlerCoreGrpcClient;
    private final TransactionStatusRepository transactionStatusRepository;
    private final ObjectMapper objectMapper;
    private final PickListRequestMapper pickListRequestMapper;

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  ButlerCoreGrpcClient butlerCoreGrpcClient,
                                  TransactionStatusRepository transactionStatusRepository,
                                  ObjectMapper objectMapper,
                                  PickListRequestMapper pickListRequestMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.butlerCoreGrpcClient = butlerCoreGrpcClient;
        this.transactionStatusRepository = transactionStatusRepository;
        this.objectMapper = objectMapper;
        this.pickListRequestMapper = pickListRequestMapper;
    }

    /**
     * Step 2 — Transforms PickInstruction into a PickListRequest and sends it to AE
     * via gor.pick-list.requests. AE will respond on gor.pick-list.response (Step 3).
     * Also called on RETRY to re-send after a retriable failure.
     */
    public void sendPicklistOrderToAE(PickInstruction pi) {
        PickListRequest request = pickListRequestMapper.toPickListRequest(pi);
        kafkaTemplate.send(pickListRequestsTopic, pi.getPickId(), request);
        log.info("Sent PickListRequest to AE topic '{}' for pickId: {}", pickListRequestsTopic, pi.getPickId());
    }

    /**
     * Mark the pick instruction as complete.
     */
    public void markPickInstructionComplete(String pickId) {
        log.info("Finalizing Workflow: Pick Instruction {} is COMPLETE.", pickId);
        // repository.updateStatus(pickId, "COMPLETED");
    }

    /**
     * Mark the pick instruction as failed.
     */
    public void markPickInstructionFailed(String pickId, String transactionId, String failureReason) {
        log.error("Pick Instruction FAILED - pickId: {}, transactionId: {}, reason: {}",
                pickId, transactionId, failureReason);
        // repository.updateStatus(pickId, "FAILED");
        // repository.setFailureReason(pickId, failureReason);
    }

    /**
     * Publishes a TransactionUpdate to gor.item-picked-events (consumed by Butler Core).
     * Called by SendItemPickedEventDelegate after the DB commit gate in validateAndPersist().
     */
    public void sendItemPickedEvent(TransactionUpdate transactionUpdate) {
        log.info("Sending item picked event to '{}' for pickId: {}, transactionId: {}",
                itemPickedEventsTopic, transactionUpdate.getPickId(), transactionUpdate.getTransactionId());
        kafkaTemplate.send(itemPickedEventsTopic, transactionUpdate.getPickId(), transactionUpdate);
        log.info("Item picked event sent for transactionId: {}", transactionUpdate.getTransactionId());
    }

    /**
     * Validates and persists a transaction update. This is the commit gate — Kafka publishing
     * must only happen after this succeeds.
     *
     * Steps:
     *   1. Duplicate check — if transactionId is already recorded as SUCCESS in DB, skip.
     *   2. Persist to transaction_status with status SUCCESS.
     *   3. Return whether the pick is now complete.
     *
     * @return Optional.empty()   if this is a duplicate — caller must NOT publish to Kafka.
     *         Optional.of(true)  if persisted and pick is now complete.
     *         Optional.of(false) if persisted and pick is still in progress.
     */
    public Optional<Boolean> validateAndPersist(String pickId, TransactionUpdate transactionUpdate) {
        String txId = transactionUpdate.getTransactionId();

        log.info("Validating transaction for pickId: {}, transactionId: {}, status: {}",
                pickId, txId, transactionUpdate.getStatus());

        // Step 1 — duplicate check
        if (txId != null && !txId.isEmpty()) {
            try {
                Optional<TransactionStatus> existing = transactionStatusRepository.findById(txId);
                if (existing.isPresent() && "SUCCESS".equals(existing.get().getStatus())) {
                    log.warn("Duplicate transaction — txId: {} for pickId: {} already recorded as SUCCESS. Skipping.",
                            txId, pickId);
                    return Optional.empty();
                }
            } catch (Exception e) {
                log.warn("Failed to check existing transaction status for txId: {} — proceeding with persist", txId, e);
            }
        }

        // Step 2 — persist to DB (commit point; primary key constraint guards against races)
        persistTransactionStatus(txId, pickId, "SUCCESS");

        // Step 3 — completion check
        boolean txComplete = "COMPLETED".equalsIgnoreCase(transactionUpdate.getStatus());
        log.info("Transaction persisted for pickId: {}, txId: {}, txComplete: {}", pickId, txId, txComplete);
        return Optional.of(txComplete);
    }

    /**
     * Run workflow completion cleanup: status validation against Butler Core and Kafka audit event.
     */
    public void onWorkflowComplete(String pickId, String internalStatus, String failureReason) {
        log.info("Workflow completing for pickId: {} — running cleanup and validation. internalStatus: {}",
                pickId, internalStatus);

        String externalStatus = null;
        boolean externalIsComplete = false;
        try {
            GetPickInstructionStatusResponse externalResponse = butlerCoreGrpcClient.getPickInstructionStatus(pickId);
            externalStatus = externalResponse.getStatus();
            externalIsComplete = externalResponse.getIsComplete();
            log.info("External status from Butler Core — pickId: {}, status: {}, isComplete: {}, " +
                            "totalQty: {}, processedQty: {}, remainingQty: {}",
                    pickId, externalStatus, externalIsComplete,
                    externalResponse.getTotalQty(),
                    externalResponse.getProcessedQty(),
                    externalResponse.getRemainingQty());
        } catch (Exception e) {
            log.warn("Failed to fetch external status from Butler Core for pickId: {}. " +
                    "Proceeding with cleanup using internal status only.", pickId, e);
        }

        if (externalStatus != null) {
            boolean statusMismatch = false;
            if ("COMPLETED".equals(internalStatus) && !externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is COMPLETED internally but NOT complete in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            } else if ("FAILED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is FAILED internally but COMPLETE in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            } else if ("CANCELLED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickId: {} is CANCELLED internally but COMPLETE in Butler Core (status: {})",
                        pickId, externalStatus);
                statusMismatch = true;
            }
            if (!statusMismatch) {
                log.info("Status validation PASSED — pickId: {} internal [{}] consistent with external [{}]",
                        pickId, internalStatus, externalStatus);
            }
        }

        try {
            Map<String, Object> auditEvent = new LinkedHashMap<>();
            auditEvent.put("pickId", pickId);
            auditEvent.put("internalStatus", internalStatus);
            auditEvent.put("externalStatus", externalStatus);
            auditEvent.put("externalIsComplete", externalIsComplete);
            auditEvent.put("failureReason", failureReason);
            auditEvent.put("timestamp", Instant.now().toString());
            kafkaTemplate.send("workflow-complete-events-topic", pickId, auditEvent);
            log.info("Published workflow-complete audit event to Kafka for pickId: {}", pickId);
        } catch (Exception e) {
            log.warn("Failed to publish workflow-complete audit event for pickId: {}. Non-critical.", pickId, e);
        }

        log.info("Workflow cleanup complete for pickId: {} — final status: {}", pickId, internalStatus);
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    private void persistTransactionStatus(String txId, String pickId, String status) {
        if (txId == null || txId.isEmpty()) {
            return;
        }
        try {
            TransactionStatus ts = new TransactionStatus(txId, pickId, status, Instant.now());
            transactionStatusRepository.save(ts);
        } catch (Exception e) {
            log.warn("Failed to persist transaction status {} for txId: {}", status, txId, e);
        }
    }
}
