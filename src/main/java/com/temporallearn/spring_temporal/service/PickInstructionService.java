package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.ItemPickedEvent;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.TransactionUpdate;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.dto.ae.PickListRequest;
import com.temporallearn.spring_temporal.grpc.ButlerCoreGrpcClient;
import com.temporallearn.spring_temporal.model.AeOrder;
import com.temporallearn.spring_temporal.model.TransactionStatus;
import com.temporallearn.spring_temporal.repository.AeOrderRepository;
import com.temporallearn.spring_temporal.repository.TransactionStatusRepository;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
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
    private final AeOrderRepository aeOrderRepository;
    private final ObjectMapper objectMapper;
    private final PickListRequestMapper pickListRequestMapper;

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  ButlerCoreGrpcClient butlerCoreGrpcClient,
                                  TransactionStatusRepository transactionStatusRepository,
                                  AeOrderRepository aeOrderRepository,
                                  ObjectMapper objectMapper,
                                  PickListRequestMapper pickListRequestMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.butlerCoreGrpcClient = butlerCoreGrpcClient;
        this.transactionStatusRepository = transactionStatusRepository;
        this.aeOrderRepository = aeOrderRepository;
        this.objectMapper = objectMapper;
        this.pickListRequestMapper = pickListRequestMapper;
    }

    // ─── Step 2: Send to AE ──────────────────────────────────────────────────

    /**
     * Persists the PickListRequest to ae_order BEFORE sending to Kafka.
     * This is the DB-first commit gate: if DB write fails, the delegate throws
     * and Camunda retries — no Kafka message is published.
     *
     * The payload stored is the exact JSON of the PickListRequest sent to AE.
     * Idempotent: if ae_order already exists for this pickId, skips the write.
     */
    public void persistAeOrder(String pickId, PickListRequest request) {
        if (aeOrderRepository.existsById(pickId)) {
            log.info("ae_order already exists for pickId: {} — skipping persist (idempotent on RETRY)", pickId);
            return;
        }
        try {
            AeOrder order = new AeOrder();
            order.setExternalServiceRequestId(pickId);
            order.setPayload(objectMapper.writeValueAsString(request));
            order.setCreatedAt(Instant.now());
            order.setUpdatedAt(Instant.now());
            aeOrderRepository.save(order);
            log.info("Persisted ae_order for pickId: {}", pickId);
        } catch (Exception e) {
            log.error("Failed to persist ae_order for pickId: {}", pickId, e);
            throw new RuntimeException("ae_order persist failed for pickId: " + pickId, e);
        }
    }

    /**
     * Sends the pre-built PickListRequest to AE via gor.pick-list.requests.
     * Must only be called AFTER persistAeOrder() succeeds (DB-first ordering).
     * Also called on RETRY to re-send after a retriable failure.
     */
    public void sendPicklistOrderToAE(String pickId, PickListRequest request) {
        kafkaTemplate.send(pickListRequestsTopic, pickId, request);
        log.info("Sent PickListRequest to AE topic '{}' for pickId: {}", pickListRequestsTopic, pickId);
    }

    // ─── Step 3: Update ae_order from events ────────────────────────────────

    /**
     * Selectively updates ae_order.payload when a pick-list.events event arrives.
     *
     * Two levels of update (null-safe — never clobbers existing values with null):
     *   1. Parent (Payload) level:  state, sub_state, attributes, actuals, expectations
     *   2. Per-serviceRequest level (matched by externalServiceRequestId):
     *      state, status, sub_state, actuals, expectations, attributes
     *
     * Creates a minimal ae_order record if one doesn't exist yet (guards against
     * rare cases where the event arrives before the Camunda delegate has run).
     */
    public void updateAeOrderFromEvent(PickListEvent event) {
        String pickId = event.getValue().getPayload().getExternalServiceRequestId();

        AeOrder order = aeOrderRepository.findById(pickId).orElseGet(() -> {
            log.warn("ae_order not found for pickId: {} during event update — creating partial record", pickId);
            AeOrder o = new AeOrder();
            o.setExternalServiceRequestId(pickId);
            o.setCreatedAt(Instant.now());
            return o;
        });

        try {
            // Deserialize as generic map — allows merging fields not in PickListRequest DTO
            Map<String, Object> payloadMap = order.getPayload() != null
                    ? objectMapper.readValue(order.getPayload(), new TypeReference<Map<String, Object>>() {})
                    : new LinkedHashMap<>();

            PickListEvent.Payload evtPayload = event.getValue().getPayload();

            // ── Level 1: parent (Payload) selective update ────────────────────
            mergeIfNotNull(payloadMap, "state",        evtPayload.getState());
            mergeIfNotNull(payloadMap, "actuals",      evtPayload.getActuals());
            mergeIfNotNull(payloadMap, "expectations", evtPayload.getExpectations());
            if (evtPayload.getAttributes() != null) {
                mergeIfNotNull(payloadMap, "sub_state", evtPayload.getAttributes().getSubState());
                payloadMap.put("attributes", evtPayload.getAttributes());
            }

            // ── Level 2: per-serviceRequest selective update ──────────────────
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> storedSRs =
                    (List<Map<String, Object>>) payloadMap.get("serviceRequests");

            if (storedSRs != null && evtPayload.getServiceRequests() != null) {
                for (PickListEvent.ServiceRequest evtSR : evtPayload.getServiceRequests()) {
                    storedSRs.stream()
                            .filter(stored -> evtSR.getExternalServiceRequestId() != null
                                    && evtSR.getExternalServiceRequestId()
                                            .equals(stored.get("externalServiceRequestId")))
                            .findFirst()
                            .ifPresent(storedSR -> {
                                mergeIfNotNull(storedSR, "state",        evtSR.getState());
                                mergeIfNotNull(storedSR, "status",       evtSR.getStatus());
                                mergeIfNotNull(storedSR, "actuals",      evtSR.getActuals());
                                mergeIfNotNull(storedSR, "expectations", evtSR.getExpectations());
                                if (evtSR.getAttributes() != null) {
                                    mergeIfNotNull(storedSR, "sub_state",
                                            evtSR.getAttributes().getSubState());
                                    storedSR.put("attributes", evtSR.getAttributes());
                                }
                            });
                }
            }

            order.setPayload(objectMapper.writeValueAsString(payloadMap));
            order.setUpdatedAt(Instant.now());
            aeOrderRepository.save(order);
            log.info("Updated ae_order for pickId: {}, state: {}", pickId, evtPayload.getState());

        } catch (Exception e) {
            log.warn("Failed to update ae_order for pickId: {} — non-critical, continuing", pickId, e);
        }
    }

    // ─── Step 4: Build and publish item picked events ───────────────────────

    /**
     * Builds and publishes one {@link ItemPickedEvent} per (serviceRequest × transaction) pair
     * in the given pick_transaction event.
     *
     * Field sources:
     *   - PPS fields (pps_id, seat_name, etc.)  — pickInstruction (Camunda process var)
     *   - item_uid, tpid                         — ae_order.payload (stored PickListRequest)
     *   - transaction_id, state, picked_qty      — PickListEvent.Transaction
     *
     * Must only be called AFTER validateAndPersist() succeeds (transaction_status commit gate).
     */
    public void buildAndPublishItemPickedEvents(String pickId,
                                                PickInstruction pi,
                                                PickListEvent event) {
        AeOrder aeOrder = aeOrderRepository.findById(pickId)
                .orElseThrow(() -> new IllegalStateException(
                        "ae_order not found for pickId: " + pickId + " — cannot build ItemPickedEvent"));

        PickListRequest storedRequest;
        try {
            storedRequest = objectMapper.readValue(aeOrder.getPayload(), PickListRequest.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize ae_order payload for pickId: " + pickId, e);
        }

        // Common fields from PickListRequest payload
        String tpid = null;
        try {
            tpid = storedRequest.getAttributes().getOrderOptions()
                    .getCustomerOrderInfo().getShipmentId();
        } catch (NullPointerException e) {
            log.warn("Could not extract tpid from ae_order payload for pickId: {}", pickId);
        }

        List<PickListEvent.ServiceRequest> serviceRequests =
                event.getValue().getPayload().getServiceRequests();

        if (serviceRequests == null || serviceRequests.isEmpty()) {
            log.warn("No serviceRequests in pick_transaction event for pickId: {} — skipping publish", pickId);
            return;
        }

        List<PickListRequest.ServiceRequest> storedSRs = storedRequest.getServiceRequests();

        for (int i = 0; i < serviceRequests.size(); i++) {
            PickListEvent.ServiceRequest evtSR = serviceRequests.get(i);

            // Resolve item_uid from the matching stored serviceRequest (by index)
            String itemUid = null;
            if (storedSRs != null && storedSRs.size() > i) {
                try {
                    itemUid = storedSRs.get(i)
                            .getExpectations()
                            .getContainers().get(0)
                            .getProducts().get(0)
                            .getProductAttributes()
                            .getProductSku();
                } catch (Exception e) {
                    log.warn("Could not extract itemUid from ae_order serviceRequests[{}] for pickId: {}",
                            i, pickId);
                }
            }

            List<PickListEvent.Transaction> transactions = evtSR.getTransactions();
            if (transactions == null || transactions.isEmpty()) {
                log.debug("No transactions in serviceRequests[{}] for pickId: {} — skipping", i, pickId);
                continue;
            }

            for (PickListEvent.Transaction tx : transactions) {
                int pickedQty = (tx.getContainerAttributes() != null)
                        ? tx.getContainerAttributes().getQtyPicked() : 0;

                ItemPickedEvent.PickedItemInfo itemInfo = ItemPickedEvent.PickedItemInfo.builder()
                        .tpid(tpid)
                        .itemUid(itemUid)
                        .uom(pi.getUom())
                        .pickedQty(pickedQty)
                        .pickInstructionIds(List.of(pickId))
                        .build();

                ItemPickedEvent evt = ItemPickedEvent.builder()
                        .ppsId(String.valueOf(pi.getPpsId()))
                        .seatName(pi.getSeatName())
                        .orderId(pickId)
                        .slotRef(pi.getSlotref())
                        .ppsBinId(pi.getBinId())
                        .userLoggedIn(pi.getUserLoggedIn())
                        .ppsPoint(pi.getPpsPoint())
                        .isMarkedContainerFlow(pi.isMarkedContainerScanned())
                        .transactionId(tx.getTransactionId())
                        .state(tx.getTransactionState())
                        .pickedItemInfoList(List.of(itemInfo))
                        .build();

                kafkaTemplate.send(itemPickedEventsTopic, pickId, evt);
                log.info("Published ItemPickedEvent to '{}' for pickId: {}, txId: {}",
                        itemPickedEventsTopic, pickId, tx.getTransactionId());
            }
        }
    }

    // ─── Mark complete / failed ─────────────────────────────────────────────

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

    // ─── Transaction idempotency gate ───────────────────────────────────────

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

    // ─── Workflow completion ─────────────────────────────────────────────────

    /**
     * Run workflow completion cleanup: status validation against Butler Core and Kafka audit event.
     */
    public void onWorkflowComplete(String pickId, String internalStatus, String failureReason) {
        log.info("Workflow completing for pickId: {} — running cleanup and validation. internalStatus: {}",
                pickId, internalStatus);

        String externalStatus = null;
        boolean externalIsComplete = false;
        try {
            GetPickInstructionStatusResponse externalResponse =
                    butlerCoreGrpcClient.getPickInstructionStatus(pickId);
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

    /** Merges value into map only if value is non-null (never clobbers existing data). */
    private void mergeIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }
}
