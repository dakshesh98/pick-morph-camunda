package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.dto.ItemPickedEvent;
import com.temporallearn.spring_temporal.dto.OrderUpdateEvent;
import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.ae.PickListEvent;
import com.temporallearn.spring_temporal.dto.ae.PickListRequest;
import com.temporallearn.spring_temporal.grpc.ButlerCoreGrpcClient;
import com.temporallearn.spring_temporal.model.AeOrder;
import com.temporallearn.spring_temporal.model.OutboxEvent;
import com.temporallearn.spring_temporal.model.TransactionStatus;
import com.temporallearn.spring_temporal.repository.AeOrderRepository;
import com.temporallearn.spring_temporal.repository.OutboxEventRepository;
import com.temporallearn.spring_temporal.repository.TransactionStatusRepository;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final PickListRequestMapper pickListRequestMapper;

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    @Value("${kafka.topic.order-update-events}")
    private String orderUpdateEventsTopic;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  ButlerCoreGrpcClient butlerCoreGrpcClient,
                                  TransactionStatusRepository transactionStatusRepository,
                                  AeOrderRepository aeOrderRepository,
                                  OutboxEventRepository outboxEventRepository,
                                  ObjectMapper objectMapper,
                                  PickListRequestMapper pickListRequestMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.butlerCoreGrpcClient = butlerCoreGrpcClient;
        this.transactionStatusRepository = transactionStatusRepository;
        this.aeOrderRepository = aeOrderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.pickListRequestMapper = pickListRequestMapper;
    }

    // ─── Step 2: Send to AE ──────────────────────────────────────────────────

    /**
     * Atomically (within a single DB transaction):
     *   1. Persist the PickListRequest to ae_order (idempotent — skips if already present).
     *   2. Write an outbox_event row for the pick-list.requests Kafka publish.
     *
     * The OutboxRelayService picks up the PENDING outbox row and publishes to Kafka,
     * guaranteeing delivery even if the process crashes between the DB write and Kafka send.
     *
     * On Camunda RETRY, step 1 is skipped (already persisted) but a new outbox row is
     * written so the message is re-published — idempotent on the AE side by pickInstructionId.
     */
    @Transactional
    public void persistAeOrder(String pickInstructionId, PickListRequest request) {
        try {
            // Kafka outbox payload — clean PickListRequest (no internal status fields)
            String requestJson = objectMapper.writeValueAsString(request);

            // ae_order payload — same structure with OL status injected
            @SuppressWarnings("unchecked")
            Map<String, Object> payloadMap = objectMapper.convertValue(
                    request, new TypeReference<Map<String, Object>>() {});
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> storedSRs =
                    (List<Map<String, Object>>) payloadMap.get("serviceRequests");
            if (storedSRs != null) {
                for (Map<String, Object> storedSR : storedSRs) {
                    storedSR.put("status", AEOrderTransformer.computeOlStatus(storedSR));
                }
                String orderStatus = AEOrderTransformer.computeOrderStatus(storedSRs);
                payloadMap.put("status", orderStatus);
                log.info("Status injected on creation — pickInstructionId: {}, orderStatus: {}", pickInstructionId, orderStatus);
            }
            String aeOrderPayloadJson = objectMapper.writeValueAsString(payloadMap);

            if (!aeOrderRepository.existsById(pickInstructionId)) {
                String creationOrderStatus = AEOrderTransformer.computeOrderStatus(storedSRs);
                AeOrder order = new AeOrder();
                order.setExternalServiceRequestId(pickInstructionId);
                order.setPayload(aeOrderPayloadJson);
                order.setStatus(creationOrderStatus != null ? creationOrderStatus : "created");
                order.setState("created");
                order.setCreatedAt(Instant.now());
                order.setUpdatedAt(Instant.now());
                aeOrderRepository.save(order);
                log.info("Persisted ae_order for pickInstructionId: {}", pickInstructionId);
            } else {
                log.info("ae_order already exists for pickInstructionId: {} — skipping persist (idempotent on RETRY)", pickInstructionId);
            }

            // Enqueue outbox event in the same transaction — relay publishes to Kafka
            outboxEventRepository.save(OutboxEvent.builder()
                    .aggregateId(pickInstructionId)
                    .topic(pickListRequestsTopic)
                    .payload(requestJson)
                    .status("PENDING")
                    .createdAt(Instant.now())
                    .build());
            log.info("Enqueued pick-list.requests outbox event for pickInstructionId: {}", pickInstructionId);

        } catch (Exception e) {
            log.error("Failed to persist ae_order / outbox event for pickInstructionId: {}", pickInstructionId, e);
            throw new RuntimeException("ae_order persist failed for pickInstructionId: " + pickInstructionId, e);
        }
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
    /**
     * Result of processPickTransaction — carries both the duplicate-rejection flag
     * and the derived order status so callers can decide on workflow completion.
     */
    public record ProcessResult(boolean processed, String orderStatus) {}

    public String updateAeOrderFromEvent(PickListEvent event) {
        String pickInstructionId = event.getValue().getPayload().getExternalServiceRequestId();
        String computedOrderStatus = "created";

        AeOrder order = aeOrderRepository.findById(pickInstructionId).orElseGet(() -> {
            log.warn("ae_order not found for pickInstructionId: {} during event update — creating partial record", pickInstructionId);
            AeOrder o = new AeOrder();
            o.setExternalServiceRequestId(pickInstructionId);
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
                // Merge individual fields into existing stored attributes — never replace entirely
                // so original order_options / customer_order_info are preserved for tpid extraction.
                @SuppressWarnings("unchecked")
                Map<String, Object> storedTopAttrs = (Map<String, Object>)
                        payloadMap.computeIfAbsent("attributes", k -> new LinkedHashMap<>());
                PickListEvent.PayloadAttributes ea = evtPayload.getAttributes();
                mergeIfNotNull(storedTopAttrs, "event_type",    ea.getEventType());
                mergeIfNotNull(storedTopAttrs, "sub_state",     ea.getSubState());
                mergeIfNotNull(storedTopAttrs, "cust_identity", ea.getCustIdentity());
                mergeIfNotNull(storedTopAttrs, "destination",   ea.getDestination());
                mergeIfNotNull(storedTopAttrs, "flow_name",     ea.getFlowName());
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
                                    // Merge into existing SR attributes — preserves extra_info, location etc.
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> storedSRAttrs = (Map<String, Object>)
                                            storedSR.computeIfAbsent("attributes", k -> new LinkedHashMap<>());
                                    PickListEvent.ServiceRequestAttributes sa = evtSR.getAttributes();
                                    mergeIfNotNull(storedSRAttrs, "sub_state",       sa.getSubState());
                                    mergeIfNotNull(storedSRAttrs, "orderType",       sa.getOrderType());
                                    mergeIfNotNull(storedSRAttrs, "simple_priority", sa.getSimplePriority());
                                }
                            });
                }
            }

            // ── Level 3: inject OL status per SR, then derive and inject order status ─
            if (storedSRs != null) {
                for (Map<String, Object> storedSR : storedSRs) {
                    storedSR.put("status", AEOrderTransformer.computeOlStatus(storedSR));
                }
                computedOrderStatus = AEOrderTransformer.computeOrderStatus(storedSRs);
                payloadMap.put("status", computedOrderStatus);
                log.info("Status injected — pickInstructionId: {}, orderStatus: {}", pickInstructionId, computedOrderStatus);
            }

            order.setPayload(objectMapper.writeValueAsString(payloadMap));
            order.setStatus(computedOrderStatus);
            if (evtPayload.getState() != null) {
                order.setState(evtPayload.getState());
            }
            order.setUpdatedAt(Instant.now());
            aeOrderRepository.save(order);
            log.info("Updated ae_order for pickInstructionId: {}, aeState: {}, orderStatus: {}",
                    pickInstructionId, evtPayload.getState(), computedOrderStatus);

        } catch (Exception e) {
            log.warn("Failed to update ae_order for pickInstructionId: {} — non-critical, continuing", pickInstructionId, e);
        }
        return computedOrderStatus;
    }

    // ─── Step 4: Process pick_transaction event atomically ──────────────────

    /**
     * Single atomic transaction for a complete pick_transaction event.
     *
     * <b>Pre-condition (before any write):</b> scans ALL (serviceRequest × transaction) pairs and
     * checks each transactionId against transaction_status. If ANY transactionId is already SUCCESS
     * (duplicate), the entire event is rejected — no ae_order update, no transaction_status writes,
     * no outbox entries. Returns {@code false} so the delegate loops back to waitForTransactionUpdate
     * and waits for the next valid pick_transaction event.
     *
     * <b>When all transactionIds are new,</b> atomically:
     *   1. Update ae_order with actuals/state from the event.
     *   2. For every (serviceRequest × transaction) pair:
     *      a. Persist transaction_status.
     *      b. Build ItemPickedEvent payload (reads ae_order updated in step 1).
     *      c. Write an outbox_event row — OutboxRelayService publishes to item_picked.events.
     *
     * All writes commit or roll back together.
     *
     * @return {@code true} if fully processed; {@code false} if any txId was a duplicate (event rejected).
     */
    @Transactional
    public ProcessResult processPickTransaction(String pickInstructionId, PickListEvent event, PickInstruction pi) {
        PickListEvent.Payload payload = event.getValue() != null ? event.getValue().getPayload() : null;
        if (payload == null || payload.getServiceRequests() == null) {
            log.warn("processPickTransaction — no serviceRequests in event for pickInstructionId: {}", pickInstructionId);
            return new ProcessResult(true, "created"); // nothing to process, treat as handled
        }

        // ── Pre-check: reject entire event if any txId is already recorded ──────────────
        for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
            if (sr.getTransactions() == null) continue;
            for (PickListEvent.Transaction tx : sr.getTransactions()) {
                String txId = tx.getTransactionId();
                if (txId != null && !txId.isEmpty()) {
                    Optional<TransactionStatus> existing = transactionStatusRepository.findById(txId);
                    if (existing.isPresent() && "SUCCESS".equals(existing.get().getStatus())) {
                        log.warn("Duplicate txId: {} for pickInstructionId: {} — rejecting entire pick_transaction event " +
                                 "(no AE order update, no item_picked events). " +
                                 "Delegate will loop back to waitForTransactionUpdate.", txId, pickInstructionId);
                        return new ProcessResult(false, null);
                    }
                }
            }
        }

        // ── All txIds are new — proceed with all writes atomically ───────────────────────
        // Step 1: update ae_order with actuals from the pick_transaction event
        String orderStatus = updateAeOrderFromEvent(event);

        // Step 2: persist transaction_status + enqueue outbox per transaction
        for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
            if (sr.getTransactions() == null) continue;
            for (PickListEvent.Transaction tx : sr.getTransactions()) {
                validatePersistAndEnqueue(pickInstructionId, tx.getTransactionId(), pi, event);
            }
        }

        return new ProcessResult(true, orderStatus);
    }

    /**
     * Per-transaction: persist transaction_status → build + enqueue outbox event.
     * Only called from {@link #processPickTransaction} after all txIds have passed
     * the duplicate pre-check — no duplicate check needed here.
     */
    private void validatePersistAndEnqueue(String pickInstructionId, String txId, PickInstruction pi, PickListEvent event) {
        log.info("validatePersistAndEnqueue — pickInstructionId: {}, txId: {}", pickInstructionId, txId);

        // Find the matching transaction from the event to capture its payload
        PickListEvent.Transaction matchedTx = event.getValue().getPayload().getServiceRequests()
                .stream()
                .filter(sr -> sr.getTransactions() != null)
                .flatMap(sr -> sr.getTransactions().stream())
                .filter(tx -> txId.equals(tx.getTransactionId()))
                .findFirst()
                .orElse(null);

        // Persist transaction_status with the raw transaction payload (PK constraint guards against races)
        persistTransactionStatus(txId, pickInstructionId, "SUCCESS", matchedTx);

        // Build ItemPickedEvent JSON — reads ae_order updated above in the same transaction
        String eventJson = buildItemPickedEventJson(pickInstructionId, pi, event, txId);
        if (eventJson == null) {
            log.warn("Transaction {} not found in event payload for pickInstructionId: {} — outbox entry skipped", txId, pickInstructionId);
            return;
        }

        // Save outbox event atomically with transaction_status
        outboxEventRepository.save(OutboxEvent.builder()
                .aggregateId(pickInstructionId)
                .topic(itemPickedEventsTopic)
                .payload(eventJson)
                .status("PENDING")
                .createdAt(Instant.now())
                .build());

        log.info("Enqueued ItemPickedEvent to outbox for pickInstructionId: {}, txId: {}", pickInstructionId, txId);
    }

    // ─── Mark complete / failed ─────────────────────────────────────────────

    /**
     * Mark the pick instruction as complete.
     */
    public void markPickInstructionComplete(String pickInstructionId) {
        log.info("Finalizing Workflow: Pick Instruction {} is COMPLETE.", pickInstructionId);
        // repository.updateStatus(pickInstructionId, "COMPLETED");
    }

    /**
     * Mark the pick instruction as failed.
     */
    public void markPickInstructionFailed(String pickInstructionId, String transactionId, String failureReason) {
        log.error("Pick Instruction FAILED - pickInstructionId: {}, transactionId: {}, reason: {}",
                pickInstructionId, transactionId, failureReason);
        // repository.updateStatus(pickInstructionId, "FAILED");
        // repository.setFailureReason(pickInstructionId, failureReason);
    }

    // ─── Workflow completion ─────────────────────────────────────────────────

    /**
     * Run workflow completion cleanup: status validation against Butler Core and Kafka audit event.
     */
    public void onWorkflowComplete(String pickInstructionId, String internalStatus, String failureReason) {
        log.info("Workflow completing for pickInstructionId: {} — running cleanup and validation. internalStatus: {}",
                pickInstructionId, internalStatus);

        String externalStatus = null;
        boolean externalIsComplete = false;
        try {
            GetPickInstructionStatusResponse externalResponse =
                    butlerCoreGrpcClient.getPickInstructionStatus(pickInstructionId);
            externalStatus = externalResponse.getStatus();
            externalIsComplete = externalResponse.getIsComplete();
            log.info("External status from Butler Core — pickInstructionId: {}, status: {}, isComplete: {}, " +
                            "totalQty: {}, processedQty: {}, remainingQty: {}",
                    pickInstructionId, externalStatus, externalIsComplete,
                    externalResponse.getTotalQty(),
                    externalResponse.getProcessedQty(),
                    externalResponse.getRemainingQty());
        } catch (Exception e) {
            log.warn("Failed to fetch external status from Butler Core for pickInstructionId: {}. " +
                    "Proceeding with cleanup using internal status only.", pickInstructionId, e);
        }

        if (externalStatus != null) {
            boolean statusMismatch = false;
            if ("COMPLETED".equals(internalStatus) && !externalIsComplete) {
                log.warn("STATUS MISMATCH — pickInstructionId: {} is COMPLETED internally but NOT complete in Butler Core (status: {})",
                        pickInstructionId, externalStatus);
                statusMismatch = true;
            } else if ("FAILED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickInstructionId: {} is FAILED internally but COMPLETE in Butler Core (status: {})",
                        pickInstructionId, externalStatus);
                statusMismatch = true;
            } else if ("CANCELLED".equals(internalStatus) && externalIsComplete) {
                log.warn("STATUS MISMATCH — pickInstructionId: {} is CANCELLED internally but COMPLETE in Butler Core (status: {})",
                        pickInstructionId, externalStatus);
                statusMismatch = true;
            }
            if (!statusMismatch) {
                log.info("Status validation PASSED — pickInstructionId: {} internal [{}] consistent with external [{}]",
                        pickInstructionId, internalStatus, externalStatus);
            }
        }

        try {
            Map<String, Object> auditEvent = new LinkedHashMap<>();
            auditEvent.put("pickInstructionId", pickInstructionId);
            auditEvent.put("internalStatus", internalStatus);
            auditEvent.put("externalStatus", externalStatus);
            auditEvent.put("externalIsComplete", externalIsComplete);
            auditEvent.put("failureReason", failureReason);
            auditEvent.put("timestamp", Instant.now().toString());
            kafkaTemplate.send("workflow-complete-events-topic", pickInstructionId, auditEvent);
            log.info("Published workflow-complete audit event to Kafka for pickInstructionId: {}", pickInstructionId);
        } catch (Exception e) {
            log.warn("Failed to publish workflow-complete audit event for pickInstructionId: {}. Non-critical.", pickInstructionId, e);
        }

        log.info("Workflow cleanup complete for pickInstructionId: {} — final status: {}", pickInstructionId, internalStatus);
    }

    // ─── Order update notifications ─────────────────────────────────────────

    /**
     * Publishes one {@link OrderUpdateEvent} per serviceRequest to the order_update.events topic
     * via the transactional outbox. Called for both update and pick_transaction events so
     * Butler Core can call make_and_send_order_related_notifications for its business orders.
     *
     * order_id / orderline_id come from the PickInstruction (customer order IDs),
     * not from the AE pick-list event.
     */
    @Transactional
    public void enqueueOrderUpdate(String pickInstructionId, PickInstruction pi, PickListEvent event) {
        PickListEvent.Payload payload = event.getValue() != null ? event.getValue().getPayload() : null;
        if (payload == null || payload.getServiceRequests() == null) return;

        String state    = payload.getState();
        String subState = payload.getAttributes() != null ? payload.getAttributes().getSubState() : null;

        for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
            try {
                String orderUpdateTxId = buildTransactionId(pickInstructionId, sr.getActuals());
                OrderUpdateEvent update = OrderUpdateEvent.builder()
                        .pickInstructionId(pickInstructionId)
                        .transactionId(orderUpdateTxId)
                        .orderId(pi.getOrderId())
                        .orderlineId(pi.getOrderlineId())
                        .state(state)
                        .subState(subState)
                        .actuals(sr.getActuals())
                        .build();
                outboxEventRepository.save(OutboxEvent.builder()
                        .aggregateId(pickInstructionId)
                        .topic(orderUpdateEventsTopic)
                        .payload(objectMapper.writeValueAsString(update))
                        .status("PENDING")
                        .createdAt(Instant.now())
                        .build());
                log.info("Enqueued OrderUpdateEvent | pickInstructionId: {} | orderline: {} | state: {} | sub_state: {}",
                        pickInstructionId, sr.getExternalServiceRequestId(), state, subState);
            } catch (Exception e) {
                log.warn("Failed to enqueue OrderUpdateEvent for pickInstructionId: {}, orderline: {} — non-critical",
                        pickInstructionId, sr.getExternalServiceRequestId(), e);
            }
        }
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    /**
     * Finds the specific transaction by ID across all serviceRequests in the event,
     * builds one {@link ItemPickedEvent}, and returns it serialized as JSON.
     *
     * Returns {@code null} if the transaction is not found in the event payload.
     *
     * Field sources:
     *   - PPS fields (pps_id, seat_name, etc.)  — pickInstruction (Camunda process var)
     *   - item_uid, tpid                         — ae_order.payload (stored PickListRequest)
     *   - transaction_id, state, picked_qty      — PickListEvent.Transaction (matched by txId)
     */
    private String buildItemPickedEventJson(String pickInstructionId,
                                            PickInstruction pi,
                                            PickListEvent event,
                                            String transactionId) {
        AeOrder aeOrder = aeOrderRepository.findById(pickInstructionId)
                .orElseThrow(() -> new IllegalStateException(
                        "ae_order not found for pickInstructionId: " + pickInstructionId + " — cannot build ItemPickedEvent"));

        PickListRequest storedRequest;
        try {
            storedRequest = objectMapper.readValue(aeOrder.getPayload(), PickListRequest.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize ae_order payload for pickInstructionId: " + pickInstructionId, e);
        }

        // Primary source: PickInstruction (always present); fallback: ae_order attributes.order_options
        String tpid = pi.getTpid();
        try {
            String storedTpid = storedRequest.getAttributes().getOrderOptions()
                    .getCustomerOrderInfo().getShipmentId();
            if (storedTpid != null && !storedTpid.isEmpty()) {
                tpid = storedTpid;
            }
        } catch (NullPointerException e) {
            log.debug("tpid not in ae_order order_options — using PickInstruction.tpid: {}", tpid);
        }

        List<PickListEvent.ServiceRequest> serviceRequests =
                event.getValue().getPayload().getServiceRequests();
        List<PickListRequest.ServiceRequest> storedSRs = storedRequest.getServiceRequests();

        if (serviceRequests == null) {
            log.warn("No serviceRequests in pick_transaction event for pickInstructionId: {} — skipping", pickInstructionId);
            return null;
        }

        for (int i = 0; i < serviceRequests.size(); i++) {
            List<PickListEvent.Transaction> transactions = serviceRequests.get(i).getTransactions();
            if (transactions == null) continue;

            for (PickListEvent.Transaction tx : transactions) {
                if (!transactionId.equals(tx.getTransactionId())) continue;

                // Found — resolve item_uid from matching stored serviceRequest (by index)
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
                        log.warn("Could not extract itemUid from ae_order serviceRequests[{}] for pickInstructionId: {}",
                                i, pickInstructionId);
                    }
                }

                int pickedQty = (tx.getContainerAttributes() != null)
                        ? tx.getContainerAttributes().getQtyPicked() : 0;

                Long internalOrderId = (tx.getContainerAttributes() != null)
                        ? tx.getContainerAttributes().getInternalOrderId() : null;
                String itemPickedTxId = (internalOrderId != null)
                        ? pickInstructionId + "_" + internalOrderId
                        : pickInstructionId;

                ItemPickedEvent.PickedItemInfo itemInfo = ItemPickedEvent.PickedItemInfo.builder()
                        .tpid(tpid)
                        .itemUid(itemUid)
                        .uom(pi.getUom())
                        .pickedQty(pickedQty)
                        .pickInstructionIds(List.of(pickInstructionId))
                        .build();

                ItemPickedEvent evt = ItemPickedEvent.builder()
                        .ppsId(String.valueOf(pi.getPpsId()))
                        .seatName(pi.getExtraFields() != null ? pi.getExtraFields().getSeatName() : null)
                        .orderId(pickInstructionId)
                        .slotRef(pi.getSlotLocation())
                        .ppsBinId(pi.getBinId())
                        .userLoggedIn(null)
                        .ppsPoint(null)
                        .isMarkedContainerFlow(false)
                        .transactionId(itemPickedTxId)
                        .state(tx.getTransactionState())
                        .danglingArea("bot")
                        .pickedItemInfoList(List.of(itemInfo))
                        .build();

                try {
                    return objectMapper.writeValueAsString(evt);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to serialize ItemPickedEvent for txId: " + transactionId, e);
                }
            }
        }

        log.warn("Transaction {} not found in event for pickInstructionId: {} — no ItemPickedEvent enqueued",
                transactionId, pickInstructionId);
        return null;
    }

    /**
     * Builds a transaction_id as "{pickInstructionId}_{internalOrderId}" when internal_order_id
     * is present in the first actuals container; falls back to pickInstructionId alone.
     */
    @SuppressWarnings("unchecked")
    private String buildTransactionId(String pickInstructionId, Object actuals) {
        try {
            if (actuals instanceof Map) {
                List<?> containers = (List<?>) ((Map<?, ?>) actuals).get("containers");
                if (containers != null && !containers.isEmpty()) {
                    Map<?, ?> containerAttrs = (Map<?, ?>) ((Map<?, ?>) containers.get(0)).get("containerAttributes");
                    if (containerAttrs != null) {
                        Object internalOrderId = containerAttrs.get("internal_order_id");
                        if (internalOrderId != null) {
                            return pickInstructionId + "_" + internalOrderId;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract internal_order_id from actuals for pickInstructionId: {} — using id only", pickInstructionId);
        }
        return pickInstructionId;
    }

    private void persistTransactionStatus(String txId, String pickInstructionId, String status,
                                           PickListEvent.Transaction tx) {
        if (txId == null || txId.isEmpty()) {
            return;
        }
        try {
            String txPayload = null;
            if (tx != null) {
                try {
                    txPayload = objectMapper.writeValueAsString(tx);
                } catch (Exception e) {
                    log.warn("Could not serialize transaction payload for txId: {}", txId, e);
                }
            }
            TransactionStatus ts = new TransactionStatus(txId, pickInstructionId, status, Instant.now(), txPayload);
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
