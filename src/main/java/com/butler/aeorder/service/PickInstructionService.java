package com.butler.aeorder.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.ItemPickedEvent;
import com.butler.aeorder.dto.OrderUpdateEvent;
import com.butler.aeorder.dto.PickInstruction;
import com.butler.aeorder.dto.ae.PickListEvent;
import com.butler.aeorder.dto.ae.PickListRequest;
import com.butler.aeorder.grpc.ButlerCoreGrpcClient;
import com.butler.aeorder.model.AeOrder;
import com.butler.aeorder.model.OutboxEvent;
import com.butler.aeorder.model.TransactionStatus;
import com.butler.aeorder.repository.AeOrderRepository;
import com.butler.aeorder.repository.OutboxEventRepository;
import com.butler.aeorder.repository.TransactionStatusRepository;
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

    // ─── Step 4: Process pick-list event (unified, event_type-agnostic) ────────

    /**
     * Unified handler for every pick-list event regardless of event_type.
     * @return derived order status (created | pending | complete | released)
     */
    @Transactional
    public String processPickListEvent(String pickInstructionId, PickListEvent event, PickInstruction pi) {
        // Step 1: always update ae_order
        String orderStatus = updateAeOrderFromEvent(event);

        PickListEvent.Payload payload = event.getValue() != null ? event.getValue().getPayload() : null;
        if (payload != null && payload.getServiceRequests() != null) {
            String state    = payload.getState();
            String subState = payload.getAttributes() != null ? payload.getAttributes().getSubState() : null;

            boolean anyTransactions = false;
            for (PickListEvent.ServiceRequest sr : payload.getServiceRequests()) {
                if (sr.getTransactions() == null || sr.getTransactions().isEmpty()) continue;
                for (PickListEvent.Transaction tx : sr.getTransactions()) {
                    anyTransactions = true;
                    String txId = tx.getTransactionId();

                    // Dedup: skip transactions already processed
                    if (txId != null && !txId.isEmpty()) {
                        Optional<TransactionStatus> existing = transactionStatusRepository.findById(txId);
                        if (existing.isPresent() && "SUCCESS".equals(existing.get().getStatus())) {
                            log.info("Duplicate txId: {} for pickInstructionId: {} — skipping", txId, pickInstructionId);
                            continue;
                        }
                        persistTransactionStatus(txId, pickInstructionId, "SUCCESS", tx);
                    }

                    String containerStatus = tx.getContainerAttributes() != null
                            ? tx.getContainerAttributes().getStatus() : null;
                    if (containerStatus == null) {
                        log.debug("Transaction {} has no containerAttributes.status — skipping dispatch", txId);
                        continue;
                    }

                    switch (containerStatus.toLowerCase()) {
                        case "loaded" ->
                                enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, "bot");
                        case "unloaded" ->
                                enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, null);
                        case "created" ->
                                enqueueTransactionOrderUpdate(pickInstructionId, tx, sr, pi, state, subState);
                        default -> log.debug(
                                "No dispatch rule for containerStatus: {} | tx: {} | pickInstructionId: {}",
                                containerStatus, txId, pickInstructionId);
                    }
                }
            }

            if (!anyTransactions) {
                log.debug("No transactions in event for pickInstructionId: {} — skipping transaction dispatch",
                        pickInstructionId);
            }
        }

        // Step 3: SR-level order update (always)
        enqueueOrderUpdate(pickInstructionId, pi, event);

        return orderStatus;
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

    /**
     * Builds and enqueues an {@link ItemPickedEvent} outbox entry for a single transaction.
     *
     * @param danglingArea "bot" for loaded/complete containers; {@code null} for unloaded containers.
     */
    private void enqueueItemPickedEventForTransaction(String pickInstructionId,
                                                      PickListEvent.Transaction tx,
                                                      PickInstruction pi,
                                                      String danglingArea) {
        try {
            int  pickedQty       = tx.getContainerAttributes() != null ? tx.getContainerAttributes().getQtyPicked() : 0;
            Long internalOrderId = tx.getContainerAttributes() != null ? tx.getContainerAttributes().getInternalOrderId() : null;
            if (internalOrderId == null) {
                throw new IllegalStateException("internalOrderId is required but not present in containerAttributes");
            }
            String itemPickedTxId = pickInstructionId + "_" + internalOrderId;

            ItemPickedEvent.PickedItemInfo itemInfo = ItemPickedEvent.PickedItemInfo.builder()
                    .tpid(pi.getTpid())
                    .itemUid(pi.getItemId())
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
                    .transactionId(itemPickedTxId)
                    .state(tx.getTransactionState())
                    .danglingArea(danglingArea)
                    .isMarkedContainerFlow(false)
                    .pickedItemInfoList(List.of(itemInfo))
                    .build();

            outboxEventRepository.save(OutboxEvent.builder()
                    .aggregateId(pickInstructionId)
                    .topic(itemPickedEventsTopic)
                    .payload(objectMapper.writeValueAsString(evt))
                    .status("PENDING")
                    .createdAt(Instant.now())
                    .build());
            log.info("Enqueued ItemPickedEvent | pickInstructionId: {} | txId: {} | containerStatus: {} | danglingArea: {}",
                    pickInstructionId, tx.getTransactionId(),
                    tx.getContainerAttributes() != null ? tx.getContainerAttributes().getStatus() : "?",
                    danglingArea);
        } catch (Exception e) {
            log.warn("Failed to enqueue ItemPickedEvent for tx: {} pickInstructionId: {} — non-critical",
                    tx.getTransactionId(), pickInstructionId, e);
        }
    }

    /**
     * Builds and enqueues an {@link OrderUpdateEvent} for a transaction whose container
     * status is {@code created} (item not yet picked).
     * The transaction itself is serialised as the {@code actuals} field.
     */
    private void enqueueTransactionOrderUpdate(String pickInstructionId,
                                               PickListEvent.Transaction tx,
                                               PickListEvent.ServiceRequest sr,
                                               PickInstruction pi,
                                               String state,
                                               String subState) {
        try {
            OrderUpdateEvent update = OrderUpdateEvent.builder()
                    .pickInstructionId(pickInstructionId)
                    .transactionId(tx.getTransactionId())
                    .orderId(pi.getOrderId())
                    .orderlineId(sr.getExternalServiceRequestId())
                    .state(state)
                    .subState(subState)
                    .actuals(objectMapper.convertValue(tx, new TypeReference<Map<String, Object>>() {}))
                    .build();
            outboxEventRepository.save(OutboxEvent.builder()
                    .aggregateId(pickInstructionId)
                    .topic(orderUpdateEventsTopic)
                    .payload(objectMapper.writeValueAsString(update))
                    .status("PENDING")
                    .createdAt(Instant.now())
                    .build());
            log.info("Enqueued OrderUpdateEvent (created container) | pickInstructionId: {} | txId: {} | orderline: {}",
                    pickInstructionId, tx.getTransactionId(), sr.getExternalServiceRequestId());
        } catch (Exception e) {
            log.warn("Failed to enqueue transaction OrderUpdateEvent for pickInstructionId: {}, txId: {} — non-critical",
                    pickInstructionId, tx.getTransactionId(), e);
        }
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
