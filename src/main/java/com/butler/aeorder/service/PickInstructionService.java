package com.butler.aeorder.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.ItemPickedEvent;
import com.butler.aeorder.dto.OrderUpdateEvent;
import com.butler.aeorder.dto.PickInstruction;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import com.butler.aeorder.dto.ae.PickListEvent;
import com.butler.aeorder.model.AeOrder;
import com.butler.aeorder.model.AeOrdersMapping;
import com.butler.aeorder.model.OutboxEvent;
import com.butler.aeorder.model.TransactionStatus;
import com.butler.aeorder.repository.AeOrderRepository;
import com.butler.aeorder.repository.AeOrdersMappingRepository;
import com.butler.aeorder.repository.OutboxEventRepository;
import com.butler.aeorder.repository.TransactionStatusRepository;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
    private final TransactionStatusRepository transactionStatusRepository;
    private final AeOrderRepository aeOrderRepository;
    private final AeOrdersMappingRepository aeOrdersMappingRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final AeOrderBuilderService aeOrderBuilderService;
    private final AeOrderPersistenceService aeOrderPersistenceService;
    private final OutboxService outboxService;

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    @Value("${kafka.topic.order-update-events}")
    private String orderUpdateEventsTopic;

    @Value("${kafka.topics.pick-instruction-response}")
    private String pickInstructionResponseTopic;

    public PickInstructionService(KafkaTemplate<String, Object> kafkaTemplate,
                                  TransactionStatusRepository transactionStatusRepository,
                                  AeOrderRepository aeOrderRepository,
                                  AeOrdersMappingRepository aeOrdersMappingRepository,
                                  OutboxEventRepository outboxEventRepository,
                                  ObjectMapper objectMapper,
                                  AeOrderBuilderService aeOrderBuilderService,
                                  AeOrderPersistenceService aeOrderPersistenceService,
                                  OutboxService outboxService) {
        this.kafkaTemplate = kafkaTemplate;
        this.transactionStatusRepository = transactionStatusRepository;
        this.aeOrderRepository = aeOrderRepository;
        this.aeOrdersMappingRepository = aeOrdersMappingRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.aeOrderBuilderService = aeOrderBuilderService;
        this.aeOrderPersistenceService = aeOrderPersistenceService;
        this.outboxService = outboxService;
    }

    // ─── Kafka-triggered workflow start ─────────────────────────────────────

    /**
     * Build AePickListRequest from the Kafka message, persist to ae_order table,
     * and publish to "pick-list.requests" via transactional outbox.
     */
    @Transactional
    public void publishPickListRequest(PickInstructionRequestMessage msg) {
        com.butler.aeorder.dto.AePickListRequest request = aeOrderBuilderService.build(msg);
        aeOrderPersistenceService.saveAeOrder(request);
        outboxService.save(pickListRequestsTopic, msg.getId(), request, "pick_list_request");
        log.info("Persisted AePickListRequest and queued to pick-list.requests for pickId: {}", msg.getId());
    }

    /**
     * Atomically mark AE order as FAILED and publish failure response to pick-instruction.response.
     * Called on the BPMN validation-failure path.
     */
    @Transactional
    public void terminateWithFailureResponse(String pickId, String status, String orderId,
            String orderlineId, String message, String errorCode, String errorsJson) {
        aeOrderPersistenceService.markAsFailed(pickId);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        if (orderlineId != null) response.put("orderline_id", orderlineId);
        if (errorCode != null) response.put("errorCode", errorCode);
        if (errorsJson != null) {
            try {
                response.put("errors", objectMapper.readValue(errorsJson, List.class));
            } catch (Exception e) {
                log.warn("Failed to parse errorsJson for pickId: {}", pickId, e);
            }
        }
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Marked AE order as FAILED and queued failure response | pickId: {}", pickId);
    }

    /**
     * Publish pick-instruction.response to notify butler_server of validation outcome.
     */
    @Transactional
    public void publishPickInstructionResponse(String pickId, String status,
            String orderId, String orderlineId, String message, String errorCode, String errorsJson) {
        boolean success = "SUCCESS".equalsIgnoreCase(status);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", pickId);
        response.put("order_id", orderId);
        response.put("status", status);
        response.put("message", message);
        if (orderlineId != null) {
            response.put("orderline_id", orderlineId);
        }
        if (!success) {
            if (errorCode != null) response.put("errorCode", errorCode);
            if (errorsJson != null) {
                try {
                    response.put("errors", objectMapper.readValue(errorsJson, List.class));
                } catch (Exception e) {
                    log.warn("Failed to parse errorsJson for pickId: {}", pickId, e);
                }
            }
        }
        outboxService.save(pickInstructionResponseTopic, pickId, response, "pick_instruction_response");
        log.info("Queued pick-instruction.response for pickId: {} | status: {}", pickId, status);
    }

    // ─── Step 3: Update ae_order from events ────────────────────────────────

    /**
     * Selectively updates structured ae_order columns when a pick-list.events event arrives.
     *
     * Two levels of update (null-safe — never clobbers existing values with null):
     *   1. Parent AeOrder: state, subState, actuals, expectations, attributes (JSONB merged)
     *   2. Child AeOrder rows (matched via ae_orders_mapping by externalServiceRequestId):
     *      state, subState, status, actuals, expectations, attributes (JSONB merged)
     *
     * Creates a minimal parent ae_order record if one doesn't exist yet (guards against
     * rare cases where the event arrives before the Camunda delegate has run).
     */
    public String updateAeOrderFromEvent(PickListEvent event) {
        String pickInstructionId = event.getPayload().getExternalServiceRequestId();
        String computedOrderStatus = "created";

        AeOrder parent = aeOrderRepository.findByExternalServiceRequestId(pickInstructionId)
                .orElseGet(() -> {
                    log.warn("ae_order not found for pickInstructionId: {} during event update — creating partial record", pickInstructionId);
                    return AeOrder.builder()
                            .externalServiceRequestId(pickInstructionId)
                            .type("PICK")
                            .state("CREATED")
                            .subState("CREATED")
                            .status("CREATED")
                            .actuals("{}")
                            .isDeleted(false)
                            .stages("[]")
                            .onHold(false)
                            .createdAt(LocalDateTime.now())
                            .updatedAt(LocalDateTime.now())
                            .build();
                });

        try {
            PickListEvent.Payload evtPayload = event.getPayload();

            // ── Level 1: parent selective update ─────────────────────────────
            if (evtPayload.getState() != null) {
                parent.setState(evtPayload.getState());
            }
            if (evtPayload.getAttributes() != null && evtPayload.getAttributes().getSubState() != null) {
                parent.setSubState(evtPayload.getAttributes().getSubState());
            }
            if (evtPayload.getActuals() != null) {
                parent.setActuals(objectMapper.writeValueAsString(evtPayload.getActuals()));
            }
            if (evtPayload.getExpectations() != null) {
                parent.setExpectations(objectMapper.writeValueAsString(evtPayload.getExpectations()));
            }
            if (evtPayload.getAttributes() != null) {
                // Merge into existing attributes JSON — preserves original fields
                Map<String, Object> storedAttrs = parent.getAttributes() != null
                        ? objectMapper.readValue(parent.getAttributes(), new TypeReference<Map<String, Object>>() {})
                        : new LinkedHashMap<>();
                PickListEvent.PayloadAttributes ea = evtPayload.getAttributes();
                mergeIfNotNull(storedAttrs, "event_type",    ea.getEventType());
                mergeIfNotNull(storedAttrs, "sub_state",     ea.getSubState());
                mergeIfNotNull(storedAttrs, "cust_identity", ea.getCustIdentity());
                mergeIfNotNull(storedAttrs, "destination",   ea.getDestination());
                mergeIfNotNull(storedAttrs, "flow_name",     ea.getFlowName());
                parent.setAttributes(objectMapper.writeValueAsString(storedAttrs));
            }

            // ── Level 2: per-child AeOrder selective update ───────────────────
            List<AeOrdersMapping> mappings = aeOrdersMappingRepository
                    .findByParentExternalServiceRequestId(pickInstructionId);
            List<Map<String, Object>> childStatusMaps = new ArrayList<>();

            for (AeOrdersMapping mapping : mappings) {
                String childId = mapping.getChildExternalServiceRequestId();
                Optional<AeOrder> childOpt = aeOrderRepository.findByExternalServiceRequestId(childId);
                if (childOpt.isEmpty()) continue;
                AeOrder child = childOpt.get();

                // Apply matching event SR fields
                if (evtPayload.getServiceRequests() != null) {
                    evtPayload.getServiceRequests().stream()
                            .filter(sr -> childId.equals(sr.getExternalServiceRequestId()))
                            .findFirst()
                            .ifPresent(evtSR -> {
                                if (evtSR.getState() != null) child.setState(evtSR.getState());
                                if (evtSR.getAttributes() != null
                                        && evtSR.getAttributes().getSubState() != null) {
                                    child.setSubState(evtSR.getAttributes().getSubState());
                                }
                                try {
                                    if (evtSR.getActuals() != null) {
                                        child.setActuals(objectMapper.writeValueAsString(evtSR.getActuals()));
                                    }
                                    if (evtSR.getExpectations() != null) {
                                        child.setExpectations(objectMapper.writeValueAsString(evtSR.getExpectations()));
                                    }
                                    if (evtSR.getAttributes() != null) {
                                        Map<String, Object> storedSRAttrs = child.getAttributes() != null
                                                ? objectMapper.readValue(child.getAttributes(), new TypeReference<Map<String, Object>>() {})
                                                : new LinkedHashMap<>();
                                        PickListEvent.ServiceRequestAttributes sa = evtSR.getAttributes();
                                        mergeIfNotNull(storedSRAttrs, "sub_state",       sa.getSubState());
                                        mergeIfNotNull(storedSRAttrs, "orderType",        sa.getOrderType());
                                        mergeIfNotNull(storedSRAttrs, "simple_priority",  sa.getSimplePriority());
                                        child.setAttributes(objectMapper.writeValueAsString(storedSRAttrs));
                                    }
                                } catch (Exception ex) {
                                    log.warn("Failed to merge SR fields for child: {}", childId, ex);
                                }
                            });
                }

                // Compute OL status for this child and save
                try {
                    Map<String, Object> srMap = new LinkedHashMap<>();
                    srMap.put("actuals", child.getActuals() != null
                            ? objectMapper.readValue(child.getActuals(), new TypeReference<Map<String, Object>>() {}) : null);
                    srMap.put("expectations", child.getExpectations() != null
                            ? objectMapper.readValue(child.getExpectations(), new TypeReference<Map<String, Object>>() {}) : null);
                    String olStatus = AEOrderTransformer.computeOlStatus(srMap);
                    child.setStatus(olStatus);
                    child.setUpdatedAt(LocalDateTime.now());
                    aeOrderRepository.save(child);
                    Map<String, Object> statusEntry = new LinkedHashMap<>();
                    statusEntry.put("status", olStatus);
                    childStatusMaps.add(statusEntry);
                } catch (Exception ex) {
                    log.warn("Failed to compute/set OL status for child: {}", childId, ex);
                }
            }

            // ── Level 3: derive aggregate order status from children ──────────
            computedOrderStatus = AEOrderTransformer.computeOrderStatus(childStatusMaps);
            parent.setStatus(computedOrderStatus);
            parent.setUpdatedAt(LocalDateTime.now());
            aeOrderRepository.save(parent);
            log.info("Updated ae_order | pickInstructionId: {}, state: {}, orderStatus: {}",
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

        PickListEvent.Payload payload = event.getPayload();
        if (payload != null && payload.getServiceRequests() != null) {
            String state    = payload.getState();
            String subState = payload.getAttributes() != null ? payload.getAttributes().getSubState() : null;

            boolean anyTransactions = false;
            boolean itemPickedDispatched = false;
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
                        case "complete" -> {
                            enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, "bot");
                            itemPickedDispatched = true;
                        }
                        case "unloaded" -> {
                            enqueueItemPickedEventForTransaction(pickInstructionId, tx, pi, "undefined");
                            itemPickedDispatched = true;
                        }
                        default -> log.debug(
                                "Transaction {} status '{}' — SR-level order_update handled by enqueueOrderUpdate",
                                txId, containerStatus);
                    }
                }
            }

            if (!anyTransactions) {
                log.debug("No transactions in event for pickInstructionId: {} — skipping transaction dispatch",
                        pickInstructionId);
            }

            // Skip order_update when item_picked.events were sent for this event
            if (itemPickedDispatched) {
                log.debug("item_picked.events dispatched for pickInstructionId: {} — suppressing order_update", pickInstructionId);
                return orderStatus;
            }
        }

        // Step 3: SR-level order update (only when no item_picked.events were sent)
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
     * Run workflow completion cleanup: publishes a Kafka audit event with the final internal status.
     */
    public void onWorkflowComplete(String pickInstructionId, String internalStatus, String failureReason) {
        log.info("Workflow completing for pickInstructionId: {} — running cleanup. internalStatus: {}",
                pickInstructionId, internalStatus);

        try {
            Map<String, Object> auditEvent = new LinkedHashMap<>();
            auditEvent.put("pickInstructionId", pickInstructionId);
            auditEvent.put("internalStatus", internalStatus);
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
     * @param danglingArea "bot" when containerStatus is {@code complete}; "undefined" when containerStatus is {@code unloaded}.
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
                    .tpid(Integer.parseInt(pi.getTpid()))
                    .itemUid(pi.getItemId())
                    .uom(pi.getUom())
                    .pickedQty(pickedQty)
                    .pickInstructionIds(List.of(pickInstructionId))
                    .build();

            ItemPickedEvent evt = ItemPickedEvent.builder()
                    .ppsId(pi.getPpsId())
                    .seatName(pi.getExtraFields() != null ? pi.getExtraFields().getSeatName() : null)
                    .orderId(pi.getOrderId())
                    .slotRef(pi.getSlotLocation())
                    .ppsBinId(pi.getBinId())
                    .transactionId(itemPickedTxId)
                    .state(tx.getTransactionState())
                    .danglingArea(danglingArea)
                    .isMarkedContainerFlow(false)
                    .pickedItemInfoList(List.of(itemInfo))
                    .build();

            outboxService.save(itemPickedEventsTopic, pickInstructionId, evt, "item_picked");
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
                    .transaction(objectMapper.convertValue(tx.getContainerAttributes(), new TypeReference<Map<String, Object>>() {}))
                    .build();
            outboxService.save(orderUpdateEventsTopic, pickInstructionId, update, "order_update");
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
        PickListEvent.Payload payload = event.getPayload();
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
                        .transaction(extractContainerAttributes(sr.getActuals()))
                        .build();
                outboxService.save(orderUpdateEventsTopic, pickInstructionId, update, "order_update");
                log.info("Enqueued OrderUpdateEvent | pickInstructionId: {} | orderline: {} | state: {} | sub_state: {}",
                        pickInstructionId, sr.getExternalServiceRequestId(), state, subState);
            } catch (Exception e) {
                log.warn("Failed to enqueue OrderUpdateEvent for pickInstructionId: {}, orderline: {} — non-critical",
                        pickInstructionId, sr.getExternalServiceRequestId(), e);
            }
        }
    }

    /**
     * Extracts containerAttributes from the first actuals container.
     * Returns null (omitted via @JsonInclude) when no containers are present.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractContainerAttributes(Object actuals) {
        try {
            if (actuals instanceof Map) {
                List<?> containers = (List<?>) ((Map<?, ?>) actuals).get("containers");
                if (containers != null && !containers.isEmpty()) {
                    Map<?, ?> first = (Map<?, ?>) containers.get(0);
                    Object attrs = first.get("containerAttributes");
                    if (attrs instanceof Map) {
                        return (Map<String, Object>) attrs;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract containerAttributes from actuals");
        }
        return null;
    }

    /**
     * Builds a transaction_id as "{pickInstructionId}_{internalOrderId}" when internal_order_id
     * is present in the first actuals container; falls back to "undefined".
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
        return "undefined";
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
