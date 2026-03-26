package com.temporallearn.spring_temporal.service;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Computes derived AE order / orderline status from actuals container data.
 *
 * <p>OL status rules (based on actuals.containers[j].containerAttributes):
 * <ul>
 *   <li>expectations.containers[0].products[0].productQuantity → OL expectation qty</li>
 *   <li>Σ actuals.containers[j].containerAttributes.qty_picked → total qty picked</li>
 *   <li>actuals.containers[j].containerAttributes.status → created | loaded | unloaded</li>
 * </ul>
 *
 * <p>OL status derivation:
 * <pre>
 *   total_qty_picked == 0                            → created
 *   0 < total_qty_picked < ol_expectation            → pending
 *   total_qty_picked == ol_expectation, any loaded   → complete
 *   total_qty_picked == ol_expectation, all unloaded → released
 * </pre>
 *
 * <p>Order status = aggregate of all OL statuses (priority: pending > complete > released > created).
 */
@Slf4j
public class AEOrderTransformer {

    private AEOrderTransformer() {}

    // ── OL status ────────────────────────────────────────────────────────────

    /**
     * Computes status for a single serviceRequest map as stored in ae_order.payload.
     *
     * @param srMap serviceRequests[i] map from ae_order.payload
     * @return computed OL status: created | pending | complete | released
     */
    public static String computeOlStatus(Map<String, Object> srMap) {
        int olExpectation = getOlExpectation(srMap);
        if (olExpectation == 0) return "created";

        List<Map<String, Object>> containers = getActualsContainers(srMap);
        if (containers.isEmpty()) return "created";

        int     totalQtyPicked = 0;
        boolean anyLoaded      = false;
        boolean allDone        = true;   // all containers are loaded or unloaded

        for (Map<String, Object> container : containers) {
            Map<String, Object> attrs = getContainerAttributes(container);
            if (attrs == null) { allDone = false; continue; }

            totalQtyPicked += toInt(attrs.get("qty_picked"));

            String  status   = (String) attrs.get("status");
            boolean isLoaded = "loaded".equalsIgnoreCase(status);
            boolean isDone   = isLoaded || "unloaded".equalsIgnoreCase(status);

            if (isLoaded) anyLoaded = true;
            if (!isDone)  allDone   = false;
        }

        if (totalQtyPicked == 0)             return "created";
        if (totalQtyPicked < olExpectation)  return "pending";
        // totalQtyPicked == olExpectation
        return (allDone && !anyLoaded) ? "released" : "complete";
    }

    // ── Order status ─────────────────────────────────────────────────────────

    /**
     * Derives order-level status from all OL statuses already injected into storedSRs.
     *
     * <p>Priority (highest wins): pending > complete > released > created
     * <ul>
     *   <li>created  → all OLs are "created"</li>
     *   <li>pending  → ≥1 OL is "pending"</li>
     *   <li>complete → all OLs are "complete" or "released", ≥1 still "complete"</li>
     *   <li>released → all OLs are "released"</li>
     * </ul>
     *
     * @param storedSRs serviceRequests list from ae_order.payload (after OL status injection)
     * @return computed order status: created | pending | complete | released
     */
    public static String computeOrderStatus(List<Map<String, Object>> storedSRs) {
        if (storedSRs == null || storedSRs.isEmpty()) return "created";

        boolean anyPending  = false;
        boolean anyComplete = false;
        boolean allReleased = true;

        for (Map<String, Object> sr : storedSRs) {
            String s = (String) sr.get("status");
            if (s == null) s = "created";

            switch (s) {
                case "pending"  -> { anyPending  = true; allReleased = false; }
                case "complete" -> { anyComplete = true; allReleased = false; }
                case "created"  -> allReleased = false;
                // "released" → allReleased stays true, no flag change needed
            }
        }

        if (anyPending)  return "pending";
        if (anyComplete) return "complete";
        if (allReleased) return "released";
        return "created";
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /**
     * Reads OL expectation qty from:
     * srMap → expectations → containers[0] → products[0] → productQuantity
     */
    @SuppressWarnings("unchecked")
    private static int getOlExpectation(Map<String, Object> srMap) {
        try {
            Map<String, Object> expectations = (Map<String, Object>) srMap.get("expectations");
            if (expectations == null) return 0;

            List<Map<String, Object>> containers = (List<Map<String, Object>>) expectations.get("containers");
            if (containers == null || containers.isEmpty()) return 0;

            List<Map<String, Object>> products = (List<Map<String, Object>>) containers.get(0).get("products");
            if (products == null || products.isEmpty()) return 0;

            return toInt(products.get(0).get("productQuantity"));
        } catch (Exception e) {
            log.warn("Failed to read OL expectation from srMap: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * Reads actuals containers list from:
     * srMap → actuals → containers
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> getActualsContainers(Map<String, Object> srMap) {
        try {
            Map<String, Object> actuals = (Map<String, Object>) srMap.get("actuals");
            if (actuals == null) return Collections.emptyList();

            List<Map<String, Object>> containers = (List<Map<String, Object>>) actuals.get("containers");
            return containers != null ? containers : Collections.emptyList();
        } catch (Exception e) {
            log.warn("Failed to read actuals containers from srMap: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Reads containerAttributes map from a single actuals container entry.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getContainerAttributes(Map<String, Object> container) {
        try {
            return (Map<String, Object>) container.get("containerAttributes");
        } catch (Exception e) {
            return null;
        }
    }

    private static int toInt(Object val) {
        return val instanceof Number ? ((Number) val).intValue() : 0;
    }
}
