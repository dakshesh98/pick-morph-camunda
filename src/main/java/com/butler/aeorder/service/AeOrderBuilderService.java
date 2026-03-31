package com.butler.aeorder.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.client.LocationApiClient;
import com.butler.aeorder.grpc.ButlerPickOrderGrpcClient;
import com.butler.aeorder.dto.AePickListRequest;
import com.butler.aeorder.dto.PickInstructionRequestMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds an AePickListRequest from a PickInstructionRequestMessage.
 *
 * Fetches enrichment data from butler_server (order node) and location-service
 * (slot location, destination group), then assembles the full AE order payload.
 */
@Service
@Slf4j
public class AeOrderBuilderService {

    private final ButlerPickOrderGrpcClient butlerPickOrderGrpcClient;
    private final LocationApiClient locationApiClient;
    private final ObjectMapper objectMapper;

    public AeOrderBuilderService(ButlerPickOrderGrpcClient butlerPickOrderGrpcClient,
                                 LocationApiClient locationApiClient,
                                 ObjectMapper objectMapper) {
        this.butlerPickOrderGrpcClient = butlerPickOrderGrpcClient;
        this.locationApiClient = locationApiClient;
        this.objectMapper = objectMapper;
    }

    /**
     * Builds a complete AePickListRequest for the given pick instruction message.
     * Fetches required enrichment data from butler_server and location-service.
     *
     * @throws RuntimeException if butler_server is unreachable or the orderline is not found
     */
    public AePickListRequest build(PickInstructionRequestMessage msg) {
        JsonNode orderNode = butlerPickOrderGrpcClient.getOrderData(msg.getOrderId());
        if (orderNode == null) {
            throw new RuntimeException(
                    "Failed to fetch order node data from butler_server for orderId: " + msg.getOrderId());
        }

        // Match the specific orderline (child) by order_node_id == msg.getOrderlineId()
        JsonNode matchedChild = findChildByOrderlineId(orderNode, msg.getOrderlineId());
        if (matchedChild == null) {
            throw new RuntimeException(
                    "No orderline found for orderlineId: " + msg.getOrderlineId()
                            + " in order: " + msg.getOrderId());
        }

        // Extract top-level order fields
        List<String> bintags = extractStringList(orderNode.path("properties").path("bintags"));
        String simplePriority = extractString(orderNode.path("properties").path("simple_priority"));

        // Extract orderline product attributes and behaviour
        JsonNode productAttrs = matchedChild.path("expectations").path("product_attributes");
        List<String> filterParams = extractStringList(productAttrs.path("filter_parameters"));
        List<String> packageParams = extractStringList(productAttrs.path("package_parameters"));
        List<String> behaviours = extractStringList(matchedChild.path("behaviour"));

        // Fetch location from location-service — non-fatal, null is acceptable
        AePickListRequest.AeLocation location = locationApiClient.getLocation(msg.getSlotId());
        if (location == null) {
            log.warn("Location data unavailable for slotId: {} — continuing with null location", msg.getSlotId());
        }

        // Fetch destination group from location-service — non-fatal, null is acceptable
        String destinationGroup = locationApiClient.getDestinationGroup(msg.getBinId());
        if (destinationGroup == null) {
            log.warn("Destination group unavailable for binId: {} — continuing with null", msg.getBinId());
        }

        return buildPickListRequest(msg, bintags, simplePriority, filterParams, packageParams, behaviours,
                location, destinationGroup);
    }

    // ─── AePickListRequest builders ──────────────────────────────────────────

    private AePickListRequest buildPickListRequest(PickInstructionRequestMessage msg,
                                                   List<String> bintags,
                                                   String simplePriority,
                                                   List<String> filterParams,
                                                   List<String> packageParams,
                                                   List<String> behaviours,
                                                   AePickListRequest.AeLocation location,
                                                   String destinationGroup) {
        return AePickListRequest.builder()
                .externalServiceRequestId(msg.getId())
                .type("PICK")
                .fulfillmentArea(List.of("assist_area"))
                .serviceRequests(List.of(buildServiceRequestLine(msg, filterParams, packageParams, location)))
                .attributes(buildTopLevelAttributes(msg, simplePriority, bintags, behaviours, destinationGroup))
                .isDeleted(false)
                .stages(List.of())
                .onHold(false)
                .build();
    }

    private AePickListRequest.AeServiceRequestLine buildServiceRequestLine(
            PickInstructionRequestMessage msg,
            List<String> filterParams,
            List<String> packageParams,
            AePickListRequest.AeLocation location) {
        AePickListRequest.AeServiceRequestLineAttributes attrs =
                AePickListRequest.AeServiceRequestLineAttributes.builder()
                        .location(location)
                        .extraInfo(null)
                        .build();
        return AePickListRequest.AeServiceRequestLine.builder()
                .externalServiceRequestId(msg.getId() + "_1")
                .type("PICK_LINE")
                .attributes(attrs)
                .expectations(buildExpectations(msg, filterParams, packageParams))
                .isDeleted(false)
                .stages(List.of())
                .onHold(false)
                .build();
    }

    private AePickListRequest.AeExpectations buildExpectations(PickInstructionRequestMessage msg,
                                                               List<String> filterParams,
                                                               List<String> packageParams) {
        return AePickListRequest.AeExpectations.builder()
                .containers(List.of(
                        AePickListRequest.AeContainer.builder()
                                .products(List.of(buildProduct(msg, filterParams, packageParams)))
                                .build()))
                .build();
    }

    private AePickListRequest.AeProduct buildProduct(PickInstructionRequestMessage msg,
                                                     List<String> filterParams,
                                                     List<String> packageParams) {
        return AePickListRequest.AeProduct.builder()
                .productQuantity(msg.getQty())
                .productAttributes(buildProductAttributes(msg, filterParams, packageParams))
                .build();
    }

    private AePickListRequest.AeProductAttributes buildProductAttributes(
            PickInstructionRequestMessage msg,
            List<String> filterParams,
            List<String> packageParams) {
        List<String> resolvedPackageParams = (packageParams != null && !packageParams.isEmpty())
                ? packageParams
                : List.of("package_name = '" + msg.getUom() + "'");
        String productSku = null;
        if (filterParams != null) {
            for (String fp : filterParams) {
                if (fp.trim().startsWith("product_sku")) {
                    int first = fp.indexOf('\'');
                    int last = fp.lastIndexOf('\'');
                    if (first >= 0 && last > first) {
                        productSku = fp.substring(first + 1, last);
                    }
                    break;
                }
            }
        }
        return AePickListRequest.AeProductAttributes.builder()
                .filterParameters(filterParams)
                .barcodes(msg.getBarcodes())
                .productSku(productSku)
                .packageParameters(resolvedPackageParams)
                .lotId(String.valueOf(msg.getTpid()))
                .build();
    }

    private AePickListRequest.AeTopLevelAttributes buildTopLevelAttributes(PickInstructionRequestMessage msg,
                                                                           String simplePriority,
                                                                           List<String> bintags,
                                                                           List<String> behaviours,
                                                                           String destinationGroup) {
        String resolvedPriority = (simplePriority != null && !simplePriority.isBlank())
                ? simplePriority : "normal";
        return AePickListRequest.AeTopLevelAttributes.builder()
                .skipStandardPickProcess(true)
                .simplePriority(resolvedPriority)
                .orderOptions(buildOrderOptions(msg, bintags, behaviours, destinationGroup))
                .build();
    }

    private AePickListRequest.AeOrderOptions buildOrderOptions(PickInstructionRequestMessage msg,
                                                               List<String> bintags,
                                                               List<String> behaviours,
                                                               String destinationGroup) {
        List<String> resolvedBintags = (bintags != null) ? bintags : List.of();
        List<String> resolvedBehaviours = (behaviours != null && !behaviours.isEmpty())
                ? behaviours : List.of("group_by_proximity");
        return AePickListRequest.AeOrderOptions.builder()
                .customerOrderInfo(AePickListRequest.AeCustomerOrderInfo.builder()
                        .orderId(msg.getOrderId())
                        .orderLineId(msg.getOrderlineId())
                        .build())
                .palletization(false)
                .orderClubbing(false)
                .orderSplitting(false)
                .orderlineSplitting(false)
                .groupingTags(AePickListRequest.AeGroupingTags.builder()
                        .missionGrouningTag("")
                        .containerGroupingTag("")
                        .binGroupingTag("")
                        .build())
                .bintags(resolvedBintags)
                .containerType(null)
                .destinationGroup(destinationGroup)
                .simplePriority("normal")
                .behaviours(resolvedBehaviours)
                .build();
    }

    // ─── Order node navigation helpers ──────────────────────────────────────

    private JsonNode findChildByOrderlineId(JsonNode orderNode, String orderlineId) {
        if (orderlineId == null || orderlineId.isBlank()) {
            log.warn("orderlineId is null or blank — cannot match child order node");
            return null;
        }
        JsonNode children = orderNode.path("children");
        if (children.isMissingNode() || !children.isArray()) {
            log.warn("No children array in order node response for orderId: {}",
                    orderNode.path("order_node_id").asText());
            return null;
        }
        for (JsonNode child : children) {
            if (orderlineId.equals(child.path("order_node_id").asText())) {
                log.info("Matched orderline child | order_node_id: {}", orderlineId);
                return child;
            }
        }
        log.warn("No child found with order_node_id: {} in children of order: {}",
                orderlineId, orderNode.path("order_node_id").asText());
        return null;
    }

    private List<String> extractStringList(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull() || !node.isArray()) {
            return null;
        }
        List<String> result = new ArrayList<>();
        for (JsonNode item : node) {
            if (!item.isNull()) {
                result.add(item.asText());
            }
        }
        return result.isEmpty() ? null : result;
    }

    private String extractString(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        String val = node.asText(null);
        return (val == null || val.isBlank()) ? null : val;
    }
}
