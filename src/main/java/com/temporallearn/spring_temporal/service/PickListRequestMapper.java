package com.temporallearn.spring_temporal.service;

import com.temporallearn.spring_temporal.dto.PickInstruction;
import com.temporallearn.spring_temporal.dto.ae.PickListRequest;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Maps a {@link PickInstruction} (internal AA model) to a {@link PickListRequest}
 * (AE contract wire format) for publishing to <tenant>.pick-list.requests.
 *
 * Field mapping rationale:
 *   pickInstructionId          → externalServiceRequestId (single order per instruction)
 *   fulfillmentArea → hardcoded "assist_area" (per LLD)
 *   tpid            → shipmentId (transport plan ID maps to AE shipment concept)
 *   slotLocation    → location fields (raw address; AE parses sub-fields on their side)
 *   extraFields.barcodes    → productAttributes.barcodes (LPN/marked-container barcodes)
 *   extraFields.productSku  → product_sku
 *   qty             → productQuantity
 *   binId           → bin_grouping_tag (used by AE for bin-level grouping)
 */
@Component
public class PickListRequestMapper {

    public PickListRequest toPickListRequest(PickInstruction pi) {
        return PickListRequest.builder()
                .externalServiceRequestId(pi.getPickInstructionId())
                .type("PICK")
                .fulfillmentArea(List.of("assist_area"))
                .attributes(buildTopLevelAttributes(pi))
                .serviceRequests(List.of(buildServiceRequest(pi)))
                .build();
    }

    // ── Private builders ─────────────────────────────────────────────────────

    private PickListRequest.TopLevelAttributes buildTopLevelAttributes(PickInstruction pi) {
        return PickListRequest.TopLevelAttributes.builder()
                .skipStandardPickProcess(false)
                .simplePriority("NORMAL")
                .orderOptions(buildOrderOptions(pi))
                .build();
    }

    private PickListRequest.OrderOptions buildOrderOptions(PickInstruction pi) {
        return PickListRequest.OrderOptions.builder()
                .customerOrderInfo(PickListRequest.CustomerOrderInfo.builder()
                        .orderId(pi.getPickInstructionId())
                        .orderLineId(pi.getPickInstructionId())
                        .masterOrderId(pi.getPickInstructionId())
                        .shipmentId(pi.getTpid())
                        .build())
                .palletization(false)
                .orderClubbing(false)
                .orderSplitting(false)
                .orderlineSplitting(false)
                .groupingTags(PickListRequest.GroupingTags.builder()
                        .missionGrouningTag(pi.getPickInstructionId())
                        .containerGroupingTag(pi.getBinId())
                        .binGroupingTag(pi.getBinId())
                        .build())
                .bintags(Collections.emptyList())
                .containerType("")
                .destinationGroup("")
                .simplePriority("NORMAL")
                .behaviours(Collections.emptyList())
                .build();
    }

    private PickListRequest.ServiceRequest buildServiceRequest(PickInstruction pi) {
        return PickListRequest.ServiceRequest.builder()
                .externalServiceRequestId(pi.getPickInstructionId() + "_1")
                .type("PICK_LINE")
                .attributes(buildServiceRequestAttributes(pi))
                .expectations(buildExpectations(pi))
                .build();
    }

    private PickListRequest.ServiceRequestAttributes buildServiceRequestAttributes(PickInstruction pi) {
        return PickListRequest.ServiceRequestAttributes.builder()
                .extraInfo(PickListRequest.ExtraInfo.builder()
                        .clientTaskId(pi.getPickInstructionId())
                        .build())
                .shelfLifeHours(0)
                .location(PickListRequest.Location.builder()
                        .displayName(pi.getSlotLocation())
                        .fullAddress(pi.getSlotLocation())
                        .addressFields(PickListRequest.AddressFields.builder()
                                .side(pi.getSlotLocation())
                                .zone(pi.getSlotLocation())
                                .level(pi.getSlotLocation())
                                .bay(pi.getSlotLocation())
                                .aisle(pi.getSlotLocation())
                                .build())
                        .build())
                .build();
    }

    private PickListRequest.Expectations buildExpectations(PickInstruction pi) {
        PickListRequest.ProductAttributes productAttributes = PickListRequest.ProductAttributes.builder()
                .lotId("")
                .filterParameters(Collections.emptyList())
                .barcodes(pi.getExtraFields() != null && pi.getExtraFields().getBarcodes() != null ? pi.getExtraFields().getBarcodes() : Collections.emptyList())
                .productSku(pi.getExtraFields() != null ? pi.getExtraFields().getProductSku() : null)
                .packageParameters(Collections.emptyList())
                .build();

        PickListRequest.Product product = PickListRequest.Product.builder()
                .productQuantity(pi.getQty())
                .productAttributes(productAttributes)
                .build();

        PickListRequest.Container container = PickListRequest.Container.builder()
                .products(List.of(product))
                .build();

        return PickListRequest.Expectations.builder()
                .containers(List.of(container))
                .build();
    }
}
