package com.temporallearn.spring_temporal.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

import lombok.Data;

@Data
public class PickInstruction {

    // ── (pick-instruction.events Kafka contract) ──────────────────
    @JsonProperty("id")
    private String pickInstructionId;               // Used as WorkflowId and OrderId

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("orderline_id")
    private String orderlineId;

    @JsonProperty("qty")
    private int qty;

    @JsonProperty("slot_id")
    private String slotLocation;                    // Inventory location slot

    @JsonProperty("uom")
    private String uom;

    @JsonProperty("tpid")
    private String tpid;

    @JsonProperty("item_id")
    private String itemId;

    @JsonProperty("pps_id")
    private int ppsId;                              // PPS station ID

    @JsonProperty("bin_id")
    private String binId;                           // Bin ID within the PPS

    @JsonProperty("extra_fields")
    private ExtraFields extraFields;

    @Data
    public static class ExtraFields {

        @JsonProperty("barcodes")
        private List<String> barcodes;              // LPN / marked-container barcodes

        @JsonProperty("product_sku")
        private String productSku;                  // Product SKU

        @JsonProperty("seat_name")
        private String seatName;                    // Seat/station name at PPS
    }
}
