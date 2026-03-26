package com.temporallearn.spring_temporal.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Outbound event published to &lt;tenant&gt;.item_picked.events (consumed by Butler Core).
 *
 * Published by SendItemPickedEventDelegate for every pick_transaction event received from AE.
 * One ItemPickedEvent is emitted per (serviceRequest × transaction) pair.
 *
 * Field sources:
 *   - PPS fields (pps_id, seat_name, pps_bin_id, etc.)  — pickInstruction Camunda process variable
 *   - item_uid, tpid                                     — ae_order.payload (stored PickListRequest)
 *   - transaction_id                                      — pickInstructionId (same as order_id)
 *   - state, picked_qty                                  — PickListEvent.Transaction (from AE event)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ItemPickedEvent {

    @JsonProperty("pps_id")
    private String ppsId;

    @JsonProperty("seat_name")
    private String seatName;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("slot_ref")
    private String slotRef;

    @JsonProperty("pps_bin_id")
    private String ppsBinId;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("state")
    private String state;

    @JsonProperty("pps_point")
    private String ppsPoint;

    @JsonProperty("user_logged_in")
    private String userLoggedIn;

    @JsonProperty("is_marked_container_flow")
    private boolean isMarkedContainerFlow;

    @JsonProperty("last_item_picked")
    private boolean lastItemPicked;

    @JsonProperty("barcode_data_scanned")
    private String barcodeDataScanned;

    @JsonProperty("dest_bar_code")
    private String destBarCode;

    @JsonProperty("dest_rollcage_barcode")
    private String destRollcageBarcode;

    @JsonProperty("dangling_area")
    private String danglingArea;

    @JsonProperty("irt_bin_serial")
    private String irtBinSerial;

    @JsonProperty("carrier_info")
    private String carrierInfo;

    @JsonProperty("checklists")
    private List<Object> checklists;

    @JsonProperty("node_pick_details")
    private List<Object> nodePickDetails;

    @JsonProperty("picked_item_info_list")
    private List<PickedItemInfo> pickedItemInfoList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PickedItemInfo {

        @JsonProperty("tpid")
        private String tpid;

        @JsonProperty("item_uid")
        private String itemUid;

        @JsonProperty("uom")
        private String uom;

        @JsonProperty("picked_qty")
        private int pickedQty;

        @JsonProperty("pick_instruction_ids")
        private List<String> pickInstructionIds;

        @JsonProperty("exception")
        private String exception;
    }
}
