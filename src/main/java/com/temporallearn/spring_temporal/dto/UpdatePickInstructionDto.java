package com.temporallearn.spring_temporal.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * DTO mirroring the UpdatePickInstructionRequest proto message.
 * Used to pass pick instruction update data from the activity layer
 * to the gRPC client/Kafka without exposing protobuf types.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UpdatePickInstructionDto {

    private List<PickedItemInfoDto> pickedItemInfoList;
    private String slotref;
    private String orderId;
    private List<NodePickDetailsDto> nodePickedDetails;
    private PpsBinIdDto ppsbinId;
    private int ppsId;
    private List<String> ppsbinList;
    private String ppsPoint;
    private boolean isMarkedContainerScanned;
    private String seatName;
    private String transactionId;
    private List<String> checklists;
    private CarrierInfoDto carrierInfo;
    private String irtBinSerial;
    private String userLoggedIn;
    private String destBarcode;
    private String destRollcageBarcode;
    private String barcodeDataScanned;
    private String danglingArea;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PickedItemInfoDto {
        private ExceptionDataDto exception;
        private int tpid;
        private String uom;
        private String itemUid;
        private List<String> pickInstructionIds;
        private int pickedQty;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExceptionDataDto {
        private int missing;
        private int unscannable;
        private int physicallyDamaged;
        private int checklistException;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PpsBinIdDto {
        private int ppsId;
        private String binId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodePickDetailsDto {
        private String id;
        private String type;
        private List<NodePickDetailsDto> children;
        private NodePickDetailsInfoDto details;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodePickDetailsInfoDto {
        private int quantity;
        private String transactionType;
        private List<String> barcodeReferences;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CarrierInfoDto {
        private String type;
        private String carrierType;
        private String barcode;
    }
}
