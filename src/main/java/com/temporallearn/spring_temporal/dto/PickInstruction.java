package com.temporallearn.spring_temporal.dto;

import java.util.List;

import lombok.Data;

@Data
public class PickInstruction {
    private String pickInstructionId;               // Used as WorkflowId and OrderId
    private String item;
    private String tpid;
    private List<String> scannableBarcodes;
    private String pickLocation;
    private String dropLocation;
    private String uom;
    private int qty;

    // PPS-related fields required by Butler Core
    private int ppsId;                   // PPS station ID
    private String binId;                // Bin ID within the PPS
    private String ppsPoint;             // PPS point identifier
    private String seatName;             // Seat/station name
    private String slotref;              // Slot reference
    private String userLoggedIn;         // User logged in at PPS
    private boolean markedContainerScanned; // Whether container was scanned
}