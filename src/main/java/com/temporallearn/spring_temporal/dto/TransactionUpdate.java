package com.temporallearn.spring_temporal.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Internal DTO representing a transaction update for a pick instruction.
 * Constructed by {@code PickListEventListener} from an inbound AE {@code gor.pick-list.events}
 * message and used to correlate the Camunda {@code TransactionUpdateMessage} signal.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionUpdate {

    private String pickInstructionId;
    private String transactionId;
    private String command;          // UPDATE, COMPLETE, CANCEL, RETRY
    private String transactionType;  // hardcoded "PICK" — no transactionType in AE contract
    private int processedQty;        // containerAttributes.qty_picked from transactions[0]
    private String status;           // IN_PROGRESS, COMPLETED, FAILED
    private String timestamp;
    private String additionalInfo;   // carries AE sub_state for tracing
}
