package com.butler.aeorder.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import java.util.List;

/**
 * DTO for messages consumed from the "pick-instruction.requests" Kafka topic.
 * Published by butler_server when a new pick instruction is created.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class PickInstructionRequestMessage {

    private String id;
    private String orderId;
    private String orderlineId;
    private int qty;
    private String slotId;
    private String uom;
    private int tpid;
    private String itemId;
    private List<String> barcodes;
    private int ppsId;
    private String binId;
}
