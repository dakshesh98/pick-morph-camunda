package com.butler.aeorder.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Entity
@Table(name = "transaction_status")
public class TransactionStatus {

    @Id
    @Column(name = "transaction_id", nullable = false)
    private String transactionId;

    @Column(name = "pick_instruction_id")
    private String pickInstructionId;

    @Column(name = "status")
    private String status; // IN_PROGRESS, SUCCESS, FAILED

    @Column(name = "last_updated")
    private Instant lastUpdated;

    /** Raw transaction data from the pick_transaction event (containerAttributes, products, etc.). */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb")
    private String payload;

    public TransactionStatus() {}

    public TransactionStatus(String transactionId, String pickInstructionId, String status, Instant lastUpdated) {
        this.transactionId = transactionId;
        this.pickInstructionId = pickInstructionId;
        this.status = status;
        this.lastUpdated = lastUpdated;
    }

    public TransactionStatus(String transactionId, String pickInstructionId, String status, Instant lastUpdated, String payload) {
        this.transactionId = transactionId;
        this.pickInstructionId = pickInstructionId;
        this.status = status;
        this.lastUpdated = lastUpdated;
        this.payload = payload;
    }

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getPickInstructionId() { return pickInstructionId; }
    public void setPickInstructionId(String pickInstructionId) { this.pickInstructionId = pickInstructionId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getLastUpdated() { return lastUpdated; }
    public void setLastUpdated(Instant lastUpdated) { this.lastUpdated = lastUpdated; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
