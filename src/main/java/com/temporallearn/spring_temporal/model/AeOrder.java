package com.temporallearn.spring_temporal.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * Persists the PickListRequest payload sent to AE for a given pick instruction.
 *
 * Lifecycle:
 *   1. Created by SendPickListToAEDelegate — payload = exact PickListRequest JSON sent to AE.
 *   2. Enriched by PickListEventListener on every inbound pick-list.events event:
 *      - Parent level: state, sub_state, attributes, actuals, expectations
 *      - Per-serviceRequest (matched by externalServiceRequestId): same fields
 *
 * The payload JSONB starts as a verbatim PickListRequest and accumulates AE event
 * fields over time. When read back in SendItemPickedEventDelegate it is deserialized
 * as PickListRequest (unknown keys like state/actuals are silently ignored by Jackson).
 */
@Entity
@Table(name = "ae_order")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AeOrder {

    /** Primary key — equals PickListRequest.externalServiceRequestId (= pickInstructionId). */
    @Id
    @Column(name = "external_service_request_id")
    private String externalServiceRequestId;

    /**
     * Full JSON of the PickListRequest sent to AE, selectively enriched with
     * state/actuals/expectations from subsequent pick-list.events.
     * Stored as JSONB for efficient querying.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", nullable = false)
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
