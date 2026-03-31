package com.butler.aeorder.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Outbox event for the Transactional Outbox pattern.
 *
 * Written in the same DB transaction as the primary write (ae_order or transaction_status).
 * OutboxRelayService polls PENDING rows every second and publishes them to Kafka,
 * then marks them PUBLISHED.
 *
 * Status lifecycle:  PENDING → PUBLISHED (success)
 *                           └→ FAILED    (Kafka error — no auto-retry; reset to PENDING manually)
 */
@Entity
@Table(name = "outbox_event")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /** Kafka message key — pickInstructionId for all current event types. */
    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    /** Target Kafka topic. */
    @Column(nullable = false)
    private String topic;

    /** Pre-serialized JSON payload sent as-is to Kafka. */
    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    /** PENDING | PUBLISHED | FAILED */
    @Column(nullable = false, length = 50)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;
}
