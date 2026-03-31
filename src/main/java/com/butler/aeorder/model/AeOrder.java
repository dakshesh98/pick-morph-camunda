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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

@Entity
@Table(name = "ae_order")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AeOrder {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "external_service_request_id", nullable = false, unique = true)
    private String externalServiceRequestId;

    @Column(name = "type", nullable = false)
    private String type;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "fulfillment_area", columnDefinition = "JSONB")
    private String fulfillmentArea;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "attributes", columnDefinition = "JSONB")
    private String attributes;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "expectations", columnDefinition = "JSONB")
    private String expectations;

    @Column(name = "state", nullable = false)
    private String state;

    @Column(name = "sub_state", nullable = false)
    private String subState;

    @Column(name = "status", nullable = false)
    private String status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "actuals", columnDefinition = "JSONB")
    private String actuals;

    @Column(name = "is_deleted", nullable = false)
    private Boolean isDeleted;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "stages", columnDefinition = "JSONB")
    private String stages;

    @Column(name = "on_hold", nullable = false)
    private Boolean onHold;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;
}
