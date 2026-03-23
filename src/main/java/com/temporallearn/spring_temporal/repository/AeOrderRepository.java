package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.AeOrder;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for the ae_order table.
 * Key: external_service_request_id (= pickId).
 */
public interface AeOrderRepository extends JpaRepository<AeOrder, String> {
}
