package com.butler.aeorder.repository;

import com.butler.aeorder.model.AeOrder;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for the ae_order table.
 * Key: external_service_request_id (= pickInstructionId).
 */
public interface AeOrderRepository extends JpaRepository<AeOrder, String> {
}
