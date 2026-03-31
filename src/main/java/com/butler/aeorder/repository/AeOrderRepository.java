package com.butler.aeorder.repository;

import com.butler.aeorder.model.AeOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AeOrderRepository extends JpaRepository<AeOrder, Long> {
    Optional<AeOrder> findByExternalServiceRequestId(String externalServiceRequestId);
}
