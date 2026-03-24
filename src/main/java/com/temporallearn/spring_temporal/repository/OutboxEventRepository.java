package com.temporallearn.spring_temporal.repository;

import com.temporallearn.spring_temporal.model.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Fetches up to 100 PENDING outbox events ordered by creation time, locking each
     * row with FOR UPDATE SKIP LOCKED.
     *
     * SKIP LOCKED ensures multiple relay instances running in parallel never pick up
     * the same row — each instance processes a distinct, non-overlapping subset.
     */
    @Query(value = """
            SELECT * FROM outbox_event
            WHERE status = 'PENDING'
            ORDER BY created_at
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingForUpdate();
}
