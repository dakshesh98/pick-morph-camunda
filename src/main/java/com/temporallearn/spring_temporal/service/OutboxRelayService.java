package com.temporallearn.spring_temporal.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.temporallearn.spring_temporal.model.OutboxEvent;
import com.temporallearn.spring_temporal.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Polls the outbox_event table every {@code outbox.relay.poll-interval-ms} milliseconds
 * (default 1 s) and publishes PENDING events to their target Kafka topics.
 *
 * Concurrency safety:
 *   OutboxEventRepository.findPendingForUpdate() uses FOR UPDATE SKIP LOCKED, so
 *   parallel relay instances each grab a non-overlapping batch — no duplicate publishes.
 *
 * Failure handling:
 *   On Kafka error the row is marked FAILED and excluded from future polls.
 *   To retry, reset status = 'PENDING' manually (or via a separate admin endpoint).
 */
@Service
@Slf4j
public class OutboxRelayService {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OutboxRelayService(OutboxEventRepository outboxEventRepository,
                               KafkaTemplate<String, Object> kafkaTemplate,
                               ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxEventRepository.findPendingForUpdate();
        if (pending.isEmpty()) return;

        log.debug("Outbox relay: processing {} PENDING event(s)", pending.size());

        for (OutboxEvent event : pending) {
            try {
                // Deserialize stored JSON to Object — KafkaTemplate's JsonSerializer
                // re-serializes it correctly without double-encoding.
                Object payload = objectMapper.readValue(event.getPayload(), Object.class);
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), payload)
                              .get(5, TimeUnit.SECONDS);

                event.setStatus("PUBLISHED");
                event.setPublishedAt(Instant.now());
                log.info("Outbox PUBLISHED: id={} topic={} aggregateId={}",
                         event.getId(), event.getTopic(), event.getAggregateId());

            } catch (Exception e) {
                log.error("Outbox FAILED to publish: id={} topic={} aggregateId={} error={}",
                          event.getId(), event.getTopic(), event.getAggregateId(), e.getMessage(), e);
                event.setStatus("FAILED");
            }
        }

        outboxEventRepository.saveAll(pending);
    }
}
