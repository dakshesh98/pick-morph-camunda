package com.butler.aeorder.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans (auto-created on startup if absent).
 *
 * Topics owned and created by this service (AA):
 *   - pick-list.requests    — outbound to AE to initiate a pick
 *   - item_picked.events    — outbound to Butler Core (renamed: underscores per AE contract)
 *   - pick-instruction.events — inbound from upstream system (Step 1 Kafka trigger)
 *
 * Topics owned by external systems (not declared here):
 *   - pick-list.response    — created by AE, consumed by PickListResponseListener
 *   - pick-list.events      — created by AE, consumed by PickListEventListener
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.pick-list-requests}")
    private String pickListRequestsTopic;

    @Value("${kafka.topic.item-picked-events}")
    private String itemPickedEventsTopic;

    @Value("${kafka.topic.pick-instructions}")
    private String pickInstructionsTopic;

    @Value("${kafka.topics.pick-instructions-request}")
    private String pickInstructionsRequestTopic;

    @Value("${kafka.topics.pick-instruction-response}")
    private String pickInstructionResponseTopic;

    @Value("${kafka.topics.workflow-complete-events}")
    private String workflowCompleteEventsTopic;

    @Value("${kafka.topic.order-update-events}")
    private String orderUpdateEventsTopic;

    @Bean
    public NewTopic pickListRequestsTopic() {
        return TopicBuilder.name(pickListRequestsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic itemPickedEventsTopic() {
        return TopicBuilder.name(itemPickedEventsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickInstructionsTopic() {
        return TopicBuilder.name(pickInstructionsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickInstructionsRequestTopicBean() {
        return TopicBuilder.name(pickInstructionsRequestTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic pickInstructionResponseTopicBean() {
        return TopicBuilder.name(pickInstructionResponseTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic workflowCompleteEventsTopicBean() {
        return TopicBuilder.name(workflowCompleteEventsTopic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderUpdateEventsTopicBean() {
        return TopicBuilder.name(orderUpdateEventsTopic).partitions(1).replicas(1).build();
    }
}
