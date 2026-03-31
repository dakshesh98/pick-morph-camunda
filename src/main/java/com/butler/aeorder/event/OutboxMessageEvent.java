package com.butler.aeorder.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class OutboxMessageEvent extends ApplicationEvent {

    private final java.util.UUID outboxEventId;

    public OutboxMessageEvent(Object source, java.util.UUID outboxEventId) {
        super(source);
        this.outboxEventId = outboxEventId;
    }
}
