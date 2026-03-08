package org.example.alert.application.port;

import org.example.alert.domain.model.queue.InternalPriceEvent;
import org.example.alert.domain.model.queue.PriceEventMessage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class InternalMessageTransform implements Transform<PriceEventMessage, InternalPriceEvent> {

    @Override
    public InternalPriceEvent transform(PriceEventMessage input) {
        return InternalPriceEvent.builder()
                .symbol(input.getSymbol())
                .source(input.getSource())
                .price(input.getBidPrice())
                .timestamp(Instant.ofEpochMilli(input.getTimestamp()))
                .eventId(UUID.randomUUID().toString())
                .build();
    }
}
