package com.supplychain.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Flattened view of the `shipment-events` envelope (Flink POJO: public fields + no-arg ctor). */
public class ShipmentEvent {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String eventId;
    public String eventType;
    public String trackingId;
    public String carrier;
    public String status;
    /** Business time of the carrier scan (ms). */
    public long eventTime;
    /** When Tracking Ingestion received it (ms). */
    public long receivedAt;
    /** When the Event Publisher put it on Kafka (ms) - also the Kafka record timestamp. */
    public long publishedAt;
    /** Only on ShipmentBooked: promised delivery (ms). */
    public long promisedDelivery;

    public ShipmentEvent() {}

    public static ShipmentEvent parse(String json) {
        try {
            JsonNode n = MAPPER.readTree(json);
            JsonNode p = n.path("payload");
            ShipmentEvent e = new ShipmentEvent();
            e.eventId = n.path("event_id").asText(null);
            e.eventType = n.path("event_type").asText("");
            e.trackingId = n.hasNonNull("tracking_id") ? n.get("tracking_id").asText() : n.path("aggregate_id").asText(null);
            e.publishedAt = n.path("published_at").asLong(n.path("recorded_at").asLong(0));
            e.carrier = p.path("carrier").asText("UNKNOWN");
            e.status = p.path("status").asText(null);
            e.eventTime = p.path("event_time").asLong(n.path("occurred_at").asLong(e.publishedAt));
            e.receivedAt = p.path("received_at").asLong(n.path("recorded_at").asLong(e.publishedAt));
            e.promisedDelivery = p.path("promised_delivery").asLong(0);
            return e;
        } catch (Exception ex) {
            return null;  // poison message: dropped (counted by the caller)
        }
    }

    public boolean isCarrierStatus() {
        return "CarrierStatusReported".equals(eventType) && status != null && trackingId != null;
    }

    public boolean isBooking() {
        return "ShipmentBooked".equals(eventType) && trackingId != null;
    }
}
