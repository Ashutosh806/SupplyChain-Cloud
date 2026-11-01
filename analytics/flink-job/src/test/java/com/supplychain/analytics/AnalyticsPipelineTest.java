package com.supplychain.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Member 4 - runs the real analytics dataflow (1-minute windows + keyed state) on a local Flink
 * mini-cluster with events shaped exactly like the ones on the `shipment-events` Kafka topic.
 * No Kafka or EMR needed:   cd analytics/flink-job && mvn test
 */
class AnalyticsPipelineTest {
    static final ObjectMapper M = new ObjectMapper();
    static final long T0 = 1_800_000_000_000L - (1_800_000_000_000L % 60_000);   // minute-aligned
    static final long HOUR = 3_600_000L;
    static List<JsonNode> results;

    /** A ShipmentBooked envelope as written by the Booking Service. */
    static String booking(String tid, String carrier, long publishedAt, long promised) throws Exception {
        ObjectNode e = M.createObjectNode().put("event_id", "b-" + tid).put("event_type", "ShipmentBooked")
                .put("aggregate_id", tid).put("tracking_id", tid).put("occurred_at", publishedAt)
                .put("recorded_at", publishedAt).put("published_at", publishedAt).put("source", "booking-service");
        e.set("payload", M.createObjectNode().put("tracking_id", tid).put("carrier", carrier)
                .put("promised_delivery", promised));
        return M.writeValueAsString(e);
    }

    /** A CarrierStatusReported envelope as written by the Tracking Ingestion Service. */
    static String scan(String tid, String carrier, String status, long eventTime, long receivedAt, long publishedAt)
            throws Exception {
        ObjectNode e = M.createObjectNode().put("event_id", "c-" + tid + status).put("event_type", "CarrierStatusReported")
                .put("aggregate_id", tid).put("tracking_id", tid).put("occurred_at", eventTime)
                .put("recorded_at", receivedAt).put("published_at", publishedAt).put("source", "tracking-ingestion");
        e.set("payload", M.createObjectNode().put("carrier", carrier).put("status", status)
                .put("event_time", eventTime).put("received_at", receivedAt));
        return M.writeValueAsString(e);
    }

    @BeforeAll
    static void runPipeline() throws Exception {
        List<String> input = new ArrayList<>();
        // minute 0: DHL 3 scans on 2 shipments (1 DELAYED), FEDEX 1 scan
        input.add(booking("S1", "DHL", T0 - 5_000, T0 + 2 * HOUR));
        input.add(scan("S1", "DHL", "PICKED_UP", T0 - 30 * HOUR, T0 + 1_000, T0 + 1_200));
        input.add(scan("S2", "DHL", "PICKED_UP", T0 - 10 * HOUR, T0 + 2_000, T0 + 2_500));
        input.add(scan("S2", "DHL", "DELAYED", T0 - 5 * HOUR, T0 + 3_000, T0 + 3_100));
        input.add(scan("S3", "FEDEX", "IN_TRANSIT", T0 - HOUR, T0 + 4_000, T0 + 4_400));
        input.add("{not json");                                                           // poison message
        // minute 1: S1 delivered on time (30 h), S2 delivered 90 min late (10.5 h)
        input.add(scan("S1", "DHL", "DELIVERED", T0, T0 + 61_000, T0 + 61_300));
        input.add(booking("S2", "DHL", T0 - 20 * HOUR, T0 - HOUR));
        input.add(scan("S2", "DHL", "DELIVERED", T0 + 30 * 60_000, T0 + 62_000, T0 + 62_100));
        // heartbeat (as the Event Publisher sends) closes the minute-1 windows
        input.add("{\"event_type\":\"Heartbeat\",\"published_at\":" + (T0 + 200_000) + "}");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // one ordered "partition", like a Kafka partition per key
        DataStream<String> raw = env.fromData(input).assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((s, ts) -> {
                            try { return M.readTree(s).path("published_at").asLong(0); }
                            catch (Exception e) { return 0L; }
                        }));
        results = new ArrayList<>();
        try (CloseableIterator<String> it = AnalyticsPipeline.build(raw).executeAndCollect("analytics-test")) {
            while (it.hasNext()) results.add(M.readTree(it.next()));
        }
    }

    static JsonNode find(String type, String field, String value, long windowStart) {
        return results.stream().filter(r -> r.path("type").asText().equals(type)
                        && (field == null || r.path(field).asText().equals(value))
                        && (windowStart < 0 || r.path("window_start").asLong() == windowStart))
                .findFirst().orElse(null);
    }

    @Test
    void carrierWindowCountsEventsAndUniqueShipments() {
        JsonNode dhl = find("carrier_window", "carrier", "DHL", T0);
        assertNotNull(dhl, "no DHL window for minute 0: " + results);
        assertEquals(3, dhl.get("events").asInt());
        assertEquals(2, dhl.get("unique_shipments").asInt());
        assertEquals(1, dhl.get("delayed").asInt());
        assertEquals(T0 + 60_000, dhl.get("window_end").asLong());
        assertEquals(1, find("carrier_window", "carrier", "FEDEX", T0).get("events").asInt());
        JsonNode next = find("carrier_window", "carrier", "DHL", T0 + 60_000);
        assertEquals(2, next.get("delivered").asInt());
    }

    @Test
    void ingestLagIsMeasured() {
        JsonNode dhl = find("carrier_window", "carrier", "DHL", T0);
        assertEquals(500, dhl.get("max_ingest_lag_ms").asLong());
        assertTrue(dhl.get("avg_ingest_lag_ms").asDouble() > 0);
    }

    @Test
    void keyedStateComputesTransitAndDelayEvenWhenBookingArrivesLate() {
        JsonNode s1 = find("shipment_transit", "tracking_id", "S1", -1);
        assertEquals(30.0, s1.get("transit_hours").asDouble());
        assertTrue(s1.get("on_time").asBoolean());
        JsonNode s2 = find("shipment_transit", "tracking_id", "S2", -1);
        assertEquals(10.5, s2.get("transit_hours").asDouble());
        assertEquals(90.0, s2.get("delay_minutes").asDouble());
        assertFalse(s2.get("on_time").asBoolean());
    }

    @Test
    void deliveryWindowAggregatesKpis() {
        JsonNode w = find("delivery_window", null, null, T0 + 60_000);
        assertNotNull(w, "no delivery window: " + results);
        assertEquals(2, w.get("deliveries").asInt());
        assertEquals(20.25, w.get("avg_transit_hours").asDouble());
        assertEquals(0.5, w.get("on_time_ratio").asDouble());
    }

    @Test
    void everyResultHasTheFieldsTheAnalyticsApiReads() {
        Set<String> types = new HashSet<>();
        for (JsonNode r : results) {
            String type = r.path("type").asText();
            types.add(type);
            List<String> required = type.equals("carrier_window")
                    ? List.of("carrier", "window_start", "window_end", "events", "unique_shipments", "avg_ingest_lag_ms")
                    : type.equals("shipment_transit")
                    ? List.of("tracking_id", "carrier", "delivered_at", "transit_hours", "delay_minutes", "on_time")
                    : List.of("window_start", "deliveries", "avg_transit_hours", "on_time_ratio");
            for (String f : required) assertTrue(r.has(f), type + " is missing " + f);
        }
        assertEquals(Set.of("carrier_window", "shipment_transit", "delivery_window"), types);
    }

    @Test
    void parsesKafkaEnvelopes() throws Exception {
        ShipmentEvent b = ShipmentEvent.parse(booking("X1", "DHL", T0, T0 + HOUR));
        assertTrue(b.isBooking());
        assertEquals(T0 + HOUR, b.promisedDelivery);
        ShipmentEvent c = ShipmentEvent.parse(scan("X1", "DHL", "PICKED_UP", T0, T0 + 10, T0 + 20));
        assertTrue(c.isCarrierStatus());
        assertEquals("PICKED_UP", c.status);
        assertEquals("DHL", c.carrier);
    }
}
