package com.supplychain.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Runs the real pipeline on an in-memory stream (no Kafka) in a local Flink mini-cluster and
 * prints the results. Used to verify window + keyed-state logic before deploying to EMR:
 *   mvn -q test-compile exec:java -Dexec.classpathScope=test -Dexec.mainClass=com.supplychain.analytics.LocalPipelineRun
 */
public final class LocalPipelineRun {
    public static void main(String[] args) throws Exception {
        ObjectMapper m = new ObjectMapper();
        long t0 = 1_800_000_000_000L - (1_800_000_000_000L % 60_000);   // aligned to a minute
        long hour = 3_600_000L;
        List<String> input = new ArrayList<>();
        // minute 0: 3 DHL events on 2 shipments (one DELAYED), 1 FEDEX event; S1 booked
        input.add(booking(m, "S1", "DHL", t0 - 5_000, t0 + 2 * hour));
        input.add(carrier(m, "S1", "DHL", "PICKED_UP", t0 - 30 * hour, t0 + 1_000, t0 + 1_200));
        input.add(carrier(m, "S2", "DHL", "PICKED_UP", t0 - 10 * hour, t0 + 2_000, t0 + 2_500));
        input.add(carrier(m, "S2", "DHL", "DELAYED", t0 - 5 * hour, t0 + 3_000, t0 + 3_100));
        input.add(carrier(m, "S3", "FEDEX", "IN_TRANSIT", t0 - 1 * hour, t0 + 4_000, t0 + 4_400));
        // minute 1: S1 delivered (30h transit, on time), S2 delivered late
        input.add(carrier(m, "S1", "DHL", "DELIVERED", t0, t0 + 61_000, t0 + 61_300));
        input.add(booking(m, "S2", "DHL", t0 - 20 * hour, t0 - 1 * hour));
        input.add(carrier(m, "S2", "DHL", "DELIVERED", t0 + 30 * 60_000, t0 + 62_000, t0 + 62_100));
        // minute 3 heartbeat to push the watermark past minute 2
        input.add("{\"event_type\":\"Heartbeat\",\"published_at\":" + (t0 + 200_000) + "}");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // keep per-key order like a Kafka partition does
        DataStream<String> raw = env.fromData(input).assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((s, ts) -> {
                            try { return m.readTree(s).path("published_at").asLong(); }
                            catch (Exception e) { return 0L; }
                        }));
        AnalyticsPipeline.build(raw).print();
        env.execute("local-pipeline-run");
    }

    static String booking(ObjectMapper m, String tid, String carrier, long publishedAt, long promised) throws Exception {
        return m.writeValueAsString(m.createObjectNode()
                .put("event_id", "b-" + tid).put("event_type", "ShipmentBooked").put("tracking_id", tid)
                .put("published_at", publishedAt).put("recorded_at", publishedAt).put("occurred_at", publishedAt)
                .set("payload", m.createObjectNode().put("carrier", carrier).put("promised_delivery", promised)));
    }

    static String carrier(ObjectMapper m, String tid, String carrier, String status, long eventTime,
                          long receivedAt, long publishedAt) throws Exception {
        return m.writeValueAsString(m.createObjectNode()
                .put("event_id", "c-" + tid + status).put("event_type", "CarrierStatusReported").put("tracking_id", tid)
                .put("published_at", publishedAt).put("recorded_at", receivedAt).put("occurred_at", eventTime)
                .set("payload", m.createObjectNode().put("carrier", carrier).put("status", status)
                        .put("event_time", eventTime).put("received_at", receivedAt)));
    }
}
