"""Kafka (Amazon MSK) helpers."""
import json
import logging
import threading
import time
from typing import Callable

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config, metrics

log = logging.getLogger(__name__)

TOPIC_SPECS = {
    config.TOPIC_SHIPMENT_EVENTS: 6,
    config.TOPIC_ANALYTICS_RESULTS: 3,
}


def base_config() -> dict:
    return {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "security.protocol": config.KAFKA_SECURITY_PROTOCOL,
        "client.id": f"{config.SERVICE_NAME}-{config.POD_NAME}",
    }


def ensure_topics(replication: int = 2) -> None:
    """Idempotently create the topics the system needs."""
    admin = AdminClient(base_config())
    try:
        existing = set(admin.list_topics(timeout=10).topics.keys())
    except KafkaException as exc:
        log.warning("cannot list topics", extra={"error": str(exc)})
        return
    brokers = len(admin.list_topics(timeout=10).brokers) or 1
    wanted = [NewTopic(t, num_partitions=p, replication_factor=min(replication, brokers))
              for t, p in TOPIC_SPECS.items() if t not in existing]
    if not wanted:
        return
    for topic, fut in admin.create_topics(wanted).items():
        try:
            fut.result()
            log.info("created topic", extra={"topic": topic})
        except Exception as exc:  # already exists / race with another replica
            log.info("topic create skipped", extra={"topic": topic, "reason": str(exc)})


def make_producer(**overrides) -> Producer:
    conf = base_config() | {
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    }
    conf.update(overrides)
    return Producer(conf)


class ConsumerLoop:
    """Background consumer thread with at-least-once semantics.

    Offsets are *stored* only after the handler succeeds and are committed by
    librdkafka's auto-commit, so a crash re-delivers unprocessed events.
    Handlers must therefore be idempotent (they are: event_id based dedup).
    """

    def __init__(self, group_id: str, topics: list[str], handler: Callable[[dict, object], None],
                 offset_reset: str = "earliest"):
        self.group_id = group_id
        self.topics = topics
        self.handler = handler
        self.offset_reset = offset_reset
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._thread: threading.Thread | None = None
        self.consumer: Consumer | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name=f"consumer-{self.group_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def suspend(self) -> None:
        """Leave the consumer group (used to simulate a crashed node)."""
        self._paused.set()

    def resume(self) -> None:
        self._paused.clear()

    def _new_consumer(self) -> Consumer:
        c = Consumer(base_config() | {
            "group.id": self.group_id,
            "auto.offset.reset": self.offset_reset,
            "enable.auto.commit": True,
            "enable.auto.offset.store": False,
            "session.timeout.ms": 10000,
        })
        c.subscribe(self.topics)
        return c

    def _run(self) -> None:
        while not self._stop.is_set():
            if self._paused.is_set():
                if self.consumer is not None:
                    self.consumer.close()
                    self.consumer = None
                    log.warning("consumer left group (node suspended)")
                time.sleep(0.5)
                continue
            if self.consumer is None:
                try:
                    self.consumer = self._new_consumer()
                except Exception as exc:
                    log.error("consumer create failed", extra={"error": str(exc)})
                    time.sleep(3)
                    continue
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("kafka error", extra={"error": str(msg.error())})
                continue
            try:
                event = json.loads(msg.value())
            except Exception:
                log.error("dropping undecodable message", extra={"topic": msg.topic(), "offset": msg.offset()})
                self.consumer.store_offsets(msg)
                continue
            attempt = 0
            while not self._stop.is_set():
                try:
                    self.handler(event, msg)
                    metrics.KAFKA_CONSUMED.labels(msg.topic(), event.get("event_type", event.get("type", "?"))).inc()
                    self.consumer.store_offsets(msg)
                    break
                except Exception as exc:
                    attempt += 1
                    metrics.KAFKA_CONSUME_ERRORS.labels(msg.topic()).inc()
                    log.warning("handler failed, retrying", extra={
                        "error": str(exc), "attempt": attempt, "event_id": event.get("event_id")})
                    time.sleep(min(5, 0.2 * 2 ** attempt))
                    if self._paused.is_set():
                        break
        if self.consumer is not None:
            self.consumer.close()
