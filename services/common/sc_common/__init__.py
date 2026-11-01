"""Shared library for the supply-chain microservices.

Kept intentionally small: config, structured logging, Prometheus metrics,
Postgres access + migrations, the transactional outbox, Kafka helpers and the
canonical event envelope. Every service image copies this package in.
"""

__all__ = ["config", "logging_setup", "metrics", "db", "outbox", "kafka", "events", "web"]
