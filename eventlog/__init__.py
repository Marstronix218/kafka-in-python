"""Educational, Kafka-inspired distributed event log."""

from .client import ClusterClient, Consumer

__all__ = ["ClusterClient", "Consumer"]
