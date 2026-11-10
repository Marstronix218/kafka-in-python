"""Fault injection for a broker's network traffic, changed at runtime through /faults.

Rules match outgoing calls (to a peer broker or the controller) and incoming requests
handled by the broker's HTTP server. An empty table changes nothing.

Kafka reference: trogdor NetworkPartitionFaultSpec and DegradedNetworkFaultSpec."""

from __future__ import annotations

import random
import threading
import time
from typing import Callable
from urllib.parse import urlparse

from .common import UNAVAILABLE, APIError


# drop: the request never arrives. drop_response: it is handled, but the reply is lost.
# delay: it arrives late. duplicate: it is handled twice.
ACTIONS = ("drop", "drop_response", "delay", "duplicate")
DIRECTIONS = ("out", "in")
MAX_DELAY_SECONDS = 60.0
_RULE_FIELDS = {"action", "direction", "to", "path", "probability", "seconds"}


def _number(rule: dict, name: str, default: float) -> float:
    value = rule.get(name, default)
    # bool is a subclass of int, and true must not pass as 1.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"fault rule {name} must be a number, got {value!r}")
    return float(value)


def normalize_rule(rule: object, targets: set[str] | None = None) -> dict:
    """Return the rule with every field filled in. Raises ValueError if it is invalid.

    targets, when given, are the names allowed in "to" besides "*".
    """
    if not isinstance(rule, dict):
        raise ValueError(f"a fault rule must be a JSON object, got {rule!r}")
    unknown = set(rule) - _RULE_FIELDS
    if unknown:
        raise ValueError(f"unknown fault rule fields: {', '.join(sorted(unknown))} "
                         f"(allowed: {', '.join(sorted(_RULE_FIELDS))})")
    action = rule.get("action")
    if action not in ACTIONS:
        raise ValueError(f"fault rule action must be one of {', '.join(ACTIONS)}, got {action!r}")
    direction = rule.get("direction", "out")
    if direction not in DIRECTIONS:
        raise ValueError(f"fault rule direction must be 'out' or 'in', got {direction!r}")
    to = rule.get("to", "*")
    if not isinstance(to, str) or not to:
        raise ValueError(f"fault rule 'to' must be a broker id, 'controller' or '*', got {to!r}")
    if direction == "in" and to != "*":
        raise ValueError("'to' only applies to outgoing rules; an incoming rule matches "
                         "requests from anyone")
    if targets is not None and to != "*" and to not in targets:
        raise ValueError(f"fault rule 'to' must be '*' or one of {', '.join(sorted(targets))}, "
                         f"got {to!r}")
    path = rule.get("path", "*")
    if not isinstance(path, str) or not (path == "*" or path.startswith("/")):
        raise ValueError(f"fault rule path must start with '/' or be '*', got {path!r}")
    probability = _number(rule, "probability", 1.0)
    if not 0.0 <= probability <= 1.0:
        raise ValueError(f"fault rule probability must be between 0 and 1, got {probability}")
    seconds = _number(rule, "seconds", 0.0)
    if action == "delay" and not 0.0 < seconds <= MAX_DELAY_SECONDS:
        raise ValueError(f"a delay rule needs seconds between 0 and {MAX_DELAY_SECONDS:g}, "
                         f"got {seconds}")
    if action != "delay" and seconds:
        raise ValueError("seconds only applies to delay rules")
    return {"action": action, "direction": direction, "to": to, "path": path,
            "probability": probability, "seconds": seconds}


class FaultInjector:
    """A thread-safe table of fault rules for one broker.

    The first rule that matches a request and passes its probability roll is applied,
    and the others are skipped. Rolls come from a random.Random seeded with ``seed``,
    so a test or demo that sets the seed sees the same faults on every run.
    """

    def __init__(self, seed: int | None = None, targets: set[str] | None = None,
                 sleep: Callable[[float], None] = time.sleep):
        self._lock = threading.Lock()
        self._rules: list[dict] = []
        self._random = random.Random(seed)
        self._targets = targets
        self._sleep = sleep

    def rules(self) -> list[dict]:
        with self._lock:
            return [dict(rule) for rule in self._rules]

    def set_rules(self, rules: object, seed: int | None = None) -> list[dict]:
        """Replace the whole table and return it. An empty list clears it.

        A seed restarts the probability rolls. Raises ValueError for an invalid rule,
        and then the table is left unchanged.
        """
        if not isinstance(rules, list):
            raise ValueError(f"rules must be a list, got {rules!r}")
        if seed is not None and (isinstance(seed, bool) or not isinstance(seed, int)):
            raise ValueError(f"seed must be an integer, got {seed!r}")
        normalized = [normalize_rule(rule, self._targets) for rule in rules]
        with self._lock:
            self._rules = normalized
            if seed is not None:
                self._random.seed(seed)
        return self.rules()

    def _pick(self, direction: str, target: str, path: str) -> dict | None:
        with self._lock:
            for rule in self._rules:
                if (rule["direction"] != direction or rule["to"] not in ("*", target)
                        or rule["path"] not in ("*", path)):
                    continue
                probability = rule["probability"]
                # Rolls only happen for probabilities strictly between 0 and 1, so
                # adding an always-on rule doesn't shift the rolls of the others.
                if probability >= 1.0 or (probability > 0.0
                                          and self._random.random() < probability):
                    return dict(rule)
        return None

    def outgoing(self, target: str, path: str, send: Callable[[], dict],
                 timeout: float) -> dict:
        """Call send() for a request to target, with the first matching rule applied.

        Raises APIError with status 503 when the rule loses the request or its reply,
        worded like request_json's own network errors.
        """
        if not self._rules:
            return send()
        rule = self._pick("out", target, urlparse(path).path)
        if rule is None:
            return send()
        where = f"{target} {path}"
        action = rule["action"]
        if action == "drop":
            raise APIError(UNAVAILABLE, f"cannot reach {where}: dropped by a fault rule")
        if action == "delay":
            # A request held past the caller's timeout looks the same as a lost one.
            if rule["seconds"] >= timeout:
                self._sleep(timeout)
                raise APIError(UNAVAILABLE, f"no response from {where}: a fault rule delayed "
                                            f"it {rule['seconds']:g}s, past the {timeout:g}s "
                                            f"timeout")
            self._sleep(rule["seconds"])
            return send()
        if action == "duplicate":
            try:
                send()
            except APIError:
                pass
            return send()
        try:
            send()
        except APIError:
            pass
        raise APIError(UNAVAILABLE, f"no response from {where}: a fault rule dropped the reply")

    def incoming(self, path: str) -> str | None:
        """Apply a matching delay rule, and return "drop", "drop_response" or "duplicate"
        if one of those matched instead, else None. The HTTP handler acts on the name."""
        if not self._rules:
            return None
        rule = self._pick("in", "*", path)
        if rule is None:
            return None
        if rule["action"] == "delay":
            self._sleep(rule["seconds"])
            return None
        return rule["action"]
