"""Start a controller and brokers as local subprocesses, for the benchmark and the demo.

Each process is `python -m eventlog ...` on a free 127.0.0.1 port, with its data in a
temporary directory that is removed on stop()."""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable

from .client import ClusterClient
from .common import APIError, request_json


ROOT = Path(__file__).resolve().parents[1]
STARTUP_SECONDS = 10.0


def free_ports(count: int) -> list[int]:
    """Return count ports that were free a moment ago, all different."""
    # Holding every socket open until the end stops the OS from handing out a port twice.
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def wait_until(condition: Callable[[], object], timeout: float, what: str,
               interval: float = 0.05) -> object:
    """Call condition until it returns something truthy, and return that.

    APIError and OSError count as "not yet". Raises TimeoutError naming `what`.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while True:
        try:
            result = condition()
            if result:
                return result
        except (APIError, OSError) as exc:
            last_error = exc
        if time.monotonic() >= deadline:
            detail = f" (last error: {last_error})" if last_error else ""
            raise TimeoutError(f"timed out after {timeout:g}s waiting for {what}{detail}")
        time.sleep(interval)


class LocalCluster:
    """A controller and `brokers` brokers (b1, b2, ...) running on this machine.

    Use as a context manager, or call start() and stop(). stop() terminates every
    process it started, so wrap use in try/finally. Process output goes to
    <work_dir>/logs/<name>.log.
    """

    def __init__(self, brokers: int = 3, topics: dict | None = None,
                 heartbeat_timeout: float = 2.0, work_dir: str | Path | None = None):
        if topics is None:
            topics = {"events": {"partitions": 3, "replication_factor": min(3, brokers)}}
        self._own_dir = work_dir is None
        self.work_dir = Path(tempfile.mkdtemp(prefix="eventlog-")) if work_dir is None \
            else Path(work_dir)
        try:
            self.work_dir.mkdir(parents=True, exist_ok=True)
            (self.work_dir / "logs").mkdir(exist_ok=True)

            controller_port, *broker_ports = free_ports(brokers + 1)
            self.controller_url = f"http://127.0.0.1:{controller_port}"
            self.broker_urls = {f"b{index}": f"http://127.0.0.1:{port}"
                                for index, port in enumerate(broker_ports, start=1)}
            self.heartbeat_timeout = heartbeat_timeout
            self.config = {"brokers": self.broker_urls, "topics": topics}
            self.config_path = self.work_dir / "config.json"
            self.config_path.write_text(json.dumps(self.config, indent=2), encoding="utf-8")
        except BaseException:
            # Nothing is running yet, and the caller never gets an object to stop().
            if self._own_dir:
                shutil.rmtree(self.work_dir, ignore_errors=True)
            raise
        self.processes: dict[str, subprocess.Popen] = {}
        self.client = ClusterClient(self.controller_url)

    def __enter__(self) -> LocalCluster:
        try:
            self.start()
        except BaseException:
            self.stop()
            raise
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    def start(self) -> None:
        """Start the controller and every broker, then wait until each partition has a
        leader and a full ISR."""
        port = self.controller_url.rsplit(":", 1)[1]
        self._spawn("controller", "controller", "--config", str(self.config_path),
                    "--port", port, "--heartbeat-timeout", str(self.heartbeat_timeout))
        self._wait_for_process("controller", lambda: request_json(self.controller_url + "/health"))
        for broker_id in self.broker_urls:
            self.start_broker(broker_id, wait=False)
        for broker_id in self.broker_urls:
            self._wait_for_process(
                broker_id, lambda url=self.broker_urls[broker_id]: request_json(url + "/health"))
        self.wait_for_full_isr(STARTUP_SECONDS)

    def start_broker(self, broker_id: str, wait: bool = True) -> None:
        """Start (or restart) a broker on its old port and data directory."""
        if self.running(broker_id):
            raise ValueError(f"broker {broker_id} is already running")
        port = self.broker_urls[broker_id].rsplit(":", 1)[1]
        self._spawn(broker_id, "broker", "--config", str(self.config_path), "--id", broker_id,
                    "--controller", self.controller_url, "--port", port,
                    "--data-dir", str(self.data_dir(broker_id)))
        if wait:
            self._wait_for_process(
                broker_id, lambda: request_json(self.broker_urls[broker_id] + "/health"))

    def data_dir(self, broker_id: str) -> Path:
        """Where a broker keeps its partition files. Removed by stop()."""
        return self.work_dir / "data" / broker_id

    def kill_broker(self, broker_id: str) -> None:
        """SIGKILL a broker: no shutdown code runs, as in a crash or power loss."""
        process = self.processes[broker_id]
        process.kill()
        process.wait(timeout=5)

    def stop_broker(self, broker_id: str) -> None:
        """SIGTERM a broker and wait for it to exit."""
        _terminate(self.processes[broker_id])

    def running(self, name: str) -> bool:
        process = self.processes.get(name)
        return process is not None and process.poll() is None

    def stop(self) -> None:
        """Stop every process, then remove the work directory if this object made it."""
        for name in reversed(list(self.processes)):
            if self.running(name):
                self.processes[name].terminate()
        for process in self.processes.values():
            _terminate(process)
        if self._own_dir:
            shutil.rmtree(self.work_dir, ignore_errors=True)

    def partitions(self) -> list[tuple[str, int]]:
        return [(topic, partition) for topic, spec in self.config["topics"].items()
                for partition in range(spec["partitions"])]

    def wait_for_full_isr(self, timeout: float = 30.0) -> None:
        """Wait until every partition has a leader and all its replicas in the ISR."""
        def full() -> bool:
            for topic, partition in self.partitions():
                route = self.client.route(topic, partition)
                if route["leader"] is None or len(route["isr"]) < len(route["replicas"]):
                    return False
            return True
        wait_until(full, timeout, "every partition to have a leader and a full ISR")

    def leader(self, topic: str, partition: int) -> str:
        return self.client.route(topic, partition)["leader"]

    def _spawn(self, name: str, *arguments: str) -> None:
        log = (self.work_dir / "logs" / f"{name}.log").open("ab")
        try:
            self.processes[name] = subprocess.Popen(
                [sys.executable, "-m", "eventlog", *arguments], cwd=ROOT,
                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        finally:
            # The child has its own copy of the file descriptor.
            log.close()

    def _wait_for_process(self, name: str, ready: Callable[[], object]) -> None:
        def check() -> object:
            code = self.processes[name].poll()
            if code is not None:
                log = (self.work_dir / "logs" / f"{name}.log").read_text(errors="replace")
                raise RuntimeError(f"{name} exited with code {code} during startup:\n{log}")
            return ready()
        wait_until(check, STARTUP_SECONDS, f"{name} to answer /health")


def _terminate(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)
