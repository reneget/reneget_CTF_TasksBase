import logging
import random
import time
from threading import Event, Lock, Thread

random.seed(20260328)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("clicker-stream-a")

SUBSYSTEMS = ["router", "collector", "dashboard", "auth", "worker", "notify"]
EVENTS = [
    "synthetic_check",
    "cache_refresh",
    "grafana_probe",
    "retention_scan",
    "dashboard_prefetch",
    "trace_fanout",
    "ticket_sync",
]
STATUSES = ["ok", "retry", "cached", "warmup", "skip"]

_NOISE_LOCK = Lock()
_NOISE_STOP = Event()
_NOISE_THREAD: Thread | None = None


def fake_log_id() -> str:
    value = random.randint(100, 999)
    while value == 405:
        value = random.randint(100, 999)
    return f"LogID{value}"


def emit_boot_noise(target_logger: logging.Logger, lines: int = 40) -> None:
    for index in range(lines):
        target_logger.info(
            "STREAM_A_BOOT seq=%d id=%s subsystem=%s status=%s",
            index,
            fake_log_id(),
            random.choice(SUBSYSTEMS),
            random.choice(STATUSES),
        )


def _noise_loop(target_logger: logging.Logger, min_delay: float, max_delay: float) -> None:
    while not _NOISE_STOP.is_set():
        target_logger.info(
            "STREAM_A id=%s subsystem=%s event=%s latency_ms=%d status=%s trace=%06d",
            fake_log_id(),
            random.choice(SUBSYSTEMS),
            random.choice(EVENTS),
            random.randint(2, 900),
            random.choice(STATUSES),
            random.randint(100000, 999999),
        )
        if random.random() < 0.2:
            target_logger.warning(
                "STREAM_A_WARN id=%s subsystem=%s reason=%s retry_in_ms=%d",
                fake_log_id(),
                random.choice(SUBSYSTEMS),
                random.choice(["slow_path", "cold_cache", "jitter", "stale_snapshot"]),
                random.randint(50, 900),
            )
        _NOISE_STOP.wait(timeout=random.uniform(min_delay, max_delay))


def start_noise_stream(
    target_logger: logging.Logger | None = None,
    *,
    boot_lines: int = 0,
    min_delay: float = 0.15,
    max_delay: float = 0.55,
) -> None:
    global _NOISE_THREAD
    with _NOISE_LOCK:
        if _NOISE_THREAD is not None and _NOISE_THREAD.is_alive():
            return
        _NOISE_STOP.clear()
        stream_logger = target_logger or logger
        if boot_lines:
            emit_boot_noise(stream_logger, lines=boot_lines)
        _NOISE_THREAD = Thread(
            target=_noise_loop,
            args=(stream_logger, min_delay, max_delay),
            name="clicker-noise-stream",
            daemon=True,
        )
        _NOISE_THREAD.start()


def stop_noise_stream(timeout: float = 1.0) -> None:
    global _NOISE_THREAD
    with _NOISE_LOCK:
        if _NOISE_THREAD is None:
            return
        _NOISE_STOP.set()
        _NOISE_THREAD.join(timeout=timeout)
        _NOISE_THREAD = None


def main() -> None:
    emit_boot_noise(logger)
    start_noise_stream(logger)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        stop_noise_stream()


if __name__ == "__main__":
    main()
