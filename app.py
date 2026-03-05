import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

import pika

try:
    import RPi.GPIO as GPIO
except Exception:  # pragma: no cover - allows local/non-Pi runs
    class _MockGPIO:
        BCM = "BCM"
        OUT = "OUT"
        _pin_state = {}

        @staticmethod
        def setmode(_mode):
            pass

        @staticmethod
        def setwarnings(_enabled):
            pass

        @staticmethod
        def setup(_pin, _mode, initial=False):
            _MockGPIO._pin_state[_pin] = initial

        @staticmethod
        def output(_pin, _value):
            _MockGPIO._pin_state[_pin] = _value

        @staticmethod
        def input(_pin):
            return _MockGPIO._pin_state.get(_pin, True)

        @staticmethod
        def cleanup(_pin=None):
            pass

    GPIO = _MockGPIO()


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("pump-worker")

STATUS_PENDING = "pending"
STATUS_EXECUTING = "executing"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_EXPIRED = "expired"

ALLOWED_STATUSES = {
    STATUS_PENDING,
    STATUS_EXECUTING,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_EXPIRED,
}

PUMP_SIGNAL_ON = False
PUMP_SIGNAL_OFF = True


class Settings:
    rabbit_host = os.getenv("RABBIT_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBIT_PORT", "5672"))
    rabbit_user = os.getenv("RABBIT_USER", "guest")
    rabbit_pass = os.getenv("RABBIT_PASS", "guest")
    rabbit_vhost = os.getenv("RABBIT_VHOST", "/")
    rabbit_heartbeat = int(os.getenv("RABBIT_HEARTBEAT", "60"))

    tasks_queue = os.getenv("TASKS_QUEUE", "tasks")
    task_statuses_queue = os.getenv("TASK_STATUSES_QUEUE", "task-statuses")
    worker_status_queue = os.getenv("WORKER_STATUS_QUEUE", os.getenv("WORKER_STATUS_QUEUE", "worker-status"))

    worker_id = os.getenv("WORKER_ID", os.getenv("WORKER_ID", "raspberry-pi-worker"))
    max_task_age_seconds = int(os.getenv("MAX_TASK_AGE_SECONDS", "300"))
    heartbeat_interval_seconds = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10"))
    inter_task_delay_seconds = int(os.getenv("INTER_TASK_DELAY_SECONDS", "5"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_dt(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized).astimezone(timezone.utc)
    raise ValueError(f"Unsupported timestamp format: {value}")


def _build_pump_pin_mapping() -> Dict[int, int]:
    raw_map = os.getenv("PUMP_PIN_MAP", "").strip()
    if raw_map:
        parsed = json.loads(raw_map)
        return {int(k): int(v) for k, v in parsed.items()}

    mapping = {}
    for pump in range(1, 5):
        env_name = f"PUMP{pump}_PIN"
        if env_name in os.environ:
            mapping[pump] = int(os.environ[env_name])

    if not mapping:
        mapping = {1: 17, 2: 27, 3: 22, 4: 23}

    return mapping


PUMP_PIN_MAPPING = _build_pump_pin_mapping()


def _connection_parameters() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(Settings.rabbit_user, Settings.rabbit_pass)
    return pika.ConnectionParameters(
        host=Settings.rabbit_host,
        port=Settings.rabbit_port,
        virtual_host=Settings.rabbit_vhost,
        credentials=credentials,
        heartbeat=Settings.rabbit_heartbeat,
    )


def _declare_queues(channel) -> None:
    channel.queue_declare(queue=Settings.tasks_queue, durable=True)
    channel.queue_declare(queue=Settings.task_statuses_queue, durable=True)
    channel.queue_declare(queue=Settings.worker_status_queue, durable=True)


def _publish_json(channel, queue_name: str, payload: dict) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=json.dumps(payload),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def publish_task_status(channel, task_number: str, status: str, message: str) -> None:
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Invalid task status: {status}")

    event = {
        "taskNumber": task_number,
        "status": status,
        "changedAt": _iso_utc(_utc_now()),
        "message": message,
    }
    _publish_json(channel, Settings.task_statuses_queue, event)
    LOGGER.info("Task %s -> %s", task_number, status)


def publish_worker_status(channel, message: str) -> None:
    event = {
        "workerId": Settings.worker_id,
        "status": "up",
        "message": message,
        "updatedAt": _iso_utc(_utc_now()),
    }
    _publish_json(channel, Settings.worker_status_queue, event)


def setup_gpio() -> None:
    GPIO.setwarnings(False)
    GPIO.setmode(GPIO.BCM)
    for pin in PUMP_PIN_MAPPING.values():
        GPIO.setup(pin, GPIO.OUT, initial=PUMP_SIGNAL_OFF)
        GPIO.output(pin, PUMP_SIGNAL_OFF)


def set_pump_enabled(pin: int, enabled: bool) -> None:
    signal = PUMP_SIGNAL_ON if bool(enabled) else PUMP_SIGNAL_OFF
    GPIO.output(pin, signal)


def disable_pump_safe(pin: int) -> None:
    try:
        set_pump_enabled(pin, False)
    except Exception as exc:  # pragma: no cover - hardware dependent
        LOGGER.error("Failed to disable pin %s: %s", pin, exc)


def log_pump_pin_states() -> None:
    states = []
    for pump_number, pin in sorted(PUMP_PIN_MAPPING.items()):
        try:
            signal = GPIO.input(pin)
            state = "ON" if signal == PUMP_SIGNAL_ON else "OFF"
            states.append(f"pump={pump_number} pin={pin} state={state} signal={signal}")
        except Exception as exc:  # pragma: no cover - hardware dependent
            states.append(f"pump={pump_number} pin={pin} state=UNKNOWN error={exc}")
    LOGGER.info("GPIO startup states: %s", "; ".join(states))


def heartbeat_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        connection = None
        try:
            connection = pika.BlockingConnection(_connection_parameters())
            channel = connection.channel()
            _declare_queues(channel)

            while not stop_event.is_set():
                publish_worker_status(channel, "Worker alive")
                for _ in range(Settings.heartbeat_interval_seconds):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
        except Exception as exc:
            LOGGER.error("Heartbeat publisher error: %s", exc)
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                connection.close()


def process_task(channel, payload: dict) -> None:
    task_number = str(payload.get("taskNumber", "")).strip()
    if not task_number:
        raise ValueError("taskNumber is required")

    pump_number = int(payload["pumpNumber"])
    seconds = int(payload["timeSeconds"])
    created_at = _parse_dt(payload.get("createdAt"))

    if seconds < 0:
        raise ValueError("timeSeconds must be >= 0")

    if pump_number not in PUMP_PIN_MAPPING:
        publish_task_status(channel, task_number, STATUS_FAILED, f"Pump {pump_number} is not configured")
        return

    if created_at is not None:
        max_age = timedelta(seconds=Settings.max_task_age_seconds)
        if _utc_now() - created_at > max_age:
            publish_task_status(
                channel,
                task_number,
                STATUS_EXPIRED,
                f"Task expired before processing (older than {Settings.max_task_age_seconds} seconds)",
            )
            return

    pin = PUMP_PIN_MAPPING[pump_number]

    try:
        set_pump_enabled(pin, True)
    except Exception as exc:
        publish_task_status(channel, task_number, STATUS_FAILED, f"Failed to enable pump: {exc}")
        disable_pump_safe(pin)
        return

    publish_task_status(channel, task_number, STATUS_EXECUTING, f"Pump {pump_number} enabled")

    deadline = time.monotonic() + seconds
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 1.0))
    finally:
        disable_pump_safe(pin)

    publish_task_status(channel, task_number, STATUS_COMPLETED, f"Pump {pump_number} disabled")


def run_consumer(stop_event: threading.Event) -> None:
    connection = pika.BlockingConnection(_connection_parameters())
    channel = connection.channel()
    _declare_queues(channel)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, _properties, body):
        try:
            payload = json.loads(body)
            process_task(ch, payload)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            LOGGER.exception("Task processing failed: %s", exc)
            try:
                payload = json.loads(body)
                task_number = str(payload.get("taskNumber", "")).strip()
                if task_number:
                    publish_task_status(ch, task_number, STATUS_FAILED, f"Task processing error: {exc}")
            except Exception:
                LOGGER.error("Unable to publish failed status for malformed task message")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        finally:
            time.sleep(Settings.inter_task_delay_seconds)

    channel.basic_consume(queue=Settings.tasks_queue, on_message_callback=callback, auto_ack=False)

    LOGGER.info("Consumer started. Waiting for tasks on queue '%s'", Settings.tasks_queue)
    while not stop_event.is_set():
        connection.process_data_events(time_limit=1)

    if connection.is_open:
        connection.close()


def main() -> None:
    LOGGER.info("Pump pin mapping: %s", PUMP_PIN_MAPPING)
    setup_gpio()
    log_pump_pin_states()

    stop_event = threading.Event()
    hb_thread = threading.Thread(target=heartbeat_loop, args=(stop_event,), daemon=True)
    hb_thread.start()

    try:
        run_consumer(stop_event)
    finally:
        stop_event.set()
        for pin in PUMP_PIN_MAPPING.values():
            disable_pump_safe(pin)
        try:
            GPIO.cleanup()
        except Exception:
            pass


if __name__ == "__main__":
    main()
