import json
import logging

import paho.mqtt.client as paho

log = logging.getLogger(__name__)


class MqttNode:
    """Thin paho wrapper: JSON payloads, auto-resubscribe, retained online/offline (LWT) availability."""

    def __init__(self, client_id: str, host: str, port: int, availability_topic: str | None = None,
                 node_id: str | None = None):
        self.host, self.port = host, port
        self.availability_topic = availability_topic
        self.node_id = node_id or client_id
        self.client = paho.Client(paho.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.reconnect_delay_set(1, 10)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self._subs: list[tuple[str, int]] = []
        self._on_connect_hooks = []
        if availability_topic:
            self.client.will_set(availability_topic, self._availability("offline"), qos=1, retain=True)

    def _availability(self, status: str) -> str:
        return json.dumps({"node": self.node_id, "status": status})

    def subscribe(self, topic_filter: str, handler, qos: int = 1) -> None:
        """handler(topic: str, payload: dict). Exceptions in a handler are logged, never raised into paho."""
        def wrapper(_client, _userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
            except ValueError:
                log.warning("non-JSON payload on %s", msg.topic)
                return
            try:
                handler(msg.topic, payload)
            except Exception:
                log.exception("handler failed for %s", msg.topic)

        self._add_subscription(topic_filter, wrapper, qos)

    def monitor(self, topic_filter: str, handler, qos: int = 1) -> None:
        """handler(topic, raw: bytes, qos: int, retain: bool): every message undecoded, for traffic inspection."""
        def wrapper(_client, _userdata, msg):
            try:
                handler(msg.topic, msg.payload, msg.qos, bool(msg.retain))
            except Exception:
                log.exception("monitor handler failed for %s", msg.topic)

        self._add_subscription(topic_filter, wrapper, qos)

    def _add_subscription(self, topic_filter: str, callback, qos: int) -> None:
        self.client.message_callback_add(topic_filter, callback)
        self._subs.append((topic_filter, qos))
        if self.client.is_connected():
            self.client.subscribe(topic_filter, qos)

    def on_connect(self, hook) -> None:
        self._on_connect_hooks.append(hook)

    def publish(self, topic: str, payload: dict, qos: int = 1, retain: bool = False):
        return self.client.publish(topic, json.dumps(payload), qos=qos, retain=retain)

    def _on_connect(self, client, _userdata, _flags, reason_code, _properties):
        if reason_code.is_failure:
            log.error("MQTT connect refused: %s", reason_code)
            return
        log.info("connected to MQTT broker %s:%s", self.host, self.port)
        for topic_filter, qos in self._subs:
            client.subscribe(topic_filter, qos)
        if self.availability_topic:
            client.publish(self.availability_topic, self._availability("online"), qos=1, retain=True)
        for hook in self._on_connect_hooks:
            hook()

    def _on_disconnect(self, _client, _userdata, _flags, reason_code, _properties):
        if reason_code.is_failure:
            log.warning("lost connection to MQTT broker (%s); paho will retry", reason_code)
        else:
            log.info("disconnected from MQTT broker")

    def start(self) -> None:
        # connect_async + loop_start: the process can start before the broker and paho keeps retrying.
        self.client.connect_async(self.host, self.port, keepalive=30)
        self.client.loop_start()

    def stop(self) -> None:
        if self.availability_topic and self.client.is_connected():
            info = self.client.publish(self.availability_topic, self._availability("offline"), qos=1, retain=True)
            info.wait_for_publish(timeout=2)
        self.client.disconnect()
        self.client.loop_stop()
