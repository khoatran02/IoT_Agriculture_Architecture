import logging

log = logging.getLogger(__name__)


class RelayActuator:
    """A relay on a GPIO pin driving a fan or pump. Starts OFF; switching is idempotent."""

    def __init__(self, name: str, gpio: int | None = None):
        self.name, self.gpio = name, gpio
        self.state = "OFF"

    def set(self, state: str) -> bool:
        """Drive the relay. Returns True if the state actually changed."""
        if state not in ("ON", "OFF"):
            raise ValueError(f"invalid state {state!r}")
        if state == self.state:
            return False
        self.state = state
        log.info("%s: GPIO%s -> %s", self.name, self.gpio, "HIGH" if state == "ON" else "LOW")
        return True
