"""MQTT topic scheme: agri/<site>/...  (one place so every process agrees)."""


def sensor(site: str, name: str) -> str:
    return f"agri/{site}/sensor/{name}"


def sensor_filter(site: str) -> str:
    return f"agri/{site}/sensor/+"


def actuator_cmd(site: str, name: str) -> str:
    return f"agri/{site}/actuator/{name}/cmd"


def actuator_cmd_filter(site: str) -> str:
    return f"agri/{site}/actuator/+/cmd"


def actuator_status(site: str, name: str) -> str:
    return f"agri/{site}/actuator/{name}/status"


def actuator_status_filter(site: str) -> str:
    return f"agri/{site}/actuator/+/status"


def availability(site: str, node: str) -> str:
    return f"agri/{site}/node/{node}/availability"


def availability_filter(site: str) -> str:
    return f"agri/{site}/node/+/availability"


def meta(site: str, node: str) -> str:
    return f"agri/{site}/node/{node}/meta"


def actuator_name(topic: str) -> str:
    """agri/<site>/actuator/<name>/<cmd|status> -> <name>"""
    return topic.split("/")[-2]


def node_id(topic: str) -> str:
    """agri/<site>/node/<id>/availability -> <id>"""
    return topic.split("/")[-2]
