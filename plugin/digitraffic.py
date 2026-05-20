# digitraffic.py from https://github.com/sgofferj/trakbridge-plugin-ais-digitraffic.git
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html

"""
Digitraffic AIS Plugin for TrakBridge
"""

import os
import json
import asyncio
from urllib.parse import urlparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, cast

import aiohttp
import paho.mqtt.client as mqtt
from plugins.base_plugin import (
    BaseGPSPlugin,
    PluginConfigField,
)
from services.logging_service import get_module_logger

# Initialize module logger
logger = get_module_logger(__name__)

# AIS Type to (Description, CoT Suffix) mapping
AIS_TYPE_DATA = {
    0: ("Not available (default)", "-S-X"),
    20: ("Wing in ground (WIG), all ships of this type", "-S-X"),
    21: ("Wing in ground (WIG), Hazardous category A", "-S-X"),
    22: ("Wing in ground (WIG), Hazardous category B", "-S-X"),
    23: ("Wing in ground (WIG), Hazardous category C", "-S-X"),
    24: ("Wing in ground (WIG), Hazardous category D", "-S-X"),
    30: ("Fishing", "-S-X-F"),
    31: ("Towing", "-S-X-M-T-O"),
    32: ("Towing: length exceeds 200m or breadth exceeds 25m", "-S-X-P"),
    33: ("Dredging or underwater ops", "-S-X-M"),
    34: ("Diving ops", "-S-X-M"),
    35: ("Military ops", "-S-C"),
    36: ("Sailing", "-S-X-R"),
    37: ("Pleasure Craft", "-S-X-P"),
    40: ("High speed craft (HSC), all ships of this type", "-S-X-A"),
    41: ("High speed craft (HSC), Hazardous category A", "-S-X-A"),
    42: ("High speed craft (HSC), Hazardous category B", "-S-X-A"),
    43: ("High speed craft (HSC), Hazardous category C", "-S-X-A"),
    44: ("High speed craft (HSC), Hazardous category D", "-S-X-A"),
    49: ("High speed craft (HSC), No additional information", "-S-X-A"),
    50: ("Pilot Vessel", "-S-X"),
    51: ("Search and Rescue vessel", "-S-N"),
    52: ("Tug", "-S-X-M-T-U"),
    53: ("Port Tender", "-S-X-M-T-U"),
    54: ("Anti-pollution equipment", "-S-X"),
    55: ("Law Enforcement", "-S-X-L"),
    58: ("Medical Transport", "-S-N-M"),
    59: ("Noncombatant ship according to RR Resolution No. 18", "-S-N"),
    60: ("Passenger, all ships of this type", "-S-X-M-P"),
    61: ("Passenger, Hazardous category A", "-S-X-M-H"),
    62: ("Passenger, Hazardous category B", "-S-X-M-H"),
    63: ("Passenger, Hazardous category C", "-S-X-M-H"),
    64: ("Passenger, Hazardous category D", "-S-X-M-H"),
    69: ("Passenger, No additional information", "-S-X-M-P"),
    70: ("Cargo, all ships of this type", "-S-X-M-C"),
    71: ("Cargo, Hazardous category A", "-S-X-M-H"),
    72: ("Cargo, Hazardous category B", "-S-X-M-H"),
    73: ("Cargo, Hazardous category C", "-S-X-M-H"),
    74: ("Cargo, Hazardous category D", "-S-X-M-H"),
    75: ("Cargo, Reserved for future use", "-S-X-M-C"),
    76: ("Cargo, Reserved for future use", "-S-X-M-C"),
    77: ("Cargo, Reserved for future use", "-S-X-M-C"),
    78: ("Cargo, Reserved for future use", "-S-X-M-C"),
    79: ("Cargo, No additional information", "-S-X-M-C"),
    80: ("Tanker, all ships of this type", "-S-X-M-O"),
    81: ("Tanker, Hazardous category A", "-S-X-M-H"),
    82: ("Tanker, Hazardous category B", "-S-X-M-H"),
    83: ("Tanker, Hazardous category C", "-S-X-M-H"),
    84: ("Tanker, Hazardous category D", "-S-X-M-H"),
    85: ("Tanker, Reserved for future use", "-S-X-M-O"),
    86: ("Tanker, Reserved for future use", "-S-X-M-O"),
    87: ("Tanker, Reserved for future use", "-S-X-M-O"),
    88: ("Tanker, Reserved for future use", "-S-X-M-O"),
    89: ("Tanker, No additional information", "-S-X-M-O"),
    90: ("Other Type, all ships of this type", "-S"),
    91: ("Other Type, Hazardous category A", "-S"),
    92: ("Other Type, Hazardous category B", "-S"),
    93: ("Other Type, Hazardous category C", "-S"),
    94: ("Other Type, Hazardous category D", "-S"),
    99: ("Other Type, no additional information", "-S"),
}


def load_json_db(file_path: Optional[str]) -> Any:
    """Load JSON database from file path."""
    if file_path and os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                logger.debug(f"Successfully loaded JSON DB from {file_path}")
                return data
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Failed to load JSON DB from {file_path}: {e}")
            return None
    logger.debug(f"JSON DB file not found or path not provided: {file_path}")
    return None


# pylint: disable=too-many-instance-attributes

class DigitrafficAISPlugin(BaseGPSPlugin):  # type: ignore[misc]
    """Digitraffic AIS integration"""

    PLUGIN_NAME = "ais_digitraffic"

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._buffer: Dict[str, Dict[str, Any]] = {}
        self._mqtt_client: Optional[mqtt.Client] = None
        self._mqtt_task: Optional[asyncio.Task[None]] = None
        self._stop_mqtt = False
        self._countries_db: Optional[Dict[str, List[str]]] = None
        self._shadowfleet_db: Optional[List[Dict[str, Any]]] = None
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._last_cache_save = 0.0

        # Load metadata cache if configured
        cache_path = config.get("metadata_cache_path")
        if cache_path:
            self._metadata_cache = load_json_db(cache_path) or {}
            if self._metadata_cache:
                now = datetime.now(timezone.utc).timestamp()
                for vessel in self._metadata_cache.values():
                    if "last_seen" not in vessel:
                        vessel["last_seen"] = now
                logger.info(
                    f"Digitraffic: Loaded {len(self._metadata_cache)} vessels from metadata cache"
                )

        self._allowed_ais_types: Optional[List[int]] = None
        types_str = cast(str, config.get("ais_types", "")).strip()
        if types_str:
            try:
                self._allowed_ais_types = [
                    int(t.strip()) for t in types_str.split(",") if t.strip()
                ]
                logger.info(
                    f"Digitraffic: Filtering for AIS types: {self._allowed_ais_types}"
                )
            except (ValueError, TypeError) as e:
                logger.error(f"Digitraffic: Failed to parse ais_types config: {e}")

    @classmethod
    def get_plugin_name(cls) -> str:
        return cls.PLUGIN_NAME

    @property
    def plugin_name(self) -> str:
        return self.PLUGIN_NAME

    @property
    def plugin_metadata(self) -> Dict[str, Any]:
        return {
            "display_name": "Digitraffic AIS Plugin",
            "description": "Get AIS data from Digitraffic MQTT over WebSockets",
            "icon": "fas fa-ship",
            "category": "custom",
            "min_poll_interval": 5,
            "hide_cot_type": True,
            "config_fields": [
                PluginConfigField(
                    name="mqtt_url",
                    label="MQTT URL",
                    field_type="text",
                    required=True,
                    default_value="wss://meri.digitraffic.fi:443/mqtt",
                    help_text="Digitraffic MQTT WebSocket URL",
                ),
                PluginConfigField(
                    name="username",
                    label="Username",
                    field_type="text",
                    required=False,
                    default_value="digitraffic",
                    help_text="MQTT username",
                ),
                PluginConfigField(
                    name="password",
                    label="Password",
                    field_type="password",
                    required=False,
                    sensitive=True,
                    default_value="digitrafficPassword",
                    help_text="MQTT password",
                ),
                PluginConfigField(
                    name="ais_types",
                    label="AIS Types Filter",
                    field_type="text",
                    required=False,
                    help_text="Comma-separated list of AIS type numbers to show (e.g., 35,55,70). Leave empty for all.",
                ),
                PluginConfigField(
                    name="countries_db_path",
                    label="Path to Countries JSON",
                    field_type="filepath",
                    required=False,
                    help_text="File containing country-based affiliation for MMSIs.",
                ),
                PluginConfigField(
                    name="shadowfleet_db_path",
                    label="Path to shadowfleet.json",
                    field_type="filepath",
                    required=False,
                    help_text="File containing shadowfleet vessel information.",
                ),
                PluginConfigField(
                    name="metadata_cache_path",
                    label="Vessel Metadata Cache Path",
                    field_type="filepath",
                    required=False,
                    help_text="Path to save persistent vessel metadata to disk.",
                ),
            ],
            "help_sections": [
                {
                    "title": "Overview",
                    "content": [
                        "This plugin connects to Digitraffic's MQTT broker via WebSockets.",
                        "It buffers incoming messages and returns them at the specified update interval.",
                    ],
                }
            ],
        }

    def _get_affil(self, mmsi: str) -> List[str]:
        if not self._countries_db:
            return ["u", "unknown"]
        mid = mmsi[:3]
        if mid in self._countries_db:
            return self._countries_db[mid]
        return ["o", "unknown"]

    def _get_cot_type_suffix(self, ais_type: int) -> str:
        if ais_type in AIS_TYPE_DATA:
            return AIS_TYPE_DATA[ais_type][1]
        return "-S-X"

    def _get_shadowfleet_info(self, imo: Optional[str]) -> Optional[Dict[str, Any]]:
        if not self._shadowfleet_db or not imo:
            return None
        imo_str = str(imo)
        for vessel in self._shadowfleet_db:
            if str(vessel.get("imo")) == imo_str:
                return vessel
        return None

    def _process_mqtt_message(self, topic: str, data: Dict[str, Any]) -> None:
        """Process message from Digitraffic MQTT."""
        parts = topic.split("/")
        if len(parts) < 3:
            return
        mmsi = parts[1]
        msg_type = parts[2]

        if mmsi not in self._buffer:
            cached = self._metadata_cache.get(mmsi, {})
            self._buffer[mmsi] = {
                "uid": f"AIS-{mmsi}",
                "mmsi": mmsi,
                "lat": None,
                "lon": None,
                "hae": 0,
                "name": cached.get("name", ""),
                "speed": 0.0,
                "course": 0.0,
                "imo": cached.get("imo"),
                "ais_type": cached.get("ais_type", 0),
                "callsign": cached.get("callsign", ""),
                "destination": "",
                "description": "",
                "timestamp": "",
                "last_seen": datetime.now(timezone.utc),
                "cot_type": "a-u-S-X",
                "last_pos_time": "",
            }

        entry = self._buffer[mmsi]
        entry["last_seen"] = datetime.now(timezone.utc)

        if msg_type == "location":
            entry["lat"] = data.get("lat")
            entry["lon"] = data.get("lon")
            entry["speed"] = data.get("sog", 0) * 0.514444
            entry["course"] = data.get("cog", 0)
            if "time" in data:
                entry["last_pos_time"] = datetime.fromtimestamp(
                    data["time"], tz=timezone.utc
                ).isoformat()
        elif msg_type == "metadata":
            if data.get("name"):
                entry["name"] = cast(str, data["name"]).strip()
            if data.get("callSign"):
                entry["callsign"] = cast(str, data["callSign"]).strip()
            if data.get("imo"):
                entry["imo"] = data["imo"]
            if data.get("type"):
                entry["ais_type"] = data["type"]
            if data.get("destination"):
                entry["destination"] = cast(str, data["destination"]).strip()

            # Update cache
            cached = self._metadata_cache.get(mmsi, {})
            self._metadata_cache[mmsi] = {
                "name": entry["name"] or cached.get("name", ""),
                "imo": entry["imo"] or cached.get("imo"),
                "ais_type": entry["ais_type"] or cached.get("ais_type", 0),
                "callsign": entry["callsign"] or cached.get("callsign", ""),
                "last_seen": datetime.now(timezone.utc).timestamp(),
            }

        # Filtering and CoT generation logic (mirrored from aisstream)
        if self._allowed_ais_types is not None:
            if entry["ais_type"] not in self._allowed_ais_types:
                if mmsi in self._buffer:
                    del self._buffer[mmsi]
                return

        if entry["lat"] is None or entry["lon"] is None:
            return

        shadow_vessel = self._get_shadowfleet_info(entry["imo"])
        affil, country = self._get_affil(mmsi)

        if shadow_vessel and shadow_vessel.get("cot"):
            cot_type = cast(str, shadow_vessel["cot"])
        else:
            suffix = self._get_cot_type_suffix(entry["ais_type"])
            cot_type = f"a-{affil}{suffix}"

        if entry.get("name"):
            display_name = entry["name"]
        elif entry.get("imo"):
            display_name = f"IMO:{entry['imo']}"
        else:
            display_name = f"MMSI:{mmsi}"
        entry["display_name"] = display_name

        remarks_parts = []
        if entry["imo"]:
            remarks_parts.append(f"IMO: {entry['imo']}")
        remarks_parts.append(f"Name (AIS): {entry['name']}")
        if shadow_vessel and shadow_vessel.get("names"):
            remarks_parts.append(f"Names (DB): {', '.join(shadow_vessel['names'])}")
        remarks_parts.append(f"MMSI (AIS): {mmsi}")
        if shadow_vessel and shadow_vessel.get("mmsi"):
            remarks_parts.append(f"MMSI (DB): {', '.join(shadow_vessel['mmsi'])}")
        remarks_parts.append(f"Country (MMSI): {country}")
        if shadow_vessel and shadow_vessel.get("flag"):
            flag = shadow_vessel["flag"]
            remarks_parts.append(
                f"Country (DB): {', '.join(flag) if isinstance(flag, list) else flag}"
            )

        ais_type_info = AIS_TYPE_DATA.get(entry["ais_type"], ("Unknown", ""))
        remarks_parts.append(f"AIS type: {entry['ais_type']} (\"{ais_type_info[0]}\")")

        if shadow_vessel and shadow_vessel.get("type"):
            vtype = shadow_vessel["type"]
            remarks_parts.append(
                f"Type (DB): {', '.join(vtype) if isinstance(vtype, list) else vtype}"
            )
        if shadow_vessel and shadow_vessel.get("operator"):
            remarks_parts.append(f"Operator: {shadow_vessel['operator']}")
        if shadow_vessel and shadow_vessel.get("sanctions_origin"):
            remarks_parts.append(
                f"Sanctions: {', '.join(shadow_vessel['sanctions_origin'])}"
            )
        if entry["destination"]:
            remarks_parts.append(f"Destination: {entry['destination']}")
        if entry["last_pos_time"]:
            remarks_parts.append(f"Last Pos: {entry['last_pos_time']}")

        tags = "#AIS"
        if shadow_vessel:
            tags += " #shadowfleet"
        remarks_parts.append(tags)

        entry["cot_type"] = cot_type
        entry["description"] = "\n".join(remarks_parts)
        entry["timestamp"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    def _save_metadata_cache(self) -> None:
        config = self.get_decrypted_config()
        path = config.get("metadata_cache_path")
        if not path:
            return
        try:
            now = datetime.now(timezone.utc).timestamp()
            expiry = 30 * 24 * 60 * 60
            self._metadata_cache = {
                m: v
                for m, v in self._metadata_cache.items()
                if (now - v.get("last_seen", 0) < expiry)
            }
            temp_path = f"{path}.tmp"
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(self._metadata_cache, f)
            os.replace(temp_path, path)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Digitraffic: Failed to save metadata cache: {e}")

    async def _mqtt_loop(self) -> None:
        """Background task for MQTT."""
        config = self.get_decrypted_config()
        url_str = cast(str, config.get("mqtt_url", "wss://meri.digitraffic.fi:443/mqtt"))
        parsed_url = urlparse(url_str)
        host = parsed_url.hostname
        if not host:
            logger.error("Digitraffic: Invalid MQTT URL")
            return
        port = parsed_url.port or (443 if parsed_url.scheme == "wss" else 1883)
        use_ws = parsed_url.scheme in ("ws", "wss")
        use_ssl = parsed_url.scheme in ("wss", "ssl")

        self._countries_db = load_json_db(config.get("countries_db_path"))
        self._shadowfleet_db = load_json_db(config.get("shadowfleet_db_path"))

        def on_connect(
            _client: mqtt.Client,
            _userdata: Any,
            _flags: Dict[str, Any],
            rc: int,
            _properties: Any = None,
        ) -> None:
            if rc == 0:
                logger.info("Digitraffic: Connected to MQTT broker")
                _client.subscribe("vessels-v2/#")
            else:
                logger.error(
                    f"Digitraffic: MQTT connection failed with result code {rc}"
                )

        def on_message(_client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
            try:
                data = json.loads(msg.payload.decode())
                self._process_mqtt_message(msg.topic, data)
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error(f"Digitraffic: Error processing MQTT message: {e}")
        self._mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,  # type: ignore[attr-defined]
            transport="websockets" if use_ws else "tcp",
        )
        if config.get("username"):
            self._mqtt_client.username_pw_set(
                config["username"], config.get("password")
            )
        if use_ssl:
            self._mqtt_client.tls_set()

        self._mqtt_client.on_connect = on_connect
        self._mqtt_client.on_message = on_message

        try:
            logger.info(f"Digitraffic: Connecting to {host}:{port}...")
            self._mqtt_client.connect(host, port, 60)
            self._mqtt_client.loop_start()

            while not self._stop_mqtt:
                await asyncio.sleep(60)
                self._save_metadata_cache()

            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Digitraffic: MQTT loop error: {e}")

    async def fetch_locations(
        self, session: aiohttp.ClientSession
    ) -> List[Dict[str, Any]]:
        # session is unused but required by BasePlugin interface
        _ = session
        if self._mqtt_task is None or self._mqtt_task.done():
            self._stop_mqtt = False
            self._mqtt_task = asyncio.create_task(self._mqtt_loop())

        now = datetime.now(timezone.utc)
        stale_limit = 600
        self._buffer = {
            m: e
            for m, e in self._buffer.items()
            if (now - e["last_seen"]).total_seconds() < stale_limit
        }

        locations = []
        for v in self._buffer.values():
            if v.get("lat") is not None and v.get("lon") is not None:
                locations.append(
                    {
                        "uid": v["uid"],
                        "cot_type": v["cot_type"],
                        "lat": v["lat"],
                        "lon": v["lon"],
                        "hae": v["hae"],
                        "name": v.get("display_name", v.get("name", v["uid"])),
                        "speed": v["speed"],
                        "course": v["course"],
                        "description": v["description"],
                        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    }
                )
        return locations

    def validate_config(self) -> bool:
        config = self.get_decrypted_config()
        return bool(config.get("mqtt_url"))

    async def test_connection(self) -> Dict[str, Any]:
        # Simple check if we can parse the URL
        config = self.get_decrypted_config()
        if not config.get("mqtt_url"):
            return {"success": False, "message": "MQTT URL not configured"}
        return {
            "success": True,
            "message": "Configuration valid. Connection is handled in background.",
        }

    def __del__(self) -> None:
        self._save_metadata_cache()
        self._stop_mqtt = True
        if self._mqtt_task and not self._mqtt_task.done():
            self._mqtt_task.cancel()
