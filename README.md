# Digitraffic AIS Plugin for TrakBridge

## Description
This plugin integrates AIS data from the Finnish Transport Infrastructure Agency's (Digitraffic) MQTT service into TrakBridge. It provides real-time vessel tracking with CoT (Cursor-on-Target) output for TAK.

## Configuration

| Field | Description | Default |
|-------|-------------|---------|
| MQTT URL | Digitraffic MQTT WebSocket URL | `wss://meri.digitraffic.fi:443/mqtt` |
| Username | MQTT username | `digitraffic` |
| Password | MQTT password | `digitrafficPassword` |
| AIS Types Filter | Comma-separated list of AIS types to show | (empty: all) |
| Path to Countries JSON | File for MMSI-to-Country mapping | - |
| Path to shadowfleet.json | File for shadowfleet metadata | - |
| Vessel Metadata Cache Path | Path to save persistent metadata | - |

## Features
- Connects via MQTT over WebSockets.
- Buffers and correlates location and metadata messages.
- Supports AIS type filtering.
- Affiliation mapping via MMSI (requires `aiscountries.json`).
- Shadowfleet database integration for enhanced metadata and custom CoT types.
- Persistent metadata caching to handle vessels between restarts.

## Container use
Example environment variables for TrakBridge:
```
PLUGIN_AIS_DIGITRAFFIC_ENABLED=true
PLUGIN_AIS_DIGITRAFFIC_COUNTRIES_DB_PATH=/app/data/aiscountries.json
```

## Copyright and License
Copyright Stefan Gofferje
Licensed under the Gnu General Public License Version 3 or higher.
