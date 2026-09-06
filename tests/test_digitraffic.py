import json
from plugin.digitraffic import DigitrafficAISPlugin, load_json_db, AIS_TYPE_DATA


def test_load_json_db(tmp_path):
    # Test valid JSON
    d = tmp_path / "test.json"
    data = {"a": 1}
    d.write_text(json.dumps(data))
    assert load_json_db(str(d)) == data

    # Test invalid path
    assert load_json_db("nonexistent.json") is None


def test_ais_type_data():
    assert AIS_TYPE_DATA[30][1] == "-S-X-F"
    assert AIS_TYPE_DATA[35][1] == "-S-C"
    assert AIS_TYPE_DATA[51][1] == "-S-N"


def test_class_tag_from_class_map(tmp_path):
    class_map = tmp_path / "aisstream_cache.json"
    class_map.write_text(
        json.dumps({"230002000": {"name": "SAILBOAT", "class": "B"}})
    )
    plugin = DigitrafficAISPlugin({"class_map_path": str(class_map)})
    plugin._refresh_class_map()
    plugin._process_mqtt_message(
        "vessels-v2/230002000/location",
        {"lat": 60.2, "lon": 25.0, "sog": 5.0, "cog": 90.0},
    )
    assert "#CLASSB" in plugin._buffer["230002000"]["description"]


def test_no_class_tag_without_class_map():
    plugin = DigitrafficAISPlugin({})
    plugin._process_mqtt_message(
        "vessels-v2/230001000/location",
        {"lat": 60.1, "lon": 24.9, "sog": 10.0, "cog": 45.0},
    )
    assert "#CLASS" not in plugin._buffer["230001000"]["description"]
