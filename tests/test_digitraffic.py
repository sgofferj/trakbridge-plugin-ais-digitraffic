import json
from plugin.digitraffic import load_json_db, AIS_TYPE_DATA

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
