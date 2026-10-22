"""
Test de l'interface Streamlit : validation d'une configuration manuelle (AppTest).

Lancer depuis la racine du projet : python -m pytest tests
"""
import json
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import Manual_Meter_Readings as m  # noqa: E402

CSV = b"releve\nHorloge 0-0:1.0.0;1-0:1.8.0 (kWh)\n01/01/2026 00:00;1,5\n01/01/2026 00:15;2,5\n"


def test_manual_config_apply_exports_json():
    key = m.file_config_key("inconnu.csv", CSV)
    at = AppTest.from_file(str(ROOT / "Manual_Meter_Readings.py"), default_timeout=60)
    at.run()
    at.session_state["files_needing_config"] = {
        key: {"name": "inconnu.csv", "lines": CSV.decode().split("\n"), "data": None, "bytes": CSV}
    }
    at.run()

    at.text_input(key=f"meter_id_{key}").set_value("11223344")
    at.number_input(key=f"header_line_{key}").set_value(2)
    at.number_input(key=f"data_start_{key}").set_value(3)
    at.button(key=f"apply_{key}").click()
    at.run()

    assert not at.exception
    assert at.session_state["files_needing_config"] == {}
    assert at.session_state["user_configs"][key]["meter_id"] == "11223344"
    outputs = at.session_state["json_outputs"]
    assert [o["mrid"] for o in outputs] == ["ESR1030711223344"]
    blocks = json.loads(outputs[0]["content"])["payload"]["MeterReadings"][0]["IntervalBlocks"]
    assert [b["IntervalReadings"][0]["value"] for b in blocks] == ["1500", "2500"]


def test_manual_config_short_file_does_not_crash():
    key = m.file_config_key("court.csv", b"x\ny")
    at = AppTest.from_file(str(ROOT / "Manual_Meter_Readings.py"), default_timeout=60)
    at.run()
    at.session_state["files_needing_config"] = {
        key: {"name": "court.csv", "lines": ["x", "y"], "data": None, "bytes": b"x\ny"}
    }
    at.run()

    assert not at.exception
