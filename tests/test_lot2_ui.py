"""
Tests de l'interface (lot 2) : récapitulatif, visualisation, paramètres mémorisés.

Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import Manual_Meter_Readings as m  # noqa: E402

DST = m.STATUS_DST_BIT


def meter(source_file, start, values, status=0, meter_id="11223344", from_xml=False, utc=False):
    readings = [(start + timedelta(minutes=15 * i), v, status) for i, v in enumerate(values)]
    return m.ParsedMeterData(
        meter_id=meter_id,
        load_profile="Profil de charge 1",
        interval="15min",
        channels={"1-0:1.8.0": {"unit": "kWh", "readings": readings}},
        source_file=source_file,
        from_xml=from_xml,
        timestamps_utc=utc,
    )


def new_app(processed_data, prefix="ESR10307", source="Amera"):
    at = AppTest.from_file(str(ROOT / "Manual_Meter_Readings.py"), default_timeout=60)
    at.run()
    at.session_state["processed_data"] = processed_data
    at.session_state["json_outputs"] = m.build_json_outputs(processed_data, source, prefix)[0]
    at.session_state["conversion_done"] = True
    at.session_state["conversion_params"] = {"source": source, "mrid_prefix": prefix}
    at.run()
    return at


def summary(at):
    return next(d.value for d in at.dataframe if "N° Compteur" in d.value.columns)


def chart_values(at):
    return list(next(d.value for d in at.dataframe if "Timestamp" in d.value.columns)["Valeur"])


def meter_select(at):
    return next(s for s in at.selectbox if s.label == "Compteur")


# ============================================================================
# E : offset de chaque borne du récapitulatif
# ============================================================================

def test_summary_offset_per_bound():
    readings = [
        (datetime(2025, 10, 28, 15, 15), 2.0, 0),
        (datetime(2025, 4, 1, 0, 0), 1.0, DST),
    ]
    data = m.ParsedMeterData("11223344", "Profil de charge 1", "15min",
                             {"1-0:1.8.0": {"unit": "kWh", "readings": readings}}, "a.csv")
    row = m.create_summary_dataframe([data], "ESR10307").iloc[0]
    assert row["Date min"] == "2025-04-01 00:00+02:00"
    assert row["Date max"] == "2025-10-28 15:15+01:00"
    assert row["N° Compteur"] == "ESR1030711223344"


def test_summary_offset_ambiguous_hour():
    # 02:30 apparaît deux fois au passage à l'heure d'hiver : l'été est la borne min, l'hiver la borne max
    ts = datetime(2025, 10, 26, 2, 30)
    data = m.ParsedMeterData("11223344", "Profil de charge 1", "15min",
                             {"1-0:1.8.0": {"unit": "kWh", "readings": [(ts, 1.0, 0), (ts, 2.0, DST)]}}, "a.csv")
    row = m.create_summary_dataframe([data], "P").iloc[0]
    assert row["Date min"] == "2025-10-26 02:30+02:00"
    assert row["Date max"] == "2025-10-26 02:30+01:00"


def test_summary_utc_and_xml():
    data = meter("a.xml", datetime(2025, 7, 1), [1.0, 2.0], status=DST, meter_id="LGZ1", from_xml=True, utc=True)
    row = m.create_summary_dataframe([data], "ESR10307").iloc[0]
    assert row["N° Compteur"] == "LGZ1"
    assert row["Date min"] == "2025-07-01 00:00+00:00"
    assert row["Date max"] == "2025-07-01 00:15+00:00"


# ============================================================================
# H : deux jeux du même compteur et du même profil
# ============================================================================

def test_visualisation_distinguishes_duplicates():
    jan = meter("janvier.csv", datetime(2026, 1, 1), [1.0, 2.0])
    feb = meter("fevrier.csv", datetime(2026, 2, 1), [7.0, 8.0])
    at = new_app([jan, feb])
    assert not at.exception

    labels = meter_select(at).options
    assert len(set(labels)) == 2
    assert "janvier.csv" in labels[0] and "fevrier.csv" in labels[1]
    assert chart_values(at) == [1.0, 2.0]

    meter_select(at).set_value(1).run()
    assert not at.exception
    assert chart_values(at) == [7.0, 8.0]


def test_visualisation_label_without_duplicate():
    at = new_app([meter("janvier.csv", datetime(2026, 1, 1), [1.0])])
    assert not at.exception
    assert meter_select(at).options == ["ESR1030711223344 (LP1)"]


# ============================================================================
# R : paramètres mémorisés à la conversion
# ============================================================================

def test_changed_prefix_keeps_conversion_params():
    at = new_app([meter("janvier.csv", datetime(2026, 1, 1), [1.0, 2.0])])
    assert not any("Convertir" in i.value for i in at.info)

    at.sidebar.text_input[0].set_value("NOUVEAU").run()
    assert not at.exception
    assert list(summary(at)["N° Compteur"]) == ["ESR1030711223344"]
    assert meter_select(at).options == ["ESR1030711223344 (LP1)"]
    assert any("Convertir" in i.value for i in at.info)


def test_manual_config_uses_conversion_params():
    csv = b"releve\nHorloge 0-0:1.0.0;1-0:1.8.0 (kWh)\n01/01/2026 00:00;1,5\n01/01/2026 00:15;2,5\n"
    key = m.file_config_key("inconnu.csv", csv)
    at = new_app([meter("janvier.csv", datetime(2026, 1, 1), [1.0])])
    at.session_state["files_needing_config"] = {
        key: {"name": "inconnu.csv", "lines": csv.decode().split("\n"), "data": None, "bytes": csv}
    }
    at.sidebar.text_input[0].set_value("NOUVEAU")
    at.sidebar.selectbox[0].set_value("Landis")
    at.run()

    at.text_input(key=f"meter_id_{key}").set_value("55667788")
    at.number_input(key=f"header_line_{key}").set_value(2)
    at.number_input(key=f"data_start_{key}").set_value(3)
    at.button(key=f"apply_{key}").click()
    at.run()

    assert not at.exception
    assert [o["mrid"] for o in at.session_state["json_outputs"]] == ["ESR1030711223344", "ESR1030755667788"]
    assert '"source": "Amera"' in at.session_state["json_outputs"][1]["content"]
    assert list(summary(at)["N° Compteur"]) == ["ESR1030711223344", "ESR1030755667788"]


def test_clear_resets_conversion_params():
    at = new_app([meter("janvier.csv", datetime(2026, 1, 1), [1.0])])
    next(b for b in at.button if b.label == "Effacer").click().run()
    assert not at.exception
    assert at.session_state["conversion_params"] is None
    assert at.session_state["processed_data"] == []
