"""
Tests de l'interface (lot 2b) : bornes au changement d'heure, libellés uniques,
mémorisation des paramètres lors d'une vraie conversion, fichiers tous à configurer.

Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from datetime import datetime
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import Manual_Meter_Readings as m  # noqa: E402
from test_timezone import build_lp2_csv  # noqa: E402

DST = m.STATUS_DST_BIT

CSV_OK = build_lp2_csv([("25/11/2025 00:00:00", "0", "1"), ("26/11/2025 00:00:00", "0", "2")])
# Compteur introuvable : configuration manuelle demandée
CSV_INCONNU = b"releve sans entete\nHorloge;Valeur;Autre\n01/01/2026 00:00;1.5;2.5\n01/01/2026 00:15;3.5;4.5\n"


def data_with(readings, source_file="a.csv", utc=False, meter_id="11223344"):
    return m.ParsedMeterData(meter_id, "Profil de charge 1", "15min",
                             {"1-0:1.8.0": {"unit": "kWh", "readings": readings}},
                             source_file, timestamps_utc=utc)


# ============================================================================
# 1 : bornes du récapitulatif comparées en instants réels
# ============================================================================

def test_summary_bounds_across_dst_change():
    # 02:45+02:00 (00:45Z) précède 02:15+01:00 (01:15Z) malgré l'heure murale
    readings = [
        (datetime(2025, 10, 26, 2, 45), 1.0, DST),
        (datetime(2025, 10, 26, 2, 15), 2.0, 0),
    ]
    row = m.create_summary_dataframe([data_with(readings)], "P").iloc[0]
    assert row["Date min"] == "2025-10-26 02:45+02:00"
    assert row["Date max"] == "2025-10-26 02:15+01:00"


def test_summary_bounds_utc_ignore_dst_bit():
    # En UTC forcé, l'offset est +00:00 : seule l'heure murale compte
    readings = [
        (datetime(2025, 10, 26, 2, 45), 1.0, DST),
        (datetime(2025, 10, 26, 2, 15), 2.0, 0),
    ]
    row = m.create_summary_dataframe([data_with(readings, utc=True)], "P").iloc[0]
    assert row["Date min"] == "2025-10-26 02:15+00:00"
    assert row["Date max"] == "2025-10-26 02:45+00:00"


# ============================================================================
# 2 : libellés des jeux de données toujours distincts
# ============================================================================

def test_labels_same_day_different_hours():
    a = data_with([(datetime(2025, 1, 1, 0, 15), 1.0, 0)], source_file="x.csv")
    b = data_with([(datetime(2025, 1, 1, 12, 15), 1.0, 0)], source_file="x.csv")
    labels = m.dataset_labels([a, b], "P")
    assert labels == [
        "P11223344 (LP1) - x.csv, 01.01.2025 00:15 - 01.01.2025 00:15",
        "P11223344 (LP1) - x.csv, 01.01.2025 12:15 - 01.01.2025 12:15",
    ]


def test_labels_identical_get_suffix():
    readings = [(datetime(2025, 1, 1, 0, 15), 1.0, 0)]
    data = [data_with(readings, source_file="x.csv") for _ in range(3)]
    labels = m.dataset_labels(data, "P")
    base = "P11223344 (LP1) - x.csv, 01.01.2025 00:15 - 01.01.2025 00:15"
    assert labels == [base, f"{base} (#2)", f"{base} (#3)"]


def test_labels_without_duplicate_unchanged():
    a = data_with([(datetime(2025, 1, 1, 0, 15), 1.0, 0)])
    b = data_with([(datetime(2025, 1, 1, 0, 15), 1.0, 0)], meter_id="99")
    assert m.dataset_labels([a, b], "P") == ["P11223344 (LP1)", "P99 (LP1)"]


# ============================================================================
# 3 : conversion réelle (file_uploader remplacé par des fichiers en mémoire)
# ============================================================================

def app_with_files(files):
    """App complète dont le file_uploader renvoie les fichiers donnés {nom: octets}."""
    def script(files):
        import io
        import streamlit as st
        import Manual_Meter_Readings as app

        def fake_uploader(*args, **kwargs):
            out = []
            for name, content in files.items():
                f = io.BytesIO(content)
                f.name = name
                out.append(f)
            return out

        original = st.file_uploader
        st.file_uploader = fake_uploader
        try:
            app.main()
        finally:
            st.file_uploader = original

    at = AppTest.from_function(script, args=(files,), default_timeout=60)
    at.run()
    return at


def convert_button(at):
    return next(b for b in at.button if b.label == "Convertir")


def test_conversion_memorizes_params():
    at = app_with_files({"12365275-LP2.csv": CSV_OK})
    assert not at.exception
    assert at.session_state["conversion_params"] is None

    at.sidebar.text_input[0].set_value("PREF")
    convert_button(at).click()
    at.run()

    assert not at.exception
    assert at.session_state["conversion_done"] is True
    assert at.session_state["conversion_params"] == {"source": m.SOURCES[0], "mrid_prefix": "PREF"}
    assert [o["mrid"] for o in at.session_state["json_outputs"]] == ["PREF12365275"]

    # Préfixe modifié après conversion : les JSON gardent les paramètres mémorisés
    at.sidebar.text_input[0].set_value("AUTRE")
    at.run()
    assert at.session_state["conversion_params"]["mrid_prefix"] == "PREF"
    assert any("Les paramètres ont changé" in i.value for i in at.info)


def test_all_files_need_manual_config():
    at = app_with_files({"inconnu.csv": CSV_INCONNU})
    convert_button(at).click()
    at.run()

    assert not at.exception
    assert at.session_state["processed_data"] == []
    assert at.session_state["json_outputs"] == []
    assert list(at.session_state["files_needing_config"]) == [m.file_config_key("inconnu.csv", CSV_INCONNU)]
    assert any(h.value == "Configuration manuelle requise" for h in at.header)
    assert any("nécessitent une configuration manuelle" in i.value for i in at.info)
    assert not at.error
