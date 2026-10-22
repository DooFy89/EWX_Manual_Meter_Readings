"""
Tests de non-régression du lot 2b (parsing CSV / XLSX) :
intervalle deviné non verrouillé, priorité OBIS sur les mots-clés d'en-tête,
statut déduit signalé, CSV à virgule avec décimales entre guillemets.

Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import Manual_Meter_Readings as m  # noqa: E402
from test_audit_lot1 import export_values  # noqa: E402
from test_lot2_csv import HEADER, build_csv, build_xlsx, daily_rows, quarter_rows  # noqa: E402
from test_timezone import build_lp2_csv  # noqa: E402


def xlsx_data(profile: str, base):
    content = build_xlsx(
        ["Horloge 0-0:1.0.0", "1-0:1.8.0 (kWh)"],
        [[datetime.strptime(ts, "%d/%m/%Y %H:%M:%S"), int(value)] for ts, _, value in base],
        profile=profile,
    )
    return m.parse_xlsx(content, "12365275.xlsx")[0]


# ============================================================================
# 1 : intervalle deviné depuis le nom du profil non verrouillé
# ============================================================================

@pytest.mark.parametrize("line, expected", [
    ("Load Profile 1", ("Profil de charge 1", "15min")),
    ("Load profile 2 - export 2021", ("Profil de charge 2", "24h")),
    ("Profil de charge 2", ("Profil de charge 2", "24h")),
    ("Load Profile 2021", ("", "15min")),
    ("Profil de charge 2021", ("", "15min")),
])
def test_extract_load_profile_isolated_number(line, expected):
    _, name, interval = m.extract_load_profile(line)

    assert (name, interval) == expected


def test_xlsx_guessed_profile_corrected_by_data():
    data = xlsx_data("Courbe export 2021", daily_rows(5))

    assert data.interval == "24h"
    assert not any("conservé" in w for w in data.warnings)


def test_xlsx_english_profile_recognized():
    data = xlsx_data("Load Profile 2 — export 2021", daily_rows(5))

    assert data.interval == "24h"
    assert not any("conservé" in w for w in data.warnings)


def test_xlsx_recognized_profile_still_locked():
    data = xlsx_data("Load Profile 1", daily_rows(5))

    assert data.interval == "15min"
    assert any("15min conservé" in w for w in data.warnings)


def test_xlsx_guessed_profile_used_as_fallback():
    # Deux relevés au quart d'heure : trop peu pour décider, l'indice « 2 » sert de repli
    data = xlsx_data("Relevé 2", quarter_rows(2))

    assert data.interval == "24h"
    assert not any("conservé" in w for w in data.warnings)


def test_csv_profile_number_not_isolated_not_locked():
    data = m.parse_csv(build_csv("Profil de charge 2021", quarter_rows(5)), "12365275.csv")[0]

    assert data.interval == "15min"


# ============================================================================
# 2 : un OBIS de mesure prime sur les mots-clés de statut
# ============================================================================

def test_measure_obis_with_status_keyword_stays_data():
    headers = ["Horloge", "État", "1-0:1.8.0 (kWh) - DST corrigé", "1-0:2.8.0 (kWh) status"]
    col_map, channels = m._build_column_map(headers)

    assert [col_map[i]["type"] for i in range(4)] == ["timestamp", "dst", "data", "data"]
    assert set(channels) == {"1-0:1.8.0", "1-0:2.8.0"}
    assert channels["1-0:1.8.0"]["unit"] == "kWh"


@pytest.mark.parametrize("header, expected", [
    ("Cl.8 0-0:1.0.0*255 Attr.2", "timestamp"),
    ("Cl.1 0-0:96.10.2*255 Attr.2", "dst"),
    ("Cl.1 0-0:96.10.1*255 Attr.2", "dst"),
    ("Cl.3 1-0:1.8.1*255 Attr.2 (kWh)", "data"),
    ("Horloge", "timestamp"),
    ("Date time", "timestamp"),
    ("État", "dst"),
    ("Status", "dst"),
    ("0-0:96.1.0 status", "dst"),
    ("0-0:96.1.0", "data"),
])
def test_real_headers_classified_as_before(header, expected):
    col_map, _ = m._build_column_map([header])

    assert col_map[0]["type"] == expected


def test_csv_and_xlsx_keep_channel_with_dst_keyword():
    header = "Cl.8 0-0:1.0.0*255 Attr.2;Cl.1 0-0:96.10.2*255 Attr.2;1-0:1.8.0 (kWh) - DST corrigé"
    csv_data = m.parse_csv(build_csv("Profil de charge 2", daily_rows(2), header), "12365275.csv")[0]
    xlsx = build_xlsx(
        header.split(";"),
        [[datetime(2025, 11, 25), 0, 1], [datetime(2025, 11, 26), 0, 2]],
        profile="Profil de charge 2",
    )
    xlsx_data_ = m.parse_xlsx(xlsx, "12365275.xlsx")[0]

    for data in (csv_data, xlsx_data_):
        assert len(data.channels["1-0:1.8.0"]["readings"]) == 2


# ============================================================================
# 3 : statut déduit des données signalé
# ============================================================================

def infer_warnings(headers, rows):
    warnings = []
    col_map, _ = m._infer_columns_from_data(headers, [";".join(r) for r in rows], ";", warnings)
    return {i: c["type"] for i, c in col_map.items()}, warnings


def test_guessed_status_column_warned():
    rows = [(ts, "8", str(100 + i)) for i, (ts, _, _) in enumerate(quarter_rows(6))]
    types, warnings = infer_warnings(["Date", "Mesure A", "B"], rows)

    assert types[1] == "dst"
    assert warnings == ["Colonne « Mesure A » interprétée comme statut DST (déduction : petits entiers)"]


@pytest.mark.parametrize("headers, status", [
    (["Date", "Status", "B"], "0"),
    (["Date", "A", "B"], "8 (DST)"),
])
def test_status_from_header_or_suffix_not_warned(headers, status):
    rows = [(ts, status, str(100 + i)) for i, (ts, _, _) in enumerate(quarter_rows(6))]
    types, warnings = infer_warnings(headers, rows)

    assert types[1] == "dst"
    assert warnings == []


def test_manual_config_reports_guessed_status():
    rows = [(ts, "8", str(100 + i)) for i, (ts, _, _) in enumerate(quarter_rows(6))]
    lines = ["Date;Mesure A;B"] + [";".join(r) for r in rows]
    config = {"meter_id": "12365275", "header_line": 0, "data_start": 1}
    data = m.parse_csv_manual_config(lines, ";", "f.csv", config).data

    assert any("« Mesure A » interprétée comme statut DST" in w for w in data.warnings)


# ============================================================================
# 4 : CSV à virgule, décimales entre guillemets et espaces
# ============================================================================

def test_csv_comma_delimiter_quoted_decimal():
    header = HEADER.replace(";", ",")
    lines = ["12365275", "Profil de charge 2", header, '25/11/2025 00:00:00, 0, "1,5"']
    data = m.parse_csv("\r\n".join(lines).encode("utf-8"), "12365275.csv", delimiter=",")[0]

    assert [r[1] for r in data.channels["1-0:1.8.0"]["readings"]] == [1.5]
    assert export_values(data) == ["1500"]  # kWh → Wh


@pytest.mark.parametrize("line, expected", [
    ("Load Profile 2 - export 2021", "24h"),
    ("Load Profile 2 (journalier)", "24h"),
    ("Profil de charge 1", "15min"),
    ("Load Profile 2A", None),
    ("Load Profile 2.1", None),
    ("Load Profile 2021", None),
])
def test_extract_load_profile_requires_isolated_number(line, expected):
    _, name, interval = m.extract_load_profile(line)
    if expected is None:
        assert name == ""
    else:
        assert interval == expected


def test_unclosed_quotes_warning():
    csv = build_lp2_csv([
        ("25/11/2025 00:00:00", "0", "1"),
        ("26/11/2025 00:00:00", "0", "2"),
    ]).replace(b"26/11/2025 00:00:00 ; 0 ; 2 ; ", b'26/11/2025 00:00:00 ; 0 ; "2\nsuite" ; ')
    data = m.parse_csv(csv, "multiligne.csv")[0]

    assert any(w.startswith("Guillemets non refermés ligne(s) 5, 6 :") for w in data.warnings)


def test_balanced_quotes_no_warning():
    csv = build_lp2_csv([
        ('"25/11/2025 00:00:00"', "0", '"1,5"'),
        ("26/11/2025 00:00:00", "0", '"le ""bon"" index"'),
    ])
    data = m.parse_csv(csv, "guillemets.csv")[0]

    assert not any("Guillemets non refermés" in w for w in data.warnings)
