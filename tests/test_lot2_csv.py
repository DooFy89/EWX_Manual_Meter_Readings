"""
Tests de non-régression du lot 2 (parsing CSV / XLSX) :
colonnes XLSX alignées sur le CSV, unités, guillemets CSV, colonnes de statut, profil déclaré.

Lancer depuis la racine du projet : python -m pytest tests
"""
import io
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import Manual_Meter_Readings as m  # noqa: E402
from test_audit_lot1 import export_values  # noqa: E402
from test_timezone import export_timestamps  # noqa: E402

LP1_LINE = "1-0:99.1.0*255(0100630100FF) Profil de charge 1"
LP2_LINE = "1-0:99.2.0*255(0100630200FF) Profil de charge 2"
HEADER = "Cl.8 0-0:1.0.0*255 Attr.2;Cl.1 0-0:96.10.2*255 Attr.2;Cl.3 1-0:1.8.0*255 Attr.2 (kWh)"


def build_csv(profile_line: str, rows, header: str = HEADER) -> bytes:
    lines = ["12365275", profile_line, header] + [";".join(row) for row in rows]
    return "\r\n".join(lines).encode("utf-8")


def daily_rows(count: int = 5):
    start = datetime(2025, 11, 25)
    return [((start + timedelta(days=i)).strftime("%d/%m/%Y %H:%M:%S"), "0", str(i + 1)) for i in range(count)]


def quarter_rows(count: int = 5):
    start = datetime(2025, 11, 25)
    return [((start + timedelta(minutes=15 * i)).strftime("%d/%m/%Y %H:%M:%S"), "0", str(i + 1)) for i in range(count)]


def build_xlsx(headers, rows, profile: str = "") -> bytes:
    import openpyxl

    wb = openpyxl.Workbook()
    info = wb.active
    info.title = "Informations"
    info.append(["Numéro de série", "12365275"])
    if profile:
        info.append(["Profil", profile])
    sheet = wb.create_sheet("Données")
    sheet.append(headers)
    for row in rows:
        sheet.append(row)
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def readings(data: m.ParsedMeterData, obis: str = "1-0:1.8.0"):
    return data.channels[obis]["readings"]


# ============================================================================
# F : même détection de colonnes en CSV et XLSX, unités élargies
# ============================================================================

@pytest.mark.parametrize("ts_header, status_header", [
    ("Timestamp", "Status"),
    ("Date/Time", "DST"),
    ("Horloge 0-0:1.0.0", "État"),
])
def test_xlsx_column_detection_like_csv(ts_header, status_header):
    content = build_xlsx(
        [ts_header, status_header, "1-0:1.8.0 (kWh)"],
        [[datetime(2025, 12, 1, 0, 0), 8, 1], [datetime(2025, 12, 1, 0, 15), 8, 2]],
        profile="Profil de charge 1",
    )
    data = m.parse_xlsx(content, "12365275.xlsx")[0]

    assert len(readings(data)) == 2
    # Statut 8 (bit DST) lu dans la colonne, pas déduit du calendrier (hiver)
    assert export_timestamps(data)[0].endswith("+02:00")
    assert not any("calendrier" in w for w in data.warnings)


def test_xlsx_and_csv_share_column_map():
    headers = ["Timestamp", "DST", "1-0:1.8.0 (MWh)", "1-0:2.8.0 [kvarh]"]
    content = build_xlsx(headers, [[datetime(2025, 12, 1), 0, 1, 2]], profile="Profil de charge 2")
    data = m.parse_xlsx(content, "12365275.xlsx")[0]

    col_map, _ = m._build_column_map(headers)
    assert set(data.channels) == {c["obis"] for c in col_map.values() if c["type"] == "data"}
    assert data.channels["1-0:1.8.0"]["unit"] == "MWh"


@pytest.mark.parametrize("header, expected", [
    ("1-0:1.8.0 (kWh)", "kWh"),
    ("1-0:1.8.0 [kWh]", "kWh"),
    ("1-0:1.8.0 (KWH)", "KWh"),
    ("1-0:1.8.0 (Wh)", "Wh"),
    ("1-0:1.8.0 [MWh]", "MWh"),
    ("1-0:1.8.0 (mWh)", "mWh"),
    ("1-0:3.8.0 (kvarh)", "kvarh"),
    ("1-0:9.8.0 (kVAh)", "kVAh"),
    ("1-0:1.6.0 (kW)", "kW"),
    ("1-0:3.6.0 (kvar)", "kvar"),
    ("1-0:9.6.0 (kVA)", "kVA"),
    ("1-0:1.7.0 (W)", "W"),
    ("Horloge 0-0:1.0.0", ""),
    ("1-0:1.8.0 (min)", ""),
])
def test_extract_unit(header, expected):
    assert m.extract_unit(header) == expected


def test_csv_mwh_column_scaled_on_export():
    header = "Cl.8 0-0:1.0.0*255 Attr.2;Cl.1 0-0:96.10.2*255 Attr.2;Cl.3 1-0:1.8.0*255 Attr.2 (MWh)"
    data = m.parse_csv(build_csv(LP2_LINE, daily_rows(2), header), "12365275.csv")[0]

    assert export_values(data) == ["1000000", "2000000"]  # MWh → Wh


# ============================================================================
# O : guillemets CSV
# ============================================================================

def test_csv_quoted_timestamps():
    rows = [(f'"{ts}"', f'"{status}"', f'"{value}"') for ts, status, value in quarter_rows(4)]
    data = m.parse_csv(build_csv(LP1_LINE, rows), "12365275.csv")[0]

    assert [r[1] for r in readings(data)] == [1, 2, 3, 4]


def test_csv_quoted_cell_with_delimiter_does_not_shift_columns():
    header = 'Cl.8 0-0:1.0.0*255 Attr.2;Commentaire;Cl.3 1-0:1.8.0*255 Attr.2 (kWh)'
    rows = [(ts, '"a;b"', value) for ts, _, value in quarter_rows(4)]
    data = m.parse_csv(build_csv(LP1_LINE, rows, header), "12365275.csv")[0]

    assert [r[1] for r in readings(data)] == [1, 2, 3, 4]


def test_csv_quoted_timestamp_with_spaces_around_delimiter():
    rows = [(f' "{ts}" ', f" {status} ", f" {value} ") for ts, status, value in quarter_rows(4)]
    data = m.parse_csv(build_csv(LP1_LINE, rows), "12365275.csv")[0]

    assert len(readings(data)) == 4


@pytest.mark.parametrize("line", [
    "a;b;c",
    " a ; b ; ",
    "25/11/2025 00:00:00;0;1,5",
    ";;",
    "",
    "abc\"d;e",
])
def test_split_csv_line_unquoted_same_as_split(line):
    expected = [c.strip() for c in line.split(";")]
    assert [c.strip() for c in m.split_csv_line(line, ";")] == expected


# ============================================================================
# Q : colonnes d'entiers et colonnes de statut (détection par contenu)
# ============================================================================

def infer(headers, rows):
    lines = [";".join(row) for row in rows]
    col_map, channels = m._infer_columns_from_data(headers, lines, ";")
    return {i: c["type"] for i, c in col_map.items()}


def test_integer_index_column_stays_data():
    rows = [(ts, "0", str(123456 + i)) for i, (ts, _, _) in enumerate(quarter_rows(6))]
    rows = [(ts, "8" if i % 2 else "0", index) for i, (ts, _, index) in enumerate(rows)]
    types = infer(["Date", "A", "B"], rows)

    assert types == {0: "timestamp", 1: "dst", 2: "data"}


def test_integer_columns_without_status_stay_data():
    rows = [(ts, str(100 + i), str(5000 + 7 * i)) for i, (ts, _, _) in enumerate(quarter_rows(6))]
    types = infer(["Date", "A", "B"], rows)

    assert types == {0: "timestamp", 1: "data", 2: "data"}


def test_status_column_from_dst_suffix_or_header():
    rows = [(ts, "8 (DST)", "12", "0") for ts, _, _ in quarter_rows(4)]

    assert infer(["Date", "A", "B", "C"], rows) == {0: "timestamp", 1: "dst", 2: "data", 3: "data"}
    assert infer(["Date", "A", "B", "Status"], rows)[3] == "dst"


# ============================================================================
# N : profil déclaré non reclassé par la détection
# ============================================================================

def test_csv_declared_lp1_kept_despite_daily_gaps():
    data = m.parse_csv(build_csv(LP1_LINE, daily_rows(5)), "12365275.csv")[0]

    assert data.interval == "15min"
    assert any("15min conservé" in w for w in data.warnings)


def test_csv_declared_lp2_kept_despite_quarter_data():
    data = m.parse_csv(build_csv(LP2_LINE, quarter_rows(5)), "12365275.csv")[0]

    assert data.interval == "24h"


def test_csv_undeclared_profile_detected_from_data():
    data = m.parse_csv(build_csv("Relevé", daily_rows(5)), "12365275.csv")[0]

    assert data.interval == "24h"
    assert data.load_profile == "Profil de charge 2"


@pytest.mark.parametrize("profile, rows, expected", [
    ("Profil de charge 1", "daily", "15min"),
    ("1-0:99.2.0", "quarter", "24h"),
    ("", "daily", "24h"),
    ("", "quarter", "15min"),
])
def test_xlsx_declared_profile_kept(profile, rows, expected):
    base = daily_rows(5) if rows == "daily" else quarter_rows(5)
    content = build_xlsx(
        ["Horloge 0-0:1.0.0", "1-0:1.8.0 (kWh)"],
        [[datetime.strptime(ts, "%d/%m/%Y %H:%M:%S"), int(value)] for ts, _, value in base],
        profile=profile,
    )
    data = m.parse_xlsx(content, "12365275.xlsx")[0]

    assert data.interval == expected


def test_manual_config_interval_kept():
    lines = build_csv(LP1_LINE, daily_rows(5)).decode("utf-8").split("\n")
    config = {"meter_id": "12365275", "header_line": 2, "data_start": 3, "interval": "15min"}
    data = m.parse_csv_manual_config(lines, ";", "f.csv", config).data

    assert data.interval == "15min"
    assert any("15min conservé" in w for w in data.warnings)


def test_manual_config_without_interval_uses_declared_profile():
    lines = build_csv(LP2_LINE, quarter_rows(5)).decode("utf-8").split("\n")
    config = {"meter_id": "12365275", "header_line": 2, "data_start": 3}
    data = m.parse_csv_manual_config(lines, ";", "f.csv", config).data

    assert data.interval == "24h"
