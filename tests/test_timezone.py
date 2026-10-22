"""
Tests de non-régression : offset UTC (+01:00 / +02:00) des horodatages exportés.

Les fichiers sont construits en mémoire (le dossier Data/ n'est pas versionné).
Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import Manual_Meter_Readings as m  # noqa: E402


def dlms_ts(dt: datetime, deviation: str = "8000", clock_status: str = "00") -> str:
    """Construit un horodatage DLMS (OctetString 12 octets hex)."""
    return (
        f"{dt.year:04X}{dt.month:02X}{dt.day:02X}FF"
        f"{dt.hour:02X}{dt.minute:02X}{dt.second:02X}00"
        f"{deviation}{clock_status}"
    )


def export_timestamps(data: m.ParsedMeterData):
    document, _ = m.convert_to_json(data, "Landis", "LGZ")
    return [
        block["IntervalReadings"][0]["timeStamp"]
        for reading in document["payload"]["MeterReadings"]
        for block in reading["IntervalBlocks"]
    ]


# ============================================================================
# decode_dlms_timestamp : ordre de priorité des sources DST
# ============================================================================

SUMMER = datetime(2026, 8, 31, 0, 0)
WINTER = datetime(2025, 12, 10, 0, 0)


@pytest.mark.parametrize("hex_ts, expected_dt, dst, source", [
    # 1. Déviation explicite (E360)
    (dlms_ts(SUMMER, "FF88", "80"), SUMMER, True, "deviation"),
    (dlms_ts(WINTER, "FFC4", "00"), WINTER, False, "deviation"),
    # Déviation 0 = UTC, converti en heure locale
    (dlms_ts(datetime(2026, 8, 30, 22, 0), "0000", "00"), SUMMER, True, "deviation"),
    (dlms_ts(datetime(2025, 12, 9, 23, 0), "0000", "00"), WINTER, False, "deviation"),
    # 3. Clock status (E450) : bit 0x80
    (dlms_ts(SUMMER, "8000", "80"), SUMMER, True, "clock_status"),
    (dlms_ts(SUMMER, "8000", "00"), SUMMER, False, "clock_status"),
    # 4. Rien de renseigné : calendrier Europe/Zurich
    (dlms_ts(SUMMER, "8000", "FF"), SUMMER, True, "calendar"),
    (dlms_ts(WINTER, "8000", "FF"), WINTER, False, "calendar"),
])
def test_decode_dlms_timestamp_sources(hex_ts, expected_dt, dst, source):
    dt, status, status_source = m.decode_dlms_timestamp(hex_ts)
    assert dt == expected_dt
    assert m.status_has_dst(status) == dst
    assert status_source == source


def test_decode_column_status_used_when_deviation_unspecified():
    # 2. Registre 0-0:96.10.x : prioritaire sur le clock status
    dt, status, source = m.decode_dlms_timestamp(dlms_ts(SUMMER, "8000", "00"), column_status=0x08)
    assert m.status_has_dst(status)
    assert source == "column"


def test_decode_deviation_wins_over_column_status():
    _, status, source = m.decode_dlms_timestamp(dlms_ts(SUMMER, "FF88", "80"), column_status=0x00)
    assert m.status_has_dst(status)
    assert source == "deviation"


def test_decode_force_utc():
    dt, _, source = m.decode_dlms_timestamp(dlms_ts(SUMMER, "8000", "FF"), force_utc=True)
    assert dt == SUMMER
    assert source == "utc"


def test_decode_invalid_timestamp():
    assert m.decode_dlms_timestamp("") == (None, 0, "")
    assert m.decode_dlms_timestamp("07E8FFFF") == (None, 0, "")


def test_calendar_dst_transitions():
    # Passage à l'heure d'été le 29.03.2026, retour à l'heure d'hiver le 25.10.2026
    assert not m.calendar_dst_status(datetime(2026, 3, 29, 0, 0))
    assert m.calendar_dst_status(datetime(2026, 3, 30, 0, 0))
    assert m.calendar_dst_status(datetime(2026, 10, 25, 0, 0))
    assert not m.calendar_dst_status(datetime(2026, 10, 26, 0, 0))
    # Heure répétée d'octobre : première occurrence (heure d'été)
    assert m.calendar_dst_status(datetime(2026, 10, 25, 2, 30))


def test_parse_dst_value():
    assert m.parse_dst_value("136", WINTER) == (136, False)
    assert m.parse_dst_value("0", SUMMER) == (0, False)
    assert m.parse_dst_value("", SUMMER) == (m.STATUS_DST_BIT, True)
    assert m.parse_dst_value(None, WINTER) == (0, True)


@pytest.mark.parametrize("text, expected", [
    ("0-0:96.10.1", True),
    ("0-0:96.10.2", True),
    ("Cl.1 0-0:96.10.2*255 Attr.2", True),
    ("0-0:96.10.20", False),
    ("0-0:96.10.3", False),
    ("1-0:1.8.1", False),
])
def test_status_register_pattern(text, expected):
    assert bool(m.STATUS_REGISTER_PATTERN.search(text)) == expected


# ============================================================================
# XML MAP110 (ProfileBuffer)
# ============================================================================

def build_profile_xml(profile: int, rows, with_status_column: bool = True) -> bytes:
    """
    rows : liste de (horodatage_hex, statut, valeur).
    Sans colonne de statut, la colonne 1 capture un registre non reconnu (0-0:96.10.3).
    """
    status_obis = f"0000600A0{profile}FF" if with_status_column else "0000600A03FF"
    value_obis = "0100010800FF" if profile == 1 else "0100010801FF"
    captures = ["0000010000FF", status_obis, value_obis]
    value_col = 2

    fields = [
        f'<Field FieldName="DD.Profile_Load{profile}.capture_objects.0.{i}.logical_name" '
        f'FieldValue="{obis}" />'
        for i, obis in enumerate(captures)
    ]
    prefix = f"DD.Profile_Load{profile}.buffer.Selector1.Response"
    for row, (ts, status, value) in enumerate(rows):
        fields.append(f'<Field FieldName="{prefix}.{row}.0" FieldType="OctetString" FieldValue="{ts}" />')
        if with_status_column:
            fields.append(f'<Field FieldName="{prefix}.{row}.1" FieldType="UInt8" FieldValue="{status}" />')
        fields.append(f'<Field FieldName="{prefix}.{row}.{value_col}" FieldType="UInt32" FieldValue="{value}" />')

    return (
        "<Root><DDID>LGZ1030789224208</DDID>"
        f'<Object ObjectName="ProfileStatus_Load0{profile}" />'
        + "".join(fields)
        + "</Root>"
    ).encode("utf-8")


def test_xml_lp2_e570_status_register_96_10_2():
    """Cas du ticket : index 24h E570, déviation non spécifiée, statut 0-0:96.10.2."""
    xml = build_profile_xml(2, [
        (dlms_ts(WINTER), "0", "100"),
        (dlms_ts(datetime(2026, 8, 1)), "8", "200"),
        (dlms_ts(SUMMER), "40", "300"),
    ])
    data = m.parse_xml(xml, "LGZ1030789224208-ReadLoadProfile2.xml")[0]

    assert data.interval == "24h"
    assert not data.timestamps_utc
    assert export_timestamps(data) == [
        "2025-12-10T00:00:00.0000000+01:00",
        "2026-08-01T00:00:00.0000000+02:00",
        "2026-08-31T00:00:00.0000000+02:00",
    ]


def test_xml_without_status_column_uses_calendar_with_warning():
    xml = build_profile_xml(2, [
        (dlms_ts(WINTER, clock_status="FF"), None, "100"),
        (dlms_ts(SUMMER, clock_status="FF"), None, "200"),
    ], with_status_column=False)
    data = m.parse_xml(xml, "sans_statut.xml")[0]

    assert export_timestamps(data) == [
        "2025-12-10T00:00:00.0000000+01:00",
        "2026-08-31T00:00:00.0000000+02:00",
    ]
    assert any("calendrier Europe/Zurich" in w for w in data.warnings)


def test_xml_empty_status_cells_fall_back_per_row():
    """Colonne de statut partiellement vide : chaque ligne vide retombe sur le clock status."""
    xml = build_profile_xml(1, [
        (dlms_ts(datetime(2026, 8, 31, 0, 0)), "8", "100"),
        (dlms_ts(datetime(2026, 8, 31, 0, 15), clock_status="80"), "", "200"),
    ])
    data = m.parse_xml(xml, "statut_partiel.xml")[0]

    assert export_timestamps(data) == [
        "2026-08-31T00:00:00.0000000+02:00",
        "2026-08-31T00:15:00.0000000+02:00",
    ]


def test_xml_explicit_utc_deviation_exports_local_time():
    xml = build_profile_xml(2, [
        (dlms_ts(datetime(2026, 8, 30, 22, 0), deviation="0000"), "0", "100"),
    ], with_status_column=False)
    data = m.parse_xml(xml, "utc.xml")[0]

    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+02:00"]


# ============================================================================
# CSV
# ============================================================================

def build_lp2_csv(rows, with_status_column: bool = True) -> bytes:
    header = "Cl.8 0-0:1.0.0*255 Attr.2 ; "
    if with_status_column:
        header += "Cl.1 0-0:96.10.2*255 Attr.2 ; "
    header += "Cl.3 1-0:1.8.1*255 Attr.2 (kWh) ; "

    lines = ["12365275", "1-0:99.2.0*255(0100630200FF) Profil de charge 2", header]
    for ts, status, value in rows:
        cells = [ts] + ([status] if with_status_column else []) + [value]
        lines.append(" ; ".join(cells) + " ; ")
    return "\n".join(lines).encode("utf-8")


def test_csv_lp2_status_register_96_10_2():
    csv = build_lp2_csv([
        ("25/11/2025 00:00:00", "0", "1"),
        ("26/07/2026 00:00:00", "8", "2"),
        ("27/07/2026 00:00:00", "8", "3"),
    ])
    data = m.parse_csv(csv, "12365275-LP2.csv")[0]

    assert "0-0:96.10.2" not in data.channels
    assert export_timestamps(data) == [
        "2025-11-25T00:00:00.0000000+01:00",
        "2026-07-26T00:00:00.0000000+02:00",
        "2026-07-27T00:00:00.0000000+02:00",
    ]


def test_csv_without_status_column_uses_calendar_with_warning():
    csv = build_lp2_csv([
        ("25/11/2025 00:00:00", None, "1"),
        ("26/07/2026 00:00:00", None, "2"),
        ("27/07/2026 00:00:00", None, "3"),
    ], with_status_column=False)
    data = m.parse_csv(csv, "12365275-LP2.csv")[0]

    assert export_timestamps(data) == [
        "2025-11-25T00:00:00.0000000+01:00",
        "2026-07-26T00:00:00.0000000+02:00",
        "2026-07-27T00:00:00.0000000+02:00",
    ]
    assert any("calendrier Europe/Zurich" in w for w in data.warnings)


# ============================================================================
# Cas limites XML
# ============================================================================

def test_xml_utc_field_type_exports_utc():
    xml = build_profile_xml(2, [(dlms_ts(SUMMER), "8", "100")]).replace(
        b'FieldType="OctetString"', b'FieldType="DateTimeUtc"'
    )
    data = m.parse_xml(xml, "utc_type.xml")[0]

    assert data.timestamps_utc
    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+00:00"]


def test_xml_deviation_wins_over_column_with_warning():
    xml = build_profile_xml(2, [(dlms_ts(SUMMER, deviation="FF88", clock_status="80"), "0", "100")])
    data = m.parse_xml(xml, "desaccord.xml")[0]

    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+02:00"]
    assert any("DST mismatch" in w and "0-0:96.10.2" in w for w in data.warnings)


def test_xml_unsupported_deviation_warns():
    xml = build_profile_xml(2, [(dlms_ts(SUMMER, deviation="FF4C"), "8", "100")])
    data = m.parse_xml(xml, "deviation_180.xml")[0]

    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+02:00"]
    assert any("Déviation DLMS non prise en charge (-180 min)" in w for w in data.warnings)


def test_xml_invalid_status_falls_back_to_clock_status():
    xml = build_profile_xml(2, [(dlms_ts(SUMMER, clock_status="80"), "n/a", "100")])
    data = m.parse_xml(xml, "statut_invalide.xml")[0]

    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+02:00"]


def test_xml_clock_status_disagreement_warns_except_repeated_hour():
    october = datetime(2026, 10, 25, 2, 30)  # heure répétée : 00 = 2e occurrence (hiver)
    xml = build_profile_xml(1, [(dlms_ts(october, clock_status="00"), "", "100")], with_status_column=False)
    data = m.parse_xml(xml, "octobre.xml")[0]
    assert export_timestamps(data) == ["2026-10-25T02:30:00.0000000+01:00"]
    assert not any("clock status" in w for w in data.warnings)

    xml = build_profile_xml(1, [(dlms_ts(SUMMER, clock_status="00"), "", "100")], with_status_column=False)
    data = m.parse_xml(xml, "ete_00.xml")[0]
    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+01:00"]
    assert any("clock status" in w for w in data.warnings)


@pytest.mark.parametrize("utc_dt, expected", [
    (datetime(2026, 3, 29, 0, 59), "2026-03-29T01:59:00.0000000+01:00"),
    (datetime(2026, 3, 29, 1, 0), "2026-03-29T03:00:00.0000000+02:00"),
    (datetime(2026, 10, 25, 0, 30), "2026-10-25T02:30:00.0000000+02:00"),
    (datetime(2026, 10, 25, 1, 30), "2026-10-25T02:30:00.0000000+01:00"),
])
def test_xml_utc_deviation_around_transitions(utc_dt, expected):
    xml = build_profile_xml(1, [(dlms_ts(utc_dt, deviation="0000"), "", "100")], with_status_column=False)
    data = m.parse_xml(xml, "utc_transition.xml")[0]

    assert export_timestamps(data) == [expected]


# ============================================================================
# XLSX
# ============================================================================

def build_lp2_xlsx(rows) -> bytes:
    import io
    import openpyxl

    wb = openpyxl.Workbook()
    info = wb.active
    info.title = "Informations"
    info.append(["Numéro de série", "12365275"])
    info.append(["Profil de charge", "Profil de charge 2"])
    data = wb.create_sheet("Données")
    data.append(["Horloge 0-0:1.0.0", "0-0:96.10.2", "1-0:1.8.1 (kWh)"])
    for row in rows:
        data.append(list(row))
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def test_xlsx_lp2_datetime_cells_and_status():
    xlsx = build_lp2_xlsx([
        (datetime(2025, 11, 25), 0, 1),
        (datetime(2026, 7, 26), 8, 2),
        (datetime(2026, 7, 27), None, 3),
    ])
    data = m.parse_xlsx(xlsx, "12365275-LP2.xlsx")[0]

    assert "0-0:96.10.2" not in data.channels
    assert export_timestamps(data) == [
        "2025-11-25T00:00:00.0000000+01:00",
        "2026-07-26T00:00:00.0000000+02:00",
        "2026-07-27T00:00:00.0000000+02:00",
    ]
    assert any("calendrier Europe/Zurich" in w and "1 point" in w for w in data.warnings)
