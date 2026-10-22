"""
Tests lot 2 (parsers XML) : colonnes du buffer identifiées par OBIS, fichiers à deux profils.

Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import Manual_Meter_Readings as m  # noqa: E402
from test_timezone import dlms_ts, export_timestamps  # noqa: E402

CLOCK = "0000010000FF"
STATUS_LP1 = "0000600A01FF"
A_PLUS = "0100010800FF"     # 1-0:1.8.0
A_MINUS = "0100020800FF"    # 1-0:2.8.0
A_PLUS_T1 = "0100010801FF"  # 1-0:1.8.1

SUMMER = datetime(2026, 8, 31, 0, 0)
WINTER = datetime(2025, 12, 10, 0, 0)


def build_xml(profiles, status_objects=True, name_format="Load{n}") -> bytes:
    """
    profiles : {numéro: (liste des OBIS hex capturés, lignes)} ; une ligne = valeurs par colonne
    (None = colonne absente). Structure MAP110 : objets DD.Profile_LoadN avec attributs
    capture_objects et buffer (lisible aussi par le parser flexible).
    """
    parts = ["<Root><DDID>LGZ1030700000001</DDID>"]
    for num, (captures, rows) in profiles.items():
        name = "DD.Profile_" + name_format.format(n=num)
        if status_objects:
            parts.append(f'<Objects ObjectName="DD.ProfileStatus_Load0{num}" ClassID="1" />')
        parts.append(f'<Objects ObjectName="{name}" ObjectLogicalName="0100630{num}00FF" ClassID="7">')
        parts.append(f'<Attributes AttributeName="{name}.capture_objects">')
        for i, obis in enumerate(captures):
            parts.append(f'<Fields FieldName="{name}.capture_objects.0.{i}.logical_name" FieldValue="{obis}" />')
        parts.append("</Attributes>")
        parts.append(f'<Attributes AttributeName="{name}.buffer">')
        for r, row in enumerate(rows):
            for c, value in enumerate(row):
                if value is None:
                    continue
                field_type = "OctetString" if c == 0 else "UInt32"
                parts.append(
                    f'<Fields FieldName="{name}.buffer.Selector1.Response.{r}.{c}" '
                    f'FieldType="{field_type}" FieldValue="{value}" />'
                )
        parts.append("</Attributes></Objects>")
    parts.append("</Root>")
    return "".join(parts).encode("utf-8")


def values(data, obis):
    return [value for _, value, _ in data.channels[obis]["readings"]]


# ============================================================================
# M : colonne 1 du buffer lue si elle porte une mesure
# ============================================================================

def test_energy_in_column_1_with_status_elsewhere():
    xml = build_xml({1: ([CLOCK, A_PLUS, STATUS_LP1, A_MINUS], [
        [dlms_ts(WINTER), "100", "0", "10"],
        [dlms_ts(SUMMER), "200", "8", "20"],
    ])})
    [data] = m.parse_xml(xml, "lp1_col1.xml")

    assert set(data.channels) == {"1-0:1.8.0", "1-0:2.8.0"}
    assert values(data, "1-0:1.8.0") == [100, 200]
    assert values(data, "1-0:2.8.0") == [10, 20]
    # Statut lu en colonne 2 : bit 0x08 -> heure d'été
    assert export_timestamps(data) == [
        "2025-12-10T00:00:00.0000000+01:00",
        "2026-08-31T00:00:00.0000000+02:00",
    ] * 2


def test_energy_in_column_1_without_status_column():
    xml = build_xml({1: ([CLOCK, A_PLUS], [
        [dlms_ts(WINTER, clock_status="00"), "100"],
        [dlms_ts(WINTER.replace(minute=15), clock_status="00"), "101"],
    ])})
    [data] = m.parse_xml(xml, "lp1_sans_statut.xml")

    assert values(data, "1-0:1.8.0") == [100, 101]


def test_status_in_column_1_is_not_a_channel():
    xml = build_xml({1: ([CLOCK, STATUS_LP1, A_PLUS], [
        [dlms_ts(SUMMER), "8", "100"],
    ])})
    [data] = m.parse_xml(xml, "lp1_standard.xml")

    assert set(data.channels) == {"1-0:1.8.0"}
    assert export_timestamps(data) == ["2026-08-31T00:00:00.0000000+02:00"]


def test_flexible_parser_reads_energy_in_column_1():
    xml = build_xml({1: ([CLOCK, A_PLUS, STATUS_LP1, A_MINUS], [
        [dlms_ts(WINTER), "100", "0", "10"],
        [dlms_ts(WINTER.replace(minute=15)), "101", "0", "11"],
    ])})
    [data] = m.parse_xml_flexible(xml, "lp1_col1.xml", "ProfileBuffer")

    assert set(data.channels) == {"1-0:1.8.0", "1-0:2.8.0"}
    assert values(data, "1-0:1.8.0") == [100, 101]


# ============================================================================
# L : un fichier contenant LP1 et LP2 donne deux profils
# ============================================================================

def two_profiles_xml(**kwargs) -> bytes:
    return build_xml({
        1: ([CLOCK, STATUS_LP1, A_PLUS], [
            [dlms_ts(WINTER), "0", "100"],
            [dlms_ts(WINTER.replace(minute=15)), "0", "101"],
        ]),
        # LP2 sans colonne de statut ni clock status : repli calendrier (avertissement propre à LP2)
        2: ([CLOCK, A_PLUS_T1], [
            [dlms_ts(WINTER, clock_status="FF"), "5000"],
            [dlms_ts(SUMMER, clock_status="FF"), "6000"],
        ]),
    }, **kwargs)


def test_two_profiles_give_two_results():
    results = m.parse_xml(two_profiles_xml(), "LP1_LP2.xml")

    assert [(d.load_profile, d.interval) for d in results] == [
        ("Profil de charge 1", "15min"),
        ("Profil de charge 2", "24h"),
    ]
    lp1, lp2 = results
    assert set(lp1.channels) == {"1-0:1.8.0"}
    assert set(lp2.channels) == {"1-0:1.8.1"}
    assert values(lp1, "1-0:1.8.0") == [100, 101]
    assert values(lp2, "1-0:1.8.1") == [5000, 6000]
    assert lp1.warnings == []
    assert any("calendrier Europe/Zurich" in w for w in lp2.warnings)
    assert export_timestamps(lp2) == [
        "2025-12-10T00:00:00.0000000+01:00",
        "2026-08-31T00:00:00.0000000+02:00",
    ]


def test_two_profiles_detected_from_buffers_only():
    results = m.parse_xml_profile_buffer(two_profiles_xml(status_objects=False), "LP1_LP2.xml")

    assert [d.load_profile for d in results] == ["Profil de charge 1", "Profil de charge 2"]


def test_profile_buffer_accepts_load0n_field_names():
    xml = build_xml({2: ([CLOCK, A_PLUS_T1], [
        [dlms_ts(WINTER, clock_status="00"), "5000"],
    ])}, name_format="Load0{n}")
    [data] = m.parse_xml_profile_buffer(xml, "LP2_Load02.xml")

    assert data.load_profile == "Profil de charge 2"
    assert values(data, "1-0:1.8.1") == [5000]
