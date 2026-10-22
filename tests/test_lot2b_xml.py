"""
Tests lot 2b (parser XML) : repli flexible par profil quand un seul des profils détectés échoue.

Lancer depuis la racine du projet : python -m pytest tests
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import Manual_Meter_Readings as m  # noqa: E402
from test_timezone import dlms_ts  # noqa: E402
from test_lot2_xml import (  # noqa: E402
    A_PLUS, A_PLUS_T1, CLOCK, STATUS_LP1, WINTER, two_profiles_xml, values,
)


def build_xml(profiles) -> bytes:
    """
    profiles : {numéro: (liste des OBIS hex, lignes, groupe des capture_objects)}.
    Groupe 0 = chemin standard ; groupe 1 = chemin lisible seulement par le parser flexible.
    """
    parts = ["<Root><DDID>LGZ1030700000001</DDID>"]
    for num, (captures, rows, group) in profiles.items():
        name = f"DD.Profile_Load{num}"
        parts.append(f'<Objects ObjectName="DD.ProfileStatus_Load0{num}" ClassID="1" />')
        parts.append(f'<Objects ObjectName="{name}" ObjectLogicalName="0100630{num}00FF" ClassID="7">')
        parts.append(f'<Attributes AttributeName="{name}.capture_objects">')
        for i, obis in enumerate(captures):
            parts.append(
                f'<Fields FieldName="{name}.capture_objects.{group}.{i}.logical_name" FieldValue="{obis}" />'
            )
        parts.append("</Attributes>")
        parts.append(f'<Attributes AttributeName="{name}.buffer">')
        for r, row in enumerate(rows):
            for c, value in enumerate(row):
                field_type = "OctetString" if c == 0 else "UInt32"
                parts.append(
                    f'<Fields FieldName="{name}.buffer.Selector1.Response.{r}.{c}" '
                    f'FieldType="{field_type}" FieldValue="{value}" />'
                )
        parts.append("</Attributes></Objects>")
    parts.append("</Root>")
    return "".join(parts).encode("utf-8")


LP1_ROWS = [
    [dlms_ts(WINTER), "0", "100"],
    [dlms_ts(WINTER.replace(minute=15)), "0", "101"],
]
LP2_ROWS = [
    [dlms_ts(WINTER, clock_status="00"), "5000"],
    [dlms_ts(WINTER.replace(day=11), clock_status="00"), "5100"],
]


def test_mixed_file_recovers_lp1_with_flexible_parser():
    # LP1 lisible seulement par le parser flexible, LP2 au format standard
    xml = build_xml({
        1: ([CLOCK, STATUS_LP1, A_PLUS], LP1_ROWS, 1),
        2: ([CLOCK, A_PLUS_T1], LP2_ROWS, 0),
    })
    results = m.parse_xml(xml, "mixte.xml")

    assert [d.load_profile for d in results] == ["Profil de charge 1", "Profil de charge 2"]
    lp1, lp2 = results
    assert values(lp1, "1-0:1.8.0") == [100, 101]
    assert values(lp2, "1-0:1.8.1") == [5000, 5100]
    # LP2 reste celui du parser standard (non dupliqué, non remplacé)
    [standard_lp2] = m.parse_xml_profile_buffer(xml, "mixte.xml")
    assert (lp2.channels, lp2.warnings) == (standard_lp2.channels, standard_lp2.warnings)
    assert not any("aucune donnée exploitable" in w for d in results for w in d.warnings)


def test_detected_profile_without_data_gives_warning():
    # LP1 détecté (objet de statut et buffer) mais sans capture_objects : illisible partout
    xml = build_xml({
        1: ([], LP1_ROWS, 0),
        2: ([CLOCK, A_PLUS_T1], LP2_ROWS, 0),
    })
    results = m.parse_xml(xml, "lp1_vide.xml")

    assert [d.load_profile for d in results] == ["Profil de charge 2"]
    [warning] = [w for w in results[0].warnings if "aucune donnée exploitable" in w]
    assert warning.startswith("Profil de charge 1 : ")
    assert "capture_objects non trouvés" in warning


def test_standard_file_does_not_call_flexible_parser(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("parser flexible appelé inutilement")

    monkeypatch.setattr(m, "parse_xml_flexible", fail)
    xml = two_profiles_xml()
    results = m.parse_xml(xml, "LP1_LP2.xml")

    expected = m.parse_xml_profile_buffer(xml, "LP1_LP2.xml")
    assert [d.load_profile for d in results] == ["Profil de charge 1", "Profil de charge 2"]
    assert [(d.channels, d.warnings) for d in results] == [(d.channels, d.warnings) for d in expected]


def test_all_standard_failures_still_report_missing_profile():
    # LP1 illisible partout, LP2 lisible seulement par le flexible : le repli global
    # récupère LP2 et doit signaler LP1
    xml = build_xml({
        1: ([], LP1_ROWS, 0),
        2: ([CLOCK, A_PLUS_T1], LP2_ROWS, 1),
    })
    results = m.parse_xml(xml, "lp1_vide_lp2_flexible.xml")

    assert [d.load_profile for d in results] == ["Profil de charge 2"]
    assert values(results[0], "1-0:1.8.1") == [5000, 5100]
    [warning] = [w for w in results[0].warnings if "aucune donnée exploitable" in w]
    assert warning.startswith("Profil de charge 1 : ")
