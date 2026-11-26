"""Tests unitaires : logique pure, sans Kafka ni PostgreSQL.  ->  pytest -q"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from energypulse import quality
from energypulse.events import demand_event, generation_event, hour_iso, weather_event
from energypulse.http import SourceRejected, SourceUnavailable, get_json
from energypulse.sources import eia_api, eia_files, open_meteo
from energypulse.validation import validate

NOW = hour_iso(datetime.now(timezone.utc) - timedelta(hours=3))


# ---------- événements ----------
def test_event_id_is_deterministic():
    a = generation_event("CISO", NOW, "SUN", 100, "eia_api")
    b = generation_event("CISO", NOW, "SUN", 250, "eia_file")
    c = generation_event("CISO", NOW, "WND", 100, "eia_api")
    assert a["event_id"] == b["event_id"]      # même mesure -> même identifiant
    assert a["event_id"] != c["event_id"]


def test_hour_iso_normalises_to_utc_hour():
    dt = datetime(2026, 6, 1, 14, 37, 12, tzinfo=timezone(timedelta(hours=-5)))
    assert hour_iso(dt) == "2026-06-01T19:00:00+00:00"


# ---------- validation ----------
def test_valid_events_pass():
    assert validate(generation_event("ERCO", NOW, "WND", 21000, "t")) == []
    assert validate(demand_event("PJM", NOW, "t", demand_mw=90000, interchange_mw=-3000)) == []
    assert validate(weather_event("NYIS", NOW, "t", temperature_c=-5, cloud_cover_pct=80)) == []


def test_small_negative_solar_tolerated_but_absurd_value_rejected():
    assert validate(generation_event("CISO", NOW, "SUN", -35, "t")) == []
    assert any("hors bornes" in e for e in validate(generation_event("CISO", NOW, "SUN", -99999, "t")))


def test_battery_can_be_negative():
    assert validate(generation_event("CISO", NOW, "BAT", -5000, "t")) == []


@pytest.mark.parametrize("event, expected", [
    (generation_event("XXXX", NOW, "SUN", 1, "t"), "zone inconnue"),
    (generation_event("CISO", NOW, "ZZZ", 1, "t"), "filière inconnue"),
    (generation_event("CISO", NOW, "NG", "12", "t"), "non numérique"),
    (generation_event("CISO", "2099-01-01T00:00:00+00:00", "NG", 1, "t"), "futur"),
    (generation_event("CISO", "hier", "NG", 1, "t"), "illisible"),
    (demand_event("CISO", NOW, "t"), "aucune valeur"),
    (weather_event("CISO", NOW, "t", temperature_c=85), "hors bornes"),
    ({"dataset": "generation"}, "manquant"),
])
def test_invalid_events_are_rejected(event, expected):
    assert any(expected in e for e in validate(event))


def test_weather_forecast_can_be_in_future_but_not_electricity():
    future = hour_iso(datetime.now(timezone.utc) + timedelta(days=1))
    assert validate(weather_event("CISO", future, "t", temperature_c=20)) == []
    assert validate(generation_event("CISO", future, "SUN", 10, "t"))


# ---------- parsing des sources ----------
def test_eia_api_period_hour_ending_converted_to_hour_start():
    rows = [{"period": "2026-06-01T01", "respondent": "CISO", "fueltype": "SUN", "value": "1234"}]
    event = eia_api.parse_fuel_type_rows(rows)[0]
    assert event["period_utc"] == "2026-06-01T00:00:00+00:00"
    assert event["mw"] == 1234 and event["fuel"] == "SUN"


def test_eia_api_region_rows_grouped_per_hour():
    rows = [{"period": "2026-06-01T05", "respondent": "PJM", "type": t, "value": v}
            for t, v in (("D", "90000"), ("DF", "91000"), ("NG", "88000"), ("TI", "-2000"))]
    events = eia_api.parse_region_rows(rows)
    assert len(events) == 1
    assert events[0]["demand_mw"] == 90000 and events[0]["interchange_mw"] == -2000


def test_eia_api_pagination():
    pages = [{"response": {"total": "3", "data": [{"a": 1}, {"a": 2}]}},
             {"response": {"total": "3", "data": [{"a": 3}]}}]
    with patch.object(eia_api, "get_json", side_effect=pages) as mocked:
        rows = eia_api.fetch("fuel-type-data", "k", datetime(2026, 6, 1), datetime(2026, 6, 2))
    assert len(rows) == 3 and mocked.call_count == 2


def test_eia_file_row_to_events():
    row = {"Balancing Authority": "ERCO", "UTC Time at End of Hour": "06/01/2026 1:00:00 AM",
           "Demand (MW)": "45,000", "Demand Forecast (MW)": "44000", "Net Generation (MW)": "46000",
           "Total Interchange (MW)": "1000",
           "Net Generation (MW) from Wind without Integrated Battery Storage": "20000",
           "Net Generation (MW) from Solar without Integrated Battery Storage": "", }
    period = eia_files._period(row)
    assert period == "2026-06-01T00:00:00+00:00"
    events = eia_files.row_to_events(period, row)
    gen = [e for e in events if e["dataset"] == "generation"]
    assert [(e["fuel"], e["mw"]) for e in gen] == [("WND", 20000)]   # colonne vide ignorée
    dem = [e for e in events if e["dataset"] == "demand"][0]
    assert dem["demand_mw"] == 45000                                  # « 45,000 » compris


def test_eia_file_names_for_period():
    names = eia_files.files_for_period(datetime(2026, 6, 29), datetime(2026, 7, 2))
    assert names == ["EIA930_BALANCE_2026_Jan_Jun.csv", "EIA930_BALANCE_2026_Jul_Dec.csv"]


def test_open_meteo_columns_to_rows():
    payload = {"hourly": {"time": ["2026-06-01T00:00", "2026-06-01T01:00"],
                          "temperature_2m": [20.5, None], "wind_speed_100m": [12.0, None],
                          "shortwave_radiation": [0, None], "cloud_cover": [10, None]}}
    events = open_meteo.parse(payload, "CISO", "test")
    assert len(events) == 1                       # heure entièrement vide ignorée
    assert events[0]["temperature_c"] == 20.5


# ---------- HTTP : relances ----------
def _response(status, json_data=None):
    r = MagicMock(status_code=status, text="err")
    r.json.return_value = json_data
    return r


def test_get_json_retries_then_succeeds():
    session = MagicMock()
    session.get.side_effect = [_response(504), requests.Timeout("lent"), _response(200, {"ok": 1})]
    with patch("energypulse.http.time.sleep"):
        assert get_json("http://x", {}, session=session) == {"ok": 1}
    assert session.get.call_count == 3


def test_get_json_gives_up():
    session = MagicMock()
    session.get.return_value = _response(503)
    with patch("energypulse.http.time.sleep"), pytest.raises(SourceUnavailable):
        get_json("http://x", {}, attempts=3, session=session)


def test_get_json_does_not_retry_on_bad_key():
    session = MagicMock()
    session.get.return_value = _response(403)
    with pytest.raises(SourceRejected):
        get_json("http://x", {}, session=session)
    assert session.get.call_count == 1


# ---------- qualité ----------
def test_grade():
    assert quality.grade(2, 4, 8) == "ok"
    assert quality.grade(5, 4, 8) == "warning"
    assert quality.grade(9, 4, 8) == "breach"
    assert quality.grade(0.90, 0.98, 0.95, higher_is_worse=False) == "breach"


def test_drop_anomaly_needs_both_zscore_and_percentage():
    assert quality.is_drop_anomaly(5000, 10000, 1000)[0]          # -50 %, z=-5
    assert not quality.is_drop_anomaly(9000, 10000, 100)[0]       # z=-10 mais seulement -10 %
    assert not quality.is_drop_anomaly(5000, 10000, 4000)[0]      # -50 % mais z=-1.25 (zone très variable)


def test_solar_anomaly_is_weather_aware():
    ratio = 20.0  # 20 MW par W/m²
    assert not quality.is_solar_anomaly(0, 50, ratio)[0]           # nuit / ciel couvert : normal
    assert not quality.is_solar_anomaly(15000, 800, ratio)[0]      # attendu 16 000 : normal
    assert quality.is_solar_anomaly(4000, 800, ratio)[0]           # plein soleil mais 25 % : anomalie


def test_incident_lifecycle():
    assert quality.incident_action("breach", False) == "open"
    assert quality.incident_action("warning", True) == "update"
    assert quality.incident_action("ok", True) == "resolve"
    assert quality.incident_action("ok", False) == "none"


# ---------- consumer : routage ----------
def test_consumer_routes_valid_and_invalid_messages():
    import json

    from energypulse import consumer

    def msg(topic, value, offset):
        m = MagicMock()
        m.topic.return_value, m.value.return_value, m.offset.return_value = topic, value, offset
        return m

    good = generation_event("CISO", NOW, "SUN", 100, "t")
    messages = [
        msg("energy.generation", json.dumps(good).encode(), 1),
        msg("energy.generation", b"pas du json", 2),
        msg("energy.demand", json.dumps(good).encode(), 7),   # mauvais topic
        msg("energy.dlq", json.dumps({"reasons": ["x"], "origin_topic": "energy.generation"}).encode(), 3),
    ]
    batches, dead, stats = consumer.route(messages)
    assert len(batches["generation"]) == 1
    assert len(dead) == 3
    assert stats["energy.generation"] == [2, 2]
