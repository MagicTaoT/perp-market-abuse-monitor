from __future__ import annotations

import json

import pytest

from gold_basis_mm_bot.squeeze_engine.structural_scoring import score_structure


def complete(**overrides):
    values = {
        "price_change_1h_pct": "0",
        "price_change_4h_pct": "0",
        "price_change_24h_pct": "0",
        "price_percentile_21d": "50",
        "oi_change_1h_pct": "0",
        "oi_change_4h_pct": "0",
        "oi_change_24h_pct": "0",
        "oi_usd": "0",
        "oi_float_pct": "0",
        "oi_fdv_pct": "0",
        "oi_float_percentile_21d": "50",
        "funding_bps_per_interval": "0",
        "funding_change_1h_bps": "0",
        "funding_change_4h_bps": "0",
        "funding_percentile_7d": "50",
        "funding_interval_hours": "8",
        "premium_bps": "0",
        "unlock_schedule_status": "none_within_30d",
        "confirming_venues": 1,
    }
    values.update(overrides)
    return score_structure(**values)


def contribution(track, name: str):
    return next(row for row in track["contributions"] if row["name"] == name)


def test_unknown_inputs_are_not_implicitly_zero_or_rankable():
    empty = score_structure()
    for name in ("early_accumulation", "elevated_short_watch", "long_risk", "short_risk"):
        track = empty[name]
        assert track["score"] is None
        assert track["tier"] == "unknown"
        assert track["coverage_pct"] == 0
        assert track["score_range"] == {"min": 0, "max": 100}
    partial = score_structure(oi_usd="5000000")
    early = partial["early_accumulation"]
    assert early["score"] is None and early["tier"] == "partial"
    assert early["observed_score"] == 5 and early["coverage_pct"] == 10
    assert early["score_range"] == {"min": 5, "max": 95}
    assert "oi_float_pct" in partial["unknown_inputs"]
    assert partial["manipulation_claim"] is False
    json.dumps(partial)


def test_ratio_and_absolute_bands_are_multilevel_and_include_boundaries():
    below = complete(oi_float_pct="14.999", oi_fdv_pct="0.999", oi_usd="4999999")
    on = complete(oi_float_pct="15", oi_fdv_pct="1", oi_usd="5000000")
    assert contribution(below["early_accumulation"], "oi_float_pressure")["points"] == 10
    assert contribution(on["early_accumulation"], "oi_float_pressure")["points"] == 14
    assert contribution(below["early_accumulation"], "oi_fdv_pressure")["points"] == 3
    assert contribution(on["early_accumulation"], "oi_fdv_pressure")["points"] == 5
    assert contribution(below["early_accumulation"], "oi_absolute")["points"] == 2
    assert contribution(on["early_accumulation"], "oi_absolute")["points"] == 5
    assert on["early_accumulation"]["status"] == "complete"
    assert 0 <= on["early_accumulation"]["score"] <= 100


def test_funding_sign_and_interval_shift_opposite_holding_risks():
    negative = complete(funding_bps_per_interval="-12.5", funding_interval_hours="4",
                        funding_change_1h_bps="-4", funding_change_4h_bps="-8",
                        funding_percentile_7d="5")
    equivalent = complete(funding_bps_per_interval="-25", funding_interval_hours="8",
                          funding_change_1h_bps="-8", funding_change_4h_bps="-16",
                          funding_percentile_7d="5")
    positive = complete(funding_bps_per_interval="12.5", funding_interval_hours="4",
                        funding_change_1h_bps="4", funding_change_4h_bps="8",
                        funding_percentile_7d="95")
    assert negative["early_accumulation"]["score"] == equivalent["early_accumulation"]["score"]
    assert negative["short_risk"]["score"] == equivalent["short_risk"]["score"]
    assert contribution(negative["short_risk"], "negative_funding_cost")["points"] == 15
    assert contribution(negative["long_risk"], "positive_funding_cost")["points"] == 0
    assert contribution(positive["long_risk"], "positive_funding_cost")["points"] == 15
    assert contribution(positive["short_risk"], "negative_funding_cost")["points"] == 0
    assert negative["early_accumulation"]["score"] < 50  # funding alone is not accumulation


def test_missing_funding_interval_marks_all_funding_components_unknown():
    result = complete(funding_interval_hours=None)
    for track_name in ("early_accumulation", "elevated_short_watch", "long_risk", "short_risk"):
        track = result[track_name]
        assert track["score"] is None
        funding_rows = [row for row in track["contributions"] if "funding" in row["name"]]
        assert all(row["status"] == "unknown" for row in funding_rows)
        assert "funding_interval_hours" in track["unknown_inputs"]


def test_unlock_unknown_is_not_confirmed_absent_and_pressure_uses_size_and_time():
    unknown = complete(unlock_schedule_status="unknown")
    none = complete()
    event = complete(unlock_schedule_status="event", unlock_days="2", unlock_float_pct="15")
    assert unknown["elevated_short_watch"]["score"] is None
    assert unknown["long_risk"]["score"] is None
    assert contribution(unknown["elevated_short_watch"], "upcoming_unlock")["status"] == "unknown"
    assert contribution(none["elevated_short_watch"], "upcoming_unlock")["points"] == 0
    assert contribution(event["elevated_short_watch"], "upcoming_unlock")["points"] == 15
    assert contribution(event["long_risk"], "upcoming_unlock")["points"] == 20
    assert event["long_risk"]["score"] > none["long_risk"]["score"]
    with pytest.raises(ValueError, match="cannot include"):
        complete(unlock_schedule_status="none_within_30d", unlock_days="3", unlock_float_pct="5")


def test_tracks_remain_distinct_when_price_is_elevated_but_short_carry_is_dangerous():
    signal = complete(
        price_change_1h_pct="12", price_change_4h_pct="25", price_change_24h_pct="80",
        price_percentile_21d="99", oi_change_1h_pct="-8", oi_change_4h_pct="30",
        oi_change_24h_pct="60", oi_usd="150000000", oi_float_pct="35",
        oi_fdv_pct="4", oi_float_percentile_21d="99",
        funding_bps_per_interval="-60", funding_change_1h_bps="-25",
        funding_change_4h_bps="-35", funding_percentile_7d="1",
        premium_bps="30", unlock_schedule_status="event", unlock_days="2",
        unlock_float_pct="20", confirming_venues=3,
    )
    assert signal["elevated_short_watch"]["score"] >= 70
    assert signal["short_risk"]["score"] >= 65
    assert signal["elevated_short_watch"]["score"] != signal["short_risk"]["score"]
    assert signal["elevated_short_watch"]["tier"] in {"elevated", "high"}
    assert signal["short_risk"]["tier"] in {"high", "severe"}
    assert signal["manipulation_claim"] is False


@pytest.mark.parametrize("kwargs", [
    {"oi_float_pct": "NaN"}, {"oi_usd": "-1"},
    {"price_percentile_21d": "101"}, {"funding_interval_hours": "0"},
    {"unlock_schedule_status": "guess"}, {"confirming_venues": True},
    {"price_change_1h_pct": "-101"},
])
def test_invalid_units_or_ranges_fail_closed(kwargs):
    with pytest.raises(ValueError):
        score_structure(**kwargs)
