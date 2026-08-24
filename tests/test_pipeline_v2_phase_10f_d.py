"""Offline tests for the approved Phase 10F-D PR2 sampling manifest."""

from __future__ import annotations

from collections import Counter
import gzip
import hashlib

import pytest

from scripts.pipeline_v2.build_phase_10f_d_sampling_manifest import (
    CONTRACT_FIELDS,
    CONTRACT_CAP,
    FAMILY_FIELDS,
    _csv_bytes,
    _json_bytes,
    _kish,
)
from scripts.pipeline_v2.phase_10f_c_design import (
    FrameFamily,
    allocate_stratified_sample,
    draw_stage_two_sample,
    select_stage_one_families,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


def family(name: str, count: int, month: str = "2026-01") -> FrameFamily:
    return FrameFamily(
        family_id=name,
        family_id_source="kalshi_event_ticker",
        rule="PR2_M_SCHEDULED_START_SINGLE_MILESTONE",
        category="Sports",
        anchor_month=month,
        contract_count=count,
        contract_ids=tuple(f"{name}-{index}" for index in range(count)),
    )


def test_stage_one_is_deterministic_without_contract_identities():
    frame = [
        FrameFamily(
            family_id=f"F{index}", family_id_source="event", rule="PR2",
            category="Sports", anchor_month="2026-01", contract_count=2,
        )
        for index in range(10)
    ]
    allocation = {frame[0].stratum: 4}
    first = select_stage_one_families(frame, allocation)
    second = select_stage_one_families(list(reversed(frame)), allocation)
    assert [item.identity for item in first] == [item.identity for item in second]
    assert len(first) == 4


def test_approved_stage_two_cap_and_inclusion_probabilities():
    complete = [family(f"F{index}", 5) for index in range(4)]
    stratum_counts = {complete[0].stratum: 10}
    allocation = {complete[0].stratum: 4}
    rows = draw_stage_two_sample(complete, stratum_counts, allocation, CONTRACT_CAP)
    assert len(rows) == 12
    assert Counter(row["family_id"] for row in rows) == Counter(
        {f"F{index}": 3 for index in range(4)}
    )
    for row in rows:
        assert row["pi_family"] == 0.4
        assert row["pi_contract_given_family"] == 0.6
        assert row["pi_contract"] == 0.24
        assert row["contract_weight_raw"] == pytest.approx(1 / 0.24)
        assert row["family_weight_raw"] == pytest.approx(1 / 1.2)


def test_all_nonempty_month_size_strata_preserve_minimum_allocation():
    frame = [family(f"A{index}", 1, "2026-01") for index in range(20)]
    frame += [family(f"B{index}", 7, "2026-02") for index in range(10)]
    counts = Counter(item.stratum for item in frame)
    allocation = allocate_stratified_sample(counts, 10)
    assert sum(allocation.values()) == 10
    assert all(allocation[key] >= 2 for key in counts)


def test_manifest_csv_is_deterministic_gzip():
    rows = [{field: field for field in FAMILY_FIELDS}]
    first = _csv_bytes(rows, FAMILY_FIELDS, compressed=True)
    second = _csv_bytes(rows, FAMILY_FIELDS, compressed=True)
    assert first == second
    assert first[:2] == b"\x1f\x8b"
    assert gzip.decompress(first).startswith(b"family_sample_index,")


def test_manifest_fields_preserve_outcome_quarantine():
    validate_research_feature_columns(FAMILY_FIELDS)
    validate_research_feature_columns(CONTRACT_FIELDS)
    for forbidden in ("outcome", "result", "settlement_value"):
        with pytest.raises(ValueError, match="quarantined"):
            validate_research_feature_columns((*CONTRACT_FIELDS, forbidden))


def test_kish_ess_and_commit_identity_are_reconstructable():
    assert _kish([2.0, 2.0, 2.0]) == pytest.approx(3.0)
    payload = {"complete": True, "network_requests_made": 0}
    identity = hashlib.sha256(_json_bytes(payload)).hexdigest()
    assert identity == hashlib.sha256(_json_bytes(dict(payload))).hexdigest()
