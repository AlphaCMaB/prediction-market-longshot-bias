"""Build the offline Phase 10F-C estimand and probability-sampling design."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget
from scripts.pipeline_v2.phase_10f_c_design import (
    FrameFamily,
    SAMPLING_SEED,
    SCHEMA_VERSION,
    SIZE_BINS,
    allocate_stratified_sample,
    design_expectations,
    wilson_interval,
)
from scripts.pipeline_v2.study_rules import load_study_rules, validate_research_feature_columns


RULE1 = "PR1_M_FIXED_CLOCK_SINGLE_EXACT"
RULE2 = "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
MEASURES = (
    "usable_midpoint_15m",
    "usable_midpoint_60m",
    "usable_trade_close_15m",
    "usable_trade_close_60m",
)
CANDIDATE_FAMILIES = (2000, 5000, 10000)
CANDIDATE_CAPS = (1, 3, 5, 10)
RECOMMENDED_FAMILIES = 5000
RECOMMENDED_CAP = 3
EXPECTED_PLANNER_HASH = "90be78a79d5671006b65e54b2819cc8ad13e115f3875e3f8925be99c9966f41e"
EXPECTED_B2_REPORT_HASH = "b29deb4e46ae09ac9e40b393e15e2fec5212f91a3a662e212a9925f3cb641225"
EXPECTED_B2_ACCEPTANCE_HASH = "c7821ee78ea3f9b3e150b9c51e439fdc658927da7413ccc022ddb2bd6e5814b0"
OUTPUT_NAMES = (
    "phase_10f_c_estimand_design.md",
    "phase_10f_c_sampling_frame_report.json",
    "phase_10f_c_candidate_sample_sizes.csv",
    "phase_10f_c_weighting_specification.md",
    "phase_10f_c_pr1_viability_report.json",
    "phase_10f_c_missing_price_plan.md",
    "phase_10f_c_proposed_sampling_manifest_schema.json",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json(value: Any) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        + "\n"
    ).encode()


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise ValueError(f"{label} SHA-256 mismatch: expected={expected} actual={actual}")
    return actual


def _load_frame(path: Path) -> tuple[list[FrameFamily], dict[str, Any]]:
    rows: list[FrameFamily] = []
    identities: set[tuple[str, str]] = set()
    all_statuses: Counter[str] = Counter()
    anchor_valid = 0
    anchor_valid_contracts = 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        validate_research_feature_columns(reader.fieldnames or ())
        required = {
            "family_id", "family_id_source", "verified_anchor_time", "rule",
            "category", "market_existence_at_target", "market_count",
            "eligible_market_retrieval_count",
        }
        if not required.issubset(reader.fieldnames or ()):
            raise ValueError("planner is missing required outcome-blind fields")
        for row in reader:
            identity = (row["family_id"], row["family_id_source"])
            if identity in identities:
                raise ValueError("duplicate family identity in planner")
            identities.add(identity)
            anchor_valid += 1
            anchor_valid_contracts += int(row["market_count"])
            status = row["market_existence_at_target"]
            all_statuses[status] += 1
            if status != "definitely_existed_by_target":
                continue
            count = int(row["eligible_market_retrieval_count"])
            if count <= 0 or count > int(row["market_count"]):
                raise ValueError("invalid eligible contract count")
            anchor_time = row["verified_anchor_time"]
            if len(anchor_time) < 7:
                raise ValueError("invalid verified anchor timestamp")
            rows.append(
                FrameFamily(
                    family_id=identity[0], family_id_source=identity[1],
                    rule=row["rule"], category=row["category"],
                    anchor_month=anchor_time[:7], contract_count=count,
                )
            )
    if anchor_valid != 161343 or len(rows) != 112166:
        raise ValueError(f"unexpected frame counts: anchor_valid={anchor_valid} eligible={len(rows)}")
    if sum(row.contract_count for row in rows) != 4586979:
        raise ValueError("unexpected eligible-contract count")
    return rows, {
        "anchor_valid_families": anchor_valid,
        "anchor_valid_contracts": anchor_valid_contracts,
        "existence_status_counts": dict(sorted(all_statuses.items())),
    }


def _counter(rows: list[FrameFamily], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(getattr(row, field)) for row in rows).items()))


def _rates(b2: Mapping[str, Any]) -> dict[str, dict[str, dict[str, float | int]]]:
    result: dict[str, dict[str, dict[str, float | int]]] = {}
    for rule in (RULE1, RULE2):
        source = b2["by_rule"][rule]
        n = int(source["sample_tickers"])
        result[rule] = {}
        for measure in MEASURES:
            x = int(source[measure])
            lower, upper = wilson_interval(x, n)
            result[rule][measure] = {
                "successes": x, "trials": n, "point": x / n,
                "wilson_95_lower": lower, "wilson_95_upper": upper,
            }
    return result


def _ceil_or_none(target: int, rate: float) -> int | None:
    return math.ceil(target / rate) if rate > 0 else None


def _pr1_viability(rates: Mapping[str, Any]) -> dict[str, Any]:
    targets = (100, 250, 500, 1000)
    measures: dict[str, Any] = {}
    for measure in MEASURES:
        observed = rates[RULE1][measure]
        scenarios = {"observed_point": observed["point"]}
        if observed["successes"] == 0:
            scenarios.update({"planning_0_5_percent": 0.005, "planning_1_percent": 0.01})
        else:
            scenarios["wilson_95_lower"] = observed["wilson_95_lower"]
        scenarios["wilson_95_upper"] = observed["wilson_95_upper"]
        measures[measure] = {
            "b2_evidence": observed,
            "request_counts_for_usable_targets": {
                name: {str(target): _ceil_or_none(target, float(rate)) for target in targets}
                for name, rate in scenarios.items()
            },
            "null_request_count_means": "not attainable at the specified zero observed rate",
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "rule": RULE1,
        "conclusion": (
            "PR1 is not viable for scientifically useful cross-category inference under the frozen "
            "midpoint <=15m primary measure on current evidence: 0/135 B2 tickers were observable."
        ),
        "uncertainty_note": (
            "Wilson intervals are binomial planning intervals for the clustered, stratified B2 sample, "
            "not design-based population confidence intervals. They quantify uncertainty only approximately."
        ),
        "measures": measures,
        "rules_promoted": False,
        "prices_acquired": 0,
        "outcomes_accessed": 0,
    }


def _candidate_rows(
    frame: list[FrameFamily], rates: Mapping[str, Any], throughput: float,
    bytes_per_ticker: float,
) -> list[dict[str, Any]]:
    stratum_counts = Counter(row.stratum for row in frame)
    output: list[dict[str, Any]] = []
    for family_n in CANDIDATE_FAMILIES:
        allocation = allocate_stratified_sample(stratum_counts, family_n)
        for cap in CANDIDATE_CAPS:
            expected = design_expectations(frame, allocation, cap)
            tickers = float(expected["expected_sampled_tickers"])
            by_rule = expected["expected_tickers_by_rule"]
            row: dict[str, Any] = {
                "sampled_families": family_n,
                "within_family_contract_cap": cap,
                "expected_sampled_tickers": round(tickers, 3),
                "expected_requests_including_cutoff_and_probe": math.ceil(tickers) + 2,
                "expected_runtime_seconds": round((math.ceil(tickers) + 2) / throughput, 3),
                "expected_storage_bytes": math.ceil(tickers * bytes_per_ticker),
                "expected_independent_family_design_ess": round(expected["expected_independent_family_design_ess"], 3),
                "expected_family_weight_design_ess": round(expected["expected_family_weight_design_ess"], 3),
                "expected_contract_weight_design_ess": round(expected["expected_contract_weight_design_ess"], 3),
                "expected_tickers_pr1": round(float(by_rule.get(RULE1, 0)), 3),
                "expected_tickers_pr2": round(float(by_rule.get(RULE2, 0)), 3),
                "expected_tickers_by_category_json": json.dumps(expected["expected_tickers_by_category"], sort_keys=True, separators=(",", ":")),
                "expected_tickers_by_anchor_month_json": json.dumps(expected["expected_tickers_by_anchor_month"], sort_keys=True, separators=(",", ":")),
                "expected_tickers_by_family_size_bin_json": json.dumps(expected["expected_tickers_by_family_size_bin"], sort_keys=True, separators=(",", ":")),
                "recommended": family_n == RECOMMENDED_FAMILIES and cap == RECOMMENDED_CAP,
            }
            for rule_name, short in ((RULE1, "pr1"), (RULE2, "pr2")):
                mass = float(by_rule.get(rule_name, 0))
                for measure in MEASURES:
                    label = measure.replace("usable_", "")
                    estimate = rates[rule_name][measure]
                    for bound in ("point", "wilson_95_lower", "wilson_95_upper"):
                        row[f"expected_{short}_{label}_{bound}"] = round(mass * float(estimate[bound]), 3)
            output.append(row)
    return output


def _csv_bytes(rows: list[Mapping[str, Any]]) -> bytes:
    import io
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode()


def _estimand_md(recommended: Mapping[str, Any]) -> bytes:
    text = f"""# Phase 10F-C estimand and sampling design

## Population boundaries

The design keeps five populations distinct: (1) 161,343 anchor-valid in-window families; (2) 112,166 families whose market existed by t-1h; (3) a probability sample drawn only after approval; (4) sampled contracts with a valid at-or-before price under a named staleness/price rule; and (5) a later outcome-analysis sample. Missing prices never invalidate an anchor. No production sample has been drawn.

## Proposed estimands

For eligible family *i*, let `M_i` be its eligible-contract count and `z_ij` a later analysis quantity such as outcome minus ex-ante implied probability. The primary family-weighted finite-population estimand is:

`theta_F = (1/F) sum_i [(1/M_i) sum_j z_ij]`.

It represents an equally weighted eligible family followed by a uniformly selected eligible contract within that family. Calibration curves, bin differences, and Brier scores will use this target and family-clustered/design-consistent uncertainty.

The predefined secondary contract-weighted estimand is:

`theta_C = (1/N) sum_i sum_j z_ij`.

It represents a uniformly selected eligible Kalshi contract. The two estimands will never share or silently mix weights.

## Proposed probability design

Stage 1 uses stratified SRSWOR of families. Strata are verification rule × category × verified-anchor calendar month × family-size bin (`1`, `2-5`, `6-25`, `26-100`, `101-400`). Every nonempty stratum receives at least two sampled families where the approved total permits; tiny strata are censused. Remaining slots use proportional allocation and deterministic largest remainders. Stage 2 uses uniform SRSWOR of up to `c` contracts within each sampled family. Hash ranking under seed `{SAMPLING_SEED}` makes an eventual approved draw reproducible; it uses no price, outcome, or post-event field.

The recommended pilot design, subject to approval, is {RECOMMENDED_FAMILIES:,} families with cap {RECOMMENDED_CAP}. It projects {float(recommended['expected_sampled_tickers']):,.0f} tickers, {int(recommended['expected_requests_including_cutoff_and_probe']):,} requests, {float(recommended['expected_runtime_seconds'])/3600:,.2f} hours, and {int(recommended['expected_storage_bytes']):,} auditable bytes. This balances independent-family information against contract coverage; cap 3 permits within-family calibration representation without letting large families dominate request volume.

## Price definitions and reporting gates

Candidate primary price remains YES bid/ask midpoint at or before target with staleness <=15 minutes. Prespecified robustness candidates remain midpoint <=60 minutes, trade close <=15 minutes, and trade close <=60 minutes. No fallback mixes midpoint and trade close. A spread sensitivity analysis may report prespecified spread bands, but no spread exclusion becomes primary without approval.

PR1 and PR2 will be reported separately. Category results require at least 100 price-observable families and Kish design ESS >=100; otherwise they are suppressed from confirmatory interpretation and labeled descriptive. A probability bin requires at least 30 observable families and Kish ESS >=30. Coverage and ESS are evaluated before outcomes are unquarantined. Pooling PR1 and PR2 is not approved and would additionally require defensible overlap in price coverage.

## Longshot representation

Uniform within-family selection is preferred because probability cannot be used before acquisition. The eventual probability-bin distribution must be checked after acquisition without changing inclusion. If cap 3 proves inadequate, a future outcome-blind strike/order-position enrichment (for example, edge and interior order quantiles selected with known randomized probabilities) may be proposed; it is neither implemented nor approved here.
"""
    return text.encode()


def _weights_md() -> bytes:
    return """# Phase 10F-C weighting specification

Let stratum *h* contain `N_h` eligible families and sample `n_h` without replacement. Then `pi_family_i = n_h/N_h`. If family *i* has `M_i` contracts and `m_i=min(M_i,c)` are selected uniformly without replacement, `pi_contract_given_family_ij=m_i/M_i` and `pi_ij=(n_h/N_h)(m_i/M_i)`.

For the contract-weighted total, the raw Horvitz-Thompson weight is `w_C,ij=1/pi_ij`. The Hájek mean is `sum(w_C z)/sum(w_C)`; the unnormalized weights and both inclusion-probability factors remain in the manifest.

For the family-weighted target, each contract's target mass is `1/M_i`. Its raw design weight is therefore `w_F,ij=1/(pi_ij M_i)=1/(pi_family_i m_i)`. The Hájek mean is `sum(w_F z)/sum(w_F)`. Thus each sampled family's expected total family-weight mass is equal even when Kalshi lists many contracts.

Calibration-bin estimates use the same ratio form with a bin indicator in numerator and denominator. Outcomes, prices, and favorite/longshot labels never affect inclusion. Uncertainty must respect stratification, finite-population sampling, and family clustering; a family-level replicate or stratum-aware cluster bootstrap is the leading implementation, to be frozen before outcomes are read.

For price-observable contracts (`R_ij=1`), the reportable conditional estimands are `sum(R w_F z)/sum(R w_F)` and `sum(R w_C z)/sum(R w_C)`. These describe the design-weighted price-observable subset, not contracts with missing prices. No inverse-probability-of-observation correction is approved.
""".encode()


def _missing_md() -> bytes:
    return """# Phase 10F-C missing-price plan

Price observability is a post-sampling measurement property, not an anchor-validity or sampling-eligibility rule. The primary result, if estimable, is explicitly conditional on a valid midpoint no more than 15 minutes stale. It must not be described as generalizing automatically to eligible contracts with no such quote.

Before outcomes are accessed, compare observable and missing sampled contracts using design weights and only ex-ante fields: rule, category, verified-anchor month, family-size bin and count, deterministic contract position if later defined, time from market open to target, target clock/hour, verified source, and timing structure. Report weighted rates, standardized differences, family counts, and Kish ESS for each measure. Report PR1 and PR2 separately.

Do not fit or apply observation-propensity weights merely because observability differs. Such correction would require a defensible missing-at-random model and positivity; current B2 evidence—especially 0/135 PR1 midpoint observations at <=15 minutes—does not support those assumptions. Missing categories/cells are a scope limitation, not zero effects.

Planned limitation language: “Estimates apply to the probability-sampled eligible contracts for which the named pre-anchor price measure was observable within the prespecified staleness window. Contracts without an observable quote may differ systematically, so the estimates do not identify calibration or favorite–longshot bias for the entire structurally eligible universe.”
""".encode()


def _schema() -> dict[str, Any]:
    properties = {
        "family_id": {"type": "string", "minLength": 1},
        "family_id_source": {"type": "string", "minLength": 1},
        "contract_id": {"type": "string", "minLength": 1},
        "rule": {"enum": [RULE1, RULE2]},
        "category": {"type": "string", "minLength": 1},
        "anchor_month": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{2}$"},
        "family_size_bin": {"enum": list(SIZE_BINS)},
        "family_contract_count": {"type": "integer", "minimum": 1, "maximum": 400},
        "sampled_contract_count_in_family": {"type": "integer", "minimum": 1},
        "stratum_family_count": {"type": "integer", "minimum": 1},
        "stratum_sampled_family_count": {"type": "integer", "minimum": 1},
        "pi_family": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "pi_contract_given_family": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "pi_contract": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
        "contract_weight_raw": {"type": "number", "minimum": 1},
        "family_weight_raw": {"type": "number", "exclusiveMinimum": 0},
        "sampling_seed": {"const": SAMPLING_SEED},
    }
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "phase_10f_c_proposed_sampling_manifest_schema.json",
        "title": "Proposed Phase 10F sampling manifest row",
        "description": "Design artifact only; no production draw is authorized by this schema.",
        "type": "object", "additionalProperties": False,
        "required": list(properties), "properties": properties,
        "x-forbidden-field-patterns": ["outcome", "result", "settlement", "price", "profit", "return"],
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    planner_hash = _verify(args.planner, args.expected_planner_sha256, "planner")
    b2_hash = _verify(args.b2_report, args.expected_b2_report_sha256, "B2 report")
    acceptance_hash = _verify(args.b2_acceptance, args.expected_b2_acceptance_sha256, "B2 acceptance")
    rules = load_study_rules(args.config)
    frame, population = _load_frame(args.planner)
    b2 = json.loads(args.b2_report.read_text())
    acceptance = json.loads(args.b2_acceptance.read_text())
    rates = _rates(b2)
    throughput = float(acceptance["measured_total_requests_per_second"])
    bytes_per_ticker = sum(float(acceptance[key]) for key in (
        "measured_compressed_raw_bytes_per_ticker_request",
        "measured_normalized_bytes_per_ticker",
        "measured_request_commit_and_manifest_bytes_per_ticker",
    ))
    candidate_rows = _candidate_rows(frame, rates, throughput, bytes_per_ticker)
    recommended = next(row for row in candidate_rows if row["recommended"])
    strata = Counter(row.stratum for row in frame)
    report = {
        "schema_version": SCHEMA_VERSION,
        "study_rules_fingerprint": rules.fingerprint,
        "input_hashes": {"planner": planner_hash, "b2_report": b2_hash, "b2_acceptance": acceptance_hash},
        "population_boundaries": {
            **population,
            "structurally_eligible_families": len(frame),
            "structurally_eligible_contracts": sum(row.contract_count for row in frame),
            "sampled_families": 0, "sampled_contracts": 0,
            "price_observable_contracts": "not_yet_observed",
            "final_outcome_analysis_contracts": "not_yet_constructed",
        },
        "eligible_family_counts": {
            "by_rule": _counter(frame, "rule"), "by_category": _counter(frame, "category"),
            "by_anchor_month": _counter(frame, "anchor_month"), "by_family_size_bin": _counter(frame, "size_bin"),
        },
        "eligible_contract_counts": {
            "by_rule": dict(sorted(Counter({rule: sum(r.contract_count for r in frame if r.rule == rule) for rule in {r.rule for r in frame}}).items())),
            "by_category": dict(sorted({category: sum(r.contract_count for r in frame if r.category == category) for category in {r.category for r in frame}}.items())),
            "by_anchor_month": dict(sorted({month: sum(r.contract_count for r in frame if r.anchor_month == month) for month in {r.anchor_month for r in frame}}.items())),
            "by_family_size_bin": dict(sorted({size: sum(r.contract_count for r in frame if r.size_bin == size) for size in SIZE_BINS}.items())),
        },
        "stratum_definition": ["rule", "category", "verified_anchor_month", "family_size_bin"],
        "nonempty_stratum_count": len(strata),
        "strata": {"|".join(key): value for key, value in sorted(strata.items())},
        "family_size_bins": list(SIZE_BINS),
        "sampling_seed": SAMPLING_SEED,
        "recommended_design": recommended,
        "b2_planning_rates": rates,
        "price_measures_status": {
            "candidate_primary": "midpoint <=15m; not permanently frozen",
            "robustness": ["midpoint <=60m", "trade close <=15m", "trade close <=60m"],
            "spread_cutoff_approved": False,
        },
        "network_requests": 0, "prices_acquired": 0, "outcomes_accessed": 0,
        "production_sample_drawn": False, "study_rules_changed": False,
    }
    contents = {
        OUTPUT_NAMES[0]: _estimand_md(recommended),
        OUTPUT_NAMES[1]: _canonical_json(report),
        OUTPUT_NAMES[2]: _csv_bytes(candidate_rows),
        OUTPUT_NAMES[3]: _weights_md(),
        OUTPUT_NAMES[4]: _canonical_json(_pr1_viability(rates)),
        OUTPUT_NAMES[5]: _missing_md(),
        OUTPUT_NAMES[6]: _canonical_json(_schema()),
    }
    budget = StorageBudget(args.guard_root, max_bytes=args.max_generated_bytes, min_free_bytes=args.min_free_bytes)
    additional = sum(0 if (args.output_root / name).exists() else len(content) for name, content in contents.items())
    budget.check_additional(additional)
    if args.preflight_only:
        return {"preflight_only": True, "additional_bytes": additional, "storage": budget.snapshot(), "recommended_design": recommended}
    args.output_root.mkdir(parents=True, exist_ok=True)
    for name, content in contents.items():
        destination = args.output_root / name
        if destination.exists():
            if destination.read_bytes() != content:
                raise ValueError(f"immutable output conflict: {destination}")
            continue
        fd, temporary = tempfile.mkstemp(prefix=f".{name}.", dir=args.output_root)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content); handle.flush(); os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return {
        "preflight_only": False,
        "output_hashes": {name: _sha256(args.output_root / name) for name in OUTPUT_NAMES},
        "output_bytes": {name: (args.output_root / name).stat().st_size for name in OUTPUT_NAMES},
        "storage": budget.snapshot(), "recommended_design": recommended,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--planner", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_a/phase_10f_horizon_planner.csv"))
    parser.add_argument("--b2-report", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_b2/phase_10f_b2_report.json"))
    parser.add_argument("--b2-acceptance", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_b2/phase_10f_b2_acceptance_report.json"))
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument("--output-root", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_c"))
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--expected-planner-sha256", default=EXPECTED_PLANNER_HASH)
    parser.add_argument("--expected-b2-report-sha256", default=EXPECTED_B2_REPORT_HASH)
    parser.add_argument("--expected-b2-acceptance-sha256", default=EXPECTED_B2_ACCEPTANCE_HASH)
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    print(json.dumps(build(parse_args()), indent=2, sort_keys=True))
