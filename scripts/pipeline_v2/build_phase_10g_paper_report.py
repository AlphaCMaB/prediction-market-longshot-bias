"""Build deterministic paper-ready reporting from frozen Phase 10G v3 artifacts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "phase-10g-paper-report-v1"
ANALYSIS_IDENTITY = "931a1d35de134e91eee3ed71041a712414c1435fbcd37f1ffc28b263e746252e"
SOURCE_INPUTS = {
    "market_merge_report": (
        "data/pipeline_v2/market_acquisition/partitioned/merged_universes/"
        "6f8aa42abec876d3aa1f6336/merge_report.json",
        "bcc5807379454ba2e6dc645f18cf6528636fb892a2e0ab56678757d71cd11077",
    ),
    "approved_anchor_report": (
        "data/pipeline_v2/anchor_evidence/phase_10e_approved_rules/"
        "phase_10e_rule_application_report.json",
        "afa8306d8a85bd11c11c0dfe3ca005ea6ef70db88ba8f3dee8928fc33a89a5ab",
    ),
    "horizon_planner_report": (
        "data/pipeline_v2/horizon_prices/phase_10f_a/"
        "phase_10f_horizon_planner_report.json",
        "5e6f6522dc161baad3492cef64e9d38701d0b7e58aa8d642b22f4b83c3566a89",
    ),
    "sampling_frame_report": (
        "data/pipeline_v2/horizon_prices/phase_10f_c/"
        "phase_10f_c_sampling_frame_report.json",
        "d4208332f76732b83a1858e083124828efc40b764249427e91987d23d934e697",
    ),
    "sampling_commit": (
        "data/pipeline_v2/horizon_prices/phase_10f_d/phase_10f_d_commit.json",
        "832e6f6e8d5ad19403b1200a1bc2142a3f8339c7926cfd55ab271def46690c96",
    ),
    "price_observability_report": (
        "data/pipeline_v2/horizon_prices/phase_10f_e/"
        "phase_10f_e_price_observability_report.json",
        "efb2ffbd621d6c29a048992f562519c6f53879bfd86778167408f79540696dce",
    ),
    "pre_outcome_audit": (
        "data/pipeline_v2/horizon_prices/phase_10f_e/final_pre_outcome_audit/"
        "phase_10f_final_pre_outcome_audit.json",
        "8ac3fb4b1de1ede9336306589a7b17aa47df94cd1a8245ef441d6360e7c75a73",
    ),
    "analysis_report": (
        "data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3/"
        "phase_10g_analysis_report.json",
        "f8ef01f6f6f7738fdcdcc130c16effe626456e7998aba6d65b60bab76b8b3efe",
    ),
    "analysis_commit": (
        "data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3/"
        "phase_10g_commit.json",
        "3c8236bbb3731d0c43679a16efa22c1087744a755df929dbadcdd3cacf609f40",
    ),
    "weighted_estimates": (
        "data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3/"
        "phase_10g_weighted_estimates.csv",
        "a443906527eb072946bd0824b43b558c963e386d62a80dfee7c799718572b4ff",
    ),
    "calibration_bins": (
        "data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3/"
        "phase_10g_primary_calibration_bins.csv",
        "59dd0992a4ad4fede71def24dbd50dffc431c2b370255b3e169695d715a323ed",
    ),
}

SAMPLE_LABELS = {
    "primary_midpoint_15m": "Midpoint <=15m (primary)",
    "robustness_midpoint_60m": "Midpoint <=60m",
    "robustness_trade_close_15m": "Trade close <=15m",
    "robustness_trade_close_60m": "Trade close <=60m",
    "robustness_midpoint_15m_spread_lte_0_20": "Midpoint <=15m; spread <=0.20",
    "robustness_midpoint_15m_spread_lte_0_10": "Midpoint <=15m; spread <=0.10",
}
SAMPLE_ORDER = tuple(SAMPLE_LABELS)


class PaperReportError(RuntimeError):
    pass


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()


def csv_bytes(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return buffer.getvalue().encode()


def markdown_table(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> str:
    def clean(value: Any) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(fields) + " |",
        "| " + " | ".join("---" for _ in fields) + " |",
    ]
    lines.extend(
        "| " + " | ".join(clean(row.get(field, "")) for field in fields) + " |"
        for row in rows
    )
    return "\n".join(lines)


def load_sources(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    loaded: dict[str, Any] = {}
    provenance: dict[str, dict[str, Any]] = {}
    for name, (relative, expected) in SOURCE_INPUTS.items():
        path = root / relative
        actual = sha256(path)
        if actual != expected:
            raise PaperReportError(
                f"{name} SHA-256 mismatch: expected={expected} actual={actual}"
            )
        provenance[name] = {
            "path": relative,
            "sha256": actual,
            "bytes": path.stat().st_size,
        }
        if path.suffix == ".json":
            loaded[name] = json.loads(path.read_text())
        elif path.suffix == ".csv":
            with path.open(newline="") as handle:
                loaded[name] = list(csv.DictReader(handle))
        else:
            raise PaperReportError(f"unsupported source type: {path}")
    commit = loaded["analysis_commit"]
    if (
        commit.get("schema_version") != "phase-10g-frozen-outcome-analysis-v3"
        or commit.get("commit_identity") != ANALYSIS_IDENTITY
        or not commit.get("complete")
    ):
        raise PaperReportError("authoritative Phase 10G identity changed")
    analysis = loaded["analysis_report"]
    if (
        not analysis.get("complete")
        or analysis.get("confirmatory_scope") != "PR2_M_only"
    ):
        raise PaperReportError("frozen Phase 10G reporting scope changed")
    if not analysis["pre_estimation_integrity"].get(
        "checks_completed_before_estimation"
    ):
        raise PaperReportError("pre-estimation integrity gate is not complete")
    return loaded, provenance


def table_1_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    merge = data["market_merge_report"]
    anchors = data["approved_anchor_report"]
    planner = data["horizon_planner_report"]
    frame = data["sampling_frame_report"]
    sample = data["sampling_commit"]
    audit = data["pre_outcome_audit"]
    analysis = data["analysis_report"]
    rows = [
        (
            "Source event-family universe",
            merge["event_count"],
            "Complete Kalshi event universe before anchor screening",
        ),
        (
            "Verified ex-ante anchors",
            anchors["verified_family_count"],
            "PR1-M and PR2-M rules; not an attrition estimate",
        ),
        (
            "Verified anchors in frozen window",
            planner["in_window_verified_family_count"],
            "Anchor time within the pre-specified analysis window",
        ),
        (
            "Structurally eligible at t-1h",
            planner["market_existence_counts"]["definitely_existed_by_target"],
            "At least one associated market existed by the target",
        ),
        (
            "PR2 eligible family population",
            frame["eligible_family_counts"]["by_rule"][
                "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
            ],
            "Confirmatory scheduled-event-start Sports population",
        ),
        (
            "Frozen sampled families",
            sample["sampled_families"],
            "Probability sample drawn before price and outcome access",
        ),
        (
            "Frozen sampled contracts",
            sample["sampled_contracts"],
            "At most three contracts per sampled family",
        ),
        (
            "Primary price-observable contracts",
            audit["primary_observability_balance"]["observed_contracts"],
            "Valid midpoint at t-1h with no more than 15-minute staleness",
        ),
        (
            "Primary binary-resolved contracts",
            analysis["resolution_availability"]["primary_price_observable"][
                "resolved_contracts"
            ],
            "Price-observable contracts with yes/no resolution",
        ),
        (
            "Primary binary-resolved families",
            analysis["resolution_availability"]["primary_price_observable"][
                "families_with_any_resolved_contract"
            ],
            "Families contributing at least one resolved primary contract",
        ),
    ]
    return [
        {"Stage": index, "Component": component, "Count": count, "Interpretation": note}
        for index, (component, count, note) in enumerate(rows, 1)
    ]


def table_2_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    estimates = data["analysis_report"]["estimates"]
    rows = []
    for sample_name in SAMPLE_ORDER:
        result = estimates[sample_name]["family_target"]
        gap_ci = result["weighted_calibration_gap_inference"]
        contrast = result["longshot_favorite_contrast"]
        contrast_ci = contrast["inference"]
        rows.append(
            {
                "Specification": SAMPLE_LABELS[sample_name],
                "Target": "Family",
                "Contracts": result["resolved_contracts"],
                "Families": result["resolved_families"],
                "Family ESS": f"{result['family_aggregated_weight_ess']:.1f}",
                "Weighted Y-P": f"{result['weighted_calibration_gap']:.5f}",
                "Y-P 95% CI": f"[{gap_ci['ci_lower']:.5f}, {gap_ci['ci_upper']:.5f}]",
                "Longshot-favorite contrast": f"{contrast['estimate']:.5f}",
                "Contrast 95% CI": f"[{contrast_ci['ci_lower']:.5f}, {contrast_ci['ci_upper']:.5f}]",
            }
        )
    secondary = estimates["primary_midpoint_15m"]["contract_target"]
    gap_ci = secondary["weighted_calibration_gap_inference"]
    contrast = secondary["longshot_favorite_contrast"]
    contrast_ci = contrast["inference"]
    rows.append(
        {
            "Specification": "Midpoint <=15m (secondary contract target)",
            "Target": "Contract",
            "Contracts": secondary["resolved_contracts"],
            "Families": secondary["resolved_families"],
            "Family ESS": f"{secondary['family_aggregated_weight_ess']:.1f}",
            "Weighted Y-P": f"{secondary['weighted_calibration_gap']:.5f}",
            "Y-P 95% CI": f"[{gap_ci['ci_lower']:.5f}, {gap_ci['ci_upper']:.5f}]",
            "Longshot-favorite contrast": f"{contrast['estimate']:.5f}",
            "Contrast 95% CI": f"[{contrast_ci['ci_lower']:.5f}, {contrast_ci['ci_upper']:.5f}]",
        }
    )
    return rows


def table_3_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for result in data["analysis_report"]["primary_calibration_bins"]["family_target"]:
        interval = result["weighted_calibration_gap_inference"]
        rows.append(
            {
                "Probability range": result["probability_bin"],
                "Weighted mean probability": f"{result['weighted_mean_price']:.4f}",
                "Weighted observed frequency": f"{result['weighted_yes_rate']:.4f}",
                "Weighted Y-P": f"{result['weighted_calibration_gap']:.4f}",
                "Y-P 95% CI": f"[{interval['ci_lower']:.4f}, {interval['ci_upper']:.4f}]",
                "Unique families": result["resolved_families"],
                "Family ESS": f"{result['family_aggregated_weight_ess']:.1f}",
                "Frozen gate": "Pass" if result["support_gate_passed"] else "Fail",
            }
        )
    if len(rows) != 10 or any(row["Frozen gate"] != "Pass" for row in rows):
        raise PaperReportError("frozen calibration-bin support changed")
    return rows


def table_4_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    analysis = data["analysis_report"]
    audit = data["pre_outcome_audit"]
    frozen = analysis["resolution_availability"]["frozen_sample"]
    primary = analysis["resolution_availability"]["primary_price_observable"]
    balance = audit["primary_observability_balance"]
    attrition = audit["mutually_exclusive_attrition"]["primary_midpoint_15m"]
    reasons = attrition["exclusion_reasons"]
    rows = [
        (
            "Outcome availability",
            "Binary-resolved sampled contracts",
            frozen["resolved_contracts"],
            "Allowed yes/no outcome available",
        ),
        (
            "Outcome availability",
            "Unresolved/nonbinary sampled contracts",
            frozen["unresolved_contracts"],
            "Retained as missing; no replacement",
        ),
        (
            "Outcome availability",
            "Family-target weighted resolution coverage",
            f"{100*frozen['family_target_weighted_resolution_coverage']:.2f}%",
            "Design-weighted coverage",
        ),
        (
            "Outcome availability",
            "Contract-target weighted resolution coverage",
            f"{100*frozen['contract_target_weighted_resolution_coverage']:.2f}%",
            "Design-weighted coverage",
        ),
        (
            "Primary outcome sample",
            "Resolved price-observable contracts",
            primary["resolved_contracts"],
            "Contribute to primary analysis",
        ),
        (
            "Primary outcome sample",
            "Families with a resolved primary contract",
            primary["families_with_any_resolved_contract"],
            "Contribute to primary analysis",
        ),
        (
            "Primary price exclusion",
            "No pre-target candle",
            reasons["no_pre_target_candle"],
            "No eligible candle at or before target",
        ),
        (
            "Primary price exclusion",
            "Midpoint only within 15-60m",
            reasons["usable_midpoint_60m_only"],
            "Eligible for 60-minute robustness only",
        ),
        (
            "Primary price exclusion",
            "API/data failure",
            reasons["api_or_data_failure"],
            "Preserved failure; no replacement",
        ),
        (
            "Observability balance",
            "Hours-since-open standardized difference",
            f"{balance['hours_since_market_open']['standardized_mean_difference']:.3f}",
            "Observed versus unavailable-price contracts",
        ),
        (
            "Observability balance",
            "Maximum family-size share difference",
            f"{balance['family_size_bin']['maximum_absolute_share_difference']:.3f}",
            "Absolute weighted-share difference",
        ),
        (
            "Observability balance",
            "Maximum month share difference",
            f"{balance['anchor_month']['maximum_absolute_share_difference']:.3f}",
            "Absolute weighted-share difference",
        ),
        (
            "Observability balance",
            "Maximum target-hour share difference",
            f"{balance['target_utc_hour']['maximum_absolute_share_difference']:.3f}",
            "Absolute weighted-share difference",
        ),
    ]
    return [
        {"Domain": domain, "Metric": metric, "Value": value, "Interpretation": note}
        for domain, metric, value, note in rows
    ]


def methods_text(data: Mapping[str, Any]) -> str:
    sample = data["sampling_commit"]
    return f"""# Methods

## Study design and ex-ante timing

The analysis uses a Kalshi-only prediction-market design with a frozen anchor window from July 1, 2025 through June 30, 2026. Resolution and settlement timestamps were not used as research anchors because they are retrospective: conditioning the pricing horizon on information recorded after the event would create look-ahead bias. Candidate anchors were instead constructed from outcome-blind event and market metadata and evaluated using only information available before the event. The verification process distinguished fixed-clock candidates (PR1-M) from scheduled-event-start candidates tied to one official milestone (PR2-M). Candidate rules were subjected to outcome-blind audit and independent validation before their modified forms were approved and applied.

The confirmatory analysis is restricted to PR2-M scheduled-event-start Sports markets. PR1-M anchors remain valid, but the approved historical price source yielded too little qualifying PR1 midpoint coverage for a comparable confirmatory analysis. The analysis therefore does not pool PR1-M and PR2-M markets.

## Horizon and price construction

For every verified family, the target was fixed at one hour before the verified event start. The primary probability measure is the latest fully pre-target YES bid/ask midpoint whose observation time is no more than 15 minutes before the target. Both sides of the quote were required. Post-target candles, previous-price fallbacks, and mixing between quote and trade measures were prohibited. Separate pre-specified robustness definitions used midpoint staleness up to 60 minutes, documented trade closes within 15 or 60 minutes, and primary midpoints restricted to bid-ask spreads no larger than 0.20 or 0.10.

## Sampling and estimands

The eligible PR2-M population contained 64,775 families. A deterministic stratified probability sample selected {sample['sampled_families']:,} families by verified-anchor month and family-size stratum. Within each selected family, at most three eligible contracts were sampled, yielding {sample['sampled_contracts']:,} contracts. Exact first- and second-stage inclusion probabilities were retained. The primary family-target estimator gives equal target mass to each eligible family and equal mass to sampled contracts within family; its preserved family weights are used in a Hajek ratio. A separately weighted contract-target estimator represents a uniformly selected eligible contract and is reported as secondary. The weight systems were not mixed, and no observation-propensity correction was added after price availability was observed.

For contract j in family i, the calibration gap is Y_ij - P_ij, where P is the frozen pre-event price and Y is the binary YES resolution. The pre-specified favorite-longshot contrast is the weighted mean gap among contracts with P < 0.20 minus the weighted mean gap among contracts with P >= 0.80. Under this definition, the classical favorite-longshot pattern predicts a negative contrast. The calibration profile uses ten fixed 0.10 probability bins.

## Outcome quarantine and inference

Outcomes remained quarantined until anchors, sample membership, prices, inclusion probabilities, weights, exclusions, and the analysis plan were frozen and audited. The released projection contains only contract identifier, frozen sample identifier, and binary outcome. It excludes settlement timestamps, settlement values, post-resolution metadata, and fields that could alter eligibility. The outcome join was performed in memory and fingerprinted before estimation; unresolved or nonbinary rows were retained as missing outcomes without replacement or adaptive reweighting.

Uncertainty uses the pre-specified deterministic stratified family-cluster bootstrap with 10,000 replicates. Families are resampled as clusters within anchor-month by family-size strata, and each replicate recomputes the named Hajek estimator. Reported intervals are two-sided 95% percentile intervals. Overall inference required at least 500 contributing families and family-weighted effective sample size of 500; subgroup and probability-bin thresholds were likewise frozen before outcome access.
"""


def results_text(data: Mapping[str, Any]) -> str:
    analysis = data["analysis_report"]
    primary = analysis["estimates"]["primary_midpoint_15m"]["family_target"]
    gap = primary["weighted_calibration_gap"]
    gap_ci = primary["weighted_calibration_gap_inference"]
    contrast = primary["longshot_favorite_contrast"]
    contrast_ci = contrast["inference"]
    resolution = analysis["resolution_availability"]
    frozen = resolution["frozen_sample"]
    observable = resolution["primary_price_observable"]
    bins = analysis["primary_calibration_bins"]["family_target"]
    excluding_zero = sum(
        row["weighted_calibration_gap_inference"]["ci_lower"] > 0
        or row["weighted_calibration_gap_inference"]["ci_upper"] < 0
        for row in bins
    )
    return f"""# Results

## Primary sample and outcome coverage

The frozen probability sample contained {frozen['contracts']:,} contracts from {frozen['families']:,} PR2-M Sports families. Binary outcomes were available for {frozen['resolved_contracts']:,} contracts; {frozen['unresolved_contracts']:,} were unresolved or nonbinary and were retained as missing without replacement. Family-target and contract-target weighted resolution coverage were {100*frozen['family_target_weighted_resolution_coverage']:.2f}% and {100*frozen['contract_target_weighted_resolution_coverage']:.2f}%, respectively. The primary price rule yielded {observable['contracts']:,} observable contracts from {observable['families']:,} families. Of these, {observable['resolved_contracts']:,} contracts and {observable['families_with_any_resolved_contract']:,} families contributed to the primary outcome analysis.

## Overall calibration and favorite-longshot contrast

Under the primary family target, the weighted mean predicted probability was {primary['weighted_mean_price']:.4f} and the weighted realized YES frequency was {primary['weighted_yes_rate']:.4f}. The weighted calibration gap, Y-P, was {gap:.5f} (95% CI [{gap_ci['ci_lower']:.5f}, {gap_ci['ci_upper']:.5f}]). The primary family-weighted Brier score was {primary['weighted_brier_score']:.5f}; this is a secondary descriptive measure.

The pre-specified longshot-minus-favorite contrast was {contrast['estimate']:.5f} (95% CI [{contrast_ci['ci_lower']:.5f}, {contrast_ci['ci_upper']:.5f}]). The classical favorite-longshot pattern predicts a negative contrast under the frozen definition. The point estimate was small and positive, and the confidence interval included zero. Thus, in the pre-specified PR2 Sports sample, we detect no statistically distinguishable evidence of favorite-longshot bias.

## Calibration profile and robustness

All ten fixed probability bins passed the pre-specified family-count and effective-sample-size gates. Their weighted calibration gaps do not display a monotonic pattern from longshots to favorites. {excluding_zero} of the ten descriptive bin intervals excludes zero. Because all ten bins are shown, the bin was not a standalone pre-specified effect, and the profile is not monotonic, this isolated interval is not interpreted as evidence of a distinct pricing effect.

All five pre-specified family-weighted robustness contrasts had confidence intervals that included zero. The midpoint <=60-minute contrast was 0.00259 (95% CI [-0.02855, 0.03502]); the trade-close contrasts were 0.00607 [-0.03703, 0.05149] at 15 minutes and 0.00777 [-0.02906, 0.04551] at 60 minutes. Restricting the primary midpoint to spreads <=0.20 produced 0.00908 [-0.02500, 0.04491], while the <=0.10 restriction produced 0.00807 [-0.02630, 0.04453]. The secondary contract-target estimate was 0.02912 (95% CI [-0.01530, 0.07528]). These estimates agree qualitatively with the primary finding and do not replace it.

## Observability and missingness

Primary price exclusions comprised 928 contracts with no pre-target candle, 1,256 with a valid midpoint only under the 15-to-60-minute robustness window, and one preserved API/data failure. Pre-outcome diagnostics showed that price observability was not compositionally neutral: the family-weighted standardized difference in hours since market open was 0.257, and the largest absolute observed-versus-unavailable weighted-share differences were 0.197 by family-size bin, 0.123 by anchor month, and 0.027 by target UTC hour. These differences limit generalization beyond contracts with qualifying pre-target quotes.
"""


def discussion_text() -> str:
    return """# Discussion

The confirmatory analysis does not detect the classical favorite-longshot pattern in the frozen PR2-M Sports sample. The primary longshot-minus-favorite estimate is small relative to its uncertainty, and all pre-specified robustness intervals include zero. The agreement across midpoint, trade-close, staleness, and spread definitions reduces concern that the conclusion is driven by one particular price construction. It does not, however, prove that the true contrast is exactly zero.

This result is narrower than a venue-wide statement about Kalshi. The estimand pertains to scheduled-event-start Sports families that passed the modified PR2 verification rule, entered the frozen probability sample, had a valid observable price one hour before the event, and had a binary resolution available. Fixed-clock PR1-M markets could not support a comparable confirmatory analysis under the frozen historical-price source because qualifying midpoint coverage was too sparse. Other categories, horizons, platforms, and unobservable contracts may have different pricing patterns.

The result can be viewed alongside evidence from conventional betting markets, where favorite-longshot bias is often studied using realized returns or bookmaker odds. Prediction-market contracts differ in trading mechanism, participant composition, fee structure, and how information is incorporated into prices. These institutional differences could matter, but the present analysis was not designed to identify causal mechanisms and therefore does not attribute the finding to any specific market feature.

Future work should first preserve the same ex-ante verification and outcome-quarantine discipline. Useful extensions include obtaining an independently validated historical-price source for PR1-M markets, expanding to categories with adequate within-category support, and constructing comparable samples on other prediction-market platforms. Such analyses should pre-specify how platform differences, contract-family dependence, price observability, fees, and resolution conventions enter the estimand before outcomes are examined.
"""


def limitations_text() -> str:
    return """# Limitations

1. **PR2 Sports scope.** The confirmatory estimate applies to scheduled-event-start Sports markets verified under PR2-M. It does not cover all Kalshi markets. PR1-M fixed-clock anchors remain valid, but qualifying midpoint coverage under the frozen historical-price specification was too sparse for a comparable confirmatory analysis.

2. **Conditional price observability.** Inference is conditional on contracts and families with valid pre-target quotes. Observability was not random: the family-weighted standardized difference in hours since market open was 0.257, while maximum absolute observed-versus-unavailable share differences were 0.197 by family size, 0.123 by month, and 0.027 by target hour. No observation-propensity correction was used because its identifying assumptions were not considered defensible.

3. **Historical midpoint interpretation.** A bid/ask midpoint is an indicative probability, not necessarily an executable transaction price. Upper-tail spreads can be wide. The predeclared spread restrictions and separate trade-close analyses did not materially change the qualitative conclusion, but they do not eliminate all liquidity or execution concerns.

4. **Within-family dependence.** Contracts from the same event family are not independent. The primary weighting scheme gives equal target mass to families, and uncertainty was estimated by resampling complete families within the original sampling strata. Residual dependence across related families is not explicitly modeled.

5. **Resolution availability.** Seventy-eight sampled contracts were unresolved or nonbinary. They were retained as missing outcomes without replacement, sample redrawing, or adaptive reweighting. Although weighted resolution coverage exceeded 99%, unresolved observations were not distributed uniformly across months and family sizes.

6. **Statistical uncertainty.** Failure to reject a zero contrast is not proof that the true effect equals zero. The primary confidence interval indicates the range of effect sizes compatible with the frozen design and data. The ten calibration bins are descriptive jointly; one interval excluding zero is not treated as a standalone discovery because no post-hoc multiple-comparison search was specified.
"""


def executive_summary(data: Mapping[str, Any]) -> str:
    primary = data["analysis_report"]["estimates"]["primary_midpoint_15m"][
        "family_target"
    ]
    contrast = primary["longshot_favorite_contrast"]
    ci = contrast["inference"]
    return f"""# Mentor-facing executive summary

The project has completed its first fully pre-specified favorite-longshot analysis. The design is deliberately outcome-blind through sample and price construction: event anchors were verified from ex-ante metadata, the t-1h sample and weights were frozen, and only then was a minimal binary-outcome projection released.

The confirmatory sample contains {primary['resolved_contracts']:,} resolved contracts from {primary['resolved_families']:,} scheduled-event-start Sports families. The primary family-weighted longshot-minus-favorite calibration contrast is {contrast['estimate']:.5f}, with a 95% family-cluster bootstrap interval from {ci['ci_lower']:.5f} to {ci['ci_upper']:.5f}. Because the classical pattern predicts a negative contrast and this interval includes zero, the appropriate conclusion is: **in the pre-specified PR2 Sports sample, we detect no statistically distinguishable evidence of favorite-longshot bias.**

All pre-specified robustness intervals also include zero, including alternative staleness windows, trade-close prices, and two spread restrictions. The result is nevertheless conditional on price observability and should not be generalized to all Kalshi markets. The main paper limitation is that fixed-clock PR1 markets lacked sufficient historical-price coverage under the frozen source, and observable PR2 contracts differed compositionally from unavailable-price contracts.
"""


def _matplotlib():
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/pmlb-matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    matplotlib.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "legend.fontsize": 8,
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "svg.hashsalt": "phase-10g-paper-report-v1",
        }
    )
    import matplotlib.pyplot as plt

    return plt


def figure_bytes(fig: Any, extension: str) -> bytes:
    buffer = io.BytesIO()
    metadata = {"Creator": "Phase 10G deterministic paper-report generator"}
    if extension == "svg":
        metadata["Date"] = "2026-08-25"
    fig.savefig(
        buffer,
        format=extension,
        bbox_inches="tight",
        facecolor="white",
        metadata=metadata,
    )
    content = buffer.getvalue()
    if extension == "svg":
        content = b"\n".join(line.rstrip() for line in content.splitlines()) + b"\n"
    return content


def build_figures(data: Mapping[str, Any]) -> dict[str, bytes]:
    plt = _matplotlib()
    blue, orange, gray = "#0072B2", "#D55E00", "#666666"
    bins = data["analysis_report"]["primary_calibration_bins"]["family_target"]
    predicted = [row["weighted_mean_price"] for row in bins]
    observed = [row["weighted_yes_rate"] for row in bins]
    lower = [
        row["weighted_mean_price"]
        + row["weighted_calibration_gap_inference"]["ci_lower"]
        for row in bins
    ]
    upper = [
        row["weighted_mean_price"]
        + row["weighted_calibration_gap_inference"]["ci_upper"]
        for row in bins
    ]

    artifacts: dict[str, bytes] = {}
    fig, ax = plt.subplots(figsize=(6.8, 5.2))
    ax.plot(
        [0, 1],
        [0, 1],
        color=gray,
        linewidth=1,
        linestyle="--",
        label="Perfect calibration",
    )
    ax.errorbar(
        predicted,
        observed,
        yerr=[
            [point - low for point, low in zip(observed, lower)],
            [high - point for point, high in zip(observed, upper)],
        ],
        fmt="o-",
        color=blue,
        ecolor=blue,
        capsize=3,
        linewidth=1.4,
        markersize=4.5,
        label="Family-weighted bins",
    )
    ax.set(
        xlim=(0, 1),
        ylim=(0, 1),
        xlabel="Weighted predicted probability",
        ylabel="Weighted realized YES frequency",
    )
    ax.set_title("Calibration curve at the one-hour horizon")
    ax.grid(alpha=0.2, linewidth=0.6)
    ax.legend(frameon=False, loc="upper left")
    fig.text(
        0.5,
        -0.01,
        "Intervals are family-cluster bootstrap intervals for Y-P, translated to the realized-frequency axis.",
        ha="center",
        fontsize=7,
    )
    for ext in ("png", "svg"):
        artifacts[f"figure_1_calibration_curve.{ext}"] = figure_bytes(fig, ext)
    plt.close(fig)

    labels = [row["probability_bin"] for row in bins]
    gaps = [row["weighted_calibration_gap"] for row in bins]
    gap_low = [row["weighted_calibration_gap_inference"]["ci_lower"] for row in bins]
    gap_high = [row["weighted_calibration_gap_inference"]["ci_upper"] for row in bins]
    fig, ax = plt.subplots(figsize=(7.4, 4.8))
    positions = list(range(len(labels)))
    ax.errorbar(
        positions,
        gaps,
        yerr=[
            [point - low for point, low in zip(gaps, gap_low)],
            [high - point for point, high in zip(gaps, gap_high)],
        ],
        fmt="o",
        color=blue,
        ecolor=blue,
        capsize=3,
        markersize=5,
    )
    ax.axhline(0, color=gray, linewidth=1)
    ax.set_xticks(positions, labels, rotation=35, ha="right")
    ax.set(xlabel="Frozen probability bin", ylabel="Weighted calibration gap (Y-P)")
    ax.set_title("Calibration bias by probability decile")
    ax.grid(axis="y", alpha=0.2, linewidth=0.6)
    for ext in ("png", "svg"):
        artifacts[f"figure_2_decile_calibration_gap.{ext}"] = figure_bytes(fig, ext)
    plt.close(fig)

    analysis_estimates = data["analysis_report"]["estimates"]
    forest = []
    for sample_name in SAMPLE_ORDER:
        result = analysis_estimates[sample_name]["family_target"][
            "longshot_favorite_contrast"
        ]
        forest.append(
            (
                SAMPLE_LABELS[sample_name],
                result["estimate"],
                result["inference"]["ci_lower"],
                result["inference"]["ci_upper"],
                False,
            )
        )
    secondary = analysis_estimates["primary_midpoint_15m"]["contract_target"][
        "longshot_favorite_contrast"
    ]
    forest.append(
        (
            "Midpoint <=15m (contract target; secondary)",
            secondary["estimate"],
            secondary["inference"]["ci_lower"],
            secondary["inference"]["ci_upper"],
            True,
        )
    )
    fig, ax = plt.subplots(figsize=(7.6, 5.1))
    ypos = list(reversed(range(len(forest))))
    for y, (label, point, low, high, secondary_flag) in zip(ypos, forest):
        color = orange if secondary_flag else blue
        marker = "s" if secondary_flag else "o"
        ax.errorbar(
            point,
            y,
            xerr=[[point - low], [high - point]],
            fmt=marker,
            color=color,
            ecolor=color,
            capsize=3,
            markersize=5,
        )
    ax.axvline(0, color=gray, linewidth=1)
    ax.set_yticks(ypos, [item[0] for item in forest])
    ax.set(xlabel="Longshot-minus-favorite calibration contrast")
    ax.set_title("Favorite-longshot contrast across frozen specifications")
    ax.grid(axis="x", alpha=0.2, linewidth=0.6)
    ax.text(
        0.01,
        -0.14,
        "Negative values are directionally consistent with the classical favorite-longshot pattern.",
        transform=ax.transAxes,
        fontsize=7,
    )
    for ext in ("png", "svg"):
        artifacts[f"figure_3_robustness_forest.{ext}"] = figure_bytes(fig, ext)
    plt.close(fig)

    balance = data["pre_outcome_audit"]["primary_observability_balance"]
    fig, axes = plt.subplots(2, 2, figsize=(10.2, 7.4))
    hours = balance["hours_since_market_open"]
    axes[0, 0].bar(
        ["Observable", "Unavailable"],
        [hours["observed_weighted_mean"], hours["missing_weighted_mean"]],
        color=[blue, orange],
    )
    axes[0, 0].set_ylabel("Weighted mean hours")
    axes[0, 0].set_title("Hours since market open")
    panels = [
        (axes[0, 1], "family_size_bin", "Family-size composition", 0),
        (axes[1, 0], "anchor_month", "Anchor-month composition", 35),
        (axes[1, 1], "target_utc_hour", "Target-hour composition", 70),
    ]
    for ax, key, title, rotation in panels:
        values = balance[key]["share_differences"]
        labels = list(values)
        points = [100 * values[label] for label in labels]
        ax.bar(
            labels, points, color=[blue if point >= 0 else orange for point in points]
        )
        ax.axhline(0, color=gray, linewidth=0.8)
        ax.set_ylabel("Observed - unavailable (percentage points)")
        ax.set_title(title)
        ax.tick_params(axis="x", labelrotation=rotation, labelsize=7)
    fig.suptitle("Ex-ante price-observability diagnostics", y=1.01, fontsize=12)
    fig.tight_layout()
    for ext in ("png", "svg"):
        artifacts[f"figure_4_observability_diagnostics.{ext}"] = figure_bytes(fig, ext)
    plt.close(fig)
    return artifacts


def build_artifacts(data: Mapping[str, Any]) -> dict[str, bytes]:
    tables = {
        "table_1_sample_construction.csv": (
            table_1_rows(data),
            ("Stage", "Component", "Count", "Interpretation"),
        ),
        "table_2_primary_and_robustness.csv": (
            table_2_rows(data),
            (
                "Specification",
                "Target",
                "Contracts",
                "Families",
                "Family ESS",
                "Weighted Y-P",
                "Y-P 95% CI",
                "Longshot-favorite contrast",
                "Contrast 95% CI",
            ),
        ),
        "table_3_calibration_deciles.csv": (
            table_3_rows(data),
            (
                "Probability range",
                "Weighted mean probability",
                "Weighted observed frequency",
                "Weighted Y-P",
                "Y-P 95% CI",
                "Unique families",
                "Family ESS",
                "Frozen gate",
            ),
        ),
        "table_4_missingness_observability.csv": (
            table_4_rows(data),
            ("Domain", "Metric", "Value", "Interpretation"),
        ),
    }
    artifacts = {
        name: csv_bytes(rows, fields) for name, (rows, fields) in tables.items()
    }
    artifacts.update(
        {
            "METHODS.md": methods_text(data).encode(),
            "RESULTS.md": results_text(data).encode(),
            "DISCUSSION.md": discussion_text().encode(),
            "LIMITATIONS.md": limitations_text().encode(),
            "MENTOR_EXECUTIVE_SUMMARY.md": executive_summary(data).encode(),
        }
    )
    artifacts.update(build_figures(data))
    table_sections = []
    for number, (name, (rows, fields)) in enumerate(tables.items(), 1):
        title = name.removesuffix(".csv").replace("_", " ").title()
        table_sections.append(f"## {title}\n\n{markdown_table(rows, fields)}")
    artifacts["PAPER_REPORT.md"] = (
        executive_summary(data)
        + "\n"
        + methods_text(data)
        + "\n"
        + results_text(data)
        + "\n"
        + "\n\n".join(table_sections)
        + "\n\n"
        + discussion_text()
        + "\n"
        + limitations_text()
    ).encode()
    return artifacts


def publish(output_root: Path, artifacts: Mapping[str, bytes]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    expected = set(artifacts)
    existing = {path.name for path in output_root.iterdir() if path.is_file()}
    unexpected = existing - expected
    if unexpected:
        raise PaperReportError(f"unexpected reporting artifacts: {sorted(unexpected)}")
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists() and path.read_bytes() != content:
            raise PaperReportError(f"immutable reporting artifact differs: {name}")
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists():
            continue
        temp = output_root / f".{name}.tmp"
        temp.write_bytes(content)
        os.replace(temp, path)


def code_commit(root: Path, supplied: str | None) -> str:
    value = (
        supplied
        or subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
    )
    if len(value) != 40 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise PaperReportError("code commit must be a full lowercase Git SHA")
    return value


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = args.repository_root.resolve()
    data, sources = load_sources(root)
    artifacts = build_artifacts(data)
    commit = code_commit(root, args.code_commit)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "analysis_identity": ANALYSIS_IDENTITY,
        "code_commit": commit,
        "source_artifacts": sources,
        "generated_artifacts": [
            {
                "path": name,
                "sha256": hashlib.sha256(content).hexdigest(),
                "bytes": len(content),
            }
            for name, content in sorted(artifacts.items())
        ],
        "frozen_methodology_changed": False,
        "network_requests_made": 0,
        "manual_numeric_edits": 0,
    }
    artifacts = {**artifacts, "reproducibility_manifest.json": json_bytes(manifest)}
    publish(root / args.output_root, artifacts)
    return {
        "complete": True,
        "analysis_identity": ANALYSIS_IDENTITY,
        "code_commit": commit,
        "output_root": str(args.output_root),
        "artifact_count": len(artifacts),
        "output_bytes": sum(len(content) for content in artifacts.values()),
        "manifest_sha256": hashlib.sha256(
            artifacts["reproducibility_manifest.json"]
        ).hexdigest(),
        "network_requests_made": 0,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=Path("reports/phase_10g"))
    parser.add_argument("--code-commit")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (PaperReportError, OSError, ValueError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
