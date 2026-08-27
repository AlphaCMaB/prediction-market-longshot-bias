# Methodology V2 Transition Audits and Prototypes

This directory preserves the scripts that recorded the methodological
transition from retrospective resolution or settlement anchors to ex-ante
occurrence anchors. They are retained for research provenance and should not
be used as the current production pipeline.

Scripts 17–24 document the transition and its audits. In particular:

- Both script-17 variants preserve the early anchor classification and review
  history.
- Script 19 is a superseded close-time/fixed-time branch.
- Scripts 20–24 developed the event-metadata, occurrence-anchor, timing-split,
  anomaly-audit, and corrected horizon-manifest workflow.

The production-shaped scripts 25–26 are retained under
`superseded_prototypes/`. Their responsibilities are now implemented by the
side-effect-free modules and runnable stages under `scripts/pipeline_v2/`.

The pilot script under `pilots/` is preserved as evidence of the first
40-family API test and the path from pilot extraction to the production-shaped
prototype.

All archived files retain their historical repository-relative data paths and
calculations. Existing raw data, processed data, manual review files, and
outputs have not been moved. The files were archived with `git mv` to preserve
Git history, and no wrapper scripts replace them.
