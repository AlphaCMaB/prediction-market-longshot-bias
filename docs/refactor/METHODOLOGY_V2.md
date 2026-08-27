# Methodology V2

## Why the methodology changed

The original exploratory analysis selected prices relative to realized
platform resolution or settlement timestamps:

- resolution minus 24 hours
- resolution minus 48 hours
- resolution minus 168 hours

For markets whose resolution time was not known in advance, this can use
future information when aligning observations.

Those results are retained as exploratory methodology V1 results, but they
are not the primary analysis.

## Methodology V2 timing structures

Every market family must be assigned one timing structure:

1. fixed_clock
   - Outcome evaluated at a clock or calendar time known in advance.
   - Examples: cryptocurrency price at 11:00 UTC, daily temperature.

2. scheduled_event_start
   - A match, speech, debate, or similar event has a scheduled start time.
   - Prices are measured before scheduled event start, not before realized
     resolution.

3. scheduled_window
   - A tournament or multi-day event has a known window but not one clean
     resolution time.

4. deadline_window
   - An event can occur at any time before a known deadline.

5. endogenous_subevent
   - A set, map, hole, or similar event occurs at an unknown time within a
     larger event.

6. unclear
   - Available metadata is insufficient.

## Anchor priority

Preferred anchor sources:

1. market occurrence_datetime
2. verified event strike_date or scheduled start
3. explicit timestamp from contract rules
4. manual verification

Do not use contract close_time as a universal event anchor.

## Current clean analysis candidates

Strict primary analysis:

- fixed_clock
- 1 hour before occurrence time

Exploratory scheduled-event analysis:

- scheduled_event_start
- 1 hour before start
- 6 hours before start
- 12 hours before start

## Required statistical treatment

- Market contracts are not independent when they belong to one event family.
- Uncertainty must be clustered or bootstrapped by family.
- Important probability bins should contain enough independent families.
- Kelly analysis must not be run until calibration results survive
  family-level bootstrap and sensitivity tests.

## Repository rules

- Never overwrite raw API responses.
- Keep methodology V1 scripts and outputs under legacy directories.
- Keep transition and audit scripts for documentation.
- Build a smaller production pipeline for methodology V2.
- Do not run large API pulls during the refactor.
