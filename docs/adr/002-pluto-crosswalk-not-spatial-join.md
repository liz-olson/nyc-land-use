# ADR-002: Resolve project geography via the PLUTO crosswalk, not a spatial join

Status: accepted · Date: 2026-10-18

## Context

Phase 3's goal is to place every ZAP project in a council district and a community
district, and on a map. The obvious approach is a point-in-polygon spatial join: ingest
the City Council District and Community District boundary files (GeoJSON/MultiPolygon)
from NYC Open Data, load DuckDB's `spatial` extension, and join each project's lat/lon
(or lot geometry) against those polygons.

NYC's own PLUTO dataset (one row per tax lot) already carries `council` and `cd`
(community district) columns, assigned by the City itself, for every lot. ZAP already
publishes a BBL crosswalk (`project_id` <-> `bbl`) with no district information of its
own. So the same answer is reachable two ways: compute it geometrically, or look it up
through two table joins.

## Decision

Join ZAP's BBL crosswalk to PLUTO on `bbl`, and use PLUTO's own `council`/`cd` columns
as each lot's district. No boundary files are ingested, and DuckDB's `spatial`
extension is not used anywhere in this pipeline.

## Alternatives considered

- **Spatial join against council/community district boundary files:** more "correct" in
  the sense of being independently verifiable and not dependent on PLUTO having already
  done the assignment, and it's the only option for data that has no BBL at all (e.g. a
  bare address or a point). But it adds a whole ingestion source, a DuckDB extension,
  and a point-in-polygon step, for an answer PLUTO already contains for every one of the
  ~97% of projects that resolve to at least one lot. Not worth the extra moving parts
  for this project's actual data.

## Consequences

- Simpler pipeline: no boundary-file source, no `spatial` extension, no geometry
  predicates to get right.
- A real limitation: PLUTO's `council`/`cd` columns reflect **current** district lines,
  not the lines in force when a given ZAP project was filed. After a redistricting
  (council lines were last redrawn in 2023), an old project's district in this pipeline
  is the one it would be in today, not the one it was actually reviewed under. This is
  acceptable for this project's purposes (mapping where projects are, not reconstructing
  historical ULURP jurisdiction); revisit if that distinction becomes load-bearing.
- A project only gets a district if ZAP has logged at least one BBL for it, and that BBL
  still matches a lot in PLUTO's current inventory. As of this build, 8,389 of 32,964
  projects (25%) have no BBL on file at all -- almost all (8,378 of 8,389) are old,
  closed-out projects (Complete/Terminated/Withdrawn), consistent with ZAP's BBL
  tracking not having existed, or not having been backfilled, for them. A further 1,923
  projects have a BBL that doesn't match any current PLUTO lot, most plausibly because
  the lot has since been merged, subdivided, or renumbered. Together this leaves 68.6%
  of all projects (97% of projects that have a BBL at all) located.
- If this pipeline ever needs point-level geography for records with no BBL (e.g. a
  future source keyed by address or raw coordinates instead of lot), that record would
  need its own geocoding step -- PLUTO's crosswalk can't help it, since it has nothing
  to join on.

## Addendum: a self-reported fallback for projects with no resolved lot (2026-10-18)

The 32% of projects with no council district turned out not to need a spatial join
either -- they need no BBL at all, so there's no point to run point-in-polygon
math against. But most of them (8,266 of ~10,357 unlocated projects) have ZAP's own
free-text `community_district` field populated, in the same letter-plus-two-digit
scheme PLUTO's `cd` column uses (just spelled with a borough letter instead of a
digit). `int_project_geography` now parses that field as a fallback **community**
district (never a council district -- ZAP doesn't self-report those, and the two
don't map one-to-one) when it holds a single, unambiguous code; a comma/semicolon
list of several districts, or ZAP's own "00" / "CY00" not-applicable sentinel, is
left unlocated rather than guessed at. This raised the share of projects with at
least a community district from 68.6% to 93.7%, with `community_district_is_self_reported`
marking which rows came from the fallback instead of a resolved lot.
