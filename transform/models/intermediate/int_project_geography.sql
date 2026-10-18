-- One row per project, with a single council/community district resolved from its
-- linked tax lots -- and, for projects with no resolved lot at all, a fallback
-- community district parsed from ZAP's own self-reported community_district field.
--
-- A project can span multiple lots (a large development combining adjacent parcels),
-- and those lots can -- rarely -- fall in different council districts. Rather than
-- emit multiple rows per project (which would break "every project has exactly one
-- district" downstream), this picks the district shared by the most lots as that
-- project's district, and flags projects where lots disagree so that's visible
-- instead of silently picked for you.
--
-- About 25% of projects have no BBL on file at all (mostly old, closed-out projects),
-- so no PLUTO lot and no coordinates. Many of those DO have ZAP's own free-text
-- community_district field populated (e.g. "M05", "K12"), which is a one-letter
-- borough code + 2-digit district number -- the same scheme PLUTO's own cd column
-- uses, just spelled with a letter instead of a borough digit (K=Brooklyn=3,
-- M=Manhattan=1, Q=Queens=4, R=Staten Island=5, X=Bronx=2). That field is inconsistent
-- (sometimes a clean single code, sometimes a comma- or semicolon-separated list of
-- several districts, sometimes a "00" sentinel meaning "not applicable"), so this only
-- trusts the clean single-code case and leaves the rest unlocated rather than guessing.
-- This fallback can only ever supply a COMMUNITY district, never a council district --
-- ZAP doesn't self-report council districts, and the two don't map one-to-one.
--
-- Every project in stg_zap__projects gets exactly one output row here, whether or not
-- it has any lots at all -- that's the whole reason this starts from stg_zap__projects
-- rather than from int_project_lots.

with lots_with_district as (
    select
        lots.project_id,
        lots.bbl,
        pluto.council_district,
        pluto.community_district,
        pluto.latitude,
        pluto.longitude
    from {{ ref('int_project_lots') }} as lots
    left join {{ ref('stg_pluto') }} as pluto
        on lots.bbl = pluto.bbl
),

project_lot_counts as (
    select
        project_id,
        count(*) as total_lots,
        count(*) filter (where council_district is not null) as located_lots,
        count(distinct council_district) as distinct_council_districts
    from lots_with_district
    group by project_id
),

district_counts as (
    select
        project_id,
        council_district,
        community_district,
        count(*) as n_lots,
        avg(latitude) as avg_latitude,
        avg(longitude) as avg_longitude
    from lots_with_district
    where council_district is not null
    group by project_id, council_district, community_district
),

-- Rank each project's candidate districts by how many lots back it; ties broken by
-- district number, just so the result is deterministic rather than arbitrary.
ranked as (
    select
        *,
        row_number() over (
            partition by project_id
            order by n_lots desc, council_district
        ) as rn
    from district_counts
),

-- Parse ZAP's own self-reported community_district field, trusting only a single,
-- clean "letter + 2 digits" value (not a sentinel "00" and not a comma/semicolon list
-- of several districts -- those genuinely don't name one district).
self_reported as (
    select
        project_id,
        case trim(community_district)[1:1]
            when 'M' then 100
            when 'X' then 200
            when 'K' then 300
            when 'Q' then 400
            when 'R' then 500
        end + try_cast(trim(community_district)[2:3] as integer) as self_reported_community_district
    from {{ ref('stg_zap__projects') }}
    where regexp_matches(trim(community_district), '^[MXKQR][0-9]{2}$')
      and trim(community_district)[2:3] != '00'
)

select
    projects.project_id,
    ranked.council_district,
    coalesce(ranked.community_district, self_reported.self_reported_community_district) as community_district,
    ranked.avg_latitude as latitude,
    ranked.avg_longitude as longitude,
    coalesce(counts.total_lots, 0) as total_lots,
    coalesce(counts.located_lots, 0) as located_lots,
    coalesce(counts.distinct_council_districts, 0) > 1 as spans_multiple_council_districts,
    (ranked.community_district is null and self_reported.self_reported_community_district is not null)
        as community_district_is_self_reported
from {{ ref('stg_zap__projects') }} as projects
left join project_lot_counts as counts
    on counts.project_id = projects.project_id
left join ranked
    on ranked.project_id = projects.project_id
    and ranked.rn = 1
left join self_reported
    on self_reported.project_id = projects.project_id
