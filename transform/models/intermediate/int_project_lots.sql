-- One row per (project, tax lot), restricted to:
--   * projects that actually exist in stg_zap__projects (guards against a BBL
--     crosswalk row pointing at a project_id that isn't in the main projects table)
--   * rows where bbl was actually resolved (a project can have an unresolved
--     crosswalk row with bbl = null -- see stg_zap_bbl's description -- which
--     can't join to anything and isn't a usable lot)

select
    bbl.project_id,
    bbl.bbl,
    bbl.is_validated,
    bbl.validated_date
from {{ ref('stg_zap_bbl') }} as bbl
inner join {{ ref('stg_zap__projects') }} as projects
    on bbl.project_id = projects.project_id
where bbl.bbl is not null
