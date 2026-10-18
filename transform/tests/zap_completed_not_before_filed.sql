-- Singular test: flags any project whose completed_date is earlier than its
-- app_filed_date. As of Phase 2 there is exactly one known offender (2018K0358,
-- documented in docs/sources/zap_data.md) -- a source-data quirk, not something
-- this pipeline can fix. severity: warn so dbt build still surfaces it on every
-- run without blocking downstream models.
{{ config(severity = 'warn') }}

select
    project_id,
    app_filed_date,
    completed_date
from {{ ref('stg_zap__projects') }}
where completed_date is not null
  and app_filed_date is not null
  and completed_date < app_filed_date
