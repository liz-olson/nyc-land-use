-- Singular test: fails if it returns any rows. Checks that no project's
-- completed_date is earlier than its app_filed_date.
select
    project_id,
    app_filed_date,
    completed_date
from {{ ref('stg_zap__projects') }}
where completed_date is not null
  and app_filed_date is not null
  and completed_date < app_filed_date
