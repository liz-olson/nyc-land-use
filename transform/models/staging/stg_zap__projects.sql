-- One cleaned row per ZAP project: dates cast to real dates, cc_district cast to
-- an integer, mih_flag cast to a boolean, and only the latest ingested snapshot
-- of each project kept.

with source as (
    select * from {{ source('bronze', 'zap_projects') }}
),

latest_per_project as (
    -- Keep only each project's most recent snapshot: the source has one row per
    -- (project, ingest_date), since every run of `make ingest-zap` re-lands the
    -- whole dataset. Rank each project's rows from newest to oldest and keep rank 1.
    select
        *,
        row_number() over (
            partition by project_id
            order by ingest_date desc, _ingested_at desc
        ) as rn
    from source
),

renamed as (
    -- Rename and type every column. Everything in bronze is text; this is where
    -- real types get assigned. try_cast returns NULL instead of erroring on a
    -- value it can't parse, which is safer than cast() on messy source data.
    select
        project_id,
        trim(project_name) as project_name,
        trim(project_brief) as project_brief,
        trim(borough) as borough,
        try_cast(cc_district as integer) as council_district,
        trim(community_district) as community_district,
        trim(project_status) as project_status,
        trim(public_status) as public_status,
        trim(applicant_type) as applicant_type,
        trim(primary_applicant) as primary_applicant,
        trim(ulurp_numbers) as ulurp_numbers,
        trim(actions) as actions,
        try_cast(mih_flag as boolean) as is_mih,
        try_cast(app_filed_date as date) as app_filed_date,
        try_cast(certified_referred as date) as certified_referred_date,
        try_cast(approval_date as date) as approval_date,
        try_cast(completed_date as date) as completed_date,
        _ingested_at,
        _batch_id
    from latest_per_project
    where rn = 1
)

select * from renamed
