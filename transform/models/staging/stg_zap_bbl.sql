-- One cleaned row per (project, tax lot): the latest ingested snapshot of the ZAP
-- BBL crosswalk, with bbl cast to a real integer so it can join cleanly to PLUTO.

with source as (
    select * from {{ source('bronze', 'zap_bbl') }}
),

latest_per_pair as (
    select
        *,
        row_number() over (
            partition by project_id, bbl
            order by ingest_date desc, _ingested_at desc
        ) as rn
    from source
),

renamed as (
    select
        project_id,
        try_cast(bbl as bigint) as bbl,
        try_cast(validated as boolean) as is_validated,
        try_cast(validated_date as date) as validated_date
    from latest_per_pair
    where rn = 1
)

select * from renamed
