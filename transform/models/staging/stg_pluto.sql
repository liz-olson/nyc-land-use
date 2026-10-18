-- One cleaned row per NYC tax lot: the latest ingested snapshot of PLUTO, trimmed to
-- the columns this project currently uses (geography + a few descriptive fields).
-- PLUTO's own `bbl` column comes back from the API as a float-formatted string, e.g.
-- '1000010010.00000000' -- cast through double first, then to bigint, to get the
-- same clean integer that stg_zap_bbl's bbl column has.

with source as (
    select * from {{ source('bronze', 'pluto') }}
),

latest_per_lot as (
    select
        *,
        row_number() over (
            partition by bbl
            order by ingest_date desc, _ingested_at desc
        ) as rn
    from source
),

renamed as (
    select
        try_cast(try_cast(bbl as double) as bigint) as bbl,
        trim(borough) as borough,
        try_cast(block as integer) as block,
        try_cast(lot as integer) as lot,
        try_cast(cd as integer) as community_district,
        try_cast(council as integer) as council_district,
        try_cast(latitude as double) as latitude,
        try_cast(longitude as double) as longitude,
        trim(address) as address,
        trim(landuse) as landuse_code,
        try_cast(unitsres as integer) as residential_units,
        try_cast(yearbuilt as integer) as year_built
    from latest_per_lot
    where rn = 1
)

select * from renamed
