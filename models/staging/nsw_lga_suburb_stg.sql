{{
    config(
        unique_key='LGA_NAME'
    )
}}

with

source  as (

    select * from {{ ref() }}

),

renamed as (
    select
        LGA_NAME,
        SUBURB_NAME,
        ingestion_datetime,
        dbt_scd_id,
        dbt_updated_at,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        'unknown' as LGA_NAME,
        'unknown' as SUBURB_NAME,
        '1900-01-01'::timestamp as ingestion_datetime,
        'unknown' as dbt_scd_id,
        '1900-01-01'::timestamp as dbt_updated_at,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to
)
select * from unknown
union all
select * from renamed