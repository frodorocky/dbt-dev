{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source  as (

    select * from {{ ref('property_snapshot') }}

),

renamed as (
    select
        LISTING_ID,
        TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD') as DATE,
        upper(LISTING_NEIGHBOURHOOD) as LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        dbt_scd_id,
        case when dbt_updated_at = (select min(dbt_updated_at) from source) 
            then '1900-01-01'::timestamp 
            else dbt_updated_at::timestamp 
        end as dbt_updated_at,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) 
            then '1900-01-01'::timestamp 
            else dbt_valid_from::timestamp 
        end as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        0 as LISTING_ID,
        NULL::timestamp as DATE,
        'unknown' as LISTING_NEIGHBOURHOOD,
        'unknown' as PROPERTY_TYPE,
        'unknown' as ROOM_TYPE,
        NULL::int8 as ACCOMMODATES,
        'unknown' as dbt_scd_id,
        '1900-01-01'::timestamp as dbt_updated_at,
        '1900-01-01'::timestamp as dbt_valid_from,
        NULL as dbt_valid_to
)
select * from unknown
union all
select * from renamed