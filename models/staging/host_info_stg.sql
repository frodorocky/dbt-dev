{{
    config(
        unique_key='HOST_ID'
    )
}}

with

source  as (

    select * from {{ ref('host_info_snapshot') }}

),

renamed as (
    select
        HOST_ID,
        TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD') as DATE,
        HOST_NAME,
        HOST_SINCE,
        HOST_IS_SUPERHOST,
        upper(HOST_NEIGHBOURHOOD) as HOST_NEIGHBOURHOOD,
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
        0 as HOST_ID,
        null::timestamp as DATE,
        'unknown' as HOST_NAME,
        'unknown' as HOST_SINCE,
        'unknown' as HOST_IS_SUPERHOST,
        'unknown' as HOST_NEIGHBOURHOOD,
        'unknown' as dbt_scd_id,
        '1900-01-01'::timestamp as dbt_updated_at,
        '1900-01-01'::timestamp as dbt_valid_from,
        NULL as dbt_valid_to
)
select * from unknown
union all
select * from renamed