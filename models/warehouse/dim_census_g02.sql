{{
    config(
        unique_key='LGA_CODE'
    )
}}

select * from {{ ref('census_g02_nsw_lga_stg') }}