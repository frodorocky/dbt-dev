{{
    config(
        unique_key='LGA_NAME'
    )
}}

select * from {{ ref('nsw_lga_suburb_stg') }}