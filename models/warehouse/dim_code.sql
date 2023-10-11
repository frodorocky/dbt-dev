{{
    config(
        unique_key='LGA_CODE'
    )
}}

select * from {{ ref('nsw_lga_code_stg') }}