{{
    config(
        unique_key='LGA_CODE'
    )
}}

select 
    *
    from {{ ref('lga_code_stg') }}