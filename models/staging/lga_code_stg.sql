{{
    config(
        unique_key='LGA_CODE'
    )
}}

with

source  as (

    select * from "postgres"."raw"."nsw_lga_code"

),

renamed as (
    select
        LGA_CODE,
        UPPER(LGA_NAME) as LGA_NAME
    from source
)

select * from renamed