{{
    config(
        unique_key='SUBURB_NAME'
    )
}}

with

source  as (

    select * from "postgres"."raw"."nsw_lga_suburb"

),

renamed as (
    select
        LGA_NAME,
        SUBURB_NAME
    from source
)

select * from renamed