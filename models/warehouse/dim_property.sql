{{
    config(
        unique_key='LISTING_ID'
    )
}}

select 
    *
    from {{ ref('property_stg') }}