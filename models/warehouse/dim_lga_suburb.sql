{{
    config(
        unique_key='suburb_name'
    )
}}

select 
    *
    from {{ ref('lga_suburb_stg') }}