{{
    config(
        unique_key='HOST_ID'
    )
}}

select 
    *
    from {{ ref('host_info_stg') }}