{{
    config(
        unique_key='HOST_ID'
    )
}}

select 
    HOST_ID,
    HOST_NAME,
    HOST_SINCE
    HOST_IS_SUPERHOSTIsSuperhost,
    HOST_NEIGHBOURHOOD,
    ingestion_datetime DATETIME,
    dbt_scd_id INT AUTO_INCREMENT PRIMARY KEY,
    dbt_updated_at DATETIME,
    dbt_valid_from DATETIME,
    dbt_valid_to DATETIME 
    from {{ ref('airbnb_listing_stg') }}