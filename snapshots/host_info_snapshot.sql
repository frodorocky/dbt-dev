{% snapshot host_info_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='HOST_ID',
          updated_at='SCRAPED_DATE',
        )
    }}

select     
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD,
    SCRAPED_DATE
from {{ source('raw', 'listing') }}

{% endsnapshot %}