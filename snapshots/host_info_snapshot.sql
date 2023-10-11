{% snapshot host_info_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='HOST_ID',
          updated_at='SCRAPED_DATE',

        )
    }}

select * from {{ source('raw', 'host_info') }}

{% endsnapshot %}