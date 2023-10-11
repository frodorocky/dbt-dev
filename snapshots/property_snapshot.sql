{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='SCRAPED_DATE',
               )
    }}

select * from {{ source('raw', 'property') }}

{% endsnapshot %}