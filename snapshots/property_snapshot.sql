{% snapshot property_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='LISTING_ID',
          updated_at='SCRAPED_DATE',
               )
    }}

select 
  LISTING_ID,
	LISTING_NEIGHBOURHOOD,
  PROPERTY_TYPE,
  ROOM_TYPE,
  ACCOMMODATES,
  SCRAPED_DATE
from {{ source('raw', 'listing') }}

{% endsnapshot %}