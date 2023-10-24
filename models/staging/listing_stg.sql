{{
    config(
        unique_key='LISTING_ID'
    )
}}

with

source  as (

    select * from "postgres"."raw"."listing"

),

renamed as (
    select
        LISTING_ID,
        upper(LISTING_NEIGHBOURHOOD) as LISTING_NEIGHBOURHOOD,
        HOST_ID,
        TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD') as DATE,
        PRICE,
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE
    from source
)

select * from renamed