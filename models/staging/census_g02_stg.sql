{{
    config(
        unique_key='LGA_CODE_2016'
    )
}}

with

source  as (

    select * from "postgres"."raw"."census_g02_nsw_lga"

),

renamed as (
    select
        substring(LGA_CODE_2016 FROM '[0-9]+')::integer as LGA_CODE,
        Median_age_persons,
        Median_mortgage_repay_monthly,
        Median_tot_prsnl_inc_weekly,
        Median_rent_weekly,
        Median_tot_fam_inc_weekly,
        Average_num_psns_per_bedroom,
        Median_tot_hhd_inc_weekly,
        Average_household_size
    from source
)

select * from renamed