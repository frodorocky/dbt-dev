WITH AggregatedData AS (
    SELECT 
        property_type,
        room_type,
        accommodates,
        EXTRACT(MONTH FROM DATE) AS Month,
        EXTRACT(YEAR FROM DATE) AS Year,
        COUNT(CASE WHEN has_availability = 't' THEN 1 END) AS active_listings,
        COUNT(CASE WHEN has_availability = 'f' THEN 1 END) AS inactive_listings,
        MIN(CASE WHEN has_availability = 't' THEN price END) AS min_price_active,
        MAX(CASE WHEN has_availability = 't' THEN price END) AS max_price_active,
        AVG(CASE WHEN has_availability = 't' THEN price END) AS avg_price_active,
        COUNT(DISTINCT host_name) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_name END) AS superhost_count,
        AVG(CASE WHEN has_availability = 't' THEN review_scores_rating END) AS avg_review_scores_rating_active,
        SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) END) AS total_number_of_stays,
        AVG(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price END) AS avg_estimated_revenue_per_active_listing
    FROM
        {{ ref('facts_listing') }} 
    GROUP BY
        property_type, room_type, accommodates, month, year
)

SELECT 
    property_type,
    room_type,
    accommodates,
    month,
    year,
    CASE 
        WHEN (active_listings + inactive_listings) != 0 THEN (active_listings * 100.0 / (active_listings + inactive_listings))
        ELSE NULL
    END AS active_listings_rate,
    min_price_active,
    max_price_active,
    avg_price_active,
    distinct_hosts,
    CASE 
        WHEN distinct_hosts != 0 THEN (superhost_count * 100.0 / distinct_hosts)
        ELSE NULL
    END AS superhost_rate,
    avg_review_scores_rating_active,
    CASE 
        WHEN LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month) != 0 THEN 
            (active_listings - LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month)) * 100.0 / LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month)
        ELSE NULL
    END AS percentage_change_active_listings,
    CASE 
        WHEN LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month) != 0 THEN 
            (inactive_listings - LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month)) * 100.0 / LAG(inactive_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY year, month)
        ELSE NULL
    END AS percentage_change_inactive_listings,
    total_number_of_stays,
    avg_estimated_revenue_per_active_listing
FROM 
    AggregatedData
ORDER BY
    property_type, room_type, accommodates, year, month


