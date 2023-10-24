WITH AggregatedData AS (
    SELECT 
        s.LGA_NAME AS host_neighbourhood_lga,
        EXTRACT(MONTH FROM DATE) AS Month,
        EXTRACT(YEAR FROM DATE) AS Year,
        COUNT(DISTINCT host_name) AS distinct_hosts,
        SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price END) AS estimated_revenue
    FROM
        {{ ref('facts_listing') }} AS a
    JOIN
        {{ ref('dim_lga_suburb') }} AS s ON a.host_neighbourhood = s.SUBURB_NAME
    GROUP BY
        s.LGA_NAME, month, year
)

SELECT 
    host_neighbourhood_lga,
    month,
    year,
    distinct_hosts,
    estimated_revenue,
    CASE 
        WHEN distinct_hosts != 0 THEN estimated_revenue / distinct_hosts
        ELSE NULL
    END AS estimated_revenue_per_host
FROM 
    AggregatedData
ORDER BY
    host_neighbourhood_lga, year, month

