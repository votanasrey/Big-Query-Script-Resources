
DECLARE period_utc DATE;
SET period_utc = DATE_SUB(CURRENT_DATE(),INTERVAL 3 WEEK);

WITH order_table AS (
    SELECT 
        d.lg_rider_id AS rider_id,
        o.rider.lg_city_uuid,
        o.rider.lg_zone_id AS zone_id,
        DATE(o.created_at_local) AS order_date_local
        ,COUNT(DISTINCT o.order_code) as total_orders
        ,COUNT(DISTINCT d.lg_rider_id) AS total_active_riders
    FROM `fulfillment-dwh-production.pandata_curated.lg_orders` AS o
    LEFT JOIN UNNEST(rider.deliveries) AS d
    WHERE 
        o.global_entity_id = 'FP_KH'
        AND d.status IN ('completed')
        AND DATE(o.created_date_utc) >= period_utc
        AND DATE(o.created_date_local) BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 8 day) AND DATE_SUB(CURRENT_DATE(),INTERVAL 1 day)
    GROUP BY 1,2,3,4
),rider_city_table AS(
    SELECT 
        global_entity_id,
        ct.name AS city_name,
        ct.lg_city_uuid AS city_uuid,
        z.id AS zone_id, 
        z.name AS zone_name
     FROM `fulfillment-dwh-production.pandata_curated.lg_countries` AS country
    ,UNNEST(country.cities) AS ct
    , UNNEST(ct.zones) AS z
    WHERE 
        country.global_entity_id = "FP_KH"
        AND z.is_active
), rider_table AS(
    SELECT
        s.lg_rider_id AS rider_id
        ,s.lg_city_uuid
        ,DATE(s.start_at_local) AS shift_date_local
        ,SUM(s.actual_working_time_in_seconds) AS total_working_time_in_seconds
    FROM  `fulfillment-dwh-production.pandata_curated.lg_shifts` AS s
    WHERE 
        s.global_entity_id = 'FP_KH'
        AND s.state IN ('EVALUATED')
        AND DATE(s.created_date_utc) >= period_utc
        AND DATE(s.start_at_local) <= CURRENT_DATE()
        AND DATE(s.start_at_local) >= period_utc
    GROUP BY 1,2,3
),result_table AS(
    SELECT
        order_table.order_date_local,
        rider_city_table.city_name,
        rider_city_table.zone_name,
        SUM(order_table.total_orders) AS total_orders,
        SUM(order_table.total_active_riders) AS total_active_riders,
        --SUM(rider_table.total_working_time_in_seconds) AS total_working_time_in_seconds,
        --SUM(rider_table.total_working_time_in_seconds)/3600 AS total_working_hours,
        --SUM(order_table.total_orders)/(SUM(rider_table.total_working_time_in_seconds)/3600) AS UTR
    FROM rider_table
    LEFT JOIN order_table 
        ON rider_table.rider_id = order_table.rider_id 
        AND DATE(rider_table.shift_date_local)  =  DATE(order_table.order_date_local)
    LEFT JOIN rider_city_table 
        ON order_table.lg_city_uuid = rider_city_table.city_uuid
        AND order_table.zone_id = rider_city_table.zone_id
    GROUP BY 1,2,3
    ORDER BY 3 
)
SELECT * FROM result_table
WHERE total_orders IS NOT NULL
ORDER BY order_date_local DESC



