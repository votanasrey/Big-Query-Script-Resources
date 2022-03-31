

DECLARE start_date, period_time DATE;
SET period_time = DATE_SUB(CURRENT_DATE(),INTERVAL 91 DAY);
SET start_date = '2022-02-28';


WITH rider_info_table AS (
    SELECT
        r.lg_country_code
        ,r.id AS rider_id
        ,DATE(r.created_at_local) AS rider_register_date
        ,DATE(s.start_at_local) created_at_local
        ,s.state
        ,c.lg_city_uuid As city_uuid
        ,SUM(s.planned_shift_duration_in_seconds) AS total_working_time_in_seconds
        ,COUNT(DISTINCT s.uuid) AS total_shifts

    FROM fulfillment-dwh-production.pandata_curated.lg_riders AS r
        LEFT JOIN UNNEST(contracts) AS c
    LEFT JOIN fulfillment-dwh-production.pandata_curated.lg_shifts AS s 
        ON r.lg_country_code = s.lg_country_code
        AND r.id = s.lg_rider_id
    WHERE 
        r.global_entity_id = 'FP_KH'
        AND s.global_entity_id = 'FP_KH'
        -- valid shift of riders
        AND s.state IN ('EVALUATED')
        -- contract status is valid
        AND c.status IN ("VALID")
        AND r.created_at_local >= start_date
        AND s.created_at_local >= start_date
        AND s.created_date_utc >= start_date

    GROUP BY 1,2,3,4,5,6
), rider_city_table AS(
    SELECT 
        city.lg_city_uuid AS city_uuid
        ,city.name AS city_name
     FROM fulfillment-dwh-production.pandata_curated.lg_countries AS country
    ,UNNEST(country.cities) AS city
    WHERE 
        country.global_entity_id = "FP_KH"
),rider_first_active_table_1 AS(
    SELECT 
        s.lg_rider_id AS rider_id
        ,DATE(start_at_local) AS first_shift_date
        ,ROW_NUMBER() OVER(PARTITION BY s.lg_rider_id ORDER BY start_at_local) AS rider_first_shift_date
    FROM fulfillment-dwh-production.pandata_curated.lg_shifts AS s
    WHERE 
        s.global_entity_id = "FP_KH"
        AND s.created_at_local >= start_date
        AND s.created_date_utc >= start_date
        AND s.start_at_local <= CURRENT_DATE()
),  rider_first_active_table_2 AS(
    SELECT 
        rider.rider_id
        ,rider.first_shift_date
    FROM rider_first_active_table_1 AS rider
    WHERE 
        rider.rider_first_shift_date = 1
    GROUP BY 1,2
),  rider_first_active_table_3 AS(
     SELECT 
        s.lg_rider_id AS rider_id
        ,COUNT(DISTINCT DATE(s.start_at_local)) AS total_working_days
    FROM fulfillment-dwh-production.pandata_curated.lg_shifts AS s
    WHERE 
        s.global_entity_id = "FP_KH"
        AND s.created_at_local >= start_date
        AND s.created_date_utc >= start_date
        AND s.start_at_local <= CURRENT_DATE()
    GROUP BY 1
), rider_table AS(
    SELECT 
        r1.rider_id 
        ,r1.rider_register_date 
        ,r2.first_shift_date
        ,DATE(r1.created_at_local) as shift_date
        ,r4.city_name
        ,r3.total_working_days
        ,r1.state
        ,r1.total_working_time_in_seconds
        ,r1.total_shifts
    FROM rider_info_table AS r1
    LEFT JOIN rider_first_active_table_2 AS r2
        ON r1.rider_id = r2.rider_id
    LEFT JOIN rider_first_active_table_3 AS r3
        ON r1.rider_id = r3.rider_id
    LEFT JOIN rider_city_table AS r4
        ON r1.city_uuid = r4.city_uuid 

), order_result_table AS (
    SELECT 
        d.lg_rider_id AS rider_id
        ,DATE(o.created_at_local) AS order_date_local
        , COUNT(DISTINCT o.order_code) as total_orders
    FROM fulfillment-dwh-production.pandata_curated.lg_orders AS o
    LEFT JOIN UNNEST(rider.deliveries) AS d
    WHERE 
        o.global_entity_id = 'FP_KH'
        AND d.status IN ('completed')
        AND o.created_date_utc >= period_time
    GROUP BY 1,2
), result_table AS(
    SELECT 
        r.rider_id
        ,DATE(r.rider_register_date) AS rider_register_date
        ,r.first_shift_date
        ,r.city_name

        ,(CASE 
            WHEN DATE(r.shift_date) < (DATE(r.first_shift_date) + 7) THEN "1. 0-7" 
            WHEN DATE(r.shift_date) < (DATE(r.first_shift_date) + 15) THEN "2. 0-15"
        END) AS period_date
    
        ,DATE_DIFF(CURRENT_DATE(), r.first_shift_date, DAY) AS total_working_days_since_first_shift
        ,r.total_working_days
        ,IFNULL(SUM(o.total_orders),0) / r.total_working_days AS avg_orders_per_day

        --,SUM(r.total_working_time_in_seconds) as total_shift_time_in_sec
        ,SUM(r.total_working_time_in_seconds/3600) as total_shift_time_in_hour
        ,SUM(r.total_shifts) as total_shifts
        ,IFNULL(SUM(o.total_orders),0) AS total_orders
    FROM rider_table AS r
    LEFT JOIN order_result_table AS o
        ON r.rider_id = o.rider_id
        AND DATE(r.shift_date) = DATE(o.order_date_local)
    GROUP BY 1,2,3,4,5,6,7
) 
--SELECT * FROM result_table 
SELECT 
    rider_id,
    rider_register_date,
    first_shift_date,
    city_name,
    period_date,
    total_working_days_since_first_shift,
    total_working_days,
    SUM(total_shift_time_in_hour) OVER (PARTITION BY rider_id ORDER BY period_date ASC) AS total_shift_time_in_hour,
    SUM(total_shifts) OVER (PARTITION BY rider_id ORDER BY period_date ASC) AS total_shifts,
    SUM(total_orders) OVER (PARTITION BY rider_id ORDER BY period_date ASC) AS total_orders,
FROM result_table
WHERE period_date IS NOT NULL
    AND city_name IN ('Siem reap', 'Battambang')



