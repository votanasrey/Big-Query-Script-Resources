
DECLARE period DATE;
SET period = DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK);

WITH rider_shift_creating_table AS(
    SELECT 
        shift.created_date_local AS shift_created_date_local, 
        shift.lg_rider_uuid,
        shift.lg_city_uuid AS city_uuid,
        COUNT(DISTINCT shift.uuid) AS total_shift,
        COUNTIF(shift.created_by_lg_user_uuid != "0_kh") AS rider_own_created_shift,
        COUNTIF(shift.created_by_lg_user_id = 0) AS automation_created_shift,
    FROM `fulfillment-dwh-production.pandata_curated.lg_shifts` AS shift
    WHERE 
        shift.global_entity_id = 'FP_KH'
        -- selected only evaluated state of the shift
        AND shift.state IN ("EVALUATED") 
        AND shift.created_date_utc >= period
        AND shift.end_at_local IS NOT NULL
        AND shift.start_at_local IS NOT NULL
    GROUP BY 1,2,3
), country_table AS(
    SELECT 
        cities.lg_city_uuid AS city_uuid,
        cities.name AS city_name
    FROM `fulfillment-dwh-production.pandata_curated.lg_countries` AS c
    LEFT JOIN UNNEST(cities) AS cities
    WHERE   
        c.global_entity_id = 'FP_KH'
), result_table AS(
    SELECT 
        r.shift_created_date_local,
        r.lg_rider_uuid,
        c.city_name,
        r.total_shift,
        r.rider_own_created_shift,
        r.automation_created_shift
    FROM rider_shift_creating_table AS r
    LEFT JOIN country_table AS c
        ON r.city_uuid = c.city_uuid
)
SELECT * FROM result_table
--WHERE creator_shift_uuid = '0_kh'
ORDER BY shift_created_date_local, automation_created_shift DESC 


