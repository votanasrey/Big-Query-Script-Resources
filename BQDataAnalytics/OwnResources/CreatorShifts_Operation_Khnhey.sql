


DECLARE period DATE;
SET period = DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH);

WITH rider_shift_creating_table AS(
    SELECT 
        shift.created_by_lg_user_uuid AS shift_creator_uuid,
        shift.created_by_lg_user_id AS shift_creator_id,
        shift.tag,
        shift.lg_rider_uuid,
        shift.uuid AS shift_uuid,
        shift.lg_city_uuid AS city_uuid,
        shift.created_date_local AS shift_created_date_local
    FROM `fulfillment-dwh-production.pandata_curated.lg_shifts` AS shift
    WHERE 
        shift.global_entity_id = 'FP_KH'
        -- selected only evaluated state of the shift
        AND shift.state IN ("EVALUATED") 
        AND shift.created_date_utc >= period
        AND shift.end_at_local IS NOT NULL
        AND shift.start_at_local IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7

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
        r.shift_creator_uuid,
        r.shift_creator_id,
        r.shift_uuid,
        r.lg_rider_uuid,
        r.tag AS shift_tag,
        c.city_name,
        r.shift_created_date_local,
    FROM rider_shift_creating_table AS r
    LEFT JOIN country_table AS c
        ON r.city_uuid = c.city_uuid
)
SELECT * FROM result_table 
WHERE shift_tag = 'HURRIER' 


--WHERE shift_creator_id = 294192
--shift_creator_uuid FROM result_table
--WHERE shift_creator_uuid = '271231_kh'
--WHERE shift_creator_uuid != lg_rider_uuid AND shift_creator_uuid NOT IN ('0_kh')
--ORDER BY shift_created_date_local DESC 


