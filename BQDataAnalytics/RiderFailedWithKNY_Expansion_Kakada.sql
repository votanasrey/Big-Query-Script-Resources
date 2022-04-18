

WITH failed_rider_table AS (													
    SELECT 
        v.global_entity_id, 
        v.vendor_code,
        v.name,
        v.location.city,
        v.is_active,
        v.is_private,
        v.is_test,
        COUNT(DISTINCT CASE WHEN o.is_failed_order_foodpanda IS TRUE THEN o.uuid END) AS total_riders_failed,
        ROUND(SUM(DISTINCT CASE WHEN o.is_failed_order_foodpanda IS TRUE THEN a.gmv_local END),2) AS total_riders_failed_gmv,
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v													
    ON o.global_entity_id = v.global_entity_id 
    AND o.vendor_code = v.vendor_code 
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a
    ON o.uuid = a.uuid
    AND o.global_entity_id = a.global_entity_id
    AND DATE(o.created_date_local) = DATE(a.created_date_local)
WHERE 
    v.global_entity_id = 'FP_KH'													
    AND o.created_date_utc BETWEEN '2022-04-14' AND '2022-04-16' 
    AND o.created_date_local BETWEEN '2022-04-14' AND '2022-04-16'
    AND a.created_date_utc BETWEEN '2022-04-14' AND '2022-04-16' 
    AND a.created_date_local BETWEEN '2022-04-14' AND '2022-04-16'
    AND v.is_active
    AND NOT v.is_test
    AND NOT v.is_private
GROUP BY 1,2,3,4,5,6,7
)
SELECT * FROM failed_rider_table ORDER BY total_riders_failed DESC
