
DECLARE
    start_date, end_date DATE;
SET
    start_date = "2021-05-01";  --- can change start_date
SET 
    end_date = CURRENT_DATE(); --- can change start_date

SELECT 
    v.vendor_code,
    v.name AS vendor_name,
    SUM(a.gmv_local) AS total_gmv_local
FROM  `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
    ON o.vendor_code = v.vendor_code
    AND o.global_entity_id = v.global_entity_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a
    ON o.uuid = a.uuid
    AND o.global_entity_id = a.global_entity_id
WHERE 
    v.global_entity_id = 'FP_KH'
    AND a.global_entity_id = "FP_KH"
    AND o.global_entity_id = 'FP_KH'
    AND v.is_active IS TRUE
    AND v.is_private IS FALSE
    AND v.is_test IS FALSE
    AND a.created_date_local BETWEEN start_date AND end_date
    AND o.created_date_utc BETWEEN start_date AND DATE_ADD(end_date, INTERVAL 1 DAY)
    AND a.created_date_utc BETWEEN start_date AND DATE_ADD(end_date, INTERVAL 1 DAY)
GROUP BY 1,2
ORDER BY 1


