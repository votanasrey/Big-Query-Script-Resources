

DECLARE date1 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);

WITH failed_types_table AS (													
    SELECT 
        v.global_entity_id, 
        v.vendor_code,
        v.name,
        v.chain_code,
        v.chain_name,
        v.is_active,
        v.is_private,
        v.is_test,
        COUNT(DISTINCT o.uuid) AS total_orders,
        COUNT(DISTINCT CASE WHEN o.is_valid_order IS FALSE THEN o.uuid END) AS total_failed_orders,
        COUNT(DISTINCT CASE WHEN o.is_failed_order_vendor IS TRUE THEN o.uuid END) AS total_vendor_failed,
        COUNT(DISTINCT CASE WHEN o.is_failed_order_customer IS TRUE THEN o.uuid END) AS total_customer_failed,
        COUNT(DISTINCT CASE WHEN o.is_failed_order_foodpanda IS TRUE THEN o.uuid END) AS total_foodpanda_failed
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v													
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS o													
    ON v.global_entity_id = o.global_entity_id 
    AND v.vendor_code = o.vendor_code 
    AND o.created_date_utc < CURRENT_DATE() 
    AND o.created_date_local >= date1
WHERE 
    v.global_entity_id = 'FP_KH'													
    AND v.is_active
    AND NOT v.is_test
    AND NOT v.is_private
GROUP BY 1,2,3,4,5,6,7,8
), result_table AS(
    SELECT 
        global_entity_id, 
        vendor_code,
        name,
        chain_code,
        chain_name,
        is_active,
        is_private,
        is_test,
        ROUND(SAFE_DIVIDE(total_vendor_failed,total_orders),5) AS vendor_failed_rate,
        ROUND(SAFE_DIVIDE(total_customer_failed,total_orders),5) AS customer_failed_rate,
        ROUND(SAFE_DIVIDE(total_foodpanda_failed, total_orders),5) AS foodpanda_failed_rate,
    FROM failed_types_table
)
SELECT * FROM result_table 
