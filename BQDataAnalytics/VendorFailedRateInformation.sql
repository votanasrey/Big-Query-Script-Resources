

WITH data_table AS (													
    SELECT 
    v.global_entity_id, 
    v.vendor_code,
    v.name,
    v.chain_code,
    v.chain_name,													
    v.is_active,													
    v.is_private,													
    v.is_test,													
    COUNT(DISTINCT CASE WHEN is_gross_order IS TRUE THEN o.uuid END) AS L90D_total_gross_orders,												
    COUNT(DISTINCT CASE WHEN is_gross_order IS TRUE AND is_valid_order IS TRUE THEN o.uuid END) AS L90D_total_valid_orders,												
    COUNT(DISTINCT CASE WHEN is_gross_order IS TRUE AND is_valid_order IS FALSE THEN o.uuid END) AS L90D_total_vendor_fails														
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v													
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS o													
    ON v.global_entity_id = o.global_entity_id 
    AND v.vendor_code = o.vendor_code AND o.created_date_utc < CURRENT_DATE 
    AND o.created_date_local >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)													
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS salesforce_accounts													
    ON v.global_entity_id = salesforce_accounts.global_entity_id 
    AND v.vendor_code = salesforce_accounts.vendor_code													
WHERE 
    v.global_entity_id = 'FP_KH'													
    AND v.is_active IS TRUE and v.is_test IS FALSE													
GROUP BY 1,2,3,4,5,6,7,8
)													
SELECT * ,
    ROUND(SAFE_DIVIDE(L90D_total_vendor_fails, L90D_total_gross_orders),5) AS L90D_vendor_fail_rate						
FROM data_table													
													
													
													