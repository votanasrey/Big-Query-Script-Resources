
DECLARE period DATE;
SET period = DATE_TRUNC(CURRENT_DATE(), MONTH);

WITH vendors_table AS(
    SELECT DISTINCT
        vendors.vendor_code,
        vendors.name AS vendor_name,
        ROUND(SUM(accounting.gmv_local),2) AS total_gmv_local,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
        ON vendors.vendor_code = orders.vendor_code
        AND vendors.global_entity_id = orders.global_entity_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
        ON orders.uuid = accounting.uuid
        AND orders.global_entity_id = accounting.global_entity_id
    WHERE 
        vendors.global_entity_id = 'FP_KH'
        AND vendors.is_active
        AND NOT vendors.is_private
        AND NOT vendors.is_test
        AND NOT vendors.vertical_type IN ('darkstores')
        AND accounting.created_date_local >= period
        AND orders.created_date_utc >= period
        AND accounting.created_date_utc >= period
    GROUP BY 1,2
        QUALIFY ROW_NUMBER() OVER(PARTITION BY vendor_code ORDER BY total_gmv_local) <= 10
    ORDER BY total_gmv_local DESC 
    --LIMIT 10
)
SELECT * FROM vendors_table 


