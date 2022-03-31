

DECLARE period DATE;
SET period = DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH);
WITH new_vertical_table AS(
    SELECT 
        nv.global_entity_id,
        nv.vendor_code,
        nv.vendor_name,
        nv.vendor_type,
        nv.chain_code,
        nv.chain_name,
        SUM(nv.num_of_orders) AS total_orders,
        SUM(nv.valid_orders) AS total_valid_orders,
        SUM(nv.failed_orders) AS total_failed_orders,
        ROUND(SUM(nv.gmv_local),2) AS total_gmv_local,
        ROUND(SUM(nv.gfv_local),2) AS total_gfv_local,
        ROUND(SUM(nv.discount_value_local),2) AS total_discount_value_local
    FROM `fulfillment-dwh-production.pandata_report.new_verticals_daily_order` AS nv
    WHERE 
        nv.global_entity_id = 'FP_KH'
    GROUP BY 1,2,3,4,5,6
)
SELECT * FROM new_vertical_table
--WHERE total_gmv_local > 20000
--WHERE vendor_code IN (SELECT vendor_code FROM new_vertical_table GROUP BY 1 HAVING COUNT(1)>1)

