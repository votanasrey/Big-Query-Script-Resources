

DECLARE
  start_date, end_date DATE;
SET
  start_date = "2021-12-01"; --- can change start_date
SET 
    end_date = "2021-12-31"; --- can change end_date

WITH vendor_table AS(
    SELECT 
        vendor.vendor_code,
        vendor.name AS vendor_name,
        vendor.location.city AS vendor_city,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendor
    WHERE 
        vendor.global_entity_id = 'FP_KH'
        AND vendor.is_active IS TRUE
        AND vendor.is_private IS FALSE
        AND vendor.is_test IS FALSE
), gfv_table AS(
    SELECT
        orders.vendor_code,
        SUM(CASE WHEN orders.is_valid_order IS TRUE THEN accounting.gfv_local END) AS total_gfv_local,
        COUNT(DISTINCT orders.code) AS total_orders,
        COUNT(CASE WHEN orders.is_valid_order IS TRUE THEN orders.code END) AS total_valid_orders,
        COUNT(CASE WHEN orders.is_valid_order IS FALSE THEN orders.code END) AS total_failed_orders, 
    FROM
        `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
       ON orders.uuid = accounting.uuid
    WHERE
        orders.created_date_utc BETWEEN start_date AND DATE_ADD(end_date, INTERVAL 1 DAY)
        AND orders.created_date_local BETWEEN start_date AND end_date
        AND accounting.created_date_utc BETWEEN start_date AND DATE_ADD(end_date, INTERVAL 1 DAY)
        AND orders.global_entity_id = 'FP_KH'
        AND orders.is_test_order IS FALSE
    GROUP BY 1
), result_table AS (
    SELECT DISTINCT
        vendor_table.vendor_code,
        vendor_table.vendor_name,
        vendor_table.vendor_city,
        gfv_table.total_orders,
        gfv_table.total_valid_orders,
        gfv_table.total_failed_orders,
        gfv_table.total_gfv_local,
        gfv_table.total_gfv_local/gfv_table.total_valid_orders AS total_ABS
    FROM vendor_table 
    LEFT JOIN gfv_table 
        ON vendor_table.vendor_code = gfv_table.vendor_code
    WHERE 
        gfv_table.total_valid_orders != 0
    GROUP BY 1,2,3,4,5,6,7,8
)
SELECT * FROM result_table 
--WHERE vendor_code IN (SELECT vendor_code FROM result_table GROUP BY 1 HAVING COUNT(1)>1)
ORDER BY vendor_code