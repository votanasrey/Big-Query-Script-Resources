

DECLARE period_time, start_date DATE;
SET period_time = DATE_SUB(CURRENT_DATE(),INTERVAL 91 DAY);
SET start_date = "2021-10-01"; --Set created date of vendor

WITH vendor_table AS(
    SELECT 
        vendor.vendor_code,
        vendor.name AS vendor_name,
        vendor.location.city AS vendor_city,
        DATE(vendor.created_at_local) AS vendor_created_date_local,
        DATE(vendor.activated_at_local) AS vendor_activated_date_local
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendor
    WHERE 
        vendor.global_entity_id = 'FP_KH'
        AND vendor.is_active IS TRUE
        AND vendor.is_private IS FALSE
        AND vendor.is_test IS FALSE
        AND DATE(vendor.activated_at_local) >= start_date
        --AND vendor.created_at_local >= period_time
), vendor_ordering_table AS(
    SELECT 
        v.vendor_code,
        DATE(o.ordered_at_date_local) AS ordered_at_date_local,
        SUM(a.gmv_local) AS total_gmv_local,
        SUM(CASE WHEN o.is_valid_order IS TRUE THEN a.gfv_local END) AS total_gfv_local,
        COUNT(DISTINCT o.code) AS total_orders,
        COUNT(CASE WHEN o.is_valid_order IS TRUE THEN o.code END) AS total_valid_orders,
        COUNT(CASE WHEN o.is_valid_order IS FALSE THEN o.code END) AS total_failed_orders, 
    
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
    LEFT JOIN  `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
        ON v.vendor_code = o.vendor_code
        AND v.global_entity_id = o.global_entity_id
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
        AND v.created_at_local >= start_date
        AND a.created_date_local >= start_date
        AND o.created_date_utc >= start_date
        AND a.created_date_utc >= start_date
    GROUP BY 1,2
),vendor_offline_table AS(
    SELECT 
        vf.vendor_code,
        DATE(vf.report_date) AS report_date,
        SUM(IFNULL(vf.total_special_day_closed_seconds,0) + IFNULL(vf.total_unavailable_seconds,0)) AS total_closed_seconds,
        SUM(IFNULL(vf.total_open_seconds,0)) AS total_open_seconds,
        SUM(IFNULL(vf.total_special_day_closed_seconds,0) + IFNULL(vf.total_unavailable_seconds,0)) / SUM(IFNULL(vf.total_open_seconds,0)) AS total_offline_in_percentage
    FROM 
        `fulfillment-dwh-production.pandata_report.vendor_offline` AS vf
    WHERE 
        vf.global_entity_id = 'FP_KH'
        AND vf.report_date >= start_date
        AND vf.is_test IS FALSE
        AND vf.is_active IS TRUE
        AND vf.is_private IS FALSE
    GROUP BY 1,2
), result_table AS (
    SELECT DISTINCT
        vendor_table.vendor_code,
        vendor_table.vendor_name,
        vendor_table.vendor_city,
        vendor_table.vendor_created_date_local,
        vendor_table.vendor_activated_date_local,
        DATE_DIFF(current_date(),vendor_table.vendor_activated_date_local,DAY) as los,
        (CASE 
            WHEN DATE(vendor_offline_table.report_date) < (DATE(vendor_table.vendor_activated_date_local) + 30) THEN "1. 0-30" 
            WHEN DATE(vendor_offline_table.report_date) < (DATE(vendor_table.vendor_activated_date_local) + 60) THEN "2. 31-60"
            WHEN DATE(vendor_offline_table.report_date) < (DATE(vendor_table.vendor_activated_date_local) + 90) THEN "3. 61-90"
            ELSE "4. 90+"
        END) AS period_date,

        IFNULL(SUM(vendor_ordering_table.total_orders),0) AS total_orders,
        IFNULL(SUM(vendor_ordering_table.total_valid_orders),0) AS total_valid_orders,
        IFNULL(SUM(vendor_ordering_table.total_failed_orders),0) AS total_failed_orders,
        IFNULL(SUM(vendor_ordering_table.total_gfv_local),0) AS total_gfv_local,
        IFNULL(SUM(vendor_ordering_table.total_gmv_local),0) AS total_gmv_local,

        IFNULL(SUM(vendor_offline_table.total_closed_seconds),0) AS total_closed_seconds,
        SUM(vendor_offline_table.total_open_seconds) AS total_open_seconds,
        SUM(vendor_offline_table.total_closed_seconds)/SUM(vendor_offline_table.total_open_seconds) AS total_offline_in_percentage,

    FROM vendor_table 
    LEFT JOIN vendor_offline_table 
        ON vendor_table.vendor_code = vendor_offline_table.vendor_code
        AND  vendor_offline_table.report_date >= vendor_table.vendor_activated_date_local
    LEFT JOIN vendor_ordering_table 
        ON vendor_table.vendor_code = vendor_ordering_table.vendor_code
        AND vendor_ordering_table.ordered_at_date_local = vendor_offline_table.report_date
    --WHERE vendor_offline_table.report_date < DATE_ADD(vendor_table.vendor_activated_date_local, INTERVAL 90 DAY)
    GROUP BY 1,2,3,4,5,6,7
)
SELECT * FROM result_table 
--WHERE vendor_code = "cju2"
ORDER BY vendor_code



