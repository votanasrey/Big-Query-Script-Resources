
DECLARE date1, date2, date3 DATE;
SET date1 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month);
SET date2 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month);
SET date3 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 day);

WITH vendor_deal_performance AS (
    SELECT
        v.vendor_code,
        v.name AS vendor_name,
        v.vertical_type AS vendor_type,
        v.location.city AS vendor_city_name,
        di.uuid AS discount_uuid,
        di.title,
        di.description,
        di.discount_type,
        di.condition_type,
        di.start_date_local,
        di.end_date_local,
        di.amount_local, 
        di.foodpanda_ratio,
        
        --- during campaign matric ---
        DATE_DIFF(DATE(di.end_date_local), DATE(di.start_date_local), DAY) AS number_day_of_campaign,
        COUNT(DISTINCT CASE WHEN o.created_date_local BETWEEN di.start_date_local AND di.end_date_local THEN o.code END) AS total_orders_during_campaign,
        COUNT(DISTINCT CASE WHEN o.created_date_local BETWEEN di.start_date_local AND di.end_date_local 
            AND o.is_billable IS TRUE AND o.is_valid_order IS FALSE THEN o.code END) AS total_watages_during_campaign,
        ROUND(SUM(CASE WHEN o.created_date_local BETWEEN di.start_date_local AND di.end_date_local THEN a.gmv_local END),2) AS total_gmv_local_during_campaign,

        --- before campaign matric ---
        COUNT(DISTINCT CASE WHEN o.created_date_local BETWEEN DATE_SUB(di.start_date_local, INTERVAL DATE_DIFF(DATE(di.end_date_local), DATE(di.start_date_local), DAY) DAY) 
            AND di.start_date_local THEN o.code END) AS total_orders_before_campaign,    
        COUNT(DISTINCT CASE WHEN o.created_date_local BETWEEN DATE_SUB(di.start_date_local, INTERVAL DATE_DIFF(DATE(di.end_date_local), DATE(di.start_date_local), DAY) DAY) 
            AND di.start_date_local AND o.is_billable IS TRUE AND o.is_valid_order IS FALSE THEN o.code END) AS total_watages_before_campaign,

        ROUND(SUM(CASE WHEN o.created_date_local BETWEEN DATE_SUB(di.start_date_local, INTERVAL DATE_DIFF(DATE(di.end_date_local), DATE(di.start_date_local), DAY) DAY) 
            AND di.start_date_local THEN a.gmv_local 
            WHEN a.gmv_local IS NULL THEN 0 END),2) AS total_gmv_local_before_campaign,
    
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
    INNER JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS d
        ON o.global_entity_id = d.global_entity_id
        AND o.uuid = d.uuid
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_discounts` AS di
        ON d.global_entity_id = di.global_entity_id
        AND d.pd_discount_uuid = di.uuid
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
        ON o.global_entity_id = v.global_entity_id
        AND o.vendor_code = v.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a
        ON o.global_entity_id = a.global_entity_id
        AND o.uuid = a.uuid
    WHERE
        -- partition filter on the tables
        o.created_date_utc >= date1
        AND a.created_date_utc >= date1
        AND d.created_date_utc >= date1

        AND o.created_date_local BETWEEN date2 AND date3
        AND d.created_date_local BETWEEN date2 AND date3
        AND a.created_date_local BETWEEN date2 AND date3
        

        AND d.is_discount_used
        AND o.is_billable
        AND o.is_test_order IS FALSE
        AND o.global_entity_id = 'FP_KH'
        AND v.is_active
        AND NOT v.is_private
        AND NOT v.is_test
        --AND o.vendor_code IN ('t2wi')

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    ORDER BY 1 DESC
)
SELECT * FROM vendor_deal_performance ORDER BY total_orders_before_campaign DESC


