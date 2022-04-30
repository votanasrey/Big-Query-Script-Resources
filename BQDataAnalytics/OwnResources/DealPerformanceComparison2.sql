

DECLARE date1, date2, date3 DATE;
SET date1 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month);
SET date2 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month);
SET date3 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 day);

WITH deal_table AS(
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
      AND o.is_billable IS TRUE AND o.is_valid_order IS FALSE THEN o.code END) AS total_wastages_during_campaign,
    ROUND(SUM(CASE WHEN o.created_date_local BETWEEN di.start_date_local AND di.end_date_local THEN a.gmv_local END),2) AS total_gmv_local_during_campaign,

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
    v.global_entity_id = 'FP_KH'
    AND v.is_active
    AND d.is_discount_used
    AND NOT v.is_private
    AND NOT v.is_test
    AND o.is_billable
    AND o.is_test_order IS FALSE

    AND o.created_date_utc >= date1
    AND a.created_date_utc >= date1
    AND d.created_date_utc >= date1

    AND o.created_date_local BETWEEN date2 AND date3
    AND d.created_date_local BETWEEN date2 AND date3
    AND a.created_date_local BETWEEN date2 AND date3

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
), none_deal_table AS(
  SELECT
    o.created_date_local,
    o.vendor_code,
    COUNT(DISTINCT CASE WHEN o.is_valid_order THEN o.code END ) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.is_valid_order IS FALSE AND o.is_billable IS TRUE THEN o.code END ) AS total_wastages,
    SUM(CASE WHEN o.is_valid_order IS TRUE THEN a.gmv_local END ) AS total_gmv_local,
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
        ON o.global_entity_id = v.global_entity_id
        AND o.vendor_code = v.vendor_code
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a
      ON o.global_entity_id = a.global_entity_id
      AND o.uuid = a.uuid
  WHERE 
    v.global_entity_id = 'FP_KH'
    AND v.is_active
    AND NOT v.is_private
    AND NOT v.is_test
    AND o.is_billable
    AND o.is_test_order IS FALSE
    AND o.created_date_utc >= date1
    AND a.created_date_utc >= date1
    AND o.created_date_local BETWEEN date2 AND date3
    AND a.created_date_local BETWEEN date2 AND date3

  GROUP BY 1,2

), result_table AS(
  SELECT 
    d.vendor_code,
    d.vendor_name,
    d.vendor_type,
    d.vendor_city_name,
    d.discount_uuid,
    d.title,
    d.description,
    d.discount_type,
    d.condition_type,
    d.start_date_local,
    d.end_date_local,
    d.amount_local, 
    d.foodpanda_ratio,
    d.number_day_of_campaign,
    d.total_orders_during_campaign,
    d.total_wastages_during_campaign,
    d.total_gmv_local_during_campaign,

    n.total_orders,
    n.total_wastages,
    n.total_gmv_local,
  FROM deal_table AS d
  LEFT JOIN none_deal_table AS n
    ON d.vendor_code = n.vendor_code
    AND n.created_date_local <= d.start_date_local 
    AND n.created_date_local >= (d.start_date_local - DATE_DIFF(DATE(d.end_date_local), DATE(d.start_date_local), DAY))
    
)
SELECT * FROM result_table


