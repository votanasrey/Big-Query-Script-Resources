

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
    IFNULL(expedition_types,"delivery") AS expedition_types,
  

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
    LEFT JOIN UNNEST(di.expedition_types) AS expedition_types
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
    
    --set start_date_local sub within last 3months
    AND di.start_date_local >= date1
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
), none_deal_table AS(
  SELECT
    o.created_date_local,
    o.vendor_code,
    (CASE 
      WHEN o.is_pickup THEN "pickup"
      WHEN o.is_delivery THEN "delivery"
    END) AS expedition_types,
    COUNT( 
      CASE 
        WHEN o.is_valid_order IS TRUE THEN o.code 
      END) AS total_orders,
    COUNT(
      CASE 
        WHEN o.is_valid_order IS FALSE AND o.is_billable IS TRUE THEN o.code 
      END ) AS total_wastages,
    SUM(
      CASE 
        WHEN o.is_valid_order IS TRUE THEN a.gmv_local 
      END ) AS total_gmv_local,
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
    AND o.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 5 month),month)
    AND a.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 5 month),month)
  GROUP BY 1,2,3

), result_table_v1 AS(
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
    d.expedition_types,
    d.start_date_local,
    d.end_date_local,
    d.amount_local, 
    d.foodpanda_ratio,
    d.number_day_of_campaign,


    d.total_orders_during_campaign,
    d.total_wastages_during_campaign,
    ROUND(d.total_gmv_local_during_campaign,2) AS total_gmv_local_during_campaign,

    IFNULL(SUM(n.total_orders),0) AS total_orders_before_campaign,
    IFNULL(SUM(n.total_wastages),0) AS total_wastages_before_campaign,
    IFNULL(ROUND(SUM(n.total_gmv_local),2),0) AS total_gmv_local_before_campaign,

  FROM deal_table AS d
  LEFT JOIN none_deal_table AS n
    ON d.vendor_code = n.vendor_code
    AND d.expedition_types = n.expedition_types
    AND n.created_date_local <= d.start_date_local
    AND n.created_date_local >= d.start_date_local - DATE_DIFF(DATE(d.end_date_local), DATE(d.start_date_local), DAY)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
), result_table_v2 AS(
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

      STRING_AGG(d.expedition_types, "," ORDER BY LENGTH(d.expedition_types) DESC ) AS expedition_types,
      SUM(d.total_orders_during_campaign) AS total_orders_during_campaign,
      SUM(d.total_wastages_during_campaign) AS total_wastages_during_campaign,
      ROUND(SUM(d.total_gmv_local_during_campaign),2) AS total_gmv_local_during_campaign,

      SUM(d.total_orders_before_campaign) AS total_orders_before_campaign,
      SUM(d.total_wastages_before_campaign) AS total_wastages_before_campaign,
      ROUND(SUM(d.total_gmv_local_before_campaign),2) AS total_gmv_local_before_campaign,
  FROM result_table_v1 AS d
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
SELECT * FROM result_table_v2 


--WHERE vendor_code = 'tleq'
--WHERE total_gmv_local_before_campaign IS NULL OR total_orders_before_campaign IS NULL OR total_wastages_before_campaign IS NULL OR expedition_types is null
--WHERE vendor_code='nk8c'
--ORDER BY vendor_code, discount_uuid
--WHERE vendor_code ='np0m'
--WHERE total_gmv_local_before_campaign IS NULL OR total_orders_before_campaign IS NULL OR total_wastages_before_campaign IS NULL


