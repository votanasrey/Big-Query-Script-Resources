


DECLARE start_date, end_date DATE;

SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
SET end_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

CREATE OR REPLACE TABLE `foodpanda-kh-bigquery.pandata_kh.country_KH_marketing_daily_overview_report` AS
  SELECT 
    o.created_date_local AS date_local,
    EXTRACT(HOUR FROM o.created_at_local) AS hour,
    o.expedition_type,
    vendor.vertical_type AS vertical,
    NULL AS cluster, 
    vendor.location.city AS city, 
    NULL AS zone_name,
    COUNT(DISTINCT o.code) AS orders,
    COUNT(CASE WHEN o.uuid = c.first_valid_order_all_uuid THEN o.uuid END) AS nc,
    COUNT(CASE WHEN v.is_voucher_used IS FALSE AND d.is_discount_used IS FALSE THEN o.code END) AS organic_orders,
    COUNT(CASE WHEN v.is_voucher_used IS FALSE AND d.is_discount_used IS TRUE THEN o.code END) AS deal_only_orders,
    COUNT(CASE WHEN v.is_voucher_used IS TRUE AND d.is_discount_used IS FALSE THEN o.code END) AS voucher_only_orders,
    COUNT(CASE WHEN v.is_voucher_used IS TRUE AND d.is_discount_used IS TRUE THEN o.code END) AS deal_and_voucher_orders,

    IFNULL(ROUND(SUM(a.gfv_local),2),0) AS gfv_local,

    IFNULL(ROUND(SUM(CASE WHEN v.is_voucher_used IS FALSE AND d.is_discount_used IS FALSE THEN a.gfv_local END),2),0) AS organic_gfv_local,
    IFNULL(ROUND(SUM(CASE WHEN v.is_voucher_used IS TRUE OR d.is_discount_used IS TRUE THEN a.gfv_local END),2),0) AS incentivized_gfv_local,

    IFNULL(ROUND(SUM(a.gmv_local),2),0) AS gmv_local,
    IFNULL(ROUND(SUM(o.delivery_fee_local),2),0) AS delivery_fee_local,

    SUM(IFNULL(d.discount.foodpanda_subsidized_value_local, 0)) AS discount_cost_local,
    SUM(IFNULL(v.voucher.foodpanda_subsidized_value_local, 0)) AS voucher_cost_local,

    ROUND(SUM(IFNULL(v.voucher.value_local, 0)),2)  AS voucher_value_local,
    ROUND(SUM(IFNULL(d.discount.discount_amount_local, 0)),2) AS discount_value_local

  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS v 
    ON o.uuid = v.uuid 
    AND o.global_entity_id = v.global_entity_id
    AND v.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS d 
    ON o.uuid = d.uuid
    AND o.global_entity_id = d.global_entity_id 
    AND d.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a 
    ON o.uuid = a.uuid 
    AND o.global_entity_id = a.global_entity_id
    AND a.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
  LEFT JOIN `fulfillment-dwh-production.pandata_report.marketing_customers_agg_orders_dates` AS c
    ON o.global_entity_id = c.global_entity_id
    AND o.pd_customer_uuid = c.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendor 
    ON o.vendor_code = vendor.vendor_code
    AND o.global_entity_id = vendor.global_entity_id
  WHERE 
    o.global_entity_id = "FP_KH"
    AND o.is_valid_order
    AND o.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
    AND o.created_date_local BETWEEN start_date AND end_date
  GROUP BY 1,2,3,4,5,6,7
  ORDER BY 1,2 DESC
;

