


DECLARE start_date, end_date, exec_date DATE;

SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
SET end_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
SET exec_date = CURRENT_DATE();

CREATE OR REPLACE TABLE `foodpanda-kh-bigquery.pandata_kh.country_KH_marketing_deal_performance_report` AS (

  WITH discounts_info AS (
    SELECT
      uuid,
      description,
      is_subscription_discount,
    FROM `fulfillment-dwh-production.pandata_curated.offers_discounts`
    WHERE global_entity_id = 'FP_KH'
  ),

  customer_vendor_first_order AS (
    SELECT
      pd_customer_uuid,
      pd_vendor_uuid,
      MIN(created_at_local) AS first_valid_order_with_vendor_at_local
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders`
    WHERE 
      created_date_utc <= exec_date
      AND created_date_local < exec_date
      AND global_entity_id = 'FP_KH'
      AND is_valid_order
    GROUP BY 1,2
  )

  SELECT
    pd_orders.created_date_local,
    pd_vendors.location.city AS city_name,
    pd_vendors.vertical_type AS vertical_type,
    sf_accounts.owner_name AS account_owner_name,
    pd_vendors.chain_code,
    pd_vendors.chain_name,
    pd_vendors.vendor_code,
    pd_vendors.name AS vendor_name,
    pd_orders.expedition_type,
    discounts_info.description AS deal_description,
    discounts_info.is_subscription_discount,
    COUNT( CASE WHEN discount.is_discount_used THEN pd_orders.code END) AS total_deal_orders,
    COUNTIF(voucher.voucher.value_local > 0) AS total_voucher_redemptions,
    COUNT(DISTINCT CASE WHEN pd_orders.created_at_local <= TIMESTAMP_ADD(cus_customers_agg_orders.first_order_valid_at_utc, INTERVAL 7 HOUR) THEN pd_orders.pd_customer_uuid END) AS total_nc_to_fpd,
    COUNT(DISTINCT CASE WHEN pd_orders.created_at_local <= customer_vendor_first_order.first_valid_order_with_vendor_at_local THEN pd_orders.pd_customer_uuid END) AS total_nc_to_vendor,

    SUM(accounting.gfv_local) AS total_gfv_local,
    SUM(accounting.gmv_local) AS total_gmv_local,
    SUM(discount.discount.discount_amount_local) AS total_discount_value_local,
    SUM(voucher.voucher.value_local)  AS total_voucher_value_local,

    SUM(IFNULL(discount.discount.foodpanda_subsidized_value_local, 0)) AS total_discount_cost_local,
    SUM(IFNULL(voucher.voucher.foodpanda_subsidized_value_local, 0)) AS total_voucher_cost_local,

  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS discount 
        ON pd_orders.uuid = discount.uuid 
        AND discount.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date

  LEFT JOIN discounts_info
        ON discount.pd_discount_uuid = discounts_info.uuid

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS voucher 
        ON pd_orders.uuid = voucher.uuid 
        AND voucher.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
      ON pd_orders.uuid = accounting.uuid 
      AND pd_orders.global_entity_id = accounting.global_entity_id
      AND accounting.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
        ON pd_orders.pd_vendor_uuid = pd_vendors.uuid

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
        ON pd_vendors.global_vendor_id = sf_accounts.global_vendor_id

  LEFT JOIN `fulfillment-dwh-production.pandata_curated.cus_customers_agg_orders` AS cus_customers_agg_orders
        ON pd_orders.pd_customer_uuid = cus_customers_agg_orders.uuid

  LEFT JOIN customer_vendor_first_order
        ON pd_orders.pd_customer_uuid = customer_vendor_first_order.pd_customer_uuid
        AND pd_orders.pd_vendor_uuid = customer_vendor_first_order.pd_vendor_uuid
  WHERE 
    pd_orders.global_entity_id = 'FP_KH'
    AND pd_orders.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
    AND pd_orders.created_date_local BETWEEN start_date AND end_date
    AND pd_orders.is_valid_order
    AND discount.is_discount_used 
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) 


