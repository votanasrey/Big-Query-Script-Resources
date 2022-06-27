
DECLARE start_date, end_date DATE;

SET start_date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
SET end_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

CREATE OR REPLACE TABLE `foodpanda-kh-bigquery.pandata_kh.country_KH_marketing_voucher_performance_report` 
AS 
WITH voucher_redemptions_raw AS (
       SELECT
       pd_orders.created_date_local AS date,
       pd_orders.code AS code,
       pd_orders.pd_customer_uuid AS customer_id,
       pd_orders.expedition_type,
       pd_vendors.location.city AS city_name,
       pd_vendors.vendor_code,
       pd_vendors.name AS vendor_name,
       pd_vendors.chain_code,
       pd_vendors.chain_name,
       pd_vendors.vertical_type AS vertical_type,
       sf_accounts.owner_name,

       accounting.gmv_local,
       accounting.gfv_local,

       IFNULL(voucher.voucher.value_local, 0) AS voucher_value_local,
       IFNULL(voucher.voucher.foodpanda_subsidized_value_local, 0) AS voucher_cost,
       offers_vouchers.minimum_order_value_local AS mov,
       offers_vouchers.channel,
       offers_vouchers.purpose,

       -- group wallet voucher code together by its description

       IF(offers_vouchers.quantity = 1, offers_vouchers.description, offers_vouchers.voucher_code) AS customer_code,
       IF(offers_vouchers.quantity = 1, NULL, offers_vouchers.start_date_local) AS start_date_local,
       IF(offers_vouchers.quantity = 1, NULL, offers_vouchers.stop_date_local ) AS stop_date_local,
       pd_orders.created_at_local <= TIMESTAMP_ADD(customers.first_order_valid_at_utc, INTERVAL 7 HOUR) AS is_first_valid_order,
       pd_orders.created_at_local <= TIMESTAMP_ADD(customers.first_order_valid_at_utc, INTERVAL 7 HOUR) AND LOWER(pd_vendors.vertical_type) != 'restaurants' AS is_first_valid_order_with_new_verticals,


       FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
              ON pd_orders.pd_vendor_uuid = pd_vendors.uuid
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
              ON pd_vendors.vendor_code = sf_accounts.vendor_code
              AND sf_accounts.global_entity_id = pd_vendors.global_entity_id
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS voucher
              ON pd_orders.global_entity_id = voucher.global_entity_id
              AND pd_orders.uuid = voucher.uuid  
              AND voucher.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS accounting
              ON pd_orders.uuid = accounting.uuid 
              AND pd_orders.global_entity_id = accounting.global_entity_id
              AND accounting.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.offers_vouchers` AS offers_vouchers
              ON voucher.pd_voucher_uuid  = offers_vouchers.uuid
       LEFT JOIN `fulfillment-dwh-production.pandata_curated.cus_customers_agg_orders` AS customers
              ON pd_orders.pd_customer_uuid  = customers.uuid
       WHERE 
       pd_orders.global_entity_id = 'FP_KH'
       AND pd_orders.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
       AND pd_orders.created_date_local BETWEEN start_date AND end_date
       AND pd_orders.is_valid_order
       AND voucher.is_voucher_used 
)
       SELECT *,
       SUM(
              CASE
              WHEN date >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND date <= stop_date_local AND date <= LAST_DAY(CURRENT_DATE(), MONTH) THEN IFNULL(voucher_cost, 0) +
              SAFE_DIVIDE(
                     voucher_cost,
                     DATE_DIFF(CURRENT_DATE(), IF(start_date_local >= DATE_TRUNC(CURRENT_DATE(), MONTH), start_date_local, DATE_TRUNC(CURRENT_DATE(), MONTH)), DAY)
              ) *
              IFNULL(DATE_DIFF(IF(stop_date_local <= LAST_DAY(CURRENT_DATE(), MONTH), stop_date_local, LAST_DAY(CURRENT_DATE(), MONTH)), CURRENT_DATE() - 1, DAY), 0)
       ELSE 0 END ) OVER(PARTITION BY customer_code) AS run_rate
       FROM voucher_redemptions_raw


