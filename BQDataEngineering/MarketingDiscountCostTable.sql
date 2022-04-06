

WITH pickup_discounts AS
(
  SELECT
    discount_id || '_' || pd_countries.global_entity_id AS discount_uuid,
    funded_mktg
  FROM `{project_id}.pandata_report.gsheet_marketing_pickup_discounts` AS pickup
  LEFT JOIN `{project_id}.pandata_curated.shared_countries` AS pd_countries
    ON pickup.country_iso = pd_countries.country_code_iso

),

dataset AS
(
  SELECT
    pd_orders.country_name AS country,
    pd_orders.global_entity_id,
    DATE_TRUNC(DATE(pd_orders.ordered_at_local), MONTH) AS month,
    pd_discounts.uuid,
    pd_discounts.discount_type,
    pd_discounts.description,
    CAST(discount_agg.discount.attributions_foodpanda_ratio AS FLOAT64) AS ratio_foodpanda,
    CASE WHEN DATE(pd_orders.ordered_at_local) >= DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -3 MONTH), MONTH) THEN pd_vendors.name ELSE 'NA' END AS vendor_name,
    CASE WHEN DATE(pd_orders.ordered_at_local) >= DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -3 MONTH), MONTH) THEN pd_vendors.chain_name ELSE 'NA' END AS chain_name,
    CASE
      WHEN pd_orders.expedition_type = 'pickup' THEN 'Pickup'
      WHEN business_types.business_type_apac = 'concepts' THEN 'Concepts'
      WHEN business_types.business_type_apac = 'dmart' THEN 'Dmart'
      WHEN business_types.business_type_apac = 'shops' THEN 'Shops'
      WHEN business_types.business_type_apac = 'kitchens' THEN 'Kitchens'
      ELSE 'Food'
    END AS vertical_per_redemption,
    CASE
      WHEN pd_discounts.is_subscription_discount THEN 'PandaPro'
      WHEN lower(pd_discounts.description) LIKE '%corporate exclusive discount%' THEN 'Corporate'
      WHEN pd_orders.expedition_type = 'pickup' THEN 'Pickup'
      WHEN business_types.business_type_apac = 'concepts' THEN 'Concepts'
      WHEN business_types.business_type_apac = 'dmart' THEN 'Dmart'
      WHEN business_types.business_type_apac = 'shops' THEN 'Shops'
      WHEN business_types.business_type_apac = 'kitchens' THEN 'Kitchens'
      ELSE 'Food'
    END AS vertical_per_budget,
    'Subsidies' AS budget_channel,
    CASE
      WHEN pd_orders.expedition_type = 'pickup' THEN COALESCE(pickup_discounts.funded_mktg, 0)
      WHEN lower(pd_discounts.description) LIKE '%corporate exclusive discount%' THEN 0
      WHEN pd_discounts.is_subscription_discount THEN 0
      WHEN (pd_discounts.global_entity_id = 'FP_SG' AND pd_discounts.discount_type IN ('free-delivery', 'free_delivery') AND DATE(pd_orders.ordered_at_local) >= '2021-07-01') THEN 0
    ELSE 1 END AS marketing_share,
    pd_discounts.start_date_local AS start_date,
    pd_discounts.end_date_local AS end_date,
    SUM(COALESCE(-1 * accounting.discount_plus_vat_eur, 0)) AS costs_eur,
    SUM(COALESCE(-1 * accounting.discount_plus_vat_local, 0)) AS costs_lc,
    CASE WHEN pd_discounts.start_date_local <= DATE_ADD('{execution_date}', INTERVAL -1 DAY) AND pd_discounts.end_date_local >= DATE_ADD('{execution_date}', INTERVAL -1 DAY)
      AND DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(pd_orders.ordered_at_local)), MONTH) THEN
      (
      (DATE_DIFF(
        GREATEST(LEAST(pd_discounts.end_date_local, DATE_ADD(DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH), INTERVAL -1 DAY)), DATE_ADD('{execution_date}', INTERVAL -1 DAY)),
        GREATEST(pd_discounts.start_date_local, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
              ) + 1)
      /
      (DATE_DIFF(
        DATE_ADD('{execution_date}', INTERVAL -1 DAY),
        GREATEST(pd_discounts.start_date_local, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
              ) + 1)
       ) * SUM(COALESCE(-1 * accounting.discount_plus_vat_eur, 0))
    ELSE SUM(COALESCE(-1 * accounting.discount_plus_vat_eur, 0))
    END AS runrate_eur,
    CASE WHEN pd_discounts.start_date_local <= DATE_ADD('{execution_date}', INTERVAL -1 DAY) AND pd_discounts.end_date_local >= DATE_ADD('{execution_date}', INTERVAL -1 DAY)
      AND DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(pd_orders.ordered_at_local)), MONTH) THEN
      (
      (DATE_DIFF(
        GREATEST(LEAST(pd_discounts.end_date_local, DATE_ADD(DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH), INTERVAL -1 DAY)), DATE_ADD('{execution_date}', INTERVAL -1 DAY)),
        GREATEST(pd_discounts.start_date_local, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
              ) + 1)
      /
      (DATE_DIFF(
        DATE_ADD('{execution_date}', INTERVAL -1 DAY),
        GREATEST(pd_discounts.start_date_local, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
              ) + 1)
       ) * SUM(COALESCE(-1 * accounting.discount_plus_vat_local, 0))
    ELSE SUM(COALESCE(-1 * accounting.discount_plus_vat_local, 0))
    END AS runrate_lc,
    COUNT(DISTINCT pd_orders.uuid) AS orders,
    COUNT(DISTINCT CASE WHEN first_orders.is_first_valid_order_platform THEN customers.deduplicate_customer_hash_id END) AS nc
  FROM `{project_id}.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `{project_id}.pandata_curated.pd_customers_agg_dedup_ids` AS customers
    ON pd_orders.pd_customer_uuid = customers.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_vendors` AS pd_vendors
    ON pd_vendors.uuid = pd_orders.pd_vendor_uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_vendors_agg_business_types` AS business_types
    ON pd_vendors.uuid = business_types.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_orders_agg_accounting` AS acc
    ON pd_orders.uuid = acc.uuid
  LEFT JOIN UNNEST(acc.accounting) AS accounting
  LEFT JOIN `{project_id}.pandata_curated.pd_orders_agg_discounts` AS discount_agg
    ON discount_agg.uuid = pd_orders.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_discounts` AS pd_discounts
    ON pd_discounts.uuid = discount_agg.pd_discount_uuid
  LEFT JOIN `{project_id}.pandata_report.marketing_pd_orders_agg_acquisition_dates` AS first_orders
    ON pd_orders.uuid = first_orders.uuid
  LEFT JOIN pickup_discounts
    ON pickup_discounts.discount_uuid =
    (CASE WHEN discount_agg.pd_discount_uuid LIKE '%OFRD%' THEN (REPLACE(discount_agg.pd_discount_uuid, CONCAT(discount_agg.global_entity_id, "_OFRD_"), "") || "_" || discount_agg.global_entity_id) ELSE discount_agg.pd_discount_uuid END)
  WHERE is_billable
    AND first_orders.created_date_utc < '{execution_date}'
    AND DATE(pd_orders.ordered_at_local) < '{execution_date}'
    AND DATE(pd_orders.ordered_at_local) >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND pd_orders.created_date_utc < '{execution_date}'
    AND discount_agg.created_date_utc < '{execution_date}'
    AND acc.created_date_utc < '{execution_date}'
    AND pd_orders.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND discount_agg.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND acc.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND discount_agg.is_discount_used
    AND accounting.is_order_last_entry
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
)

SELECT country,
       global_entity_id,
       vertical_per_budget,
       vertical_per_redemption,
       month,
       uuid AS pd_discount_uuid,
       description AS discount_description,
       discount_type,
       vendor_name,
       chain_name,
       start_date,
       end_date,
       marketing_share,
       SUM(costs_eur * (ratio_foodpanda / 100)) AS costs_fp_eur,
       SUM(costs_eur * (1 - ratio_foodpanda / 100)) AS costs_vendor_eur,
       SUM(runrate_eur * (ratio_foodpanda / 100)) AS runrate_fp_eur,
       SUM(runrate_eur * (1 - ratio_foodpanda / 100)) AS runrate_vendor_eur,
       SUM(costs_lc * (ratio_foodpanda / 100)) AS costs_fp_lc,
       SUM(costs_lc * (1 - ratio_foodpanda / 100)) AS costs_vendor_lc,
       SUM(runrate_lc * (ratio_foodpanda / 100)) AS runrate_fp_lc,
       SUM(runrate_lc * (1 - ratio_foodpanda / 100)) AS runrate_vendor_lc,
       SUM(orders) AS orders,
       SUM(nc) AS nc,
       CURRENT_TIMESTAMP() AS last_updated_at_utc
FROM dataset
WHERE month >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

