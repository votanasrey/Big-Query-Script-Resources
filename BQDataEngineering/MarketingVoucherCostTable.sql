WITH refund_vouchers AS
(
  SELECT
    voucher_issued_date,
    country,
    voucher_id,
    voucher_code,
    order_for_voucher_issued,
    voucher_used_date,
    order_for_voucher_used,
    SUM(IFNULL(refund_amount_local, 0)) AS refund_amount_local,
    SUM(IFNULL(compensation_amount_local, 0)) AS compensation_amount_local,
    SUM(IFNULL(refund_amount_local, 0) + IFNULL(compensation_amount_local, 0)) AS total_rc_local
  FROM `{project_id}.pandata_report.refund_vouchers`
  WHERE voucher_type IN ('BUX', 'OneView')
  GROUP BY 1, 2, 3, 4, 5, 6, 7
),

mapping_vouchers AS
(
    SELECT
      *
    FROM `{project_id}.pandata_report.gsheet_marketing_voucher_channel_mapping`
    WHERE budget_channel NOT LIKE '%CRM%' AND budget_channel NOT LIKE '%Demand%'
),

dataset AS
(
  SELECT
    pd_orders.ordered_at_local,
    pd_orders.country_name,
    pd_orders.global_entity_id,
    pd_orders.uuid AS uuid,
    customers.deduplicate_customer_hash_id,
    first_orders.is_first_valid_order_platform AS is_first_valid_order,
    pd_orders.is_valid_order,
    acc.gmv_eur,
    acc.gmv_local,
    CASE
      WHEN refund_vouchers.voucher_code IS NOT NULL THEN SAFE_DIVIDE(total_rc_local, fx_rates.fx_rate_eur)
      ELSE accounting.vouchers_plus_vat_eur * (-1)
    END AS voucher_value_eur,
    CASE
      WHEN refund_vouchers.voucher_code IS NOT NULL THEN total_rc_local
      ELSE accounting.vouchers_plus_vat_local * (-1)
    END AS voucher_value_local,
    CASE
      WHEN refund_vouchers.voucher_code IS NOT NULL THEN 100
      ELSE voucher_agg.voucher.attributions_foodpanda_ratio
    END AS ratio_foodpanda,
    CASE
      WHEN pd_orders.expedition_type = 'pickup' THEN 'Pickup'
      WHEN business_types.business_type_apac = 'concepts' THEN 'Concepts'
      WHEN business_types.business_type_apac = 'dmart' THEN 'Dmart'
      WHEN business_types.business_type_apac = 'shops' THEN 'Shops'
      WHEN business_types.business_type_apac = 'kitchens' THEN 'Kitchens'
      ELSE 'Food'
     END AS vertical,
    CASE
      WHEN crm_vouchers.campaign_vertical IS NOT NULL THEN crm_vouchers.campaign_vertical
      WHEN pd_vouchers.channel = 'channel_corporate' AND pd_vouchers.purpose = 'revenue' THEN 'Corporate'
      WHEN pd_vouchers.channel = 'channel_pandapro' OR pd_vouchers.is_subscription_voucher THEN 'PandaPro'
      WHEN ARRAY_TO_STRING(pd_vouchers.expedition_types, ',') = 'pickup' THEN 'Pickup'
      WHEN ARRAY_TO_STRING(pd_vouchers.vertical_types, ',') = 'darkstores' THEN 'Dmart'
      WHEN ARRAY_TO_STRING(pd_vouchers.vertical_types, ',') = 'concepts' THEN 'Concepts'
      WHEN ARRAY_TO_STRING(pd_vouchers.vertical_types, ',') = 'kitchens' THEN 'Kitchens'
      WHEN ARRAY_TO_STRING(pd_vouchers.vertical_types, ',') IN ('concepts,kitchens', 'kitchens,concepts') THEN
        CASE
          WHEN business_types.business_type_apac = 'concepts' THEN 'Concepts'
          WHEN business_types.business_type_apac = 'kitchens' THEN 'Kitchens'
          ELSE 'Concepts and Kitchens ERROR'
        END
      WHEN 'restaurants' IN UNNEST(pd_vouchers.vertical_types) OR LENGTH(ARRAY_TO_STRING(pd_vouchers.vertical_types, ',')) = 0 THEN 'Food'
       WHEN ('darkstores' IN UNNEST(pd_vouchers.vertical_types)
          AND 'restaurants' NOT IN UNNEST(pd_vouchers.vertical_types)
          AND 'concepts' NOT IN UNNEST(pd_vouchers.vertical_types)
          AND 'kitchens' NOT IN UNNEST(pd_vouchers.vertical_types)
          ) THEN
            CASE
                WHEN business_types.business_type_apac = 'dmart' THEN 'Dmart'
                WHEN business_types.business_type_apac = 'shops' THEN 'Shops'
            ELSE 'Dmart and Shops ERROR'
            END
      ELSE 'Shops'
    END AS vertical_per_budget,
    CASE
      WHEN refund_vouchers.voucher_code IS NOT NULL THEN SAFE_DIVIDE(refund_vouchers.compensation_amount_local, refund_vouchers.total_rc_local)
      WHEN pd_vouchers.purpose = 'refund' THEN 0
      WHEN crm_vouchers.budget_channel IS NOT NULL THEN 1
      WHEN pd_vouchers.channel = 'channel_corporate' AND pd_vouchers.purpose = 'revenue' THEN 0
      WHEN (DATE(pd_orders.ordered_at_local) < '2021-08-01') AND (pd_vouchers.channel = 'channel_pandapro' OR pd_vouchers.is_subscription_voucher)
            AND NOT (pd_vouchers.global_entity_id = 'FP_TW' AND (pd_vouchers.description LIKE '85折專屬優惠' OR pd_vouchers.description LIKE '89折專屬優惠' OR pd_vouchers.description LIKE 'Pro專屬優惠'))
            AND NOT (pd_vouchers.global_entity_id = 'FP_PH' AND (pd_vouchers.description LIKE 'pandapro Welcome Voucher' OR pd_vouchers.description LIKE '#foodpandaBDayBlowout' OR pd_vouchers.description LIKE 'foodpandaBdayBlowOut 70 OFF' OR pd_vouchers.description LIKE 'Welcome to pandapro!' OR pd_vouchers.description LIKE 'welcome voucher' OR pd_vouchers.description LIKE 'P50 OFF New Subscriber'))
            AND NOT (pd_vouchers.global_entity_id = 'FP_HK' AND (pd_vouchers.description LIKE '回禮到！$30優惠券!' OR pd_vouchers.description LIKE '回禮到！$35 pandamart 優惠券！' OR pd_vouchers.description LIKE '首月月費回贈 - $30優惠券！' OR pd_vouchers.description LIKE '首月月費回贈 - $35 pandamart 優惠券！'))
          THEN 0
      WHEN DATE(pd_orders.ordered_at_local) >= '2021-08-01'
        AND (pd_vouchers.channel = 'channel_pandapro' OR pd_vouchers.is_subscription_voucher)
        AND NOT pd_vouchers.purpose = 'activation'
         THEN 0
      WHEN mapping_vouchers.cost_attribution = 'other' THEN 0
      WHEN LOWER(pd_vouchers.description) LIKE 'cashback%' THEN 0
      WHEN (LOWER(pd_vouchers.description) LIKE '%rider%' AND pd_vouchers.channel NOT IN ('channel_rider_rewards')) THEN 0
      WHEN (pd_vouchers.global_entity_id = 'FP_SG' AND pd_vouchers.type = 'delivery_fee' AND DATE(pd_orders.ordered_at_local) >= '2021-07-01') THEN 0
      ELSE 1
    END AS marketing_share,
    CASE
      WHEN ((pd_vouchers.voucher_code = refund_vouchers.voucher_code AND pd_orders.country_name = refund_vouchers.country) OR pd_vouchers.purpose = 'refund') THEN 'Customer Compensation'
      WHEN crm_vouchers.budget_channel IS NOT NULL THEN crm_vouchers.budget_channel
      WHEN LOWER(pd_vouchers.description) LIKE "%enjoy, it's on us%" THEN 'Subsidies (Demand Gen)'
      WHEN pd_vouchers.channel = 'channel_pandapro' OR pd_vouchers.is_subscription_voucher THEN 'PandaPro Vouchers'
      ELSE COALESCE(mapping_vouchers.budget_channel, 'Others')
    END AS budget_channel,
    pd_vouchers.uuid AS pd_voucher_uuid,
    pd_vouchers.voucher_code,
    pd_vouchers.type AS voucher_type,
    pd_vouchers.value AS voucher_value,
    pd_vouchers.description,
    pd_vouchers.is_unlimited,
    pd_vouchers.quantity,
    pd_vouchers.purpose,
    pd_vouchers.channel,
    pd_vouchers.start_date_local AS start_date,
    pd_vouchers.stop_date_local AS end_date,
    mapping_vouchers.cost_attribution,
  FROM `{project_id}.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `{project_id}.pandata_curated.shared_countries` AS countries
    ON pd_orders.global_entity_id = countries.global_entity_id
  LEFT JOIN `{project_id}.pandata_curated.central_dwh_fx_rates` AS fx_rates
    ON countries.currency_code_iso = fx_rates.currency_code_iso
    AND pd_orders.ordered_at_date_local = fx_rates.fx_rate_date
  LEFT JOIN `{project_id}.pandata_curated.pd_customers_agg_dedup_ids` AS customers
    ON pd_orders.pd_customer_uuid = customers.uuid
  LEFT JOIN `{project_id}.pandata_report.marketing_pd_orders_agg_acquisition_dates` AS first_orders
    ON pd_orders.uuid = first_orders.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_vendors` AS pd_vendors
    ON pd_vendors.uuid = pd_orders.pd_vendor_uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_vendors_agg_business_types` AS business_types
    ON pd_vendors.uuid = business_types.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_orders_agg_accounting` AS acc
    ON pd_orders.uuid = acc.uuid
  LEFT JOIN UNNEST(acc.accounting) AS accounting
  LEFT JOIN `{project_id}.pandata_curated.pd_orders_agg_vouchers` AS voucher_agg
    ON voucher_agg.uuid = pd_orders.uuid
  LEFT JOIN `{project_id}.pandata_curated.pd_vouchers` AS pd_vouchers
    ON voucher_agg.pd_voucher_uuid = pd_vouchers.uuid
  LEFT JOIN `{project_id}.pandata_report.crm_vouchers` AS crm_vouchers
    ON crm_vouchers.uuid = pd_vouchers.uuid
  LEFT JOIN refund_vouchers
    ON pd_vouchers.voucher_code = refund_vouchers.voucher_code
    AND pd_orders.country_name = refund_vouchers.country
  LEFT JOIN mapping_vouchers
    ON LOWER(pd_vouchers.channel) = LOWER(mapping_vouchers.channel)
    AND LOWER(pd_vouchers.purpose) = LOWER(mapping_vouchers.purpose)
    AND CAST(mapping_vouchers.year AS INT64) = 2021
  LEFT JOIN `{project_id}.pandata_curated.shared_countries` AS pd_countries
    ON pd_countries.global_entity_id = pd_orders.global_entity_id
  WHERE pd_orders.is_billable
    AND first_orders.created_date_utc < '{execution_date}'
    AND DATE(pd_orders.ordered_at_local) < '{execution_date}'
    AND DATE(pd_orders.ordered_at_local) >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND voucher_agg.is_voucher_used = TRUE
    AND pd_countries.region = 'Asia'
    AND pd_orders.created_date_utc < '{execution_date}'
    AND voucher_agg.created_date_utc < '{execution_date}'
    AND acc.created_date_utc < '{execution_date}'
    AND fx_rates.fx_rate_date < '{execution_date}'
    AND pd_orders.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND voucher_agg.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND acc.created_date_utc >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND fx_rates.fx_rate_date >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND DATE(pd_orders.ordered_at_local) >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
    AND voucher_agg.is_voucher_used = TRUE
    AND pd_countries.region = 'Asia'
    AND accounting.is_order_last_entry
),

voucher_cap AS
(
  SELECT
    pd_voucher_uuid,
    global_entity_id,
    is_unlimited,
    quantity,
    COUNT(DISTINCT uuid) AS orders,
    CASE WHEN is_unlimited = FALSE THEN quantity - COUNT(DISTINCT uuid) END AS quantity_left,
    CASE
      WHEN is_unlimited = FALSE THEN
      (quantity - COUNT(DISTINCT CASE WHEN DATE_TRUNC(DATE(ordered_at_local), MONTH) < DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH) THEN uuid END))
      * AVG(CASE WHEN DATE_TRUNC(DATE(ordered_at_local), MONTH) = DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH) THEN voucher_value_eur END)
    END AS run_rate_cap_eur,
    CASE
      WHEN is_unlimited = FALSE THEN
      (quantity - COUNT(DISTINCT CASE WHEN DATE_TRUNC(DATE(ordered_at_local), MONTH) < DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH) THEN uuid END))
      * AVG(CASE WHEN DATE_TRUNC(DATE(ordered_at_local), MONTH) = DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH) THEN voucher_value_local END)
    END AS run_rate_cap_lc,
  FROM dataset
  WHERE start_date < DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH)
    AND end_date >= DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)
  GROUP BY 1, 2, 3, 4
),

voucher_cap_vertical AS
(
  SELECT
    dataset.global_entity_id,
    dataset.vertical,
    dataset.pd_voucher_uuid,
    dataset.voucher_code,
    dataset.start_date,
    dataset.end_date,
    dataset.quantity,
    dataset.cost_attribution,
    dataset.is_unlimited,
    voucher_cap.quantity_left,
    voucher_cap.orders,
    voucher_cap.run_rate_cap_eur,
    voucher_cap.run_rate_cap_lc,
    COUNT(DISTINCT uuid) AS vertical_orders,
    SAFE_DIVIDE(COUNT(DISTINCT uuid), voucher_cap.orders) AS vertical_share,
    SAFE_DIVIDE(COUNT(DISTINCT uuid), voucher_cap.orders) * voucher_cap.run_rate_cap_eur AS run_rate_cap_vertical_eur,
    SAFE_DIVIDE(COUNT(DISTINCT uuid), voucher_cap.orders) * voucher_cap.run_rate_cap_lc AS run_rate_cap_vertical_lc,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(ordered_at_local)), MONTH)
    THEN
      CASE
        WHEN dataset.quantity <= 1 OR cost_attribution IN ('global', 'other') OR dataset.is_unlimited
          THEN
             (
              (DATE_DIFF(
                DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH),
                DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), DAY
                      ))
              /
              (DATE_DIFF(
                DATE_ADD('{execution_date}', INTERVAL -1 DAY),
                DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), DAY
                      ) + 1)
               )
        WHEN start_date <= DATE_ADD('{execution_date}', INTERVAL -1 DAY) AND end_date >= DATE_ADD('{execution_date}', INTERVAL -1 DAY)
           THEN
              (
              (DATE_DIFF(
                GREATEST(LEAST(end_date, DATE_ADD(DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH), INTERVAL -1 DAY)), DATE_ADD('{execution_date}', INTERVAL -1 DAY)),
                GREATEST(start_date, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
                      ) + 1)
              /
              (DATE_DIFF(
                DATE_ADD('{execution_date}', INTERVAL -1 DAY),
                GREATEST(start_date, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
                      ) + 1)
               )
        ELSE 1
        END
      ELSE 1
  END AS run_rate_multiplier,
  FROM dataset
  INNER JOIN voucher_cap
          ON voucher_cap.pd_voucher_uuid = dataset.pd_voucher_uuid
         AND dataset.global_entity_id = voucher_cap.global_entity_id
  WHERE start_date < DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH)
    AND end_date >= DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),

run_rates AS
(
  SELECT
      dataset.pd_voucher_uuid,
      dataset.global_entity_id,
      dataset.vertical,
      dataset.quantity,
      dataset.is_unlimited,
      dataset.cost_attribution,
      dataset.start_date,
      dataset.end_date,
      CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(ordered_at_local)), MONTH)
      THEN
        CASE
          WHEN dataset.quantity <= 1 OR dataset.cost_attribution IN ('global', 'other') OR dataset.is_unlimited
            THEN
               (
                (DATE_DIFF(
                  DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH),
                  DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), DAY
                        ))
                /
                (DATE_DIFF(
                  DATE_ADD('{execution_date}', INTERVAL -1 DAY),
                  DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), DAY
                        ) + 1)
                 )
        WHEN dataset.start_date <= DATE_ADD('{execution_date}', INTERVAL -1 DAY) AND dataset.end_date >= DATE_ADD('{execution_date}', INTERVAL -1 DAY)
           THEN
              (
              (DATE_DIFF(
                GREATEST(LEAST(dataset.end_date, DATE_ADD(DATE_ADD(DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH), INTERVAL +1 MONTH), INTERVAL -1 DAY)), DATE_ADD('{execution_date}', INTERVAL -1 DAY)),
                GREATEST(dataset.start_date, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
                      ) + 1)
              /
              (DATE_DIFF(
                DATE_ADD('{execution_date}', INTERVAL -1 DAY),
                GREATEST(dataset.start_date, DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL -1 DAY), MONTH)), DAY
                      ) + 1)
               )
        ELSE 1
        END
        ELSE 1
      END AS run_rate_multiplier,
  FROM dataset
  LEFT JOIN voucher_cap_vertical
         ON voucher_cap_vertical.pd_voucher_uuid = dataset.pd_voucher_uuid
        AND voucher_cap_vertical.global_entity_id = dataset.global_entity_id
        AND voucher_cap_vertical.vertical = dataset.vertical
  WHERE DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(ordered_at_local), MONTH)
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

dataset_combined AS
(
  SELECT
    DATE_TRUNC(DATE(ordered_at_local), MONTH) AS month,
    country_name AS country,
    dataset.global_entity_id,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN dataset.pd_voucher_uuid END AS pd_voucher_uuid,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN dataset.voucher_code END AS voucher_code,
    voucher_type,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN voucher_value END AS voucher_value_lc,
    description,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN dataset.is_unlimited END AS is_voucher_quantity_unlimited,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN dataset.quantity END AS voucher_quantity,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN voucher_cap_vertical.quantity_left END AS voucher_quantity_left,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN voucher_cap_vertical.run_rate_cap_vertical_eur END AS run_rate_cap_vertical_eur,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN voucher_cap_vertical.run_rate_cap_vertical_lc END AS run_rate_cap_vertical_lc,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) >= DATE_SUB(DATE_TRUNC(DATE(ordered_at_local), MONTH), INTERVAL 3 MONTH) THEN voucher_cap_vertical.vertical_share END AS vertical_share,
    ratio_foodpanda,
    purpose,
    channel,
    dataset.vertical,
    vertical_per_budget,
    dataset.budget_channel,
    COALESCE(budget_channel, 'Others') AS local_channel,
    FALSE AS is_revenue_reduction,
    dataset.description AS voucher_description,
    dataset.marketing_share,
    dataset.start_date,
    dataset.end_date,
    dataset.quantity,
    dataset.voucher_value,
    dataset.cost_attribution,
    dataset.is_unlimited,
    run_rates.run_rate_multiplier,
    voucher_cap_vertical.run_rate_cap_vertical_eur,
    voucher_cap_vertical.run_rate_cap_vertical_lc,
    SUM(COALESCE(voucher_value_eur, 0)) AS costs_eur,
    SUM(COALESCE(voucher_value_local, 0)) AS costs_lc,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(ordered_at_local)), MONTH)
      THEN
        CASE
          WHEN dataset.quantity <= 1 OR dataset.cost_attribution IN ('global', 'other') OR dataset.is_unlimited
            THEN SUM(COALESCE(voucher_value_eur, 0)) * run_rates.run_rate_multiplier
            ELSE LEAST(SUM(COALESCE(voucher_value_eur, 0)) * run_rates.run_rate_multiplier, GREATEST(voucher_cap_vertical.run_rate_cap_vertical_eur, SUM(COALESCE(voucher_value_eur, 0))))
          END
      ELSE SUM(COALESCE(voucher_value_eur, 0))
    END AS runrate_eur,
    CASE WHEN DATE_TRUNC(DATE_ADD('{execution_date}', INTERVAL - 1 DAY), MONTH) = DATE_TRUNC(DATE(MIN(ordered_at_local)), MONTH)
      THEN
        CASE
          WHEN dataset.quantity <= 1 OR dataset.cost_attribution IN ('global', 'other') OR dataset.is_unlimited
            THEN SUM(COALESCE(voucher_value_local, 0)) * run_rates.run_rate_multiplier
          ELSE LEAST(SUM(COALESCE(voucher_value_local, 0)) * run_rates.run_rate_multiplier, GREATEST(voucher_cap_vertical.run_rate_cap_vertical_lc, SUM(COALESCE(voucher_value_local, 0))))
          END
      ELSE SUM(COALESCE(voucher_value_local, 0))
    END AS runrate_lc,
    ROUND(SUM(gmv_eur), 2) AS gmv_eur,
    ROUND(SUM(gmv_local), 2) AS gmv_local,
    COUNT(DISTINCT uuid) AS orders,
    COUNT(DISTINCT CASE WHEN is_first_valid_order = TRUE THEN deduplicate_customer_hash_id END) AS nc,
    COUNT(DISTINCT dataset.voucher_code) AS activations
  FROM dataset
  LEFT JOIN voucher_cap_vertical
         ON voucher_cap_vertical.pd_voucher_uuid = dataset.pd_voucher_uuid
        AND voucher_cap_vertical.global_entity_id = dataset.global_entity_id
        AND voucher_cap_vertical.vertical = dataset.vertical
  LEFT JOIN run_rates
         ON run_rates.pd_voucher_uuid = dataset.pd_voucher_uuid
        AND run_rates.global_entity_id = dataset.global_entity_id
        AND run_rates.vertical = dataset.vertical
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33
)

SELECT country,
       global_entity_id,
       budget_channel,
       vertical_per_budget,
       vertical AS vertical_per_redemption,
       month,
       pd_voucher_uuid,
       voucher_code,
       voucher_description,
       voucher_type,
       voucher_value,
       quantity,
       purpose,
       channel,
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
       SUM(activations) AS activations,
       ROUND(SUM(gmv_eur), 2) AS gmv_eur,
       ROUND(SUM(gmv_local), 2) AS gmv_local
FROM dataset_combined
WHERE month >= DATE_SUB(DATE_TRUNC('{execution_date}', YEAR), INTERVAL 2 YEAR)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17