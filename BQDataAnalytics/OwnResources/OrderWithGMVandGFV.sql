

WITH country_gfv AS (
  SELECT  
    pd_orders.uuid,
    orders_agg_accounting.gfv_local,
    orders_agg_accounting.gmv_local
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS orders_agg_accounting
        ON pd_orders.uuid = orders_agg_accounting.uuid
        AND orders_agg_accounting.created_date_utc >= '2021-01-01'
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
          ON pd_vendors_agg_business_types.vendor_code = pd_orders.vendor_code
          AND pd_vendors_agg_business_types.global_entity_id = pd_orders.global_entity_id
  INNER JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
            ON pd_orders.global_entity_id = shared_countries.global_entity_id
  WHERE 
      pd_orders.created_date_utc BETWEEN "2022-01-01" AND "2022-01-31"
      AND pd_orders.global_entity_id = 'FP_KH'
      AND pd_orders.is_valid_order
      AND NOT is_test_order
      AND pd_vendors_agg_business_types.business_type_apac = 'restaurants'
  ORDER BY gfv_local DESC

)

SELECT * FROM country_gfv


