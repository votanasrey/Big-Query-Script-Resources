

WITH product_data AS (
  SELECT
    vendors.global_entity_id,
    vendors.vendor_code,
    COUNT(DISTINCT IF(products.has_dish_image, products.id, NULL)) AS vendor_product_with_image_count,
    COUNT(DISTINCT products.id) AS vendor_product_count
  
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendors
  
  LEFT JOIN vendors.menu_categories
  
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_products` AS products
      ON products.pd_menu_category_uuid = menu_categories.uuid
  
  WHERE products.is_active = TRUE
         AND products.is_deleted = FALSE
         AND LOWER(menu_categories.title) NOT LIKE '%topping%'
         AND LOWER(menu_categories.title) NOT LIKE '%choice%'
  
  GROUP BY 1, 2
),
first_order_date AS (
  SELECT
    global_entity_id,
    vendor_code,
    MIN(DATE(ordered_at_local)) AS first_valid_order_date
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders`
  WHERE created_date_utc >= '2021-01-01' AND is_valid_order
  GROUP BY 1, 2
),
 
transaction_data AS (
  SELECT
     pd_orders.global_entity_id,
     pd_orders.country_name,
 
     pd_orders.vendor_code,
      
 
     COUNT(DISTINCT IF(pd_orders.is_valid_order, pd_orders.id, NULL)) AS success_orders_l4w,
     COUNT(DISTINCT pd_orders.id) AS gross_orders_l4w,
     COUNT(DISTINCT IF(pd_orders.is_failed_order, pd_orders.id, NULL)) AS failed_orders_l4w,
 
     SUM(IF(pd_orders.is_valid_order, pd_orders_agg_accounting.gmv_eur, 0)) AS gmv_eur_l4w,
     SUM(IF(pd_orders.is_valid_order, pd_orders_agg_accounting.gmv_local, 0)) AS gmv_local_l4w,
     SUM(IF(pd_orders.is_valid_order, pd_orders_agg_accounting.gfv_eur, 0)) AS gfv_eur_l4w,
     SUM(IF(pd_orders.is_valid_order, pd_orders_agg_accounting.gfv_local, 0)) AS gfv_local_l4w
 
   FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS pd_orders
    
   LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS pd_orders_agg_accounting
          ON pd_orders_agg_accounting.global_entity_id = pd_orders.global_entity_id
          AND pd_orders_agg_accounting.uuid = pd_orders.uuid
         AND pd_orders_agg_accounting.created_date_utc >= DATE_SUB(CURRENT_DATE, INTERVAL 105 DAY)
 
   WHERE TRUE
     AND NOT (pd_orders.global_entity_id = 'FP_SG' AND pd_orders_agg_accounting.gmv_local > 100000)
     AND pd_orders.is_gross_order
      AND DATE(ordered_at_local) >= DATE_SUB(CURRENT_DATE, INTERVAL 4 WEEK) --get orders last 4 weeks
      AND pd_orders.created_date_utc >= DATE_SUB(CURRENT_DATE, INTERVAL 5 WEEK)
    
   GROUP BY 1, 2, 3
)
 
SELECT
  pdv.global_entity_id,
  pdv.vendor_code,
  pdv.name AS vendor_name,
  pd_cities.name AS city_name,
  pdv.activated_date_utc,
  first_order_date.first_valid_order_date,
  pdv.primary_cuisine,
  platform_performances.gmv_class,
  CASE
        WHEN pd_vendors_agg_business_types.business_type_apac = 'kitchens' THEN 'kitchen'
        WHEN pd_vendors_agg_business_types.business_type_apac = 'concepts' THEN 'concepts'
        WHEN pd_vendors_agg_business_types.business_type_apac = 'dmart' THEN 'pandamart'
        WHEN pd_vendors_agg_business_types.business_type_apac = 'shops' THEN 'shops'
        ELSE 'restaurants'
      END AS vendor_type,
  product_data.vendor_product_with_image_count,
  product_data.vendor_product_count,
  transaction_data.success_orders_l4w,
  transaction_data.gross_orders_l4w,
  transaction_data.failed_orders_l4w,
  SAFE_DIVIDE(transaction_data.failed_orders_l4w, transaction_data.gross_orders_l4w) AS fail_rate,
 
  transaction_data.gmv_eur_l4w,
  transaction_data.gmv_local_l4w,
  transaction_data.gfv_eur_l4w,
  transaction_data.gfv_local_l4w,
  SAFE_DIVIDE(transaction_data.gfv_local_l4w, transaction_data.success_orders_l4w) AS gfv_local
 
 
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pdv
  
LEFT JOIN product_data
  ON pdv.global_entity_id = product_data.global_entity_id
  AND pdv.vendor_code = product_data.vendor_code
    
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_cities` AS pd_cities
           ON pdv.global_entity_id = pd_cities.global_entity_id
          AND pdv.pd_city_id = pd_cities.id
  
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS sf_accounts
           ON pdv.global_entity_id = sf_accounts.global_entity_id
          AND pdv.vendor_code = sf_accounts.vendor_code
  
LEFT JOIN UNNEST(sf_accounts.platform_performances) AS platform_performances
  
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
           ON pd_vendors_agg_business_types.uuid = pdv.uuid
    
LEFT JOIN first_order_date
  ON pdv.global_entity_id = first_order_date.global_entity_id
  AND pdv.vendor_code = first_order_date.vendor_code
   
LEFT JOIN transaction_data
  ON pdv.global_entity_id = transaction_data.global_entity_id
  AND pdv.vendor_code = transaction_data.vendor_code
  
WHERE pdv.global_entity_id = 'FP_KH' --CHANGE HERE FOR COUNTRY FILTER
  --AND pd_cities.name NOT IN ('Ta Khmau', 'Phnom Penh') -- u can explore the cities name list here bong
  AND pdv.is_active
  AND NOT pdv.is_private
  AND NOT pdv.is_test
  AND LOWER(pdv.name) NOT LIKE '%test%'


