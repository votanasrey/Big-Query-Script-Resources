




DECLARE date1, date2 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK);
SET date2 = (CURRENT_DATE() - 2); 

WITH ga_vendor_table AS(
  SELECT DISTINCT
    global_entity_id,
    pd_vendor_code AS vendor_code,
    vendor_click_details.expedition_type, 



    COUNT(CASE WHEN vendor_details.vendor_funnel LIKE '1___' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_menu_counts,
    COUNT(CASE WHEN vendor_details.vendor_funnel LIKE '_1__' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_add_to_cart_counts,
    COUNT(CASE WHEN vendor_details.vendor_funnel LIKE '__1_' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_checkout_counts,
    COUNT(CASE WHEN vendor_details.vendor_funnel LIKE '___1' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_transaction_counts

  FROM `fulfillment-dwh-production.pandata_curated.ga_vendors_sessions` 

  WHERE 
    global_entity_id = 'FP_KH'
    AND is_active
    AND partition_date >= date1
    AND partition_date <= date2
    AND vendor_click_details.expedition_type IS NOT NULL
  GROUP BY 1,2,3
  ORDER BY 1 
), pd_vendor_table AS(
  SELECT 
    global_entity_id,
    vendor_code,
    name AS vendor_name,
    vertical_type AS vendor_type
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` 
  WHERE 
    global_entity_id = 'FP_KH'
), merge_table AS(
  SELECT DISTINCT
    (CASE 
      WHEN a.expedition_type = "delivery" AND (b.vendor_type = 'restaurants' OR b.vendor_type = 'street_food') THEN "food delivery"
      WHEN a.expedition_type = "pickup" AND (b.vendor_type = 'restaurants' OR b.vendor_type = 'street_food' OR b.vendor_type = 'darkstores') THEN "pickup"
      WHEN (a.expedition_type = "delivery" OR a.expedition_type = "pickup") AND b.vendor_type = 'darkstores' THEN "dmart"
      WHEN (a.expedition_type = "delivery" OR a.expedition_type != "pickup") AND (b.vendor_type != 'restaurants' OR b.vendor_type != 'street_food' OR b.vendor_type != 'darkstores') THEN "shop"
    END) AS vertical_type,
    a.vendor_code,
    b.vendor_name,
    a.total_menu_counts,
    (CASE WHEN a.total_add_to_cart_counts >= a.total_menu_counts THEN a.total_menu_counts ELSE a.total_add_to_cart_counts END) AS total_add_to_cart_counts,
    (CASE WHEN a.total_checkout_counts >= a.total_add_to_cart_counts THEN a.total_add_to_cart_counts ELSE a.total_checkout_counts END) AS total_checkout_counts,
    (CASE WHEN a.total_transaction_counts >= a.total_checkout_counts THEN a.total_checkout_counts ELSE a.total_transaction_counts END) AS total_transaction_counts,

  FROM ga_vendor_table AS a
  LEFT JOIN pd_vendor_table AS b 
    ON a.vendor_code = b.vendor_code 
    AND a.global_entity_id = b.global_entity_id 
), result_table AS(
  SELECT 
    vertical_type,
    vendor_code,
    vendor_name,
    total_menu_counts,
    (CASE WHEN total_add_to_cart_counts >= total_menu_counts THEN total_menu_counts ELSE total_add_to_cart_counts END) AS total_add_to_cart_counts,
    (CASE WHEN total_checkout_counts >= total_add_to_cart_counts THEN total_add_to_cart_counts ELSE total_checkout_counts END) AS total_checkout_counts,
    (CASE WHEN total_transaction_counts >= total_checkout_counts THEN total_checkout_counts ELSE total_transaction_counts END) AS total_transaction_counts,
  FROM merge_table 
  WHERE vertical_type IS NOT NULL
), lastest_table AS(
  SELECT 
    vertical_type,
    vendor_code,
    vendor_name,

    total_menu_counts,
    total_add_to_cart_counts,
    total_checkout_counts,
    total_transaction_counts,

    IFNULL(ROUND(SAFE_DIVIDE(total_add_to_cart_counts, total_menu_counts),2),0) AS mCVR3,
    IFNULL(ROUND(SAFE_DIVIDE(total_checkout_counts, total_add_to_cart_counts),2),0) AS mCVR4,
    IFNULL(ROUND(SAFE_DIVIDE(total_transaction_counts, total_checkout_counts),2),0) AS mCVR5,
    IFNULL(ROUND(SAFE_DIVIDE(total_transaction_counts, total_menu_counts),2),0) AS vendor_conversion_rate

  FROM result_table 
)
SELECT * FROM lastest_table GROUP BY 1

--WHERE vertical_type = "shop" AND vertical_type = "dmart" 
--WHERE vertical_type = "shop" AND vertical_type = "food delivery" 
--WHERE vendor_code IN (SELECT vendor_code FROM result_table GROUP BY 1 HAVING COUNT(1)>1)
--WHERE vendor_code = 'ap0p'
--ORDER BY 1,2,3 


