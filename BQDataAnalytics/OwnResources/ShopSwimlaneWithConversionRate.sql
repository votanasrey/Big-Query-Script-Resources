
DECLARE date1, date2 DATE;
--- declare a array to store the swimlane title list 
DECLARE swimlane_title_array ARRAY <STRING>;

SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK);
SET date2 = (CURRENT_DATE() - 2); 
SET swimlane_title_array = [
   'swimlane - Panda Picks 🐼',
    'swimlane - Exclusive Offer 💸',
    'swimlane - Supermarket and Convenience 🛒💨',
    'swimlane - Flowers and Gifts 🎁💐',
    'swimlane - Groceries and Fresh Produces 🥦🥩',
    'swimlane - Trending Now 🌟',
    'swimlane - Stay Healthy and Beautiful 👳‍♀️',
    'swimlane - New Shops Near You 🥳',
    "swimlane - foodpanda's Pet Care  🐶🐱",
    'swimlane - Electronics and Stationery 🔌📚',
    'shops_list'
];

WITH google_analytic_table AS(
  SELECT DISTINCT

    vendor_click_details.vendor_click_origin AS shop_swimlane_title,
    pd_vendor_code AS vendor_code,

    COUNT(ga_session_id) AS number_of_sessions,
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
    AND vendor_click_details.vendor_click_origin IS NOT NULL
    AND vendor_click_details.vendor_click_origin IN UNNEST(swimlane_title_array)
  GROUP BY 1,2
  ORDER BY 1 DESC
)
--------- result_table 
, result_table AS (
  SELECT 
    shop_swimlane_title,
    vendor_code,

    total_menu_counts,
    (CASE WHEN total_add_to_cart_counts >= total_menu_counts THEN total_menu_counts ELSE total_add_to_cart_counts END) AS total_add_to_cart_counts,
    (CASE WHEN total_checkout_counts >= total_add_to_cart_counts THEN total_add_to_cart_counts ELSE total_checkout_counts END) AS total_checkout_counts,
    (CASE WHEN total_transaction_counts >= total_checkout_counts THEN total_checkout_counts ELSE total_transaction_counts END) AS total_transaction_counts,

  FROM google_analytic_table

), 
--------- output table 
output_table AS (
  SELECT 
    shop_swimlane_title,
    vendor_code,

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
SELECT * FROM output_table ORDER BY 1,3 DESC

