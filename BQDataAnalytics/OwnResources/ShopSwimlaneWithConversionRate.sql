



DECLARE date1, date2 DATE;
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
  SELECT 

    vendor_click_details.vendor_click_origin AS shop_swimlane_title,
    pd_vendor_code AS vendor_code,

    COUNT(ga_session_id) AS number_of_sessions,
    COUNT(DISTINCT CASE WHEN vendor_details.vendor_funnel LIKE '1___' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_menu_counts,
    COUNT(DISTINCT CASE WHEN vendor_details.vendor_funnel LIKE '_1__' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_add_to_cart_counts,
    COUNT(DISTINCT CASE WHEN vendor_details.vendor_funnel LIKE '__1_' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_checkout_counts,
    COUNT(DISTINCT CASE WHEN vendor_details.vendor_funnel LIKE '___1' THEN vendor_details.vendor_funnel ELSE NULL END) AS total_transaction_counts
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
), result_table AS (
  SELECT 
    shop_swimlane_title,
    vendor_code,

    total_menu_counts,
    total_add_to_cart_counts,
    total_checkout_counts,
    total_transaction_counts,

    IFNULL(ROUND(SAFE_DIVIDE(total_add_to_cart_counts, total_menu_counts),2),0) AS cvr1,
    IFNULL(ROUND(SAFE_DIVIDE(total_checkout_counts, total_add_to_cart_counts),2),0) AS cvr2,
    IFNULL(ROUND(SAFE_DIVIDE(total_transaction_counts, total_checkout_counts),2),0) AS cvr3,
    IFNULL(ROUND(SAFE_DIVIDE(total_transaction_counts, total_menu_counts),2),0) AS conversion_rate

  FROM google_analytic_table

), output_table AS (
  SELECT 
    shop_swimlane_title,
    vendor_code,

    total_menu_counts,
    total_add_to_cart_counts,
    total_checkout_counts,
    total_transaction_counts,

    (CASE WHEN cvr1 > 1.0 THEN 1.0 ELSE cvr1 END) AS cvr1,
    (CASE WHEN cvr2 > 1.0 THEN 1.0 ELSE cvr2 END) AS cvr2,
    (CASE WHEN cvr3 > 1.0 THEN 1.0 ELSE cvr3 END) AS cvr3,
    (CASE WHEN conversion_rate > 1.0 THEN 1.0 ELSE conversion_rate END) AS conversion_rate,

  FROM result_table 
)

SELECT * FROM output_table 
--WHERE cvr1 > 1.0 OR cvr2 > 1.0 OR cvr3 > 1.0 
ORDER BY 1,3 DESC

