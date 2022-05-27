

WITH google_analytic_table AS(
  SELECT DISTINCT
    date_utc,
    vendor_click_details.vendor_click_origin AS shop_swimlane_title,
    COUNT(ga_session_id) AS number_of_sessions
  FROM `fulfillment-dwh-production.pandata_curated.ga_vendors_sessions` 

  WHERE 
    global_entity_id = 'FP_KH'
    AND is_active
    AND partition_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) AND (CURRENT_DATE() - 2)
    AND date_utc BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) AND (CURRENT_DATE() - 2)
    AND vendor_click_details.vendor_click_origin IS NOT NULL
    AND vendor_click_details.vendor_click_origin IN UNNEST([
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
])
  GROUP BY 1,2
)

SELECT * FROM google_analytic_table ORDER BY 1 DESC

