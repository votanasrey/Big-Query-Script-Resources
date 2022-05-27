
SELECT 
  date_utc, 
  platform,
  SUM(sl_sessions) AS total_sl_sessions,
  SUM(sl_sess_clicks) AS total_sl_session_clicks,
  SUM(sl_loaded) AS totalsl_loaded,
  SUM(sl_transactions) AS total_sl_transactions,
  SUM(sl_gmv) AS total_sl_gmv,
FROM `fulfillment-dwh-production.curated_data_shared_product_analytics.swimlane_sessions_rl` 
WHERE 
  global_entity_id = 'FP_KH'
  AND partition_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) AND (CURRENT_DATE() - 2)
  AND date_utc BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK) AND (CURRENT_DATE() - 2)
  --AND date_utc = '2022-05-16'
GROUP BY 1,2
ORDER BY 1,2 DESC