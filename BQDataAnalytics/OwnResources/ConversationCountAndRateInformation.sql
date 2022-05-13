

DECLARE date1, date2 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH);
SET date2 = (CURRENT_DATE() - 2); 

WITH google_analytic_table AS(
  SELECT 

    partition_date,

    COUNT(DISTINCT CONCAT(ga_fullvisitor_id,CAST(ga_visit_id AS STRING))) AS number_of_sessions,

    COUNT(CASE WHEN funnel LIKE '1_____' THEN funnel ELSE NULL END) AS home_counts,
    COUNT(CASE WHEN funnel LIKE '_1____' THEN funnel ELSE NULL END) AS list_counts,
    COUNT(CASE WHEN funnel LIKE '__1___' THEN funnel ELSE NULL END) AS menu_counts,
    COUNT(CASE WHEN funnel LIKE '___0__' THEN funnel ELSE NULL END) AS add_to_cart_counts,
    COUNT(CASE WHEN funnel LIKE '____0_' THEN funnel ELSE NULL END) AS checkout_counts,
    COUNT(CASE WHEN funnel LIKE '_____0' THEN funnel ELSE NULL END) AS transaction_counts,


    COUNT(DISTINCT ga_user_id) AS number_of_users_in_sessions,
    COUNT(DISTINCT CASE WHEN funnel LIKE '1_____' THEN ga_user_id ELSE NULL END) AS home_user_count,
    COUNT(DISTINCT CASE WHEN funnel LIKE '_1____' THEN ga_user_id ELSE NULL END) AS list_user_count,
    COUNT(DISTINCT CASE WHEN funnel LIKE '__1___' THEN ga_user_id ELSE NULL END) AS menu_user_count,
    COUNT(DISTINCT CASE WHEN funnel LIKE '___0__' THEN ga_user_id ELSE NULL END) AS addtocart_user_count,
    COUNT(DISTINCT CASE WHEN funnel LIKE '____0_' THEN ga_user_id ELSE NULL END) AS checkout_user_count,
    COUNT(DISTINCT CASE WHEN funnel LIKE '_____0' THEN ga_user_id ELSE NULL END) AS transaction_user_count,


    ROUND(COUNT(DISTINCT CASE WHEN is_transaction THEN ga_session_id END) / COUNT(DISTINCT ga_session_id),5) AS CVR,

    -- micro coversion rate 
    ROUND(SUM(mcvr1)/COUNT(mcvr1),5) AS mCVR1,
    ROUND(SUM(mcvr2)/COUNT(mcvr2),5) AS mCVR2,
    ROUND(SUM(mcvr3)/COUNT(mcvr3),5) AS mCVR3,
    ROUND(SUM(mcvr4)/COUNT(mcvr4),5) AS mCVR4,

    -- micro ordered conversion rate
    ROUND(SUM(mocvr1)/COUNT(mocvr1),5) AS moCVR1,
    ROUND(SUM(mocvr2)/COUNT(mocvr2),5) AS moCVR2,
    ROUND(SUM(mocvr3)/COUNT(mocvr3),5) AS moCVR3,
    ROUND(SUM(mocvr4)/COUNT(mocvr4),5) AS moCVR4,


  FROM `fulfillment-dwh-production.pandata_curated.ga_sessions`
  WHERE global_entity_id = 'FP_KH'
  AND partition_date >= date1
  AND partition_date <= date2

  GROUP BY 1
  ORDER BY 1 DESC
)
SELECT * FROM google_analytic_table

