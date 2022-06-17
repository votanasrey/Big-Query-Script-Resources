
DECLARE date1, date2 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK);
SET date2 = (CURRENT_DATE() - 2); 

WITH google_analytic_table AS(
  SELECT 

    va.ga_vendor_id,
    va.pd_vendor_code AS vendor_code,

    COUNT(DISTINCT CONCAT(ga.ga_fullvisitor_id,CAST(ga.ga_visit_id AS STRING))) AS number_of_sessions,

    COUNT(CASE WHEN ga.funnel LIKE '1_____' THEN ga.funnel ELSE NULL END) AS home_counts,
    COUNT(CASE WHEN ga.funnel LIKE '_1____' THEN ga.funnel ELSE NULL END) AS list_counts,
    COUNT(CASE WHEN ga.funnel LIKE '__1___' THEN ga.funnel ELSE NULL END) AS menu_counts,
    COUNT(CASE WHEN ga.funnel LIKE '___1__' THEN ga.funnel ELSE NULL END) AS add_to_cart_counts,
    COUNT(CASE WHEN ga.funnel LIKE '____1_' THEN ga.funnel ELSE NULL END) AS checkout_counts,
    COUNT(CASE WHEN ga.funnel LIKE '_____1' THEN ga.funnel ELSE NULL END) AS transaction_counts,


    COUNT(DISTINCT ga.ga_user_id) AS number_of_users_in_sessions,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '1_____' THEN ga.ga_user_id ELSE NULL END) AS home_user_count,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '_1____' THEN ga.ga_user_id ELSE NULL END) AS list_user_count,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '__1___' THEN ga.ga_user_id ELSE NULL END) AS menu_user_count,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '___1__' THEN ga.ga_user_id ELSE NULL END) AS addtocart_user_count,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '____1_' THEN ga.ga_user_id ELSE NULL END) AS checkout_user_count,
    COUNT(DISTINCT CASE WHEN ga.funnel LIKE '_____1' THEN ga.ga_user_id ELSE NULL END) AS transaction_user_count,


    ROUND(COUNT(DISTINCT CASE WHEN ga.is_transaction THEN ga.ga_session_id END) / COUNT(DISTINCT ga.ga_session_id),5) AS CVR,

    -- micro coversion rate 
    ROUND(IFNULL(SUM(ga.mcvr1)/COUNT(ga.mcvr1),0),5) AS mCVR1,
    ROUND(IFNULL(SUM(ga.mcvr2)/COUNT(ga.mcvr2),0),5) AS mCVR2,
    ROUND(IFNULL(SUM(ga.mcvr3)/COUNT(ga.mcvr3),0),5) AS mCVR3,
    ROUND(IFNULL(SUM(ga.mcvr4)/COUNT(ga.mcvr4),0),5) AS mCVR4,

    -- micro ordered conversion rate
    ROUND(IFNULL(SUM(ga.mocvr1)/COUNT(ga.mocvr1),0),5) AS mOCVR1,
    ROUND(IFNULL(SUM(ga.mocvr2)/COUNT(ga.mocvr2),0),5) AS mOCVR2,
    ROUND(IFNULL(SUM(ga.mocvr3)/COUNT(ga.mocvr3),0),5) AS mOCVR3,
    ROUND(IFNULL(SUM(ga.mocvr4)/COUNT(ga.mocvr4),0),5) AS mOCVR4,


  FROM `fulfillment-dwh-production.pandata_curated.ga_sessions` AS ga
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.ga_vendors_sessions` AS va
    ON ga.global_entity_id = va.global_entity_id
    AND ga.ga_session_id = va.ga_session_id

  WHERE ga.global_entity_id = 'FP_KH'
  AND va.global_entity_id = 'FP_KH'
  
  AND va.is_active

  AND ga.partition_date >= date1
  AND va.partition_date >= date1
  AND ga.partition_date <= date2
  AND va.partition_date <= date2

  GROUP BY 1,2
  ORDER BY 1 DESC
)
SELECT * FROM google_analytic_table
--WHERE vendor_code IN (SELECT vendor_code FROM google_analytic_table GROUP BY 1 HAVING COUNT(1)>1)
ORDER BY number_of_sessions DESC

