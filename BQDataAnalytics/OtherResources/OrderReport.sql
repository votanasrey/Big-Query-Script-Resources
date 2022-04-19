--AVG_DF2 ADDED!
WITH

lg_deliveries AS (

SELECT
	rdbms_id,
	order_code,
	MAX(rider_dropped_off_at_local) AS rider_dropped_off_at_local,
	MAX(rider_near_customer_at_local) AS rider_near_customer_at_local,
 FROM pandata.lg_order_deliveries
 WHERE rdbms_id = 20
   AND created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 10 YEAR)
 GROUP BY 1,2

)

SELECT
  DATE_TRUNC(DATE(orders.ordered_at_local),week) AS ordered_month,
  --orders.expected_delivery_at_local,
  
  /*
  COALESCE(lg_deliveries.rider_dropped_off_at_local,
                                       lg_deliveries.rider_near_customer_at_local,
                                       lg_orders.sent_to_vendor_at_local,
                                       orders.expected_delivery_at_local) AS actual_delivery_date,
  */
  
	IFNULL(lg_cities.name,vendor_zones.main_lg_city_name) AS order_hurrier_city,
  IFNULL(lg_zones.name,vendor_zones.main_lg_zone_name) AS order_hurrier_zone,
  COUNT(DISTINCT(DATE(orders.ordered_at_local))) AS day_count,
  COUNT(DISTINCT IF(orders.is_valid_order, orders.customer_id, NULL)) AS customer_count,
  --COUNT(DISTINCT IF(fct_orders.is_gross_order, fct_orders.id, NULL))
  COUNT(DISTINCT(CASE
                     WHEN orders.is_first_valid_order
                     THEN orders.customer_id
                     ELSE NULL
                     END
                 )) AS new_customer_count,
  COUNT(DISTINCT IF(orders.is_valid_order, orders.code, NULL)) AS order_count,
  COUNT(DISTINCT IF(orders.is_valid_order, orders.vendor_id, NULL)) AS vendor_count,
  SUM(IF(orders.is_valid_order, orders.gfv_local, 0))AS total_gfv,
  SUM(IF(orders.is_valid_order, orders.gmv_local,0)) AS total_gmv,
  --AVG(IF(orders.is_valid_order, delivery_fee_local,0)) as avg_DF,--delete this if the next row is correct
  AVG(CASE WHEN orders.is_valid_order then delivery_fee_local else null end) as avg_DF,
  COUNT(DISTINCT(IF(delivery_fee_local=0,orders.code,NULL))) as count_freedelivery,
  --COUNT(DISINCT IF(orders.is_valid_order
  AVG(IF(orders.is_valid_order, gfv_local,0)) as AFV,
  COUNT(DISTINCT (case when orders.vendor_name LIKE '%Jollibee%' and orders.is_valid_order then orders.vendor_id end)) AS Jollibee, --STUCK HERE
  COUNT(DISTINCT (case when orders.vendor_name LIKE '%McDonald%' and orders.is_valid_order then orders.vendor_id end)) AS McDo,
  COUNT(distinct (case when orders.vendor_business_type="shops" and orders.is_valid_order then orders.vendor_id end)) as shops,
  COUNT(distinct (case when orders.vendor_business_type="shops" and orders.is_valid_order then orders.code end)) as shops_orders,
  COUNT(distinct (case when orders.vendor_name LIKE '%Jollibee%' and orders.is_valid_order then orders.code end)) as jollibee_orders,
  COUNT(distinct (case when orders.vendor_name LIKE '%McDonald%' and orders.is_valid_order then orders.code end)) as mcdo_orders,
  SAFE_DIVIDE(
      SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.discount_value_local,0)*(1-(orders.discount_ratio/100))END) +
      SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.voucher_value_local,0)*(1-(orders.voucher_ratio/100))END),
      SUM(CASE WHEN orders.is_valid_order THEN orders.gmv_local END)) as subsidy,
  SAFE_DIVIDE(--chain_id is null, commission_percentage_combined
      SUM(case when orders.chain_id is null and orders.is_valid_order then orders.commission_percentage_combined end),
      COUNT(DISTINCT (case when orders.is_valid_order then orders.id end))) as longtail_commission,
 COUNT(DISTINCT IF(orders.is_failed_order, orders.id, NULL)) as failed_orders,
 SAFE_DIVIDE(
      COUNT(DISTINCT IF(orders.is_failed_order, orders.id, NULL)),
      COUNT(DISTINCT IF(orders.is_gross_order, orders.id, NULL))) as fail_rate,
  SAFE_DIVIDE(
      COUNT(DISTINCT IF(orders.is_failed_order_vendor, orders.id, NULL)),
      COUNT(DISTINCT IF(orders.is_gross_order, orders.id, NULL))) as vendor_fail,
  SAFE_DIVIDE(
      COUNT(DISTINCT IF(orders.is_failed_order_customer, orders.id, NULL))
      +
      COUNT(DISTINCT IF(orders.is_failed_order_foodpanda, orders.id,NULL)),
      COUNT(DISTINCT IF(orders.is_gross_order, orders.id, NULL))) as internal_fail,
  COUNT(DISTINCT(IF(DATE_TRUNC(DATE(orders.ordered_at_local), MONTH) = DATE_TRUNC(DATE(customers.first_order_valid_at_local), MONTH), orders.id, NULL))) count_orders_nc_same_month,
   SAFE_DIVIDE(
      SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.discount_value_local,0)*(1-(orders.discount_ratio/100))END) +
      SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.voucher_value_local,0)*(1-(orders.voucher_ratio/100))END),
      SUM(CASE WHEN orders.is_valid_order THEN orders.gmv_local END)) as vendor_sub_percent,
  SAFE_DIVIDE(
    SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.discount_value_local,0)*(1-(orders.discount_ratio/100))END)
    +
    SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.voucher_value_local,0)*(1-(orders.voucher_ratio/100))END),
    SUM(CASE WHEN orders.is_valid_order THEN orders.gmv_local END)) as DH_sub_percent_2,
  SAFE_DIVIDE(
    SUM(CASE WHEN orders.is_valid_order then orders.voucher_value_local * orders.voucher_ratio/100 end),
    SUM(CASE WHEN orders.is_valid_order THEN orders.gmv_local END)) as DH_sub_percent,
  SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.discount_value_local,0)*(1-(orders.discount_ratio/100))END)
    +
    SUM(CASE WHEN orders.is_valid_order THEN COALESCE(orders.voucher_value_local,0)*(1-(orders.voucher_ratio/100))END) as DH_sub_total,
 SUM(CASE WHEN orders.is_valid_order THEN orders.gmv_local END) as GMV_total,

FROM pandata.fct_orders AS orders

LEFT JOIN pandata.lg_orders AS lg_orders
       ON lg_orders.rdbms_id = orders.rdbms_id
      AND lg_orders.order_id = orders.id
      AND lg_orders.created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL 10 YEAR) 

LEFT JOIN lg_deliveries
       ON lg_deliveries.rdbms_id = orders.rdbms_id
      AND lg_deliveries.order_code = orders.code

LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_lg_zones` AS vendor_zones
       ON orders.rdbms_id = vendor_zones.pd_rdbms_id
      AND orders.vendor_id = vendor_zones.id

LEFT JOIN pandata.lg_zones
       ON lg_orders.rdbms_id=lg_zones.rdbms_id
      AND lg_orders.lg_zone_id=lg_zones.id

LEFT JOIN pandata.lg_cities
       ON lg_zones.rdbms_id=lg_cities.rdbms_id
      AND lg_zones.lg_city_id=lg_cities.id
/*      
LEFT JOIN pandata.dim_vendors as dim_vendors
      ON dim_vendors.rdbms_id=vendor_zones.rdbms_id
      AND dim_vendors.id=vendor_zones.vendor_id
  */    
LEFT JOIN pandata_ap_product_external.city_zone_ga_sessions_apac AS sessions
      ON sessions.city_zone_id=orders.lg_zone_id
      AND sessions.date_local=orders.created_date_local

LEFT JOIN pandata.dim_customers as customers
       ON customers.rdbms_id = orders.rdbms_id
      AND customers.id = orders.customer_id

WHERE orders.rdbms_id = 20 
  AND orders.created_date_local >= '1900-01-01'
  --AND NOT orders.is_failed_order
  --AND orders.is_gross_order
  
  AND DATE(orders.ordered_at_local)>='2021-1-01'
  --AND lg_cities.name in ('Cagayan de oro','Bacolod','Manila','South mm','Davao','Cebu','General santos','Iloilo')
    /* this is for the join of sessions table

  AND DATE(sessions.date_local)>='2019-01-01'
  AND sessions.country='Philippines'
  */
  --AND IFNULL(lg_cities.name,vendor_zones.vendor_ops_city_name) NOT IN ('Manila','Cebu','Davao')
  

GROUP BY 1,2,3
ORDER BY 2,3,1