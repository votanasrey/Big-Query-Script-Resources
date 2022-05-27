


WITH CTE AS
(
SELECT
  LEFT(STRING(o.created_date_local),7) as months,
  FORMAT_DATE("%G-%V", o.created_at_local) weeks,
  CASE WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 0 THEN "7 - Sun" 
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 1 THEN "1 - Mon"
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 2 THEN "2 - Tue"
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 3 THEN "3 - Wed"
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 4 THEN "4 - Thu"
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 5 THEN "5 - Fri"
       WHEN EXTRACT(DAYOFWEEK from o.created_date_local) - 1 = 6 THEN "6 - Sat"
       END as day_of_week,
  EXTRACT(HOUR from o.created_at_local) as hour,
  o.created_date_local as dates,
  CASE WHEN v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END as city_name,
  CASE WHEN (lower(vendor.comment) like "%sunmi%" OR o.vendor_code IN ("iph3","fgz2","rzax","ajmw","zdys","p3bi","ydqr") 
        OR lower(vendor.comment) LIKE "%ម៉ាស៊ីន%foodpanda%" OR lower(vendor.comment) LIKE "%ក្រុមហ៊ុន%ជួសជុល%" OR lower(vendor.comment) LIKE "%foodpanda%" OR lower(vendor.comment) LIKE "%ម៉ាស៊ីន%ជួសជុល%") 
        AND entity.id = "ODR_KH" THEN "pandasend (device delivery)" 
         WHEN entity.id = "ODR_KH" THEN "pandasend (normal)" 
         ELSE b.business_type_apac
         END AS business_type,
  v.chain_code, 
  v.vendor_code,
  v.name as vendor_name,
  o.order_code,
  AVG(CASE WHEN --ST_DISTANCE(deliveries.pickup_geo,deliveries.dropoff_geo) > 3000 AND 
  deliveries.dropoff_distance_manhattan_in_meters > 25000 THEN ST_DISTANCE(deliveries.pickup_geo,deliveries.dropoff_geo) 
  ELSE deliveries.dropoff_distance_manhattan_in_meters END) OVER (PARTITION BY o.order_code) AS drop_off_distance,
  ifnull(o.rider.delivery_fee_local,0)/100 AS df_rev,
FROM
  `fulfillment-dwh-production.pandata_curated.lg_orders` as o, UNNEST (rider.deliveries) AS deliveries
LEFT JOIN 
  `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
  ON o.vendor_code = v.vendor_code
  AND o.global_entity_id = v.global_entity_id
LEFT JOIN  `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` as b
  ON o.vendor_code = b.vendor_code
  AND o.global_entity_id = b.global_entity_id
WHERE
  o.created_date_utc >= DATE_TRUNC(DATE_SUB(current_date,INTERVAL 3 Month), Month)
  AND o.created_date_local >= DATE_TRUNC(DATE_SUB(current_date,INTERVAL 3 Month), Month)
  AND o.global_entity_id='FP_KH'
  AND rider.order_status = "completed"
), aggregate as 
(
  SELECT
    months,
    weeks,
    day_of_week,
    dates,
  	hour,
    city_name,
    business_type,
    chain_code,
    vendor_code,
    vendor_name,
    order_code,
    AVG(drop_off_distance) as drop_off_distance,
    AVG(df_rev) as df_rev,

  FROM CTE
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),final as 
(
  SELECT
    months,
    weeks,
    day_of_week,
    dates,
  	hour,
    city_name,
    business_type,
    chain_code,
    vendor_code,
    vendor_name,
    order_code,
    CASE WHEN LEFT(business_type,9) = "pandasend" THEN (CEIL(drop_off_distance/1000) * 0.12) + 0.53 ELSE df_rev END as df_rev,
  FROM aggregate
)
SELECT 
  months,
  weeks,
  day_of_week,
  dates,
  hour,
  city_name,
  business_type,
  chain_code, 
  vendor_code,
  vendor_name,
  SUM(df_rev) AS df_rev,
  COUNT(DISTINCT order_code) AS order_count,
  SUM(ifnull(df_rev,0))/COUNT(DISTINCT order_code) AS avg_df
FROM final
GROUP BY 1,2,3,4,5,6,7,8,9,10



