 DECLARE start_date, end_date DATE;

 SET start_date = "2022-01-01";
 SET end_date = "2022-04-28";

 WITH orders as
 (
 SELECT  
    orders.global_entity_id,
    orders.created_date_local,
    orders.vendor_code,
    orders.order_code,
    deliveries.lg_rider_id,
    deliveries.uuid,
    deliveries.id,
    deliveries.status,
    deliveries.stacked_deliveries_rank,
    CASE WHEN deliveries.stacked_deliveries_rank IS NOT NULL THEN "stacked delivery" else "non-stacked delivery" END as remark,
    deliveries.delivery_delay_in_seconds,
    --SUM(deliveries.pickup_distance_manhattan_in_meters + deliveries.dropoff_distance_manhattan_in_meters) OVER (PARTITION BY orders.order_code) AS total_delivery_distance,

    FROM `fulfillment-dwh-production.pandata_curated.lg_orders` AS orders,
    UNNEST (rider.deliveries) AS deliveries
    WHERE DATE(orders.created_date_utc) BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
    AND DATE(orders.created_date_local) BETWEEN start_date AND end_date
    AND global_entity_id = "FP_KH"
 ), vendor as
 (
    SELECT 
      v.vendor_code, 
      v.name as vendor_name,
      v.vertical,
      v.vertical_type,
      CASE WHEN v.location.city =  "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END as city_name,

    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` as v 
    WHERE global_entity_id = "FP_KH"
 ), shifts as 
 (
   SELECT
      lg_rider_id AS rider_id,
      --d.vehicle.name,
      date(start_at_local) as shift_date,
      SUM(actual_working_time_in_seconds)/60/60 as total_hours_worked
   FROM `fulfillment-dwh-production.pandata_curated.lg_shifts` AS lg_shifts
   WHERE 1=1
   AND created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
   AND created_date_local BETWEEN start_date AND end_date
   AND lg_country_code = 'kh'
   AND state = 'EVALUATED'
   AND DATE(start_at_local) BETWEEN start_date AND end_date
   GROUP BY 1,2
)
 SELECT 
    orders.created_date_local,
    --orders.vendor_code,
    --vendor.vendor_name,
    --vendor.vertical, --don't need
    vendor.city_name,
    COUNT(DISTINCT order_code) total_order_count,
    COUNT(DISTINCT uuid) as total_delivery_count,
    --SUM(rider_payment.total_local) as total_delivery_rider_payment,
    
    IFNULL(COUNT(DISTINCT CASE WHEN remark = "non-stacked delivery" THEN uuid END),0) as total_non_stacked_delivery_count,
    IFNULL(SUM(CASE WHEN remark = "non-stacked delivery" THEN delivery_delay_in_seconds END),0) as total_non_stacked_delivery_delay_in_seconds,

    --IFNULL(SUM(CASE WHEN remark = "non-stacked delivery" THEN rider_payment.total_local END),0) as total_non_stacked_delivery_rider_payment,

    IFNULL(COUNT(DISTINCT CASE WHEN remark <> "non-stacked delivery" THEN uuid END),0) as total_stacked_delivery_count,
    IFNULL(SUM(CASE WHEN remark <> "non-stacked delivery" THEN delivery_delay_in_seconds END),0) as total_stacked_delivery_delay_in_seconds,
    --IFNULL(SUM(CASE WHEN remark <> "non-stacked delivery" THEN rider_payment.total_local END),0) as total_stacked_delivery_rider_payment,

    IFNULL(COUNT(DISTINCT CASE WHEN stacked_deliveries_rank = 1 THEN uuid END),0) as total_1st_stacked_delivery_count,
    --IFNULL(SUM(CASE WHEN stacked_deliveries_rank = 1 THEN rider_payment.total_local END),0) as total_1st_stacked_delivery_rider_payment,

    IFNULL(COUNT(DISTINCT CASE WHEN stacked_deliveries_rank = 2 THEN uuid END),0) as total_2nd_stacked_delivery_count,
    --IFNULL(SUM(CASE WHEN stacked_deliveries_rank = 2 THEN rider_payment.total_local END),0) as total_2nd_stacked_delivery_rider_payment,

    IFNULL(COUNT(DISTINCT CASE WHEN stacked_deliveries_rank = 3 THEN uuid END),0) as total_3rd_stacked_delivery_count,
    --IFNULL(SUM(CASE WHEN stacked_deliveries_rank = 3 THEN rider_payment.total_local END),0) as total_3rd_stacked_delivery_rider_payment,

    IFNULL(COUNT(DISTINCT CASE WHEN stacked_deliveries_rank = 4 THEN uuid END),0) as total_4th_stacked_delivery_count,
    --IFNULL(SUM(CASE WHEN stacked_deliveries_rank = 4 THEN rider_payment.total_local END),0) as total_4th_stacked_delivery_rider_payment,
   
    COUNT(DISTINCT rider_id) as rider_count,

 FROM orders 
 LEFT JOIN vendor
 ON orders.vendor_code = vendor.vendor_code --26841
 LEFT JOIN shifts 
 ON orders.lg_rider_id = shifts.rider_id 
 AND orders.created_date_local = shifts.shift_date
 WHERE LENGTH(order_code) < 11
 --AND rider_payment.total_local IS NULL
 GROUP BY 1,2--,3,4,5