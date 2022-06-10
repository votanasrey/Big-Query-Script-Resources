
SELECT
 --created_date_local, 
 EXTRACT(YEAR FROM DATE(created_date_local)) AS rider_accepted_year_local,
 EXTRACT(MONTH FROM DATE(created_date_local)) AS rider_accepted_month_local,
 SUM(working_time_in_seconds/3600) AS working_time_in_hour, 
 SUM(all_shift_count) AS all_shift_count,
 SUM(delivery.completed_deliveries_count) AS completed_deliveries_count,
 SUM(delivery.pickup_distance_sum_in_meters) AS pickup_distance_sum_in_meters,
 SUM(delivery.dropoff_distance_sum_in_meters) AS dropoff_distance_sum_in_meters,
 SUM(delivery.pickup_distance_sum_in_meters + delivery.dropoff_distance_sum_in_meters)/1000 AS total_distance_in_km
FROM `fulfillment-dwh-production.pandata_curated.lg_daily_rider_zone_kpi` rider,
 UNNEST(deliveries_by_entity) delivery
 WHERE created_date_local >= "2022-01-01"
 AND global_entity_id = "FP_KH"
GROUP BY 1,2
ORDER BY rider_accepted_month_local DESC