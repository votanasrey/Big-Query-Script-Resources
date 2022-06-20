

DECLARE date1 DATE;
SET date1 = "2022-06-01"; -- Able to change date here "YYYY-MM-DD"

SELECT 
  po.code AS order_code,
  po.vendor_code,
  po.vendor_name,
  po.created_date_local,
  IFNULL(lo.rider.rider_late_in_seconds,0) AS rider_late_in_seconds,
  IFNULL(lo.rider.vendor_late_in_seconds,0) AS vendor_late_in_seconds,
  IFNULL(lo.rider.order_delay_in_seconds,0) AS order_delay_in_seconds,
  po.is_failed_order
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS po
LEFT JOIN `fulfillment-dwh-production.pandata_curated.lg_orders` AS lo 
  ON po.code = lo.order_code
  AND po.vendor_code = lo.vendor_code 
  AND po.created_date_local = lo.created_date_local
  AND po.global_entity_id = lo.global_entity_id
WHERE 
  po.global_entity_id = 'FP_KH' 
  AND po.is_billable 
  AND po.is_gross_order 
  AND po.created_date_utc >= date1  
  AND lo.created_date_utc >= date1  
ORDER BY lo.rider.order_delay_in_seconds DESC
LIMIT 1000

  
