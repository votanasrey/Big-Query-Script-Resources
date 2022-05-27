SELECT 
    o.created_date_local,
    order_code,
    v.vendor_code,
    v.name as vendor_name,
    CASE WHEN v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END as city_name,
    CASE WHEN LENGTH(o.order_code) < 11 THEN "Normal Order" ELSE "PandaSend Order" END order_type,
    CASE WHEN vertical_type = "darkstores" THEN "Dmart" ELSE v.vertical END as business_type,
    o.vendor.order_status,
    v.vertical_type,
    d.lg_rider_id,
    d.uuid as lg_delivery_uuid,
    IFNULL(d.rider_late_in_seconds,0) as rider_late_in_seconds,
    IFNULL(d.vendor_late_in_seconds,0) as vendor_late_in_seconds,
    IFNULL(d.delivery_delay_in_seconds,0) as delivery_delay_in_seconds,
FROM `fulfillment-dwh-production.pandata_curated.lg_orders` as o, UNNEST (o.rider.deliveries) as d
JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` as v 
ON o.vendor_code = v.vendor_code
AND o.global_entity_id = v.global_entity_id
WHERE o.created_date_utc >= DATE_TRUNC(DATE_SUB(current_date(),INTERVAL 2 MONTH), MONTH)
AND o.global_entity_id = "FP_KH"
--AND LENGTH(o.order_code) < 11