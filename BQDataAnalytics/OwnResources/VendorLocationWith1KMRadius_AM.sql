DECLARE keyword STRING;
DECLARE radius INT64;

SET keyword = "yellow%cab"; --Vendor Keyword
SET radius = 1000; --SET radius of the define vendor to get the customer location. Radius are in metre


WITH cus_last_geo AS 
(
  SELECT 
    o.pd_customer_uuid, 
    lo.rider.customer_location_geo, 
    o.vendor_name,
    ROW_NUMBER() OVER (PARTITION BY o.pd_customer_uuid ORDER BY o.created_at_utc) AS ranks
  FROM `fulfillment-dwh-production.pandata_curated.lg_orders` as lo
  JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` as o 
    ON lo.order_code = o.code 
    AND lo.global_entity_id = o.global_entity_id
    AND o.created_date_utc >= DATE_SUB(current_date() - 1, INTERVAL 3 MONTH) - 1
  WHERE lo.global_entity_id = "FP_KH"
  AND lo.created_date_utc >= DATE_SUB(current_date() - 1, INTERVAL 3 MONTH) - 1
  AND lo.created_date_local BETWEEN DATE_SUB(current_date() - 1, INTERVAL 3 MONTH) AND CURRENT_DATE() - 1
  AND o.is_valid_order 
  AND NOT o.is_test_order 
)
, exclude_list as 
(
  SELECT DISTINCT pd_customer_uuid FROM cus_last_geo WHERE LOWER(vendor_name) like CONCAT("%",LOWER(keyword),"%")
)
, vendor_geo AS
(
  SELECT 
    global_entity_id, 
    global_vendor_id,
    vendor_code, 
    name as vendor_name,
    ST_GEOGPOINT(location.longitude, location.latitude) as vendor_location_geo, 
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
  WHERE global_entity_id = "FP_KH"
  AND is_active
  AND NOT is_test
)
, result as 
(
  SELECT 
    vendor_geo.* EXCEPT(vendor_location_geo), 
    cus_last_geo.pd_customer_uuid, 
    customer_location_geo, 
    vendor_location_geo,
  FROM vendor_geo
  JOIN cus_last_geo 
    ON ST_DISTANCE(customer_location_geo, vendor_location_geo) <= radius
    AND cus_last_geo.ranks = 1
  LEFT JOIN exclude_list 
    ON cus_last_geo.pd_customer_uuid = exclude_list.pd_customer_uuid
  WHERE exclude_list.pd_customer_uuid IS NULL
    AND LOWER(vendor_geo.vendor_name) LIKE CONCAT("%",LOWER(keyword),"%")
)
SELECT * FROM result;
