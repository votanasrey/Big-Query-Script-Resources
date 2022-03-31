

DECLARE
  start_date,
  end_date date;
SET
  start_date = '2022-02-01';  -- you can also change the start date following this (year-month-day)
SET
  end_date = "2022-02-20" ;   -- you can also change the end date following this (year-month-day)
SELECT
  o.created_date_local,
  o.code,
  v.chain_code,
  v.chain_name,
  o.vendor_code,
  o.vendor_name,
  ST_X(lo.rider.customer_location_geo) AS customer_longitude,
  ST_Y(lo.rider.customer_location_geo) AS customer_latitude,
  ST_X(lo.rider.vendor.location_geo) AS vendor_longtitude,
  ST_Y(lo.rider.vendor.location_geo) AS vendor_latitude
FROM
  fulfillment-dwh-production.pandata_curated.pd_orders o
LEFT JOIN
  fulfillment-dwh-production.pandata_curated.lg_orders lo
ON
  o.global_entity_id = lo.global_entity_id
  AND o.code = lo.order_code

LEFT JOIN 
  fulfillment-dwh-production.pandata_curated.pd_vendors v
ON o.global_entity_id = v.global_entity_id
  AND o.vendor_code = v.vendor_code
WHERE
  o.created_date_utc BETWEEN DATE_SUB(start_date,INTERVAL 0 day) AND DATE_ADD(end_date,INTERVAL 1 day)
  AND lo.created_date_utc BETWEEN DATE_SUB(start_date,INTERVAL 0 day) AND DATE_ADD(end_date,INTERVAL 1 day)
  AND o.created_date_local BETWEEN start_date AND end_date
  AND lo.created_date_local BETWEEN start_date AND end_date
  AND o.global_entity_id='FP_KH'
  AND o.is_test_order IS FALSE
  AND o.is_valid_order IS TRUE
  --AND lower(o.vendor_name) LIKE "%starbuck%" --Filtering over keyword
  AND v.vendor_code IN ("zmon") --Filtering over Vendor Code
  --AND v.chain_code IN ("cg0qk") --Filtering over Chain code here
  ORDER BY 1 DESC