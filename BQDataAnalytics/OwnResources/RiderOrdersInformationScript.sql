  SELECT
    s.weekday_name,
    o.created_at_local,
    d.lg_rider_id,
    o.order_code,
    case when v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END as city,
  FROM
    `fulfillment-dwh-production.pandata_curated.lg_orders` o,
    UNNEST(rider.deliveries) d
  LEFT JOIN
    `fulfillment-dwh-production.pandata_curated.shared_dates` s
  ON
    o.created_date_local=s.date
  LEFT JOIN
    `fulfillment-dwh-production.pandata_curated.pd_orders` po
  ON
    o.global_entity_id=po.global_entity_id
    AND o.order_code=po.code
  LEFT JOIN
    `fulfillment-dwh-production.pandata_curated.pd_vendors` v
  ON
    po.global_entity_id=v.global_entity_id
    AND po.vendor_code=v.vendor_code
  WHERE
    o.created_date_utc BETWEEN current_date() - 61 AND current_date
    AND po.created_date_utc BETWEEN current_date() - 61 AND current_date
    AND o.created_date_local BETWEEN current_date() - 60 AND current_date
    AND o.global_entity_id = "FP_KH"
    AND o.vendor.order_status="completed"