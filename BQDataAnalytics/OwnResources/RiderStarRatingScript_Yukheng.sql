


SELECT
    --s.weekday_name,
    d.lg_rider_id AS rider_id,
    CASE WHEN v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END as city,
    COUNT(DISTINCT o.order_code) total_orders,
    COUNT(DISTINCT CASE WHEN IFNULL(r.ratings.rider.score ,r.ratings.vendor_food.score) IS NULL THEN o.order_code END) as total_reviewed_orders,
    AVG(IFNULL(r.ratings.rider.score,r.ratings.vendor_food.score)) as ridr_avg_rating,
    AVG(d.rider_late_in_seconds)/60 as rider_late_in_minutes,
    AVG(d.delivery_delay_in_seconds)/60 as delivery_delay_in_seconds,
  FROM `fulfillment-dwh-production.pandata_curated.lg_orders` AS o,
    UNNEST(rider.deliveries) d
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_dates` AS s
    ON o.created_date_local = s.date
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS po
    ON o.global_entity_id = po.global_entity_id
    AND o.order_code = po.code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
    ON po.global_entity_id = v.global_entity_id
    AND po.vendor_code = v.vendor_code
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.data_stream_reviews` as r
    ON o.global_entity_id = r.global_entity_id
    AND o.order_code = r.data_stream_order_id
    AND r.updated_date_utc BETWEEN CURRENT_DATE() - 31 AND CURRENT_DATE()
  
  WHERE
    o.created_date_utc BETWEEN CURRENT_DATE() - 31 AND CURRENT_DATE()
    AND po.created_date_utc BETWEEN CURRENT_DATE() - 31 AND CURRENT_DATE()
    AND o.created_date_local BETWEEN CURRENT_DATE() - 30 AND CURRENT_DATE()
    AND o.global_entity_id = "FP_KH"
    AND o.vendor.order_status="completed"
    AND CASE WHEN v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END = "Phnom Penh"
  GROUP BY 1,2
  ORDER BY total_orders DESC

  