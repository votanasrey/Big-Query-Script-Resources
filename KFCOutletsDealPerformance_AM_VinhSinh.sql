
SELECT
    o.vendor_code,
    o.vendor_name,
    v.vertical_type AS vendor_type,
    v.location.city AS vendor_city_name,
    di.title,
    di.description,
    di.discount_type,
    di.condition_type,
    di.start_date_local,
    di.end_date_local,
    di.amount_local, 
    di.foodpanda_ratio,
    COUNT(DISTINCT o.code) AS total_orders,
    ROUND(SUM(a.gmv_local),2) AS total_gmv_local,
    ROUND(SUM(discount.foodpanda_subsidized_value_local),2) AS foodpanda_paid
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
INNER JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` d
    ON o.global_entity_id=d.global_entity_id
    AND o.uuid=d.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_discounts` di
    ON d.global_entity_id =di.global_entity_id
    AND d.pd_discount_uuid=di.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` v
    ON o.global_entity_id =v.global_entity_id
    AND o.vendor_code=v.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` a
    ON o.global_entity_id =a.global_entity_id
    AND o.uuid=a.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` m
    ON o.global_entity_id =m.global_entity_id
    AND o.uuid=m.uuid
WHERE
    o.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month)
    AND o.created_date_local BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month) 
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)
    AND d.created_date_utc >=DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month)
    AND d.created_date_local BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)
    AND a.created_date_utc >=DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month)
    AND a.created_date_local BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month)
        AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 day)
    AND m.created_date_utc >=DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month)
    AND d.is_discount_used
    AND o.is_billable
    AND o.is_test_order IS FALSE
    AND o.global_entity_id = 'FP_KH'
    AND o.vendor_code IN ('t7wo', 't8kk', 't5fr', 't5am', 't9mq', 't5cd', 't3kt', 't7xo', 'v8yy', 'c4nl', 'k9du', 'w8o0')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1 DESC



