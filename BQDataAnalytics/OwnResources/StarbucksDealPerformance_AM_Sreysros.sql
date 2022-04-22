
DECLARE date1, date2, date3 DATE;
SET date1 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 3 month),month);
SET date2 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month),month);
SET date3 = DATE_SUB(CURRENT_DATE(), INTERVAL 1 day);
WITH CTE AS (
    SELECT
    DATE(o.ordered_at_date_local) AS ordered_date_local,
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
    COUNT(CASE WHEN o.is_valid_order IS TRUE THEN o.code END) AS total_valid_orders,
    COUNT(CASE WHEN o.is_valid_order IS FALSE THEN o.code END) AS total_failed_orders,
    COUNT(CASE WHEN o.is_valid_order IS FALSE AND o.is_billable IS TRUE THEN o.code END) AS total_wastages,
    ROUND(SUM(a.gmv_local),2) AS total_gmv_local,
    ROUND(SUM(a.gfv_local),2) AS total_gfv_local,
    ROUND(SUM(discount.foodpanda_subsidized_value_local),2) AS foodpanda_paid
FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS o
INNER JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS d
    ON o.global_entity_id = d.global_entity_id
    AND o.uuid = d.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_discounts` AS di
    ON d.global_entity_id = di.global_entity_id
    AND d.pd_discount_uuid = di.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
    ON o.global_entity_id = v.global_entity_id
    AND o.vendor_code = v.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS a
    ON o.global_entity_id = a.global_entity_id
    AND o.uuid = a.uuid
LEFT JOIN `fulfillment-dwh-production.pandata_report.marketing_pd_orders_agg_acquisition_dates` AS m
    ON o.global_entity_id = m.global_entity_id
    AND o.uuid = m.uuid
WHERE
    o.created_date_utc >= "2020-01-01"
    AND o.created_date_local BETWEEN "2020-01-01" AND "2022-03-01"
    AND d.created_date_utc >= "2020-01-01"
    AND d.created_date_local BETWEEN "2020-01-01" AND "2022-03-01"
    AND a.created_date_utc >= "2020-01-01"
    AND a.created_date_local BETWEEN "2020-01-01" AND "2022-03-01"
    AND m.created_date_utc >= "2020-01-01"
    AND d.is_discount_used
    AND o.is_billable
    AND o.is_test_order IS FALSE
    AND o.global_entity_id = 'FP_KH'
    AND LOWER(v.name) LIKE ("%starbucks%")
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
ORDER BY 1,2 DESC
)
SELECT * FROM CTE 
--WHERE total_gmv_local != total_gfv_local

