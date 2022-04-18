
DECLARE period1, period2 DATE;
DECLARE foodpanda_kh_entity STRING;
SET foodpanda_kh_entity = 'FP_KH';
SET period1 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH);
SET period2 = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 10 MONTH), MONTH);
WITH customer_pickup_table AS(
    SELECT
        orders.pd_customer_uuid,
        ROUND(SUM(CASE WHEN orders.is_valid_order IS TRUE THEN payment.amount_local END),2) AS order_amount_local,
        ROUND(SUM(CASE WHEN orders.is_valid_order IS TRUE THEN acc.gmv_local END ),2) AS order_gmv_local,
        COUNTIF(orders.is_valid_order IS TRUE) AS total_customer_valid_orders
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN UNNEST(orders.payments) AS payment
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` AS acc
        ON orders.uuid = acc.uuid
    WHERE 
        orders.created_date_utc BETWEEN period2 AND period1
        AND acc.created_date_utc BETWEEN period2 AND period1
        AND orders.global_entity_id = foodpanda_kh_entity
        AND acc.global_entity_id = foodpanda_kh_entity
        --filter on PICKUP only
        AND orders.is_pickup
        AND NOT orders.is_test_order
        AND NOT orders.is_failed_order
        AND orders.is_valid_order
    GROUP BY 1
), customer_inactive_pickup_table AS(
    SELECT
        orders.pd_customer_uuid,
        COUNTIF(orders.is_valid_order IS TRUE) AS total_customer_valid_orders,
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    WHERE 
        orders.created_date_utc >= period1
        AND orders.global_entity_id = foodpanda_kh_entity
        --filter on PICKUP only
        AND orders.is_pickup
        AND NOT orders.is_test_order
        AND NOT orders.is_failed_order
        AND orders.is_valid_order
    GROUP BY 1
), result_table AS(
    SELECT
        c1.pd_customer_uuid,
        c1.order_amount_local,
        c1.order_gmv_local,
        --CASE WHEN c1.order_amount_local IS NULL THEN 0 ELSE c1.order_amount_local END AS order_amount_local,
        --CASE WHEN c1.order_gmv_local IS NULL THEN 0 ELSE c1.order_gmv_local END AS order_gmv_local,
        c1.total_customer_valid_orders
    FROM customer_pickup_table AS c1
    LEFT JOIN customer_inactive_pickup_table AS c2
        ON  c1.pd_customer_uuid = c2.pd_customer_uuid
    WHERE c2.pd_customer_uuid IS NULL
    
)
SELECT * FROM result_table

--WHERE pd_customer_uuid = '45950_FP_KH'
--WHERE pd_customer_uuid = '500050412_FP_KH'
--WHERE pd_customer_uuid = '545809_FP_KH'

