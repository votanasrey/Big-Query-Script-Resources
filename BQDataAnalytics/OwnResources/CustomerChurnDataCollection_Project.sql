
DECLARE period DATE;
DECLARE foodpanda_kh_entity STRING;
SET foodpanda_kh_entity = 'FP_KH';
SET period = DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH);

WITH customer_churn_table AS(
    SELECT
        orders.pd_customer_uuid,
        orders.is_fraud_level AS fraud_level,
        DATE_DIFF(CURRENT_DATE(), DATE(agg_order.last_order_valid_at_utc), DAY) AS number_of_day_customer_retention,
        ROUND(SUM(payment.amount_local),2) AS order_amount_local,
        COUNT(orders.code) AS total_customer_orders,
        COUNTIF(orders.is_valid_order IS TRUE) AS total_customer_valid_orders,
        COUNTIF(orders.is_valid_order IS FALSE) AS total_customer_failed_orders,
        COUNT(CASE WHEN orders.is_valid_order IS FALSE AND orders.is_billable IS TRUE THEN orders.code END) AS total_customer_wastages,
        COUNT(CASE WHEN order_discounts.is_discount_used THEN order_discounts.uuid END) AS total_customer_order_with_discount,
        ROUND(SUM(CASE WHEN order_discounts.is_discount_used THEN order_discounts.discount.discount_amount_local END),2) AS total_discount_amount_local,
        COUNT( CASE WHEN order_vouchers.is_voucher_used THEN order_vouchers.uuid END) AS total_customer_order_with_vouchers,
        ROUND(SUM(CASE WHEN order_vouchers.is_voucher_used THEN order_vouchers.voucher.value_local END),2) AS total_voucher_amount_local
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` AS orders
    LEFT JOIN UNNEST(orders.payments) AS payment
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS order_discounts
        ON orders.uuid = order_discounts.uuid
        AND orders.global_entity_id = order_discounts.global_entity_id
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS order_vouchers
        ON orders.uuid = order_vouchers.uuid    
        AND orders.global_entity_id = order_vouchers.global_entity_id   
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_customers_agg_orders` AS agg_order
        ON orders.pd_customer_uuid = agg_order.uuid 
        AND orders.global_entity_id = agg_order.global_entity_id
    WHERE 
        orders.created_date_utc >= period
        AND order_vouchers.created_date_utc >= period
        AND order_discounts.created_date_utc >= period
        AND orders.global_entity_id = foodpanda_kh_entity
        AND order_discounts.global_entity_id = foodpanda_kh_entity
        AND order_vouchers.global_entity_id = foodpanda_kh_entity
        AND NOT orders.is_test_order
        AND orders.is_gross_order
    GROUP BY 1,2,3
)
SELECT * FROM customer_churn_table
--WHERE fraud_level IS TRUE
--WHERE number_of_day_customer_retention > 28

