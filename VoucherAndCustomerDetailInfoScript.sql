



SELECT DISTINCT 
    pd_orders_agg_vouchers.pd_voucher_uuid,
    pd_vouchers.id AS voucher_id,
    pd_vouchers.type AS voucher_type,
    pd_vouchers.customer_code,
    pd_orders_agg_vouchers.voucher.current_foodpanda_ratio AS voucher_ratio,
    pd_orders_agg_vouchers.voucher.value_eur AS voucher_value_eur,
    pd_orders_agg_vouchers.voucher.value_local AS voucher_value_local,
    pd_vouchers.value AS voucher_value
FROM
    `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS pd_orders_agg_vouchers
INNER JOIN
    `fulfillment-dwh-production.pandata_curated.pd_vouchers` AS pd_vouchers
    ON pd_orders_agg_vouchers.pd_voucher_uuid = pd_vouchers.uuid
    AND pd_orders_agg_vouchers.global_entity_id = pd_vouchers.global_entity_id
    AND pd_orders_agg_vouchers.voucher.type = pd_vouchers.type
    AND pd_orders_agg_vouchers.voucher.current_foodpanda_ratio = pd_vouchers.foodpanda_ratio
WHERE
    pd_orders_agg_vouchers.created_date_utc >= '2022-01-01'
    AND pd_vouchers.global_entity_id = 'FP_KH'
    AND pd_orders_agg_vouchers.global_entity_id = 'FP_KH'
    --AND pd_orders_agg_vouchers.voucher.value_local <> pd_vouchers.value
ORDER BY pd_vouchers.type ASC



