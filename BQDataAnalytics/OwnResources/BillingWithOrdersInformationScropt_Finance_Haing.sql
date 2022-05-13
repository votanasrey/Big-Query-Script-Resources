

DECLARE start_date, end_date DATE;
SET start_date = "2022-01-01"; -- able to change start_date here
SET end_date = "2022-02-01"; -- able to change end_date here

  SELECT
    o.global_entity_id ,
    o.code as order_code,
    o.payment_type.title as payment_type,
    o.country_name AS country,
    pd_vendors.location.city AS vendor_city,
    o.delivery_provider.title,
    o.expedition_type,
    o.vendor_code,
    o.vendor_name,
    o.is_billable,
    o.is_own_delivery,
    o.is_valid_order,
    o.container_price_local,
    o.total_value_local,
    o.delivery_fee_local,
    o.delivery_fee_original_local,
    o.status_code,
    o.status_name,
    o.created_date_local,
    o.ordered_at_local,
    acc.initial_gfv_local,
    acc.gfv_local,
    acc.total_food_price_local,
    acc.vat_amount_local,
    CASE when vo.voucher.value_local is NULL then 0
     ELSE vo.voucher.value_local
      END AS total_voucher,
    CASE when vo.voucher.foodpanda_subsidized_value_local is NULL then 0
     ELSE round(vo.voucher.foodpanda_subsidized_value_local,2)
      END as foodpanda_voucher,
    CASE when vo.voucher.vendor_subsidized_value_local is NULL then 0
     ELSE round(vo.voucher.vendor_subsidized_value_local,2)
      END as vendor_voucher,
    CASE when dis.discount.discount_amount_local is NULL then 0
     ELSE round(dis.discount.discount_amount_local,2)
      END as total_discount,
    CASE when dis.discount.foodpanda_subsidized_value_local is NULL then 0
     ELSE round(dis.discount.foodpanda_subsidized_value_local,2)
      END as foodpanda_disc,
    CASE when dis.discount.vendor_subsidized_value_local is null then 0
     ELSE round(dis.discount.vendor_subsidized_value_local,2)
      END as vendor_disc,
-----------
    CASE when vo.voucher.foodpanda_subsidized_value_local is NULL then 0
     ELSE round(vo.voucher.foodpanda_subsidized_value_local,2) END
       + CASE when dis.discount.foodpanda_subsidized_value_local is NULL then 0
     ELSE round(dis.discount.foodpanda_subsidized_value_local,2)
      END as total_fp_disc_voucher, --- total fp disc+voucher
-----------
    CASE when vo.voucher.vendor_subsidized_value_local is NULL then 0
     ELSE round(vo.voucher.vendor_subsidized_value_local,2) END
     + CASE when dis.discount.vendor_subsidized_value_local is null then 0
     ELSE round(dis.discount.vendor_subsidized_value_local,2)
     END as total_vendor_disc_voucher, --- total vendor disc+voucher
    FROM `fulfillment-dwh-production.pandata_curated.pd_orders` o
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` acc
      ON o.uuid = acc.uuid
      AND acc.created_date_utc BETWEEN DATE_SUB(start_date, INTERVAL 1 DAY) AND end_date
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` vo
      ON o.uuid = vo.uuid
      and vo.created_date_utc between DATE_SUB(start_date, INTERVAL 1 DAY) and end_date
    left join `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` dis
      on o.uuid = dis.uuid
      and dis.created_date_utc between DATE_SUB(start_date, INTERVAL 1 DAY) and end_date
    left join `fulfillment-dwh-production.pandata_curated.pd_vendors` pd_vendors
      on pd_vendors.uuid = o.pd_vendor_uuid
  WHERE TRUE
    AND o.is_billable
    AND NOT o.is_test_order
    AND o.ordered_at_date_local between DATE_SUB(start_date, INTERVAL 1 DAY) and end_date
    AND o.country_name = 'Cambodia'
    AND o.global_entity_id = 'FP_KH'
    AND o.created_date_utc between DATE_SUB(start_date, INTERVAL 1 DAY) and end_date





