
SELECT
    d.uuid,
    va.uuid,
    discount.discount_type,
    discount.attributions_foodpanda_ratio, 
    discount.current_foodpanda_ratio,
    discount.discount_amount_local, 
    discount.discount_amount_local*discount.current_foodpanda_ratio/100 as fp_current_ratio,
    discount.discount_amount_local*discount.attributions_foodpanda_ratio/100 as fp_attribution_ratio,
    discount.foodpanda_subsidized_value_local,
FROM `fulfillment-dwh-production.pandata_curated.pd_discounts` AS d
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_discounts` AS va 
    ON va.pd_discount_uuid = d.uuid 
    AND va.global_entity_id = d.global_entity_id
WHERE  
    d.global_entity_id = 'FP_PK'
    va.global_entity_id = 'FP_PK'
    AND va.created_date_utc >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    -- and discount.attributions_foodpanda_ratio != discount.current_foodpanda_ratio
    -- and discount.discount_amount_local > 0
    --AND va.uuid IN ('158320972_FP_PK','153648300_FP_PK')
    -- and discount.current_foodpanda_ratio <= 1

