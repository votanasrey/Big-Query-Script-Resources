WITH a AS
(
SELECT DISTINCT
    dis.pd_discount_uuid,
    v.vendor_code,
    v.name AS vendor_name,
    v.location.city AS vendor_location,
    v.chain_code,
    v.chain_name,
    v.budget,
    v.primary_cuisine AS main_cuisine,
    ARRAY_TO_STRING(v.vertical_types,',') AS vendor_type,
    v.business_type,
    dis.title AS discount_title,
    dis.is_active AS discount_is_active,
    dis.discount_type AS discount_type,
    dis.condition_type AS discount_condition_type,
    gmv.gmv_class AS gmv_class,
    --gmv.order_class AS order_class,
    --dis.weekday AS discount_weekday,
    ARRAY_TO_STRING(dis.expedition_types, ',') AS discount_expedition_types,
    dis.description AS discount_description,
    dis.amount_local AS discount_amount_local,
    dis.foodpanda_ratio AS foodpanda_ratio,
    dis.start_date_local AS start_date_campaign,
    dis.end_date_local AS end_date_campaign,
    pd_dis.discount_mgmt_created_by AS created_by,
    pd_dis.discount_mgmt_updated_by AS updated_by
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
    ,UNNEST(v.discounts) AS dis
    ,UNNEST(v.cuisines) AS vc
LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` gmv
    ON v.vendor_code = gmv.vendor_code AND v.global_entity_id = gmv.global_entity_id
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_discounts` AS pd_dis
    ON dis.pd_discount_uuid = pd_dis.uuid 
    AND v.global_entity_id = pd_dis.global_entity_id
WHERE 
    v.global_entity_id = 'FP_KH'
    AND dis.start_date_local >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 90 day), MONTH)
    AND v.is_active IS TRUE
    AND v.is_test IS FALSE
    ORDER BY dis.start_date_local
)
SELECT * FROM a

