DECLARE start_date, end_date, v_start_date, v_end_date DATE;

SET start_date = "2021-10-01"; -- Order start Date
SET end_date = "2021-10-30"; -- Order end Date

SET v_start_date = "2021-10-01"; -- Vendor start Date
SET v_end_date = "2021-12-30"; -- Vendor end Date


WITH sf_contract AS  
(
    SELECT 
        sf_acc.id, 
        sf_acc.sf_owner_id,
        sf_acc.global_entity_id, 
        sf_acc.global_vendor_id, 
        sf_acc.vendor_code, 
        sf_acc.name,
        v.vertical_type,
        v.vertical,
        CASE WHEN v.location.city = "Ta Khmau" THEN "Phnom Penh" ELSE v.location.city END city_name,
        sf_acc.gmv_class, 
        sf_acc.status, 
        sf_acc.status_reason, 
        sf_con.commission_percentage, 
        sf_con.start_date_local, 
        sf_con.end_date_local,
        sf_con.is_tiered_commission,
        sf_con.is_deleted,
        v.activated_at_local,
    FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` as sf_acc
    JOIN `fulfillment-dwh-production.pandata_curated.sf_contracts` as sf_con
        ON sf_acc.global_entity_id = sf_con.global_entity_id
        AND sf_acc.id = sf_con.sf_account_id
    JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
        ON sf_acc.global_entity_id = v.global_entity_id
        AND sf_acc.global_vendor_id = v.global_vendor_id
    WHERE sf_acc.global_entity_id = "FP_KH"
    AND sf_acc.status IN ("Active")
    ORDER by vendor_code, start_date_local
), comission_w_trial AS
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY global_vendor_id ORDER BY start_date_local, end_date_local) ranks FROM sf_contract
    WHERE end_date_local IS NOT NULL
), comission_wo_trial AS
(
    SELECT *, ROW_NUMBER() OVER (PARTITION BY global_vendor_id ORDER BY start_date_local DESC, end_date_local DESC) ranks FROM sf_contract
    WHERE end_date_local IS NULL
), sku as 
(
    SELECT 
        v.vendor_code,
        v.activated_at_local, 
        COUNT(DISTINCT products.uuid) sku,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` as v, UNNEST (v.menu_categories) as menu_categories
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_products` as products
    ON menu_categories.uuid = products.pd_menu_category_uuid
    AND v.global_entity_id = products.global_entity_id
    LEFT JOIN UNNEST(products.images) images
    WHERE products.uuid IS NOT NULL
    AND products.is_active
    AND products.is_deleted IS FALSE
    AND v.vertical_type NOT IN ("restaurants","street_food")
    AND NOT v.is_private 
    AND v.global_entity_id = "FP_KH"
    GROUP BY 1,2
)
SELECT 
    comission_wo_trial.global_entity_id, 
    comission_wo_trial.id as sf_account_id,
    comission_wo_trial.sf_owner_id as sf_owner_id,
    comission_wo_trial.global_vendor_id, 
    comission_wo_trial.vendor_code,
    comission_wo_trial.name as vendor_name,
    DATE(comission_wo_trial.activated_at_local) as activated_date_local,
    comission_wo_trial.vertical_type,
    comission_wo_trial.city_name,
    comission_wo_trial.gmv_class,
    IFNULL(comission_w_trial.commission_percentage,comission_wo_trial.commission_percentage) as comission_w_trial,
    comission_wo_trial.commission_percentage as comission_wo_trial,
    sku.sku, 
    COUNT(DISTINCT CASE WHEN o.is_valid_order THEN o.code END) total_valid_orders,
    IFNULL(SUM(CASE WHEN o.is_valid_order THEN oa.gfv_local END),0) gfv_local,
    SAFE_DIVIDE(IFNULL(SUM(CASE WHEN o.is_valid_order THEN oa.gfv_local END),0),COUNT(DISTINCT CASE WHEN o.is_valid_order THEN o.code END)) afv_local,
FROM comission_wo_trial 
LEFT JOIN comission_w_trial
ON comission_wo_trial.global_vendor_id = comission_w_trial.global_vendor_id
AND comission_wo_trial.ranks = comission_w_trial.ranks
JOIN sku
ON comission_wo_trial.vendor_code = sku.vendor_code
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` as o 
ON comission_wo_trial.vendor_code = o.vendor_code 
AND comission_wo_trial.global_entity_id = o.global_entity_id
AND o.created_date_utc BETWEEN DATE_SUB(start_date, interval 1 DAY) AND end_date
AND o.created_date_local BETWEEN start_date AND end_date
AND NOT o.is_test_order
AND o.is_gross_order
AND o.global_entity_id = "FP_KH"
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` as oa
ON o.uuid = oa.uuid
AND comission_wo_trial.global_entity_id = oa.global_entity_id
AND oa.created_date_utc BETWEEN DATE_SUB(start_date, interval 1 DAY) AND end_date
AND oa.created_date_local BETWEEN start_date AND end_date
WHERE comission_wo_trial.ranks = 1
AND DATE(comission_wo_trial.activated_at_local) BETWEEN v_start_date AND v_end_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13




/*
SELECT * FROM `fulfillment-dwh-production.pandata_curated.sf_contracts`
WHERE sf_account_id = "0016900002kiENuAAM";

SELECT DISTINCT status
from `fulfillment-dwh-production.pandata_curated.sf_accounts` as sf 
WHERE global_entity_id = "FP_KH";*/