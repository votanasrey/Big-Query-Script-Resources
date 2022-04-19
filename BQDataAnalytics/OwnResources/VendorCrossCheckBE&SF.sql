WITH sf_op as
(
    SELECT 
        global_entity_id, 
        global_vendor_id, 
        business_type, 
        type, 
        close_date_local
    FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` as sf_op 
    WHERE global_entity_id = "FP_KH"
    AND business_type IN ("New Business","Win Back")
    AND close_date_local >= "2022-01-01"
)
, sf_acc as
(
    SELECT sf_op.*, sf.status, sf.status_reason 
    FROM sf_op 
    JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` as sf 
    ON sf_op.global_entity_id = sf.global_entity_id
    AND sf_op.global_vendor_id = sf.global_vendor_id
    WHERE sf.status = "Active"
)
SELECT sf_acc.*, v.is_active, v.vendor_code, v.is_private, v.is_test, v.activated_at_local
FROM sf_acc 
JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
ON sf_acc.global_entity_id = v.global_entity_id
AND sf_acc.global_vendor_id = v.global_vendor_id
AND not v.is_active
