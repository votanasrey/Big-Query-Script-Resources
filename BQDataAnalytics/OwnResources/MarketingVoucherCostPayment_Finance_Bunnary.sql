
WITH CTE AS (
    SELECT 
        pd_voucher_uuid,
        voucher_code,
        voucher_description,
        voucher_type,
        voucher_value,
        purpose,
        budget_channel,
        vertical_per_redemption,
        start_date,
        end_date,
        month,
        SUM(orders) AS total_orders,
        ROUND(SUM(gmv_local),2) AS gmv_local,
        ROUND(SUM(costs_fp_lc),2) AS costs_fp_lc,
        ROUND(SUM(costs_vendor_lc),2) AS costs_vendor_lc
    FROM `fulfillment-dwh-production.pandata_report.marketing_cost_report_vouchers_v2` 
    WHERE 
        global_entity_id = 'FP_KH'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
) 
SELECT * FROM CTE 

--WHERE pd_voucher_uuid = 'OFRV_10_FP_KH'
--WHERE pd_voucher_uuid = 'OFRV_7561297386849479_FP_KH'
--WHERE pd_voucher_uuid = 'OFRV_7565992840355210_FP_KH'
--WHERE pd_voucher_uuid IN (SELECT pd_voucher_uuid FROM CTE GROUP BY 1 HAVING COUNT(1)>1)

ORDER BY pd_voucher_uuid, month



