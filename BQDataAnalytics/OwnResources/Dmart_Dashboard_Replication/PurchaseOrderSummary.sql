
-- Change the Month
DECLARE selected_month DATE DEFAULT '2022-01-01';
-- Change the Country:
DECLARE selected_country STRING DEFAULT 'FP_KH';
--('FP_PH','FP_TH','FP_PK','FP_TW','FP_SG','FP_MY','FP_MM','FP_KH','FP_LA','FP_JP','FP_BD','FP_HK')


-- Update (09-06-2022):
-- Updated to use tables in pandata_curated

SELECT src.common_name AS Country
, po.purchase_order_reference_number AS PO_Number
, ROUND(po.total_cost, COALESCE(src.decimal_places, 2)) AS PO_Value
, warehouse_info.currency_code_iso AS Currency
, warehouse_info.dmart_imt_warehouse_name AS Store_Name
, dmart_imt_warehouse_uuid AS Store_ID # below is removed, thus replacing with this
-- , platform_vendor_id AS Store_ID         # Removed by Central as of 7 June 2022. https://deliveryhero.slack.com/archives/C01MPJD1RRN/p1654600714334759
, po.dmart_supplier_portal_supplier_id AS Supplier_ID
, po.supplier_name_local AS Supplier_Name
, po.created_at_local AS PO_Creation_Date
, po.order_status AS State
FROM `fulfillment-dwh-production.pandata_curated.dmart_purchase_orders` AS po
LEFT JOIN `fulfillment-dwh-production.pandata_curated.dmart_sources` AS src
ON po.global_entity_id = src.global_entity_id
WHERE src.region='Asia'
  AND po.purchase_order_reference_number NOT LIKE "%UPDATE%"
  AND DATE_TRUNC(DATE(po.created_date_local), MONTH) = selected_month
  AND src.global_entity_id = selected_country

ORDER BY PO_Number