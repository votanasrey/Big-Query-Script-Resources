
DECLARE date1, date2 DATE;
SET date1 = "2021-01-01";

WITH voucher_table AS(
  SELECT 
    DATE(a.voucher.created_at_local) AS report_date,
    SUBSTRING(STRING(DATE(a.voucher.created_at_local)), 1,7) AS report_month,
    a.global_entity_id,
    c.location.city, 
    a.voucher.voucher_code,
    b.expedition_type,
    c.vertical_type,
    COUNT(DISTINCT CASE WHEN a.is_voucher_used AND b.is_valid_order THEN b.code END) AS total_voucher_valid_orders,
    COUNT(CASE WHEN b.uuid = d.first_valid_order_all_uuid THEN d.uuid END) AS total_foodpanda_new_customers,
    SUM(CASE WHEN a.is_voucher_used AND b.is_valid_order THEN a.voucher.value_local END) AS total_voucher_value_local
  FROM `fulfillment-dwh-production.pandata_curated.pd_orders_agg_vouchers` AS a
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_orders` AS b
    ON a.global_entity_id = b.global_entity_id
    AND a.uuid = b.uuid 
  LEFT JOIN `fulfillment-dwh-production.pandata_report.marketing_customers_agg_orders_dates` AS d
    ON b.global_entity_id = d.global_entity_id
    AND b.pd_customer_uuid = d.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS c
    ON b.global_entity_id = c.global_entity_id
    AND b.vendor_code = c.vendor_code
  WHERE 
    a.global_entity_id = 'FP_KH'
    AND a.created_date_utc >= date1
    AND b.created_date_utc >= date1
    AND UPPER(a.voucher.voucher_code) IN 
    ('VISABOC',
    'VISAICBC',
    'VISACPB',
    'VISACBP',
    'VISACMB',
    'VISAUCB',
    'VISACUB',
    'VISACAB',
    'VISASTTC',
    'VISAJTR',
    'VISAACL',
    'VISAVTN',
    'VISAMCJ',
    'VISAABA',
    'VISAFTB',
    'VISACIMB',
    'VISABIDC',
    'VISAPPCB',
    'VISAAEON',
    'VISASHB',
    'VISABRED',
    'VISAMB',
    'VISASTP',
    'VISACPO',
    'VISAPB',
    'VISAKBD',
    'VISACUBC',
    'VISAAEON'
  )
  GROUP BY 1,2,3,4,5,6,7   
)
SELECT * FROM voucher_table
ORDER BY 1 DESC

