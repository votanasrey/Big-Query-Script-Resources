
WITH sf_case_table AS (
  SELECT 
    a.id AS case_id,
    a.sf_owner_id,
    a.sf_account_id,
    a.case_number,
    a.subject,
    a.description,
    a.status,
    a.closed_reason,
    a.created_at_local,
    a.last_modified_at_local,
    a.closed_at_local, 
    a.due_at_local,
    ROW_NUMBER() OVER(PARTITION BY a.case_number ORDER BY a.last_modified_at_local DESC) AS ranking
  FROM `fulfillment-dwh-production.pandata_curated.sf_cases` AS a 
  WHERE 
    a.global_entity_id = "FP_KH"
    AND DATE(a.created_at_local) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH) 
), sf_account_table AS(
  SELECT 
    b.sf_owner_id,
    b.id AS sf_account_id,
    b.vendor_code,
    b.name AS vendor_name
  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` As b 
  WHERE 
    b.global_entity_id = 'FP_KH'
), result_table AS (
  SELECT DISTINCT 
    a.case_id,
    a.sf_owner_id,
    a.sf_account_id,
    b.vendor_code,
    b.vendor_name,
    a.case_number,
    a.subject,
    a.description,
    a.status,
    a.closed_reason,
    a.closed_at_local,
    a.created_at_local,
    a.due_at_local AS transfer_date_local,
    a.last_modified_at_local
  FROM sf_case_table AS a 
  LEFT JOIN sf_account_table AS b 
    ON a.sf_account_id = b.sf_account_id 
  	AND a.sf_owner_id = b.sf_owner_id 
  WHERE a.ranking = 1
)
SELECT * FROM result_table


  