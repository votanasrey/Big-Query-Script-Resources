
WITH sf_table AS(
  SELECT
    c.close_date_local,
    a.vendor_uuid, 
    a.vendor_code,
    a.name AS vendor_name,

    a.type AS vendor_type,
    a.status,


    b.name,
    b.product,
    b.status,
    b.is_deleted,
    IFNULL(b.listed_price_local,0) AS listed_price_local,
    IFNULL(b.discount_local,'0') AS discount_local,
    IFNULL(b.total_amount_local,0) AS total_amount_local,
    b.start_date_local,
    ROW_NUMBER() OVER (PARTITION BY c.global_entity_id, c.sf_account_id ORDER BY c.close_date_local ) = 1 AS is_earliest

  FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS a
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_additional_charges` AS b
    ON a.id = b.sf_account_id
    AND a.global_entity_id = b.global_entity_id
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_opportunities` AS c
    ON a.id = c.sf_account_id
    AND a.global_entity_id = c.global_entity_id
  WHERE
    a.global_entity_id = 'FP_KH' 
    AND a.vendor_code IS NOT NULL
    
    AND b.global_entity_id = 'FP_KH'
    AND a.status != 'Terminated'
), order_table AS (
  SELECT
    o.vendor_code,
    o.vendor_name,
    SUM(IFNULL(a.gfv_local, 0)) AS gfv,
    COUNT(DISTINCT o.code) AS order_count
  FROM
    `fulfillment-dwh-production.pandata_curated.pd_orders` o
  LEFT JOIN
    `fulfillment-dwh-production.pandata_curated.pd_orders_agg_accounting` a
  ON
    o.uuid = a.uuid
  WHERE
    o.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month), MONTH)
    AND a.created_date_utc >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 2 month), MONTH)
    AND o.created_date_local BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 1 month), MONTH)  -------first day of (n-1)th month
          AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(),month),INTERVAL 1 day) ---------last day of (n-1)th month
    AND o.is_billable
    AND o.global_entity_id = 'FP_KH'
  GROUP BY 1,2
), result_table AS(
  SELECT

    s.vendor_code,  
    o.vendor_name,

    s.close_date_local,
    s.name,
    s.product,
    s.discount_local,
    s.listed_price_local,
    s.total_amount_local,
    s.is_deleted,
    s.start_date_local,

    DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 1 month), MONTH) AS start_date,
    DATE_SUB(DATE_TRUNC(CURRENT_DATE(),month),INTERVAL 1 day) AS end_date,
    CASE
      WHEN DATE(close_date_local) BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 1 month), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(),month),INTERVAL 1 day) THEN TRUE ELSE FALSE END AS is_new,
    IFNULL(o.gfv, 0) AS gfv_local,
    IFNULL(o.order_count, 0) AS total_orders,
    CASE
      WHEN IFNULL(o.gfv, 0) > 3 AND DATE(close_date_local) NOT BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(),INTERVAL 1 month), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(),month),INTERVAL 1 day) THEN 3
    ELSE 0 END AS listing_fee
  FROM sf_table AS s
  INNER JOIN order_table AS o
    ON s.vendor_code = o.vendor_code
  WHERE
    IFNULL(o.gfv,0) > 3
    AND DATE(close_date_local) BETWEEN '2021-03-01' AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(),month),INTERVAL 1 day)
    AND s.is_earliest
)

SELECT * FROM result_table
    


