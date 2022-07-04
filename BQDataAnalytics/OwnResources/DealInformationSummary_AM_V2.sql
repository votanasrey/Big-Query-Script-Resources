

WITH discount_table_1 AS(
  SELECT
      dis.pd_discount_uuid,
      v.vendor_code,
      v.name AS vendor_name,
      dis.title AS discount_title,
      dis.condition_type AS discount_condition_type,
      dis.description AS discount_description,
      dis.amount_local AS discount_amount_local,
      pd_dis.minimum_order_value_local AS mov_local, 
      pd_dis.condition_type,
      pd_dis.products.category AS category,
      dis.foodpanda_ratio AS foodpanda_ratio,
      dis.start_date_local AS start_date_campaign,
      dis.end_date_local AS end_date_campaign,
      pd_dis.discount_mgmt_created_by AS created_by,
      pd_dis.discount_mgmt_updated_by AS updated_by
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v
      ,UNNEST(v.discounts) AS dis
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
), discount_table_2 AS(
  SELECT 
      pd_discount_uuid,
      vendor_code,
      vendor_name,
      discount_title,
      discount_condition_type,
      discount_description,
      discount_amount_local,
      mov_local, 
      condition_type,
      c AS category,
      foodpanda_ratio,
      start_date_campaign,
      end_date_campaign,
      created_by,
      updated_by
  FROM discount_table_1
  , UNNEST(category) AS c
), category_table AS(
  SELECT 
    v.vendor_code,
    m.id AS category_id,
    m.title 
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
  ,UNNEST(v.menu_categories) AS m
  WHERE 
    global_entity_id = 'FP_KH'
    AND v.is_active
    AND NOT v.is_test
    AND NOT v.is_private
), result_table AS (
  SELECT DISTINCT
      a.pd_discount_uuid,
      a.vendor_code,
      a.vendor_name,
      a.discount_title,
      a.discount_condition_type,
      a.discount_description,
      a.discount_amount_local,
      a.mov_local, 
      a.condition_type,
      a.category,
      b.title,
      a.foodpanda_ratio,
      a.start_date_campaign,
      a.end_date_campaign,
      a.created_by,
      a.updated_by
  FROM discount_table_2 AS a
  LEFT JOIN category_table AS b 
    ON a.vendor_code = b.vendor_code 
    AND CAST(a.category AS int64) = b.category_id 
)
SELECT * FROM result_table
ORDER BY start_date_campaign DESC

