

WITH vendor_product_table AS (
  SELECT DISTINCT 
    v.vendor_code, 
    v.name AS vendor_name, 
    menu_categories.id AS menu_categories_id,
    menu_categories.title AS menu_categories_title,
    p.id AS product_id,
    p.title AS product_title,
    p.is_active,
    p.is_deleted,
    DATE(p.product_created_at_local) AS product_created_date_local,
    DATE(p.product_updated_at_local) AS product_updated_date_local
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
  LEFT JOIN UNNEST(v.menus) AS menus
  LEFT JOIN UNNEST(v.menu_categories) AS menu_categories
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_products` AS p
    ON menu_categories.uuid = p.pd_menu_category_uuid
  LEFT JOIN UNNEST(p.variations) AS variations
  LEFT JOIN UNNEST(products_agg_menus) pam
  WHERE 
    v.global_entity_id = 'FP_KH' 
    AND menus.is_active = TRUE
    AND menus.is_deleted = FALSE
    AND menu_categories.is_deleted = FALSE
    AND (menu_categories.title != "Toppings" OR menu_categories.title IS NULL)
    AND p.is_active = TRUE
    AND p.is_deleted = FALSE
    AND variations.is_deleted = FALSE
    AND (variations.master_category_title != "Toppings" OR variations.master_category_title IS NULL)
    AND pam.pd_menu_product_id IS NOT NULL
    and pam.is_deleted = FALSE
)
SELECT * FROM vendor_product_table 
--WHERE product_id IN (SELECT product_id FROM vendor_product_table GROUP BY 1 HAVING COUNT(1)>1)
ORDER BY vendor_code 


