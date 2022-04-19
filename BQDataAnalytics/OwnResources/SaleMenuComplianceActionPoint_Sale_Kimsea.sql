

--DECLARE period_date DATE;
--SET period_date = DATE_SUB(CURRENT_DATE(), INTERVAL 3 WEEK);

WITH vendor_table AS(
    SELECT
        v.global_entity_id,
        v.global_vendor_id,
        v.vendor_code,  
        v.name, 
        v.vertical_type,
        activation.pd_activation_date_local,
        activation.sf_activation_date_local,
        COUNT(DISTINCT products.uuid) AS product_number
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
        ,UNNEST (v.menu_categories) AS menu_categories 
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_products` AS products 
        ON menu_categories.uuid = products.pd_menu_category_uuid 
        AND v.global_entity_id = products.global_entity_id
    LEFT JOIN `fulfillment-dwh-production.pandata_report.pandora_pd_vendors_agg_activation_dates` AS activation 
        ON v.vendor_code = activation.vendor_code
        AND v.global_entity_id = activation.global_entity_id 
    WHERE 
        v.global_entity_id = "FP_KH" 
        AND v.is_active
        AND NOT v.is_private
        AND NOT v.is_test
        AND v.vertical_type IN ("restaurants","street_food") 
        AND products.uuid IS NOT NULL 
        AND products.is_active 
        AND NOT products.is_deleted 
        --AND activation.sf_activation_date_local >= period_date
    GROUP BY 1,2,3,4,5,6,7
    ORDER BY 1,4
), account_table AS(
    SELECT 
        account.global_entity_id,
        account.global_vendor_id,
        account.vendor_code,
        account.name AS vendor_name,
        account.type AS account_type,
        account.sf_owner_id AS account_owner_id,
        account.sf_parent_account_id AS account_manager_id
    FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS account
    WHERE 
        account.global_entity_id = 'FP_KH'
        AND NOT account.is_deleted
        AND account.vendor_code IS NOT NULL
), result_table AS(
    SELECT
        account_table.global_vendor_id,
        account_table.vendor_code,
        account_table.vendor_name,
        account_table.account_type,
        account_table.account_owner_id,
        account_table.account_manager_id,
        vendor_table.pd_activation_date_local,
        vendor_table.sf_activation_date_local,
        vendor_table.vertical_type,
        vendor_table.product_number,
        (CASE 
            WHEN vendor_table.vertical_type IN ('restaurants') AND vendor_table.product_number >= 10 THEN "10 and above"
            WHEN vendor_table.vertical_type IN ('restaurants') AND vendor_table.product_number < 10 THEN "Less than 10"
            WHEN vendor_table.vertical_type IN ('street_food') AND vendor_table.product_number >= 5 THEN "5 and above"
            WHEN vendor_table.vertical_type IN ('street_food') AND vendor_table.product_number < 5 THEN "Less than 5"
        END) AS stock_keeping_unit
    FROM vendor_table
    LEFT JOIN account_table 
        ON vendor_table.vendor_code = account_table.vendor_code
        AND vendor_table.global_entity_id = account_table.global_entity_id
)
SELECT * FROM result_table
--WHERE vendor_code IN (SELECT vendor_code FROM result_table GROUP BY 1 HAVING COUNT(1)>1)

