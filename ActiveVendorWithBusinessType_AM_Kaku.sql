



WITH vendor_table AS (
    SELECT 
        vendor.global_vendor_id AS vendor_grid_id,
        vendor.vendor_code,
        vendor.name AS vendor_name,
        vendor.is_active AS vendor_is_active,
        vendor.location.city AS vendor_city,
        vb.business_type_apac AS vendor_business_type,

        owner.name AS owner_name,
        owner.email AS owner_email
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS vendor
    LEFT JOIN UNNEST(vendor.owners) AS owner
    LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS vb
        ON vendor.vendor_code = vb.vendor_code
        AND vendor.global_entity_id = vb.global_entity_id
    WHERE 
        vendor.global_entity_id = 'FP_KH'
        AND vendor.is_active
        AND NOT vendor.is_private
        AND NOT vendor.is_test
        AND vb.business_type_apac IN ('restaurants', 'shops')
)
SELECT * FROM vendor_table
--WHERE vendor_code IN (SELECT vendor_code FROM vendor_table GROUP BY 1 HAVING COUNT(1)>1)


