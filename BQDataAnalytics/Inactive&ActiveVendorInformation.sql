

WITH close_date AS (							
    SELECT * EXCEPT (is_earliest) FROM (							
        SELECT							    
        a.global_entity_id,							
        a.close_date_local,							
        b.vendor_code,							
        b.status,							
        ROW_NUMBER() OVER (PARTITION BY a.global_entity_id, a.sf_account_id ORDER BY a.close_date_local ) = 1 AS is_earliest							
        FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` AS a							
        LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` AS b							
            ON a.global_entity_id=b.global_entity_id							
            AND a.sf_account_id=b.id							
        WHERE							
            a.business_type IN ('New Business')							
            AND a.sf_account_id IS NOT NULL							
            AND a.global_entity_id = 'FP_KH')							
    WHERE
        is_earliest							
        AND vendor_code IS NOT NULL
),zone AS (							
    SELECT							
        country.global_entity_id,							
        country.name AS country_name,							
        ct.name AS city_name,							
        z.name AS zone_name,							
        z.id AS zone_id,							
        z.shape.geo AS zone_geo							
    FROM `fulfillment-dwh-production.pandata_curated.lg_countries` country,							
    UNNEST(cities) ct,							
    UNNEST(ct.zones) z							
    WHERE							
        country.global_entity_id='FP_KH'							
        AND z.is_active
), result as (							
    SELECT DISTINCT 
        v.code,							
        z.zone_name,							
        z.city_name,							
        st_y(location_geo) AS latitude,							
        st_x(location_geo) AS longitude,							
        v.vertical_type							
    FROM `fulfillment-dwh-production.pandata_curated.lg_vendors` as v							
    LEFT JOIN zone as z							
        ON v.global_entity_id = z.global_entity_id							
        WHERE st_contains(z.zone_geo,v.location_geo) IS TRUE							
        AND vertical_type <> "courier_business"							
), final as (							
    select * from result where code in (select code from result group by 1 HAVING COUNT (1)> 1)							
        AND zone_name <> "Phnom penh"							
    UNION ALL							
    select * from result where code in (select code from result group by 1 HAVING COUNT (1) = 1)							
    ORDER BY 1							
)							
SELECT							
    v.vendor_code,							
    v.name AS vendor_name,							
    v.chain_name,							
    a.status AS sf_status,							
    v.is_active AS be_active_status,							
    a.global_vendor_id AS grid_id,							
    DATE(cd.close_date_local) AS activation_date_sf,							
    DATE(v.activated_at_local) AS activation_date_be,							
    IFNULL(final.city_name,v.location.city) as city_name,							
    IFNULL(final.zone_name,v.location.city) as zone_name,							
    v.location.latitude,							
    v.location.longitude,							    
    o.email,							
    --v.contact_number,							
    v.vertical_type,							
    a.id as sf_acct_id,							
    a.owner_name,							
    --dim_vendors.sf_account_owner_name,							
    CASE WHEN c.gmv_class IS NULL THEN 'D' ELSE c.gmv_class END AS gmv_class							
FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` v,							
    UNNEST(owners) o							
LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` c							
    ON v.global_entity_id=c.global_entity_id							
    AND v.vendor_code=c.vendor_code							
LEFT JOIN `fulfillment-dwh-production.pandata_curated.sf_accounts` a							
    ON v.global_entity_id=a.global_entity_id							
    AND v.vendor_code=a.vendor_code							
LEFT JOIN close_date AS cd							
    ON v.global_entity_id=cd.global_entity_id							
    AND v.vendor_code=cd.vendor_code							
LEFT JOIN final							
ON v.vendor_code=final.code							
WHERE							
    v.global_entity_id='FP_KH'							
    AND v.is_test IS FALSE							
    AND v.is_active IS TRUE							
							
							