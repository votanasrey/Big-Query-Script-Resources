


WITH zone AS 
(
    SELECT
        country.name as country_name,
        ct.name AS city_name,
        ct.id as city_id,
        z.name AS zone_name,
        z.id as zone_id,
        z.shape.geo as shape_geo,
    FROM fulfillment-dwh-production.pandata_curated.lg_countries country,
    UNNEST(cities) ct,
    UNNEST(ct.zones) z
        WHERE country.global_entity_id='FP_KH'
        AND z.is_active
), vendor as 
(
    SELECT 
        global_entity_id,  
        v.vendor_code as vendor_code,
        v.name as vendor_name,
        v.vertical_type,
        ST_GEOGPOINT(v.location.longitude,v.location.latitude) as location_geo,
        v.location.latitude vendor_latitude,
        v.location.longitude vendor_longtitude,
    FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` as v
    WHERE global_entity_id = "FP_KH"
    AND is_active
    AND not is_test
    AND not is_private
) --14046 
SELECT vendor.*,zone.zone_name 
FROM vendor 
LEFT JOIN zone
ON st_contains(zone.shape_geo,vendor.location_geo) IS TRUE
where zone.zone_name IS NULL
ORDER BY zone.zone_name
