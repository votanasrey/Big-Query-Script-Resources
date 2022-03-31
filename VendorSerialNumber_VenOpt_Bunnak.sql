WITH
  close_date AS (
  SELECT
    * EXCEPT (is_earliest)
  FROM (
    SELECT
      a.global_entity_id,
      a.close_date_local,
      b.vendor_code,
      b.status,
      ROW_NUMBER() OVER (PARTITION BY a.global_entity_id, a.sf_account_id ORDER BY a.close_date_local ) = 1 AS is_earliest
    FROM
      `fulfillment-dwh-production.pandata_curated.sf_opportunities` a
    LEFT JOIN
      `fulfillment-dwh-production.pandata_curated.sf_accounts` b
    ON
      a.global_entity_id=b.global_entity_id
      AND a.sf_account_id=b.id
    WHERE
      a.business_type IN ('New Business')
      AND a.sf_account_id IS NOT NULL
      AND a.global_entity_id = 'FP_KH' )
  WHERE
    is_earliest
    AND vendor_code IS NOT NULL)
SELECT
  v.vendor_code,
  v.name AS vendor_name,
  v.chain_name,
  a.status AS sf_status,
  v.is_active AS be_active_status,
  v.is_private AS be_private_status,
  a.global_vendor_id AS grid_id,
  DATE(cd.close_date_local) AS activation_date_sf,
  DATE(v.activated_at_local) AS activation_date_be,
  v.location.city AS city_name,
  v.location.latitude,
  v.location.longitude,
  o.email,
  v.contact_number,
  v.vertical_type,
  a.id as sf_acct_id,
  a.owner_name,
  --dim_vendors.sf_account_owner_name,
  CASE
    WHEN c.gmv_class IS NULL THEN 'D'
  ELSE
  c.gmv_class
END
  AS gmv_class,
  STRING_AGG(CONCAT(vd.serial_number," (",CASE WHEN vd.is_active THEN "Active" ELSE "Inactive"END,")"), ",") AS vendor_serial_number,
FROM
  `fulfillment-dwh-production.pandata_curated.pd_vendors` v,
  UNNEST(owners) o
LEFT JOIN
  `fulfillment-dwh-production.pandata_report.vendor_gmv_class` c
ON
  v.global_entity_id=c.global_entity_id
  AND v.vendor_code=c.vendor_code
LEFT JOIN
  `fulfillment-dwh-production.pandata_curated.sf_accounts` a
ON
  v.global_entity_id=a.global_entity_id
  AND v.vendor_code=a.vendor_code
LEFT JOIN
  close_date cd
ON
  v.global_entity_id=cd.global_entity_id
  AND v.vendor_code=cd.vendor_code
LEFT JOIN 
  `fulfillment-dwh-production.pandata_curated.lg_vendor_devices` AS vd
ON 
  v.global_entity_id = vd.global_entity_id
  AND v.vendor_code = vd.vendor_code
WHERE
  v.global_entity_id='FP_KH'
  AND v.is_test IS FALSE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

WITH
  close_date AS (
  SELECT
    * EXCEPT (is_earliest)
  FROM (
    SELECT
      a.global_entity_id,
      a.close_date_local,
      b.vendor_code,
      b.status,
      ROW_NUMBER() OVER (PARTITION BY a.global_entity_id, a.sf_account_id ORDER BY a.close_date_local ) = 1 AS is_earliest
    FROM
      `fulfillment-dwh-production.pandata_curated.sf_opportunities` a
    LEFT JOIN
      `fulfillment-dwh-production.pandata_curated.sf_accounts` b
    ON
      a.global_entity_id=b.global_entity_id
      AND a.sf_account_id=b.id
    WHERE
      a.business_type IN ('New Business')
      AND a.sf_account_id IS NOT NULL
      AND a.global_entity_id = 'FP_KH' )
  WHERE
    is_earliest
    AND vendor_code IS NOT NULL)
SELECT
  v.vendor_code,
  v.name AS vendor_name,
  v.chain_name,
  a.status AS sf_status,
  v.is_active AS be_active_status,
  v.is_private AS be_private_status,
  a.global_vendor_id AS grid_id,
  DATE(cd.close_date_local) AS activation_date_sf,
  DATE(v.activated_at_local) AS activation_date_be,
  v.location.city AS city_name,
  v.location.latitude,
  v.location.longitude,
  o.email,
  v.contact_number,
  v.vertical_type,
  a.id as sf_acct_id,
  a.owner_name,
  --dim_vendors.sf_account_owner_name,
  CASE
    WHEN c.gmv_class IS NULL THEN 'D'
  ELSE
  c.gmv_class
END
  AS gmv_class,
  STRING_AGG(CONCAT(vd.serial_number," (",CASE WHEN vd.is_active THEN "Active" ELSE "Inactive"END,")"), ",") AS vendor_serial_number,
FROM
  `fulfillment-dwh-production.pandata_curated.pd_vendors` v,
  UNNEST(owners) o
LEFT JOIN
  `fulfillment-dwh-production.pandata_report.vendor_gmv_class` c
ON
  v.global_entity_id=c.global_entity_id
  AND v.vendor_code=c.vendor_code
LEFT JOIN
  `fulfillment-dwh-production.pandata_curated.sf_accounts` a
ON
  v.global_entity_id=a.global_entity_id
  AND v.vendor_code=a.vendor_code
LEFT JOIN
  close_date cd
ON
  v.global_entity_id=cd.global_entity_id
  AND v.vendor_code=cd.vendor_code
LEFT JOIN 
  `fulfillment-dwh-production.pandata_curated.lg_vendor_devices` AS vd
ON 
  v.global_entity_id = vd.global_entity_id
  AND v.vendor_code = vd.vendor_code
WHERE
  v.global_entity_id='FP_KH'
  AND v.is_test IS FALSE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18