


WITH CTE AS(

  SELECT DISTINCT

    v.global_vendor_id AS vendor_grid_id,
    d.vendor_code AS vendor_backend_id,
    d.vendor_id AS rps_id, 
    d.device_id,

    v.name AS vendor_name,
    v.location.city AS vendor_city,
    v.vertical_type,
    CASE WHEN v.is_active IS TRUE THEN "ACTIVE VENDOR" ELSE "TERMINATED VENDOR" END AS vendor_status,
    DATE(v.created_at_local) AS vendor_created_date_local,

    d.serial_number AS device_serial_number,
    d.hardware.iccid AS sim_serial_number,
    d.is_active AS is_device_active,


    ROW_NUMBER() OVER(PARTITION BY serial_number ORDER BY next_created_at_utc DESC) AS serial_number_ranking

  FROM `fulfillment-dwh-production.pandata_curated.lg_vendor_devices` AS d
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` AS v
    ON d.vendor_code = v.vendor_code 
    AND d.global_entity_id = v.global_entity_id
  WHERE 
    d.global_entity_id = 'FP_KH'
    AND d.is_the_latest_version
    AND d.is_ngt_device
  ORDER BY 1
), result_table AS(
  SELECT * EXCEPT(serial_number_ranking) FROM CTE 
  WHERE serial_number_ranking = 1
)
SELECT * FROM result_table 

