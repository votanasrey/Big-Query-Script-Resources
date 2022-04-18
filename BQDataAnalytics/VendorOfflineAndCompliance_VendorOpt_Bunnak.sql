

WITH raw_offline AS (		
    SELECT		
        RO.vendor_code,	
        RO.global_entity_id,			
        g.gmv_class,		
        SUM(RO.total_open_seconds) AS total_open_seconds,		
        SUM(RO.total_special_day_closed_seconds) AS total_special_day_closed_seconds,			
        SUM(RO.total_unavailable_seconds) AS total_unavailable_seconds
FROM		
    `fulfillment-dwh-production.pandata_report.vendor_offline` AS RO
LEFT JOIN `fulfillment-dwh-production.pandata_report.vendor_gmv_class` AS g		
    ON RO.vendor_code = g.vendor_code
    AND RO.global_entity_id = g.global_entity_id
WHERE		
    report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)		
GROUP BY 1,2,3

)		

SELECT		
    *	
FROM raw_offline		
WHERE global_entity_id = 'FP_KH'		
		