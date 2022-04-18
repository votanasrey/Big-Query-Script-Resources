

WITH vendor_terrmination_table AS(
        SELECT DISTINCT 				
        account.global_vendor_id,
        account.vendor_code,
        account.name AS account_name,
        account.restaurant_city,
        account.vendor_grade,	
        account.gmv_class,
        account.status					
    FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` AS account
    WHERE	
        account.global_entity_id = 'FP_KH'
        AND account.status = 'Terminated'						
    ORDER BY account.vendor_code	
)
SELECT * FROM vendor_terrmination_table					
						
						
						
						
						