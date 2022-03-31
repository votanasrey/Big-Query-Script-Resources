
WITH sf_opportunities as
(
    SELECT 
        global_entity_id,
        global_vendor_id,
        sf_account_id, 
        business_type,
        name,
        stage_name,
        --opportunity_quotes.status,
        close_date_local,
        created_date_local,
        ROW_NUMBER() OVER (PARTITION BY global_vendor_id ORDER BY close_date_local DESC, stage_name) ranks,
    FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` as sf_opportunities 
    WHERE global_entity_id = "FP_KH"
    AND business_type = "Upgrade/Upsell"
    AND name = "Upgrade/Upsell NB VF Deal"
), sf_account as 
(
    SELECT 
    sf_acc.global_entity_id,
    sf_acc.global_vendor_id,
    sf_acc.vendor_code,
    sf_acc.gmv_class,
    sf_acc.name, 
    sf_acc.status,
    sf_acc.status_reason,
    sf_acc.last_modified_date,
    sf_opportunities.stage_name,
    sf_opportunities.created_date_local,
    FROM `fulfillment-dwh-production.pandata_curated.sf_accounts` as sf_acc   
    LEFT JOIN sf_opportunities
    ON sf_opportunities. global_entity_id = sf_acc.global_entity_id
    AND sf_opportunities.global_vendor_id = sf_acc.global_vendor_id
    AND sf_opportunities.ranks = 1
    WHERE sf_acc.global_entity_id = "FP_KH"
    AND sf_acc.vendor_code IS NOT NULL
), activate as
(
    SELECT 
        global_entity_id,
        global_vendor_id,
        sf_account_id, 
        business_type,
        name,
        stage_name,
        --opportunity_quotes.status,
        close_date_local,
        created_date_local,
        ROW_NUMBER() OVER (PARTITION BY global_vendor_id ORDER BY close_date_local DESC, stage_name) ranks,
    FROM `fulfillment-dwh-production.pandata_curated.sf_opportunities` as sf_opportunities 
    WHERE global_entity_id = "FP_KH"
    AND business_type IN ("New Business", "Win Back")
    AND stage_name = "Closed Won"
)
SELECT DISTINCT
  sf_account.global_entity_id
, sf_account.global_vendor_id
, sf_account.vendor_code
, sf_account.gmv_class
, sf_account.name
, sf_account.status as sf_status
, sf_account.status_reason as sf_status_reason
, activate.close_date_local as sf_activate_date
--, sf_account.last_modified_date as sf_last_modified_date
, sf_account.stage_name as sf_nb_deal_status
, sf_account.created_date_local as sf_nb_deal_created_date_local
, vendors.activated_at_local as be_activated_at_local
/*, discount.title
, discount.start_date_local
, discount.end_date_local*/
, IFNULL(discount.is_active, false) as nb_enable_on_app
FROM  sf_account
LEFT JOIN activate
ON sf_account.global_vendor_id = activate.global_vendor_id
AND activate.ranks = 1
LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors` as vendors 
ON sf_account.global_entity_id = vendors.global_entity_id
AND sf_account.global_vendor_id = vendors.global_vendor_id
LEFT JOIN UNNEST (vendors.discounts) as discount
ON lower(discount.title) like "%newbie%"
AND discount.is_active 
WHERE activate.close_date_local >= (current_date() - 90)