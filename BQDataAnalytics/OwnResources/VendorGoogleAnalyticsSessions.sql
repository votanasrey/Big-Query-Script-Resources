
DECLARE date1 DATE;
SET date1 = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

WITH
--
vendortable AS (
  SELECT
    pd_vendors.vendor_code,
    pd_vendors.name AS vendor_name,
    pd_vendors.global_entity_id,
    pd_vendors_agg_business_types.business_type_apac AS vendor_type,
    shared_countries.name AS country,
    shared_countries.timezone AS timezone
  FROM `fulfillment-dwh-production.pandata_curated.pd_vendors` AS pd_vendors
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.pd_vendors_agg_business_types` AS pd_vendors_agg_business_types
         ON pd_vendors.uuid = pd_vendors_agg_business_types.uuid
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.shared_countries` AS shared_countries
         ON pd_vendors.global_entity_id = shared_countries.global_entity_id
  WHERE pd_vendors.global_entity_id = 'FP_KH' 
  AND pd_vendors_agg_business_types.global_entity_id = 'FP_KH' 
  AND shared_countries.global_entity_id = 'FP_KH'  
),
--
hitlevel AS (
  SELECT
    ga_sessions.visit_type,
    ga_sessions.session_start_timestamp_utc AS session_start_timestamp,
    ga_events.date_utc,
    ga_events.country,
    IF(ga_events.platform IN ('iOS','Android'), ga_events.platform, 'Web') AS platform,
    ga_events.event_action,
    ga_events.event_category,
    ga_events.event_label,
    LOWER(ga_events.cd150) AS vendor_code,
    ga_events.ga_session_id AS session_id,
    ga_events.ga_fullvisitor_id AS fullvisitor_id,
    CONCAT(ga_events.ga_session_id, ga_events.hit_number) AS session_hit_id,
  FROM `fulfillment-dwh-production.pandata_curated.ga_events` AS ga_events
  LEFT JOIN `fulfillment-dwh-production.pandata_curated.ga_sessions` AS ga_sessions
         ON ga_events.ga_session_id = ga_sessions.ga_session_id
  WHERE ga_events.partition_date >= date1
    AND ga_sessions.partition_date >= date1
    AND event_action IN (
      'app.opened',
      'home_screen.loaded',
      'home_screen.clicked',
      'shop_tab.clicked',
      'fork.loaded',
      'fork_pandamart.clicked',
      'darkstore.shown',
      'darkstore.loaded',
      'shop_list.loaded',
      'shop_details.loaded',
      'add_cart.clicked',
      'checkout.loaded',
      'transaction'
    )
    AND ga_events.global_entity_id = 'FP_KH'
    AND ga_sessions.global_entity_id = 'FP_KH'
),
--
sessionlevel AS (
  SELECT
    session_id,
    fullvisitor_id,
    hitlevel.date_utc,
    hitlevel.session_start_timestamp,
    hitlevel.country,
    hitlevel.platform,
    hitlevel.visit_type,
    hitlevel.vendor_code,
    -- hits
    COUNT(DISTINCT(IF(event_action = 'shop_details.loaded', session_hit_id, NULL))) menu_hits,
    COUNT(DISTINCT(IF(event_action = 'add_cart.clicked', session_hit_id, NULL))) atc_hits,
    COUNT(DISTINCT(IF(event_action = 'checkout.loaded', session_hit_id, NULL))) checkout_hits,
    COUNT(DISTINCT(IF(event_action = 'transaction', session_hit_id, NULL))) transaction_hits,
  FROM hitlevel
  GROUP BY
    session_id,
    fullvisitor_id,
    date_utc,
    session_start_timestamp,
    country,
    platform,
    visit_type,
    vendor_code
),
--
vendorlevel AS (
  SELECT
    sessionlevel.date_utc,
    DATE(sessionlevel.session_start_timestamp, vendortable.timezone) date_local,
    sessionlevel.country,
    sessionlevel.platform,
    sessionlevel.visit_type,
    sessionlevel.vendor_code,
    vendortable.vendor_type,
    CASE
      WHEN vendortable.vendor_type = 'dmart' THEN 'darkstores'
      WHEN vendortable.vendor_type = 'shops' THEN 'shops'
      WHEN vendortable.vendor_type IN ('concepts','restaurants','kitchens','caterers') THEN 'restaurants'
      ELSE 'other'
    END AS be_vendor_type_parent,
    vendortable.vendor_name,
    vendortable.global_entity_id,
    -- hits
    IFNULL(SUM(menu_hits), 0) AS menu_hits,
    IFNULL(SUM(atc_hits), 0) AS atc_hits,
    IFNULL(SUM(checkout_hits), 0) AS checkout_hits,
    IFNULL(SUM(transaction_hits), 0) AS transaction_hits,
    -- sessions
    COUNT(DISTINCT(IF(menu_hits > 0, session_id, NULL))) AS menu_sessions,
    COUNT(DISTINCT(IF(atc_hits > 0, session_id, NULL))) AS atc_sessions,
    COUNT(DISTINCT(IF(checkout_hits > 0, session_id, NULL))) AS checkout_sessions,
    COUNT(DISTINCT(IF(transaction_hits > 0, session_id, NULL))) AS transaction_sessions,
    -- closed funnel sessions
    COUNT(DISTINCT(IF(menu_hits > 0 AND atc_hits > 0, session_id, NULL))) AS menu_n_atc_sessions,
    COUNT(DISTINCT(IF(atc_hits > 0 AND checkout_hits > 0, session_id, NULL))) AS atc_n_checkout_sessions,
    COUNT(DISTINCT(IF(checkout_hits > 0 AND transaction_hits > 0, session_id, NULL))) AS checkout_n_transaction_sessions,
    COUNT(DISTINCT(IF(menu_hits > 0 AND transaction_hits > 0, session_id, NULL))) AS menu_n_transaction_sessions,
    -- users
    COUNT(DISTINCT(IF(menu_hits > 0, fullvisitor_id, NULL))) AS menu_users,
    COUNT(DISTINCT(IF(atc_hits > 0, fullvisitor_id, NULL))) AS atc_users,
    COUNT(DISTINCT(IF(checkout_hits > 0, fullvisitor_id, NULL))) AS checkout_users,
    COUNT(DISTINCT(IF(transaction_hits > 0, fullvisitor_id, NULL))) AS transaction_users,
  FROM sessionlevel
  LEFT JOIN vendortable
         ON sessionlevel.country = vendortable.country
        AND sessionlevel.vendor_code = vendortable.vendor_code
  GROUP BY
    date_utc,
    date_local,
    country,
    platform,
    visit_type,
    vendor_code,
    vendor_type,
    be_vendor_type_parent,
    vendor_name,
    global_entity_id
)

SELECT * FROM vendorlevel WHERE global_entity_id = 'FP_KH' AND vendor_type = 'shops'


