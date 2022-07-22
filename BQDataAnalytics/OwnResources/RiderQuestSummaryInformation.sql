

SELECT 
    a.global_entity_id,
    a.uuid, 
    a.lg_country_code,
    a.status,
    a.type,
    a.sub_type,
    a.name,
    a.description,
    a.eligibility_description,
    a.is_active,
    a.is_pay_below_threshold,
    a.is_negative,
    a.minimum_acceptance_rate,
    a.no_show_limit,
    a.created_at_local,
    a.approved_at_local,
    a.updated_at_local,
    a.start_at_local,
    a.end_at_local,

    -- applies to 
    al.lg_city_ids,

    -- cost factors
    cf.min_threshold,
    cf.type AS cost_factor_type,
    cf.amount_local

  FROM `fulfillment-dwh-production.pandata_curated.lg_payments_quest_rules` AS a ,
  UNNEST(a.applies_to) AS al,
  UNNEST(a.cost_factors) AS cf
  WHERE a.global_entity_id = 'FP_KH'
  ORDER BY 2


