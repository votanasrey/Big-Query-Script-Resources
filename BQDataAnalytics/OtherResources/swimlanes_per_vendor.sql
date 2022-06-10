-- CREATE TEMP FUNCTION TopN(arr ANY TYPE, n INT64) AS (
--   ARRAY(SELECT x FROM UNNEST(arr) AS x WITH OFFSET off WHERE off < n ORDER BY off)
-- );

WITH
config as (
    select
    --this dates represents data for the week between 4/10 to 10/10
    date('2021-09-05') as from_interval,
    date('2021-09-11') as to_interval
)

,extract_dist_restaurants as
(SELECT 
    base.request_id
    ,restaurant_ids
    ,count(swimlane_id) as swimlane_count
FROM `dhh-digital-analytics-dwh.global_cl.global_swimlanes_cl` AS base,
  UNNEST(swimlane_array) AS swimlane
  --,unnest(topN(swimlane.restaurant_ids,5)) as restaurant_ids
  ,unnest(swimlane.restaurant_ids) as restaurant_ids
WHERE DATE(_PARTITIONTIME) between (select from_interval from config) and (select to_interval from config)
  AND IF(ARRAY_LENGTH(swimlane.restaurant_ids)>0,1,0) = 1
  and country_name in ('Thailand')
  group by 1,2
)

select 
restaurant_ids
,sum(swimlane_count) as swimlane_count
--,count(request_id)
from extract_dist_restaurants 
group by 1
order by 1



--short version



SELECT 
    restaurant_ids
    ,count(swimlane_id) as swimlane_count
FROM `fulfillment-dwh-production.curated_data_shared_product_analytics.swimlane_requests_sessions` AS base,
  UNNEST(swimlane_array) AS swimlane
  ,unnest(swimlane.restaurant_ids) as restaurant_ids
WHERE 
    --this dates represents data for the week between 4/10 to 10/10
partition_date between '2021-09-05' and '2021-09-11'
  AND IF(ARRAY_LENGTH(swimlane.restaurant_ids)>0,1,0) = 1
  and country_name in ('Thailand')
  group by 1
  order by 1