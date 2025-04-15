/*  
    ************************** 
    *       Multiplier       *
    ************************** 
*/

select min(core_date), max(core_date), count(*) from DATASCIENCE.ANALYSIS.accretive_multiplier;

select min(core_date), max(core_date), count(*) from DATASCIENCE.ANALYSIS.AZIRA_Multiplier;


WITH daily_counts AS (
    SELECT 
        'Accretive' as source,
        core_date,
        COUNT(DISTINCT h3_index) as unique_h3_cells,
        avg(multiplier) as avg_multiplier,
        median(multiplier) as median_multiplier,
        stddev(multiplier) as std_multiplier,
        COUNT(*) as total_multipliers
    FROM DATASCIENCE.ANALYSIS.ACCRETIVE_MULTIPLIER
    WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
    GROUP BY core_date
    
    UNION ALL

    SELECT 
        'Azira' as source,
        core_date,
        COUNT(DISTINCT h3_index) as unique_h3_cells,
        avg(multiplier) as avg_multiplier,
        median(multiplier) as median_multiplier,
        stddev(multiplier) as std_multiplier,
        COUNT(*) as total_multipliers
    FROM DATASCIENCE.ANALYSIS.AZIRA_Multiplier
    WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
    GROUP BY core_date
)
select * from daily_counts;

/*  
    ************************** 
    *    Matched Devices     *
    ************************** 
*/

select distinct frame_id, match_info:SITE_LAT as frame_lat, match_info:SITE_LON as frame_lon 
from DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES 
where owner_id <> 712
ORDER BY RANDOM() 
LIMIT 10;

select min(core_date), max(core_date), count(*) from DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES;


select min(core_date), max(core_date), count(*) from DATALAKE_TEST.MATCHING.STAGE_2__STATIONARY_DEVICES
where core_date between '2025-01-01' and '2025-01-14';


/* Total Matches */
WITH accretive_totals AS (
  SELECT 
    core_date,
    COUNT(*) as total_matches,
    count(distinct device_hash, device_geog) as distinct_devices
  FROM DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES
  group by core_date
  -- where owner_id = 712
  -- and core_date = '2025-01-09'
),
azira_totals AS (
  SELECT 
    core_date,
    COUNT(*) as total_matches,
    count(distinct device_hash, device_geog) as distinct_devices
  FROM DATALAKE_TEST.MATCHING.STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
  group by core_date
  -- and owner_id = 712
  -- and core_date = '2025-01-09'
)
SELECT 
  'Accretive' as source,
  core_date,
  total_matches,
  distinct_devices
FROM accretive_totals
UNION ALL
SELECT 
  'Azira' as source,
  core_date,
  total_matches,
  distinct_devices
FROM azira_totals
UNION ALL
SELECT
    'Ratio' as source,
    ac.core_date,
    ac.total_matches/az.total_matches as ratio,
    ac.distinct_devices/az.distinct_devices as ratio_distinct_devices
FROM azira_totals az
JOIN accretive_totals ac
ON az.core_date = ac.core_date;


/* Frame Matches */

WITH azira_frame_metrics AS (
  SELECT 
    'Azira' as source,
    core_date,
    frame_id,
    COUNT(DISTINCT device_hash) as unique_devices,
    COUNT(*) as total_matches
  FROM DATALAKE_TEST.MATCHING.STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
  GROUP BY frame_id, core_date
),
accretive_frame_metrics AS (
  SELECT 
    'Accretive' as source,
    core_date,
    frame_id,
    COUNT(DISTINCT device_hash) as unique_devices,
    COUNT(*) as total_matches
  FROM DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
  GROUP BY frame_id, core_date
),
joined_frame_metrics AS (
    SELECT 
        az.frame_id,
        az.core_date,
        az.unique_devices as azira_unique_devices,
        az.total_matches as azira_total_matches,
        ac.unique_devices as accretive_unique_devices,
        ac.total_matches as accretive_total_matches,
        ac.unique_devices/az.unique_devices as ratio_unique_devices,
        ac.total_matches/az.total_matches as ratio_total_matches
    FROM azira_frame_metrics az
    JOIN accretive_frame_metrics ac
    ON az.frame_id = ac.frame_id
)
SELECT 
    'Unique Devices' as metric,
    core_date,
    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_1,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ratio_unique_devices) AS PERC_99
FROM joined_frame_metrics
group by core_date
UNION ALL
SELECT 
    'Total Matches' as metric,
    core_date,
    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_1,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY ratio_total_matches) AS PERC_99
FROM joined_frame_metrics
group by core_date;


  SELECT 
    'Azira' as source,
  FROM DATALAKE_TEST.MATCHING.STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
    and creative_id is null
  GROUP BY core_date, creative_id;


/* Market Matches */
WITH azira_frame_metrics AS (
  SELECT 
    'Azira' as source,
    core_date,
    frame_id,
    market_id,
    COUNT(DISTINCT device_hash) as unique_devices,
    COUNT(*) as total_matches
  FROM DATALAKE_TEST.MATCHING.STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
  GROUP BY core_date, market_id, frame_id
),
accretive_frame_metrics AS (
  SELECT 
    'Accretive' as source,
    core_date,
    frame_id,
    market_id,
    COUNT(DISTINCT device_hash) as unique_devices,
    COUNT(*) as total_matches
  FROM DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES
  WHERE core_date BETWEEN '2025-01-01' AND '2025-01-14'
  GROUP BY core_date, market_id, frame_id
),
joined_creative_metrics AS (
    SELECT 
        az.market_id,
        az.frame_id,
        az.core_date,
        az.unique_devices as azira_unique_devices,
        az.total_matches as azira_total_matches,
        ac.unique_devices as accretive_unique_devices,
        ac.total_matches as accretive_total_matches,
        ac.unique_devices/az.unique_devices as ratio_unique_devices,
        ac.total_matches/az.total_matches as ratio_total_matches
    FROM azira_frame_metrics az
    JOIN accretive_frame_metrics ac
    ON az.market_id = ac.market_id
    and az.core_date = ac.core_date
    and az.frame_id = ac.frame_id
)
SELECT 
    m.marketname,
    count(distinct j.frame_id) as total_frames,
    median(ratio_unique_devices) as median_ratio_unique_devices,
    avg(ratio_unique_devices) as avg_ratio_unique_devices,
    median(ratio_total_matches) as median_ratio_total_matches,
    avg(ratio_total_matches) as avg_ratio_total_matches
FROM joined_creative_metrics j
left join SM_PROD_POSTGRESQL."iris"."MarketInfo" m on j.market_id = m.marketid
group by m.marketname;

/*  
    ************************** 
    *    Silver Impression     *
    ************************** 
*/

select * from DATASCIENCE.ANALYSIS.SILVER_HOURLY_IMPRESSIONS_ROLLUP_AZIRA limit 10;

select * from DATASCIENCE.ANALYSIS.SILVER_HOURLY_IMPRESSIONS_ROLLUP_ACCRETIVE limit 10;


WITH az_set AS (
    -- Distinct SF device IDs from the base dataset
    SELECT matchable_id as frame_id, core_date,
    sum(likely_to_see_matches) as az_lts, 
    sum(likely_to_see_matches + opportunity_to_see_matches) as az_ots,
    sum(likely_to_see_matches + opportunity_to_see_matches + chance_to_see_matches) as az_cts
    FROM DATASCIENCE.ANALYSIS.SILVER_HOURLY_IMPRESSIONS_ROLLUP_AZIRA
    group by frame_id, core_date
),
ac_set AS (
    -- Distinct PP device IDs from the base dataset
    SELECT matchable_id as frame_id, core_date,
    sum(likely_to_see_matches) as ac_lts, 
    sum(likely_to_see_matches + opportunity_to_see_matches) as ac_ots,
    sum(likely_to_see_matches + opportunity_to_see_matches + chance_to_see_matches) as ac_cts
    FROM DATASCIENCE.ANALYSIS.SILVER_HOURLY_IMPRESSIONS_ROLLUP_ACCRETIVE
    group by frame_id, core_date
),
overlap as (
    select 
    ac.frame_id, 
    ac.core_date,
    ac_lts, az_lts, 
    ac_ots, az_ots, 
    ac_cts, az_cts, 
    DIV0(ac_lts, az_lts) as lts_ratio,
    DIV0(ac_ots, az_ots) as ots_ratio,
    DIV0(ac_cts, az_cts) as cts_ratio,
    from ac_set ac
    join az_set az on ac.frame_id = az.frame_id
    and ac.core_date = az.core_date
)
SELECT 
    'Gross' as metric,
    core_date,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY cts_ratio) AS percentile_5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cts_ratio) AS percentile_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cts_ratio) AS percentile_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cts_ratio) AS percentile_75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cts_ratio) AS percentile_95,
FROM 
    overlap
GROUP BY core_date
UNION ALL
SELECT
    'OTS' as metric,
    core_date,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY ots_ratio) AS percentile_5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ots_ratio) AS percentile_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ots_ratio) AS percentile_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ots_ratio) AS percentile_75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ots_ratio) AS percentile_95,
from    
    overlap
GROUP BY core_date
UNION ALL
SELECT
    'LTS' as metric,
    core_date,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY lts_ratio) AS percentile_5,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lts_ratio) AS percentile_25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lts_ratio) AS percentile_50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lts_ratio) AS percentile_75,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lts_ratio) AS percentile_95,
from    
    overlap
GROUP BY core_date;




select f.frameid as SM_ID,  c.customername,f.METADATA:"frameMedia" as Venue_Media_Type,CSFR.site_lat,CSFR.site_lon, f.frametype, f.expositionangle,f.expositionwidth, f.frameheight,f.framewidth, f.frameelevation
from SM_PROD_POSTGRESQL."selene"."Frame" f
left join SM_PROD_POSTGRESQL."helios"."Customer" c on f.customerid = c.customerid
left join DATALAKE.SILVER.STATIONARY__CUSTOMER_SITE_FRAME_REL_VIEW CSFR on f.frameid = CSFR.frame_id
where f.frameid in (73517, 62431, 2287, 77079, 76444);


select * from data where frameid in (73517, 62431, 2287, 77079, 76444);

select * from DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES where frame_id in (73517, 62431, 2287, 77079, 76444);


SELECT frame_id FROM DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES where owner_id <> 712 ORDER BY RANDOM() LIMIT 5;
