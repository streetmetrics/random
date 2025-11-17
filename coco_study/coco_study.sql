/*
    ********************************************************************************
    *                          COCO ROBOT OOH CASE STUDY                          *
    *                          StreetMetrics Analysis                             *
    ********************************************************************************
    
    OBJECTIVE: Demonstrate robots as a legitimate, high-performing OOH medium
    
    NARRATIVE GOALS:
    1. Validate Scale - Show Coco's network is large, active, and measurable
    2. Prove Consistency - Demonstrate reliable performance across campaigns
    3. Highlight Impact - Quantify exposure efficiency vs. traditional OOH
    4. Support Flexibility - Provide data for ongoing case studies
    
    COCO DETAILS:
    - Owner ID: 1513
    - Medium: Transit (robots are classified as transit/mobile OOH)
    - Date Range: TBD (recommend 2024-01-01 to 2024-12-31 for full year analysis)
    
    DATA ARCHITECTURE NOTES:
    ==================================================================================
    The StreetMetrics pipeline has two phases:
    
    PHASE 1: MATCHING (Campaign-Independent)
    - GPS signals received from Coco robots
    - Matching occurs for ANY GPS signal → exposed devices stored in BRONZE layer
    - Tables: DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES
    - Contains: DEVICE_HASH, ASSET_ID, DEVICE_GEOG, H3 cells, VIEWABILITY, etc.
    - NO CAMPAIGN_ID at this stage - just raw exposure data
    
    PHASE 2: CAMPAIGN ROLLUP (Post-Matching)
    - Campaign definitions applied to matched exposures
    - Metrics aggregated into hourly/daily bins by campaign
    - Tables: DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__*
    - Contains: CAMPAIGN_ID, MATCHABLE_CLASS, MATCHABLE_ID, aggregated metrics
    - MATCHABLE_CLASS = 'TRANSIT' for robots (IMPORTANT: always filter by this!)
    - MATCHABLE_ID = ASSET_ID (individual robot identifier)
    - Use WHERE MATCHABLE_CLASS = 'TRANSIT' in all INTERIM_DAG queries for Coco
    
    KEY TABLES:
    - DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__IMPRESSIONS_HOURLY ⭐ PRIMARY FOR METRICS
    - DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__PERFORMANCE_HOURLY (joins impressions + coverage)
    - DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__UFI__CAMPAIGN_TOTAL (Uniques/Frequency/Impressions)
    - DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES (raw device-level exposures, no campaign info)
*/

--------------------------------------------------------------------------------
-- PHASE 1: RAW IMPRESSION ROLLUP (CAMPAIGN-INDEPENDENT WITH MULTIPLIERS)
--------------------------------------------------------------------------------
-- Purpose: Calculate total impressions from raw matched exposures with multipliers
-- This is independent of campaign definitions - pure matching output
-- Multipliers adjust for device representativeness based on H3 zone and time

WITH multiplied_exposures AS (
    SELECT 
        TD.CORE_DATE,
        TD.OWNER_ID,
        TD.MARKET_ID,
        TD.ASSET_ID,
        TD.DEVICE_HASH,
        TD.VIEWABILITY,
        TD.DECAYED_VALUE,
        COALESCE(MP.MULTIPLIER, 1) AS MULTIPLIER,
        COALESCE(MP.MULTIPLIER, 1) * TD.DECAYED_VALUE AS MP_VALUE
    FROM 
        DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES TD
        LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
            MP.CORE_DATE = TD.CORE_DATE
            AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
            AND MP.H3_INDEX = TD.H3I_7
    WHERE 
        TD.OWNER_ID = 1513
        AND TD.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND TD.MARKET_ID <> 88
),
robot_stats AS (
    SELECT
        ASSET_ID,
        COUNT(DISTINCT CORE_DATE) AS DAYS_DRIVEN
    FROM multiplied_exposures
    GROUP BY ASSET_ID
),
coverage_stats AS (
    SELECT
        PT.ASSET_ID,
        SUM(PT.PATH_LENGTH) AS TOTAL_DISTANCE_METERS,
        SUM(PT.PATH_LENGTH) * 0.000621371 AS TOTAL_DISTANCE_MILES,
        SUM(PT.END_TIME - PT.START_TIME) AS TOTAL_TIME_SECONDS
    FROM DATALAKE.BRONZE.PATHS AS PT
    WHERE PT.CUSTOMER_ID = 1513
        AND PT.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND PT.MARKET_ID <> 88
    GROUP BY PT.ASSET_ID
),
mileage_rollup AS (
    SELECT
        SUM(TOTAL_DISTANCE_MILES) AS TOTAL_MILES,
        TOTAL_MILES / 614 AS AVG_MILES_PER_BOT
    FROM coverage_stats
),
final_stats AS (
    SELECT 
        SUM(CASE WHEN VIEWABILITY = 'OPPORTUNITY_TO_SEE' THEN MP_VALUE ELSE 0 END) AS TOTAL_IMPRESSIONS_OTS,
        SUM(MP_VALUE) AS TOTAL_IMPRESSIONS_GROSS,
        (SUM(CASE WHEN VIEWABILITY = 'OPPORTUNITY_TO_SEE' THEN MP_VALUE ELSE 0 END) + SUM(MP_VALUE)) / 2.0 AS AVG_OTS_GROSS,
        COUNT(*) AS TOTAL_MATCHED_EXPOSURES,
        COUNT(DISTINCT ASSET_ID) AS UNIQUE_ROBOTS,
        COUNT(DISTINCT MARKET_ID) AS MARKETS,
        TOTAL_IMPRESSIONS_OTS / (SELECT AVG(DAYS_DRIVEN) FROM robot_stats) / UNIQUE_ROBOTS AS AVG_OTS_IMPRESSIONS_PER_DAY_PER_BOT,
        TOTAL_IMPRESSIONS_GROSS / (SELECT AVG(DAYS_DRIVEN) FROM robot_stats) / UNIQUE_ROBOTS AS AVG_GROSS_IMPRESSIONS_PER_DAY_PER_BOT,
        AVG_OTS_GROSS / (SELECT AVG(DAYS_DRIVEN) FROM robot_stats) / UNIQUE_ROBOTS AS OTS_GROSS_PER_IMPRESSION_PER_DAY
    FROM multiplied_exposures
)
SELECT
    TO_CHAR(fs.TOTAL_IMPRESSIONS_OTS, '999,999,999,999') AS IMPRESSIONS_OTS,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_GROSS, '999,999,999,999') AS IMPRESSIONS_GROSS,
    TO_CHAR(fs.AVG_OTS_GROSS, '999,999,999,999') AS AVG_OTS_GROSS,
    TO_CHAR(fs.UNIQUE_ROBOTS, '999,999,999,999') AS UNIQUE_ROBOTS,
    TO_CHAR(fs.MARKETS, '999,999,999,999') AS MARKETS,
    (SELECT AVG(DAYS_DRIVEN) FROM robot_stats) AS AVG_DAYS_DRIVEN,
    TO_CHAR(fs.AVG_OTS_IMPRESSIONS_PER_DAY_PER_BOT, '999,999,999,999') AS AVG_OTS_IMPRESSIONS_PER_DAY_PER_BOT,
    TO_CHAR(fs.AVG_GROSS_IMPRESSIONS_PER_DAY_PER_BOT, '999,999,999,999') AS AVG_GROSS_IMPRESSIONS_PER_DAY_PER_BOT,
    TO_CHAR(fs.OTS_GROSS_PER_IMPRESSION_PER_DAY, '999,999,999,999') AS OTS_GROSS_PER_IMPRESSION_PER_DAY,
    -- Mileage metrics
    TO_CHAR(mr.TOTAL_MILES, '999,999,999,999') AS TOTAL_MILES_DRIVEN,
    TO_CHAR(mr.AVG_MILES_PER_BOT, '999,999,999') AS AVG_MILES_PER_BOT,
    -- Impressions per mile metrics
    TO_CHAR(fs.TOTAL_IMPRESSIONS_OTS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS IMPRESSIONS_OTS_PER_MILE,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_GROSS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS IMPRESSIONS_GROSS_PER_MILE,
    TO_CHAR(fs.AVG_OTS_GROSS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS AVG_OTS_GROSS_PER_MILE,
    TO_CHAR((fs.TOTAL_IMPRESSIONS_OTS / NULLIF(mr.TOTAL_MILES, 0)) / fs.UNIQUE_ROBOTS, '999,999') AS AVG_IMPRESSIONS_OTS_PER_MILE_PER_BOT,
    TO_CHAR((fs.TOTAL_IMPRESSIONS_GROSS / NULLIF(mr.TOTAL_MILES, 0)) / fs.UNIQUE_ROBOTS, '999,999') AS AVG_IMPRESSIONS_GROSS_PER_MILE_PER_BOT
FROM final_stats fs
CROSS JOIN mileage_rollup mr;

-- Market-level breakdown with same metrics
WITH multiplied_exposures AS (
    SELECT 
        TD.CORE_DATE,
        TD.OWNER_ID,
        TD.MARKET_ID,
        TD.ASSET_ID,
        TD.DEVICE_HASH,
        TD.VIEWABILITY,
        TD.DECAYED_VALUE,
        COALESCE(MP.MULTIPLIER, 1) AS MULTIPLIER,
        COALESCE(MP.MULTIPLIER, 1) * TD.DECAYED_VALUE AS MP_VALUE
    FROM 
        DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES TD
        LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
            MP.CORE_DATE = TD.CORE_DATE
            AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
            AND MP.H3_INDEX = TD.H3I_7
    WHERE 
        TD.OWNER_ID = 1513
        AND TD.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND TD.MARKET_ID <> 88
),
robot_stats_by_market AS (
    SELECT
        MARKET_ID,
        ASSET_ID,
        COUNT(DISTINCT CORE_DATE) AS DAYS_DRIVEN
    FROM multiplied_exposures
    GROUP BY MARKET_ID, ASSET_ID
),
avg_days_by_market AS (
    SELECT
        MARKET_ID,
        AVG(DAYS_DRIVEN) AS AVG_DAYS_DRIVEN_PER_BOT
    FROM robot_stats_by_market
    GROUP BY MARKET_ID
),
coverage_stats_by_market AS (
    SELECT
        PT.MARKET_ID,
        PT.ASSET_ID,
        SUM(PT.PATH_LENGTH) AS TOTAL_DISTANCE_METERS,
        SUM(PT.PATH_LENGTH) * 0.000621371 AS TOTAL_DISTANCE_MILES,
        SUM(PT.END_TIME - PT.START_TIME) AS TOTAL_TIME_SECONDS
    FROM DATALAKE.BRONZE.PATHS AS PT
    WHERE PT.CUSTOMER_ID = 1513
        AND PT.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND PT.MARKET_ID <> 88
    GROUP BY PT.MARKET_ID, PT.ASSET_ID
),
mileage_rollup_by_market AS (
    SELECT
        MARKET_ID,
        SUM(TOTAL_DISTANCE_MILES) AS TOTAL_MILES,
        AVG(TOTAL_DISTANCE_MILES) AS AVG_MILES_PER_BOT,
        COUNT(DISTINCT ASSET_ID) AS BOTS_WITH_PATHS,
        SUM(TOTAL_TIME_SECONDS) AS TOTAL_TIME_SECONDS,
        AVG(TOTAL_TIME_SECONDS) AS AVG_TIME_SECONDS_PER_BOT
    FROM coverage_stats_by_market
    GROUP BY MARKET_ID
),
final_stats_by_market AS (
    SELECT 
        MARKET_ID,
        SUM(CASE WHEN VIEWABILITY = 'OPPORTUNITY_TO_SEE' THEN MP_VALUE ELSE 0 END) AS TOTAL_IMPRESSIONS_OTS,
        SUM(MP_VALUE) AS TOTAL_IMPRESSIONS_GROSS,
        (SUM(CASE WHEN VIEWABILITY = 'OPPORTUNITY_TO_SEE' THEN MP_VALUE ELSE 0 END) + SUM(MP_VALUE)) / 2.0 AS AVG_OTS_GROSS,
        COUNT(*) AS TOTAL_MATCHED_EXPOSURES,
        COUNT(DISTINCT ASSET_ID) AS UNIQUE_ROBOTS,
        COUNT(DISTINCT DEVICE_HASH) AS UNIQUE_DEVICES
    FROM multiplied_exposures
    GROUP BY MARKET_ID
)
SELECT
    fs.MARKET_ID,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_OTS, '999,999,999,999') AS IMPRESSIONS_OTS,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_GROSS, '999,999,999,999') AS IMPRESSIONS_GROSS,
    TO_CHAR(fs.AVG_OTS_GROSS, '999,999,999,999') AS AVG_OTS_GROSS,
    TO_CHAR(fs.TOTAL_MATCHED_EXPOSURES, '999,999,999,999') AS MATCHED_EXPOSURES,
    TO_CHAR(fs.UNIQUE_ROBOTS, '999,999') AS UNIQUE_ROBOTS,
    TO_CHAR(fs.UNIQUE_DEVICES, '999,999,999') AS UNIQUE_DEVICES,
    ROUND(ad.AVG_DAYS_DRIVEN_PER_BOT, 2) AS AVG_DAYS_DRIVEN,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_OTS / NULLIF(ad.AVG_DAYS_DRIVEN_PER_BOT, 0) / NULLIF(fs.UNIQUE_ROBOTS, 0), '999,999,999') AS AVG_OTS_IMPRESSIONS_PER_DAY_PER_BOT,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_GROSS / NULLIF(ad.AVG_DAYS_DRIVEN_PER_BOT, 0) / NULLIF(fs.UNIQUE_ROBOTS, 0), '999,999,999') AS AVG_GROSS_IMPRESSIONS_PER_DAY_PER_BOT,
    TO_CHAR(fs.AVG_OTS_GROSS / NULLIF(ad.AVG_DAYS_DRIVEN_PER_BOT, 0) / NULLIF(fs.UNIQUE_ROBOTS, 0), '999,999,999') AS OTS_GROSS_PER_DAY_PER_BOT,
    -- Mileage metrics
    TO_CHAR(mr.TOTAL_MILES, '999,999,999') AS TOTAL_MILES_DRIVEN,
    TO_CHAR(mr.AVG_MILES_PER_BOT, '999,999') AS AVG_MILES_PER_BOT,
    TO_CHAR(mr.BOTS_WITH_PATHS, '999,999') AS BOTS_WITH_PATH_DATA,
    TO_CHAR(mr.TOTAL_TIME_SECONDS / 3600.0, '999,999,999') AS TOTAL_HOURS_DRIVEN,
    TO_CHAR(mr.AVG_TIME_SECONDS_PER_BOT / 3600.0, '999,999') AS AVG_HOURS_PER_BOT,
    -- Impressions per mile metrics
    TO_CHAR(fs.TOTAL_IMPRESSIONS_OTS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS IMPRESSIONS_OTS_PER_MILE,
    TO_CHAR(fs.TOTAL_IMPRESSIONS_GROSS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS IMPRESSIONS_GROSS_PER_MILE,
    TO_CHAR(fs.AVG_OTS_GROSS / NULLIF(mr.TOTAL_MILES, 0), '999,999,999') AS AVG_OTS_GROSS_PER_MILE,
    TO_CHAR((fs.TOTAL_IMPRESSIONS_OTS / NULLIF(mr.TOTAL_MILES, 0)) / NULLIF(fs.UNIQUE_ROBOTS, 0), '999,999') AS AVG_IMPRESSIONS_OTS_PER_MILE_PER_BOT,
    TO_CHAR((fs.TOTAL_IMPRESSIONS_GROSS / NULLIF(mr.TOTAL_MILES, 0)) / NULLIF(fs.UNIQUE_ROBOTS, 0), '999,999') AS AVG_IMPRESSIONS_GROSS_PER_MILE_PER_BOT
FROM final_stats_by_market fs
LEFT JOIN avg_days_by_market ad ON fs.MARKET_ID = ad.MARKET_ID
LEFT JOIN mileage_rollup_by_market mr ON fs.MARKET_ID = mr.MARKET_ID
ORDER BY fs.TOTAL_IMPRESSIONS_GROSS DESC;


--------------------------------------------------------------------------------
-- PHASE 2: CAMPAIGN-LEVEL METRICS
--------------------------------------------------------------------------------
-- Purpose: Calculate campaign-level metrics

WITH campaign_metrics as (
    SELECT 
        CAMPAIGN_ID,
        CAMPAIGNREF,
        COUNT(DISTINCT CORE_DATE) AS DAYS,
        COUNT(DISTINCT MATCHABLE_ID) AS UNIQUE_ROBOTS, 
        SUM(IMPRESSIONS_OTS)::INT AS TOTAL_IMPRESSIONS_OTS,
        SUM(IMPRESSIONS_GROSS)::INT AS TOTAL_IMPRESSIONS_GROSS,
        ((SUM(IMPRESSIONS_OTS) + SUM(IMPRESSIONS_GROSS)) / 2.0)::INT AS AVG_OTS_GROSS
    FROM DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__IMPRESSIONS_HOURLY imp
        LEFT JOIN SM_PROD_POSTGRESQL."selene"."Campaign" c on imp.CAMPAIGN_ID = c.CAMPAIGNID
    WHERE OWNER_ID = 1513
        AND CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'  -- ADJUST DATE RANGE AS NEEDED
        and CAMPAIGN_ID in (242331,240232,239916,238909)
    GROUP BY CAMPAIGN_ID, CAMPAIGNREF
),
mileage_rollup as (
    SELECT
        CAMPAIGN_ID,
        SUM(COVERAGE_DISTANCE_METER) AS TOTAL_DISTANCE_METERS,
        SUM(COVERAGE_DISTANCE_METER) * 0.000621371 AS TOTAL_DISTANCE_MILES
    FROM DATALAKE.INTERIM_DAG.CAMPAIGN_METRICS__PERFORMANCE_HOURLY
    WHERE OWNER_ID = 1513
        AND CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND CAMPAIGN_ID in (242331,240232,239916,238909)
    GROUP BY CAMPAIGN_ID
),
final_stats as (
    SELECT
        campaign_metrics.CAMPAIGN_ID,
        CAMPAIGNREF,
        DAYS,
        UNIQUE_ROBOTS,
        TOTAL_IMPRESSIONS_OTS,
        TOTAL_IMPRESSIONS_GROSS,
        AVG_OTS_GROSS,
        TOTAL_DISTANCE_MILES,
        TOTAL_IMPRESSIONS_OTS /TOTAL_DISTANCE_MILES AS IMPRESSIONS_OTS_PER_MILE,
        TOTAL_IMPRESSIONS_GROSS / TOTAL_DISTANCE_MILES AS IMPRESSIONS_GROSS_PER_MILE,
        AVG_OTS_GROSS / TOTAL_DISTANCE_MILES AS AVG_OTS_GROSS_PER_MILE,
        TOTAL_IMPRESSIONS_OTS / UNIQUE_ROBOTS / DAYS AS IMPRESSIONS_OTS_PER_BOT_PER_DAY,
        TOTAL_IMPRESSIONS_GROSS / UNIQUE_ROBOTS / DAYS AS IMPRESSIONS_GROSS_PER_BOT_PER_DAY,
        AVG_OTS_GROSS / UNIQUE_ROBOTS / DAYS AS AVG_OTS_GROSS_PER_BOT_PER_DAY
    FROM campaign_metrics
    JOIN mileage_rollup ON campaign_metrics.CAMPAIGN_ID = mileage_rollup.CAMPAIGN_ID
)
SELECT 
    CAMPAIGN_ID,
    CAMPAIGNREF,
    DAYS,
    UNIQUE_ROBOTS,
    TO_CHAR(TOTAL_IMPRESSIONS_OTS, '999,999,999,999') AS TOTAL_IMPRESSIONS_OTS,
    TO_CHAR(TOTAL_IMPRESSIONS_GROSS, '999,999,999,999') AS TOTAL_IMPRESSIONS_GROSS,
    TO_CHAR(AVG_OTS_GROSS, '999,999,999,999') AS AVG_OTS_GROSS,
    TO_CHAR(TOTAL_DISTANCE_MILES, '999,999,999,999') AS TOTAL_DISTANCE_MILES,
    TO_CHAR(IMPRESSIONS_OTS_PER_MILE, '999,999,999,999') AS IMPRESSIONS_OTS_PER_MILE,
    TO_CHAR(IMPRESSIONS_GROSS_PER_MILE, '999,999,999,999') AS IMPRESSIONS_GROSS_PER_MILE,
    TO_CHAR(AVG_OTS_GROSS_PER_MILE, '999,999,999,999') AS AVG_OTS_GROSS_PER_MILE,
    TO_CHAR(IMPRESSIONS_OTS_PER_BOT_PER_DAY, '999,999,999,999') AS IMPRESSIONS_OTS_PER_BOT_PER_DAY,
    TO_CHAR(IMPRESSIONS_GROSS_PER_BOT_PER_DAY, '999,999,999,999') AS IMPRESSIONS_GROSS_PER_BOT_PER_DAY,
    TO_CHAR(AVG_OTS_GROSS_PER_BOT_PER_DAY, '999,999,999,999') AS AVG_OTS_GROSS_PER_BOT_PER_DAY
FROM final_stats
ORDER BY TOTAL_IMPRESSIONS_GROSS DESC;

-- UFI metrics
select 
    campaign_id,
    TO_CHAR(uniques_ots, '999,999,999,999') as uniques_ots,
    TO_CHAR(uniques_gross, '999,999,999,999') as uniques_gross,
    TO_CHAR(impressions_ots, '999,999,999,999') as impressions_ots,
    TO_CHAR(impressions_gross, '999,999,999,999') as impressions_gross,
    TO_CHAR(frequency_ots, '999,999,999,999') as frequency_ots,
    TO_CHAR(frequency_gross, '999,999,999,999') as frequency_gross
from datalake.interim_dag.campaign_metrics__ufi__campaign_total where owner_id = 1513 and campaign_id in (242331,240232,239916,238909);

-- Reach PCT by Market
select 
    adgroup_id,
    f.flightref,
    TO_CHAR(uniques_ots, '999,999,999,999') as uniques_ots,
    TO_CHAR(uniques_gross, '999,999,999,999') as uniques_gross,
    TO_CHAR(impressions_ots, '999,999,999,999') as impressions_ots,
    TO_CHAR(impressions_gross, '999,999,999,999') as impressions_gross,
    TO_CHAR(frequency_ots, '999,999,999,999') as frequency_ots,
    TO_CHAR(frequency_gross, '999,999,999,999') as frequency_gross
from datalake.interim_dag.campaign_metrics__ufi__adgroup_total ufi
left join SM_PROD_POSTGRESQL."selene"."Flight" f on ufi.adgroup_id = f.FLIGHTID
 where owner_id = 1513 and campaign_id in (242331,240232,239916,238909);

 select marketid,marketmetrics:marketpop from SM_PROD_POSTGRESQL."iris"."MarketInfo" where marketid in (30,97,194);

--------------------------------------------------------------------------------
-- PHASE 3: Growth Metrics
--------------------------------------------------------------------------------
-- Purpose: Calculate growth metrics
-- Growth metrics are calculated by comparing the current month's metrics to the previous month's metrics
-- Growth metrics are calculated by comparing the current month's metrics to the previous month's metrics

-- Monthly trend with multipliers
WITH multiplied_exposures AS (
    SELECT 
        DATE_TRUNC('MONTH', TD.CORE_DATE) AS MONTH,
        TD.DEVICE_HASH,
        COALESCE(MP.MULTIPLIER * TD.decayed_value, 1) AS MULTIPLIER
    FROM 
        DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES TD
        LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
            MP.CORE_DATE = TD.CORE_DATE
            AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
            AND MP.H3_INDEX = TD.H3I_7
    WHERE 
        TD.OWNER_ID = 1513
        AND TD.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND TD.MARKET_ID <> 88
),
device_avg_multiplier AS (
    -- Average multiplier per device per month
    SELECT
        MONTH,
        DEVICE_HASH,
        AVG(MULTIPLIER) AS AVG_MULTIPLIER
    FROM multiplied_exposures
    GROUP BY MONTH, DEVICE_HASH
),
monthly_mileage AS (
    SELECT
        DATE_TRUNC('MONTH', PT.CORE_DATE) AS MONTH,
        SUM(PT.PATH_LENGTH) * 0.000621371 AS TOTAL_MILES,
        COUNT(DISTINCT PT.ASSET_ID) AS UNIQUE_BOTS_WITH_PATHS,
        SUM(PT.END_TIME - PT.START_TIME) AS TOTAL_TIME_SECONDS
    FROM DATALAKE.BRONZE.PATHS AS PT
    WHERE PT.CUSTOMER_ID = 1513
        AND PT.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
        AND PT.MARKET_ID <> 88
    GROUP BY DATE_TRUNC('MONTH', PT.CORE_DATE)
),
monthly_stats AS (
    -- REACH by summing the averaged multiplier for all distinct devices in each month
    SELECT
        MONTH,
        SUM(AVG_MULTIPLIER) AS MONTHLY_REACH,
        COUNT(DISTINCT DEVICE_HASH) AS UNIQUE_DEVICES
    FROM device_avg_multiplier
    GROUP BY MONTH
)
SELECT
    ms.MONTH,
    TO_CHAR(ms.MONTHLY_REACH, '999,999,999,999') AS MONTHLY_REACH,
    TO_CHAR(ms.UNIQUE_DEVICES, '999,999,999') AS UNIQUE_DEVICES,
    TO_CHAR(mm.TOTAL_MILES, '999,999,999') AS TOTAL_MILES_DRIVEN
FROM monthly_stats ms
LEFT JOIN monthly_mileage mm ON ms.MONTH = mm.MONTH
ORDER BY ms.MONTH;

-- --------------------------------------------------------------------------------
-- -- MARGINAL REACH ANALYSIS: Robots vs Stationary (Billboards)
-- --------------------------------------------------------------------------------
-- -- Purpose: Quantify the incremental/marginal reach that robots provide
-- -- compared to traditional stationary OOH (billboards)
-- -- Market 194 (Los Angeles), June 2025 only

-- WITH stationary_device_set AS (
--     -- Get distinct device hashes reached by stationary (billboards)
--     SELECT DISTINCT SD.DEVICE_HASH
--     FROM DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES SD
--     WHERE SD.MARKET_ID = 194  -- LOS ANGELES
--         AND SD.CORE_DATE BETWEEN '2025-06-01' AND '2025-06-30'
-- ),
-- stationary_reach AS (
--     -- Calculate total stationary reach (baseline)
--     SELECT
--         COUNT(DISTINCT SD.DEVICE_HASH) AS TOTAL_DEVICES_STATIONARY,
--         SUM(COALESCE(MP.MULTIPLIER, 1) * SD.DECAYED_VALUE) AS TOTAL_WEIGHTED_REACH_STATIONARY
--     FROM DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES SD
--     LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
--         MP.CORE_DATE = SD.CORE_DATE
--         AND MP.TIME_BIN_1H = SD.DEVICE_TIME_BIN_HOUR
--         AND MP.H3_INDEX = SD.H3I_7
--     WHERE SD.MARKET_ID = 194
--         AND SD.CORE_DATE BETWEEN '2025-06-01' AND '2025-06-30'
-- ),
-- marginal_robot_devices AS (
--     -- Get ONLY robot devices NOT already reached by stationary
--     SELECT
--         TD.DEVICE_HASH,
--         COALESCE(MP.MULTIPLIER, 1) * TD.DECAYED_VALUE AS WEIGHTED_VALUE
--     FROM DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES TD
--     LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
--         MP.CORE_DATE = TD.CORE_DATE
--         AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
--         AND MP.H3_INDEX = TD.H3I_7
--     WHERE TD.OWNER_ID = 1513  -- Coco
--         AND TD.MARKET_ID = 194
--         AND TD.CORE_DATE BETWEEN '2025-06-01' AND '2025-06-30'
--         AND TD.DEVICE_HASH NOT IN (SELECT DEVICE_HASH FROM stationary_device_set)
-- ),
-- marginal_reach AS (
--     -- Calculate marginal reach from robots
--     SELECT
--         COUNT(DISTINCT DEVICE_HASH) AS MARGINAL_DEVICES,
--         SUM(WEIGHTED_VALUE) AS MARGINAL_WEIGHTED_REACH
--     FROM marginal_robot_devices
-- )
-- SELECT
--     'Market 194 (LOS ANGELES) - June 2025' AS ANALYSIS_PERIOD,
    
--     -- Baseline stationary reach
--     TO_CHAR(sr.TOTAL_DEVICES_STATIONARY, '999,999,999') AS STATIONARY_DEVICES,
--     TO_CHAR(sr.TOTAL_WEIGHTED_REACH_STATIONARY, '999,999,999,999') AS STATIONARY_WEIGHTED_REACH,
    
--     -- Marginal reach added by robots
--     TO_CHAR(mr.MARGINAL_DEVICES, '999,999,999') AS MARGINAL_DEVICES_FROM_ROBOTS,
--     TO_CHAR(mr.MARGINAL_WEIGHTED_REACH, '999,999,999,999') AS MARGINAL_WEIGHTED_REACH_FROM_ROBOTS,
    
--     -- Combined reach (stationary + marginal robots)
--     TO_CHAR(sr.TOTAL_DEVICES_STATIONARY + mr.MARGINAL_DEVICES, '999,999,999') AS COMBINED_TOTAL_DEVICES,
--     TO_CHAR(sr.TOTAL_WEIGHTED_REACH_STATIONARY + mr.MARGINAL_WEIGHTED_REACH, '999,999,999,999') AS COMBINED_WEIGHTED_REACH,
    
--     -- Lift percentages (how much robots increase reach)
--     ROUND((mr.MARGINAL_DEVICES::FLOAT / NULLIF(sr.TOTAL_DEVICES_STATIONARY, 0)) * 100, 2) AS DEVICE_REACH_LIFT_PCT,
--     ROUND((mr.MARGINAL_WEIGHTED_REACH / NULLIF(sr.TOTAL_WEIGHTED_REACH_STATIONARY, 0)) * 100, 2) AS WEIGHTED_REACH_LIFT_PCT,
    
--     -- Marginal reach as % of combined total
--     ROUND((mr.MARGINAL_DEVICES::FLOAT / NULLIF(sr.TOTAL_DEVICES_STATIONARY + mr.MARGINAL_DEVICES, 0)) * 100, 2) AS PCT_MARGINAL_OF_COMBINED_DEVICES,
--     ROUND((mr.MARGINAL_WEIGHTED_REACH / NULLIF(sr.TOTAL_WEIGHTED_REACH_STATIONARY + mr.MARGINAL_WEIGHTED_REACH, 0)) * 100, 2) AS PCT_MARGINAL_OF_COMBINED_WEIGHTED
    
-- FROM stationary_reach sr
-- CROSS JOIN marginal_reach mr;

--------------------------------------------------------------------------------
-- PHASE 4: Visualization Data And Exposures
--------------------------------------------------------------------------------
-- This data is perfect for FOLIUM heatmaps showing:
-- - Impression density by H3 cell
-- - Route-level performance
-- - Market comparison heat maps
-- Top performing H3 zones (resolution 7 = ~5km² areas)
-- This uses raw matched exposure data (campaign-independent)

CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.COCO_EXPOSURES AS
SELECT 
    TD.CORE_DATE,
    TD.PATH_ID,
    TD.MARKET_ID,
    TD.ASSET_ID,
    TD.DEVICE_HASH,
    TD.DEVICE_TIME,
    TD.DEVICE_TIME_BIN_HOUR,
    TD.MATCH_INFO:MATCH_DISTANCE_METER::FLOAT AS MATCH_DISTANCE_METER,
    TD.MATCH_INFO:PATH_LENGTH_METER::FLOAT AS PATH_LENGTH_METER,
    TD.H3I_7,
    TD.H3I_9,
    COALESCE(MP.MULTIPLIER, 1) * TD.DECAYED_VALUE AS MP_VALUE,
    TD.DEVICE_GEOG
FROM DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES TD
LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
    MP.CORE_DATE = TD.CORE_DATE
    AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
    AND MP.H3_INDEX = TD.H3I_7
WHERE TD.OWNER_ID = 1513
    AND TD.CORE_DATE BETWEEN '2025-01-01' AND '2025-12-31'
    AND TD.VIEWABILITY IN ('LIKELIHOOD_TO_SEE', 'OPPORTUNITY_TO_SEE');

    select count(*) from DS_SANDBOX.SUNAY.COCO_EXPOSURES;

--------------------------------------------------------------------------------
-- COCO EXPOSURES ANALYSIS
--------------------------------------------------------------------------------


-- Average number of unique bots a device sees per day
WITH device_daily_bots AS (
    SELECT
        DEVICE_HASH,
        CORE_DATE,
        COUNT(DISTINCT ASSET_ID) AS UNIQUE_BOTS_PER_DAY
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
    GROUP BY DEVICE_HASH, CORE_DATE
    HAVING COUNT(*) >= 5
)
SELECT
    TO_CHAR(AVG(UNIQUE_BOTS_PER_DAY), '999.99') AS AVG_UNIQUE_BOTS_PER_DEVICE_PER_DAY,
    TO_CHAR(MIN(UNIQUE_BOTS_PER_DAY), '999') AS MIN_BOTS_PER_DAY,
    TO_CHAR(MAX(UNIQUE_BOTS_PER_DAY), '999') AS MAX_BOTS_PER_DAY,
    TO_CHAR(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY UNIQUE_BOTS_PER_DAY), '999.99') AS MEDIAN_BOTS_PER_DAY,
    TO_CHAR(COUNT(*), '999,999,999') AS TOTAL_DEVICE_DAYS
FROM device_daily_bots;

-- Average number of unique bots a device sees per week
WITH device_weekly_bots AS (
    SELECT
        DEVICE_HASH,
        DATE_TRUNC('WEEK', CORE_DATE) AS WEEK,
        COUNT(DISTINCT ASSET_ID) AS UNIQUE_BOTS_PER_WEEK
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
    GROUP BY DEVICE_HASH, WEEK
    HAVING COUNT(*) >= 20
)
SELECT
    TO_CHAR(AVG(UNIQUE_BOTS_PER_WEEK), '999.99') AS AVG_UNIQUE_BOTS_PER_DEVICE_PER_WEEK,
    TO_CHAR(MIN(UNIQUE_BOTS_PER_WEEK), '999') AS MIN_BOTS_PER_WEEK,
    TO_CHAR(MAX(UNIQUE_BOTS_PER_WEEK), '999') AS MAX_BOTS_PER_WEEK,
    TO_CHAR(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY UNIQUE_BOTS_PER_WEEK), '999.99') AS MEDIAN_BOTS_PER_WEEK,
    TO_CHAR(COUNT(*), '999,999,999') AS TOTAL_DEVICE_WEEKS
FROM device_weekly_bots;

-- Average number of unique bots a device sees per month
WITH device_monthly_bots AS (
    SELECT
        DEVICE_HASH,
        DATE_TRUNC('MONTH', CORE_DATE) AS MONTH,
        COUNT(DISTINCT ASSET_ID) AS UNIQUE_BOTS_PER_MONTH
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
    GROUP BY DEVICE_HASH, MONTH
    HAVING COUNT(*) >= 60
)
SELECT
    TO_CHAR(AVG(UNIQUE_BOTS_PER_MONTH), '999.99') AS AVG_UNIQUE_BOTS_PER_DEVICE_PER_MONTH,
    TO_CHAR(MIN(UNIQUE_BOTS_PER_MONTH), '999') AS MIN_BOTS_PER_MONTH,
    TO_CHAR(MAX(UNIQUE_BOTS_PER_MONTH), '999') AS MAX_BOTS_PER_MONTH,
    TO_CHAR(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY UNIQUE_BOTS_PER_MONTH), '999.99') AS MEDIAN_BOTS_PER_MONTH,
    TO_CHAR(COUNT(*), '999,999,999') AS TOTAL_DEVICE_MONTHS
FROM device_monthly_bots;

-- Average time between seeing different robots
WITH device_exposure_sequence AS (
    -- Get exposures ordered by time for each device
    SELECT
        DEVICE_HASH,
        ASSET_ID,
        DEVICE_TIME,
        LAG(ASSET_ID) OVER (PARTITION BY DEVICE_HASH ORDER BY DEVICE_TIME) AS PREV_ASSET_ID,
        LAG(DEVICE_TIME) OVER (PARTITION BY DEVICE_HASH ORDER BY DEVICE_TIME) AS PREV_DEVICE_TIME
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
),
different_bot_transitions AS (
    -- Filter for cases where the robot changed between exposures
    SELECT
        DEVICE_HASH,
        ASSET_ID AS CURRENT_BOT,
        PREV_ASSET_ID AS PREVIOUS_BOT,
        DEVICE_TIME - PREV_DEVICE_TIME AS TIME_DELTA_SECONDS
    FROM device_exposure_sequence
    WHERE PREV_ASSET_ID IS NOT NULL
        AND ASSET_ID != PREV_ASSET_ID  -- Different robot
),
devices_with_multiple_bots AS (
    -- Only include devices that saw at least 2 different robots
    SELECT DISTINCT DEVICE_HASH
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
    GROUP BY DEVICE_HASH
    HAVING COUNT(DISTINCT ASSET_ID) >= 10
)
SELECT
    'TIME BETWEEN DIFFERENT ROBOT EXPOSURES' AS METRIC_CATEGORY,
    TO_CHAR(COUNT(*), '999,999,999') AS TOTAL_BOT_TRANSITIONS,
    TO_CHAR(COUNT(DISTINCT dbt.DEVICE_HASH), '999,999,999') AS DEVICES_WITH_MULTIPLE_BOTS,
    TO_CHAR(AVG(TIME_DELTA_SECONDS), '999,999,999') AS AVG_SECONDS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(AVG(TIME_DELTA_SECONDS) / 60.0, '999,999,999') AS AVG_MINUTES_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(AVG(TIME_DELTA_SECONDS) / 3600.0, '999,999.99') AS AVG_HOURS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(MEDIAN(TIME_DELTA_SECONDS), '999,999,999') AS MEDIAN_SECONDS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(MEDIAN(TIME_DELTA_SECONDS) / 60.0, '999,999.99') AS MEDIAN_MINUTES_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(MEDIAN(TIME_DELTA_SECONDS) / 3600.0, '999,999.99') AS MEDIAN_HOURS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(MIN(TIME_DELTA_SECONDS) / 60.0, '999,999.99') AS MIN_MINUTES_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(MAX(TIME_DELTA_SECONDS) / 3600.0, '999,999,999.99') AS MAX_HOURS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TIME_DELTA_SECONDS) / 3600.0, '999,999.99') AS P25_HOURS_BETWEEN_DIFFERENT_BOTS,
    TO_CHAR(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TIME_DELTA_SECONDS) / 3600.0, '999,999.99') AS P75_HOURS_BETWEEN_DIFFERENT_BOTS
FROM different_bot_transitions dbt
INNER JOIN devices_with_multiple_bots dm ON dbt.DEVICE_HASH = dm.DEVICE_HASH;

--------------------------------------------------------------------------------
-- PYTHON VISUALIZATIONS: H3 DENSITY PLOTS
--------------------------------------------------------------------------------

-- H3_9 density plot for LA Market (194)
-- Resolution 9 H3 cells (~0.1 km² area, ~350m average edge length)
SELECT
    H3I_9,
    COUNT(*) AS EXPOSURE_COUNT,
    COUNT(DISTINCT DEVICE_HASH) AS UNIQUE_DEVICES,
    COUNT(DISTINCT ASSET_ID) AS UNIQUE_BOTS,
    SUM(MP_VALUE) AS TOTAL_IMPRESSIONS,
    COUNT(DISTINCT CORE_DATE) AS DAYS_ACTIVE,
    -- Get representative lat/lon for the H3 cell (using first point as proxy)
    ST_Y(ANY_VALUE(DEVICE_GEOG)) AS LAT,
    ST_X(ANY_VALUE(DEVICE_GEOG)) AS LON,
    -- Temporal breakdown
    COUNT(DISTINCT CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(DEVICE_TIME)) BETWEEN 6 AND 11 THEN DEVICE_HASH END) AS MORNING_DEVICES,
    COUNT(DISTINCT CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(DEVICE_TIME)) BETWEEN 12 AND 17 THEN DEVICE_HASH END) AS AFTERNOON_DEVICES,
    COUNT(DISTINCT CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(DEVICE_TIME)) BETWEEN 18 AND 23 THEN DEVICE_HASH END) AS EVENING_DEVICES,
    -- Average exposure distance in this cell
    AVG(MATCH_DISTANCE_METER) AS AVG_DISTANCE_METERS,
    -- Date range for context
    MIN(CORE_DATE) AS FIRST_EXPOSURE_DATE,
    MAX(CORE_DATE) AS LAST_EXPOSURE_DATE
FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
WHERE MARKET_ID = 194  -- Los Angeles
    AND H3I_9 IS NOT NULL
GROUP BY H3I_9
HAVING EXPOSURE_COUNT >= 10  -- Filter out cells with very sparse data
ORDER BY TOTAL_IMPRESSIONS DESC;





-- Daily Impressions (Morning, Afternoon, Evening) in LA Market (194)
-- For visualizations: sum impressions (MP_VALUE) by day and daypart

SELECT
    ROUND(
        SUM(
            CASE
                THEN MP_VALUE ELSE 0
            END
        ) 
        / NULLIF(SUM(MP_VALUE), 0) * 100, 2
    ) AS MORNING_IMPRESSIONS_PCT,
    ROUND(
        SUM(
            CASE
                WHEN EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(DEVICE_TIME))) BETWEEN 11 AND 17
                THEN MP_VALUE ELSE 0
            END
        ) 
        / NULLIF(SUM(MP_VALUE), 0) * 100, 2
    ) AS AFTERNOON_IMPRESSIONS_PCT,
    ROUND(
        SUM(
            CASE
                WHEN EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(DEVICE_TIME))) BETWEEN 18 AND 24
                THEN MP_VALUE ELSE 0
            END
        )
        / NULLIF(SUM(MP_VALUE), 0) * 100, 2
    ) AS EVENING_IMPRESSIONS_PCT
FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES
WHERE MARKET_ID = 194
    AND H3I_9 IS NOT NULL;






--------------------------------------------------------------------------------
-- PHASE 5: Campaign-Specific Affinity Analysis
--------------------------------------------------------------------------------

/*
CAMPAIGN AUDIENCE RECOMMENDATIONS:

📺 NETFLIX / HBO (Entertainment Content):
   1. movie_goers - Core entertainment audience
   2. heavy_ctv_viewers - Streaming device users
   3. young_professionals - Key demographic for premium content

🛒 AMAZON (E-commerce/Retail):
   1. big_box_shoppers - Retail shopping behavior
   2. grab_and_go_purchasers - Convenience-oriented buyers
   3. fast_foodies - Quick purchase, convenience mindset
*/


--------------------------------------------------------------------------------
-- SECTION 5A: NETFLIX CAMPAIGN - AFFINITY PENETRATION & REACH
-- Campaign: 238909 | Markets: LA (194), Chicago (89), Miami (82)
--------------------------------------------------------------------------------

WITH netflix_exposures AS (
    -- Get all device exposures for Netflix campaign
    SELECT DISTINCT
        DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(SD.DEVICE_HASH) AS DEVICE_HASH_BIN,
        SD.DEVICE_HASH,
        SD.MARKET_ID,
        SD.H3I_9,
        ST_X(SD.DEVICE_GEOG) AS LON,
        ST_Y(SD.DEVICE_GEOG) AS LAT,
        SD.MP_VALUE,
        CASE 
            WHEN SD.MARKET_ID = 194 THEN 'Los Angeles'
            WHEN SD.MARKET_ID = 89 THEN 'Chicago'
            WHEN SD.MARKET_ID = 82 THEN 'Miami'
        END AS MARKET_NAME
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES SD
    WHERE SD.MARKET_ID IN (194, 89, 82)  -- LA, Chicago, Miami
        AND SD.CORE_DATE BETWEEN '2025-02-17' AND '2025-04-15'  -- Netflix campaign dates
),
affinity_reach AS (
    -- Join to device affinities
    SELECT 
        ne.MARKET_NAME,
        DA.AUDIENCE_ATTRIBUTE AS AFFINITY,
        COUNT(DISTINCT ne.DEVICE_HASH) AS UNIQUE_DEVICES,
        SUM(ne.MP_VALUE) AS TOTAL_IMPRESSIONS
    FROM netflix_exposures ne
    JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
        ne.DEVICE_HASH_BIN = DA.DEVICE_HASH_BIN
        AND ne.DEVICE_HASH = DA.DEVICE_HASH
    WHERE DA.AUDIENCE_ATTRIBUTE IN (
        'movie_goers',
        'heavy_ctv_viewers',
        'young_professionals'
    )
    GROUP BY ne.MARKET_NAME, DA.AUDIENCE_ATTRIBUTE
),
total_reach AS (
    -- Calculate total campaign reach per market
    SELECT
        MARKET_NAME,
        COUNT(DISTINCT DEVICE_HASH) AS TOTAL_DEVICES,
        SUM(MP_VALUE) AS TOTAL_IMPRESSIONS
    FROM netflix_exposures
    GROUP BY MARKET_NAME
)
SELECT
    ar.MARKET_NAME,
    ar.AFFINITY,
    TO_CHAR(ar.UNIQUE_DEVICES, '999,999,999') AS AFFINITY_DEVICES,
    TO_CHAR(ar.TOTAL_IMPRESSIONS, '999,999,999') AS AFFINITY_IMPRESSIONS,
    TO_CHAR(tr.TOTAL_DEVICES, '999,999,999') AS CAMPAIGN_TOTAL_DEVICES,
    
    -- Penetration rate: % of campaign reach with this affinity
    ROUND((ar.UNIQUE_DEVICES::FLOAT / NULLIF(tr.TOTAL_DEVICES, 0)) * 100, 2) AS PENETRATION_RATE_PCT,
    
    -- Average impressions per affinity device
    ROUND(ar.TOTAL_IMPRESSIONS / NULLIF(ar.UNIQUE_DEVICES, 0), 2) AS AVG_IMP_PER_DEVICE,
    
    -- Share of total impressions delivered to this affinity
    ROUND((ar.TOTAL_IMPRESSIONS / NULLIF(tr.TOTAL_IMPRESSIONS, 0)) * 100, 2) AS IMPRESSION_SHARE_PCT
    
FROM affinity_reach ar
JOIN total_reach tr ON ar.MARKET_NAME = tr.MARKET_NAME
ORDER BY ar.MARKET_NAME, ar.UNIQUE_DEVICES DESC;


--------------------------------------------------------------------------------
-- SECTION 5B: HBO CAMPAIGN - AFFINITY GEOGRAPHIC HEATMAP DATA
-- Campaign: 242331 | Market: LA (194) only
--------------------------------------------------------------------------------

-- PURPOSE: Generate H3_9 density data for mapping affinity audiences
-- VISUAL: Interactive folium heatmap showing where target affinities cluster

SELECT 
    DA.AUDIENCE_ATTRIBUTE AS AFFINITY,
    SD.H3I_9,
    ANY_VALUE(ST_X(SD.DEVICE_GEOG)) AS LON,
    ANY_VALUE(ST_Y(SD.DEVICE_GEOG)) AS LAT,
    COUNT(DISTINCT SD.DEVICE_HASH) AS UNIQUE_DEVICES,
    SUM(SD.MP_VALUE) AS TOTAL_IMPRESSIONS,
    ROUND(AVG(SD.MP_VALUE), 2) AS AVG_IMP_PER_EXPOSURE
FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES SD
JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
    SD.DEVICE_HASH_BIN = DA.DEVICE_HASH_BIN
    AND SD.DEVICE_HASH = DA.DEVICE_HASH
WHERE SD.MARKET_ID = 194  -- LA only
    AND SD.CORE_DATE BETWEEN '2025-10-05' AND '2025-11-08'  -- HBO campaign dates
    AND DA.AUDIENCE_ATTRIBUTE IN (
        'movie_goers',
        'heavy_ctv_viewers',
        'young_professionals'
    )
    AND SD.H3I_9 IS NOT NULL
GROUP BY DA.AUDIENCE_ATTRIBUTE, SD.H3I_9
HAVING COUNT(DISTINCT SD.DEVICE_HASH) >= 5  -- Privacy threshold
ORDER BY AFFINITY, UNIQUE_DEVICES DESC;


--------------------------------------------------------------------------------
-- SECTION 5C: AMAZON CAMPAIGN - AFFINITY CROSS-OVERLAP ANALYSIS
-- Campaign: 240232 | Market: LA (194) only
--------------------------------------------------------------------------------

-- PURPOSE: Identify devices with multiple retail-oriented affinities
-- VISUAL: Venn diagram or overlap matrix showing affinity combinations

WITH amazon_devices AS (
    SELECT DISTINCT
        SD.DEVICE_HASH_BIN,
        SD.DEVICE_HASH,
        SD.MP_VALUE
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES SD
    WHERE SD.MARKET_ID = 194
        AND SD.CORE_DATE BETWEEN '2025-07-15' AND '2025-08-18'
),
device_affinities AS (
    SELECT
        ad.DEVICE_HASH,
        MAX(CASE WHEN DA.AUDIENCE_ATTRIBUTE = 'big_box_shoppers' THEN 1 ELSE 0 END) AS HAS_BIG_BOX,
        MAX(CASE WHEN DA.AUDIENCE_ATTRIBUTE = 'grab_and_go_purchasers' THEN 1 ELSE 0 END) AS HAS_GRAB_GO,
        MAX(CASE WHEN DA.AUDIENCE_ATTRIBUTE = 'fast_foodies' THEN 1 ELSE 0 END) AS HAS_FAST_FOOD,
        SUM(ad.MP_VALUE) AS TOTAL_IMPRESSIONS
    FROM amazon_devices ad
    JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
        ad.DEVICE_HASH_BIN = DA.DEVICE_HASH_BIN
        AND ad.DEVICE_HASH = DA.DEVICE_HASH
    WHERE DA.AUDIENCE_ATTRIBUTE IN (
        'big_box_shoppers',
        'grab_and_go_purchasers',
        'fast_foodies'
    )
    GROUP BY ad.DEVICE_HASH
)
SELECT
    -- Affinity combination
    CASE 
        WHEN HAS_BIG_BOX = 1 AND HAS_GRAB_GO = 1 AND HAS_FAST_FOOD = 1 THEN 'All 3 Affinities'
        WHEN HAS_BIG_BOX = 1 AND HAS_GRAB_GO = 1 THEN 'Big Box + Grab-and-Go'
        WHEN HAS_BIG_BOX = 1 AND HAS_FAST_FOOD = 1 THEN 'Big Box + Fast Food'
        WHEN HAS_GRAB_GO = 1 AND HAS_FAST_FOOD = 1 THEN 'Grab-and-Go + Fast Food'
        WHEN HAS_BIG_BOX = 1 THEN 'Big Box Only'
        WHEN HAS_GRAB_GO = 1 THEN 'Grab-and-Go Only'
        WHEN HAS_FAST_FOOD = 1 THEN 'Fast Food Only'
    END AS AFFINITY_COMBINATION,
    
    COUNT(DISTINCT DEVICE_HASH) AS UNIQUE_DEVICES,
    TO_CHAR(COUNT(DISTINCT DEVICE_HASH), '999,999') AS DEVICES_FORMATTED,
    TO_CHAR(SUM(TOTAL_IMPRESSIONS), '999,999,999') AS TOTAL_IMPRESSIONS,
    ROUND(AVG(TOTAL_IMPRESSIONS), 2) AS AVG_IMP_PER_DEVICE,
    
    -- % of total affinity audience
    ROUND((COUNT(DISTINCT DEVICE_HASH)::FLOAT / SUM(COUNT(DISTINCT DEVICE_HASH)) OVER ()) * 100, 2) AS PCT_OF_AFFINITY_AUDIENCE
    
FROM device_affinities
GROUP BY AFFINITY_COMBINATION
ORDER BY UNIQUE_DEVICES DESC;


--------------------------------------------------------------------------------
-- SECTION 5D: TIME-OF-DAY BY AFFINITY (ALL CAMPAIGNS)
-- PURPOSE: Show when each affinity segment is most active
--------------------------------------------------------------------------------

WITH affinity_temporal AS (
    SELECT 
        DA.AUDIENCE_ATTRIBUTE AS AFFINITY,
        EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', TO_TIMESTAMP(SD.DEVICE_TIME))) AS HOUR_OF_DAY,
        SUM(SD.MP_VALUE) AS IMPRESSIONS
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES SD
    JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
        SD.DEVICE_HASH_BIN = DA.DEVICE_HASH_BIN
        AND SD.DEVICE_HASH = DA.DEVICE_HASH
    WHERE SD.MARKET_ID = 194  -- LA market
        AND DA.AUDIENCE_ATTRIBUTE IN (
            'movie_goers',
            'heavy_ctv_viewers',
            'young_professionals',
            'big_box_shoppers',
            'grab_and_go_purchasers',
            'fast_foodies'
        )
    GROUP BY DA.AUDIENCE_ATTRIBUTE, HOUR_OF_DAY
),
total_by_affinity AS (
    SELECT
        AFFINITY,
        SUM(IMPRESSIONS) AS TOTAL_IMPRESSIONS
    FROM affinity_temporal
    GROUP BY AFFINITY
)
SELECT
    at.AFFINITY,
    at.HOUR_OF_DAY,
    TO_CHAR(at.IMPRESSIONS, '999,999,999') AS IMPRESSIONS,
    ROUND((at.IMPRESSIONS / NULLIF(ta.TOTAL_IMPRESSIONS, 0)) * 100, 2) AS PCT_OF_AFFINITY_TOTAL,
    
    -- Peak hour indicator
    CASE 
        WHEN at.IMPRESSIONS = MAX(at.IMPRESSIONS) OVER (PARTITION BY at.AFFINITY) THEN '🔥 PEAK'
        ELSE ''
    END AS PEAK_FLAG
    
FROM affinity_temporal at
JOIN total_by_affinity ta ON at.AFFINITY = ta.AFFINITY
ORDER BY at.AFFINITY, at.HOUR_OF_DAY;


--------------------------------------------------------------------------------
-- SECTION 5E: AFFINITY INDEX (LIFT VS BASELINE POPULATION)
-- PURPOSE: Show over/under-indexing of target affinities vs. general population
--------------------------------------------------------------------------------

WITH campaign_affinity_rate AS (
    -- % of Coco-exposed devices with each affinity
    SELECT 
        DA.AUDIENCE_ATTRIBUTE AS AFFINITY,
        COUNT(DISTINCT SD.DEVICE_HASH) AS AFFINITY_DEVICES,
        (SELECT COUNT(DISTINCT DEVICE_HASH) FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES WHERE MARKET_ID = 194) AS TOTAL_DEVICES,
        (COUNT(DISTINCT SD.DEVICE_HASH)::FLOAT / 
         (SELECT COUNT(DISTINCT DEVICE_HASH) FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES WHERE MARKET_ID = 194)) * 100 AS AFFINITY_RATE
    FROM DS_SANDBOX.SUNAY.COCO_EXPOSURES SD
    JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
        SD.DEVICE_HASH_BIN = DA.DEVICE_HASH_BIN
        AND SD.DEVICE_HASH = DA.DEVICE_HASH
    WHERE SD.MARKET_ID = 194
        AND DA.AUDIENCE_ATTRIBUTE IN (
            'movie_goers',
            'heavy_ctv_viewers',
            'young_professionals',
            'big_box_shoppers',
            'grab_and_go_purchasers',
            'fast_foodies'
        )
    GROUP BY DA.AUDIENCE_ATTRIBUTE
),
-- NOTE: Would need baseline population rates from DEVICE_AFFINITIES for full index calculation
-- Placeholder: Assume national baseline is ~15% for illustration purposes
baseline AS (
    SELECT 15.0 AS BASELINE_RATE  -- Would calculate this from full DEVICE_AFFINITIES table
)
SELECT
    car.AFFINITY,
    TO_CHAR(car.AFFINITY_DEVICES, '999,999') AS DEVICES_REACHED,
    ROUND(car.AFFINITY_RATE, 2) AS CAMPAIGN_PENETRATION_PCT,
    
    -- Index: 100 = at parity, >100 = over-indexed, <100 = under-indexed
    ROUND((car.AFFINITY_RATE / NULLIF(b.BASELINE_RATE, 0)) * 100, 0) AS AFFINITY_INDEX,
    
    CASE
        WHEN (car.AFFINITY_RATE / NULLIF(b.BASELINE_RATE, 0)) * 100 > 120 THEN '⬆️ STRONG OVER-INDEX'
        WHEN (car.AFFINITY_RATE / NULLIF(b.BASELINE_RATE, 0)) * 100 > 100 THEN '↗️ OVER-INDEX'
        WHEN (car.AFFINITY_RATE / NULLIF(b.BASELINE_RATE, 0)) * 100 < 80 THEN '⬇️ UNDER-INDEX'
        ELSE '➡️ AT PARITY'
    END AS INDEX_INTERPRETATION
    
FROM campaign_affinity_rate car
CROSS JOIN baseline b
ORDER BY car.AFFINITY_RATE DESC;