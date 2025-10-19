


with geo_filtered_frames as (
select 
    f.frameid,
    f.frameref,
    s.sitelon,
    s.sitelat
from sm_prod_postgresql."selene"."Frame" f
    join sm_prod_postgresql."selene"."Site" s on f.siteid = s.siteid
where 
    s.sitelon IS NOT NULL AND s.sitelat IS NOT NULL AND
    s.sitelon between -180 and 180 AND
    s.sitelat between -90 and 90 AND
    ST_DWithin(
        ST_MAKEPOINT(s.sitelon, s.sitelat),
        ST_MAKEPOINT(-90.08111, 29.95083),
        8000
    )
)
select count(*) from (
    select frame_id from datalake.bronze.stage_2__stationary_devices where core_date between '2025-01-01' and '2025-02-09'
    and frame_id in (select frameid from geo_filtered_frames)
) as t;




-- =====================================================================================
-- SUPER BOWL (Caesars Superdome) — Stationary Devices → Weighted Hourly (Temp Table)
-- Window: 4 weeks before through 2 weeks after 2025-02-09
-- Radius: 8,000 meters (Snowflake ST_DWithin uses meters)
-- Mirrors logic of:
--   - CAMPAIGN_DEVICES__EXTRACT__STATIONARY
--   - CAMPAIGN_DEVICES__WEIGHTED_HOURLY__STATIONARY
-- Output: TMP__STATIONARY_EVENT__WEIGHTED_HOURLY
-- =====================================================================================

-- USE DATABASE DATALAKE;
-- USE SCHEMA INTERIM_DAG;

CREATE OR REPLACE TEMP TABLE DS_SANDBOX.SUNAY.TMP__STATIONARY_EVENT__WEIGHTED_HOURLY AS
WITH params AS (
  SELECT
      TO_DATE('2025-02-09')                              AS event_date,
      DATEADD(week, -4, TO_DATE('2025-02-09'))           AS window_start,
      DATEADD(week,  2, TO_DATE('2025-02-09'))           AS window_end,
      -90.08111                                          AS event_lon,  -- Superdome (lon)
      29.95083                                           AS event_lat,  -- Superdome (lat)
      8000                                               AS radius_meters
),
geo_filtered_frames AS (
  SELECT 
      f.frameid      AS frame_id,
      f.frameref     AS frame_ref,
      s.sitelon      AS site_lon,
      s.sitelat      AS site_lat
  FROM sm_prod_postgresql."selene"."Frame" f
  JOIN sm_prod_postgresql."selene"."Site"  s
    ON f.siteid = s.siteid
  CROSS JOIN params p
  WHERE s.sitelon IS NOT NULL
    AND s.sitelat IS NOT NULL
    AND s.sitelon BETWEEN -180 AND 180
    AND s.sitelat BETWEEN  -90 AND  90
    AND ST_DWithin(
          ST_MAKEPOINT(s.sitelon, s.sitelat),
          ST_MAKEPOINT(p.event_lon, p.event_lat),
          p.radius_meters
        )
),
/* --------------------------------------------------------------------
   EXTRACT step (mirrors CAMPAIGN_DEVICES__EXTRACT__STATIONARY logic)
   NOTE: We’re not joining to CAMPAIGN_COMPOSITION here; we scope by geo.
--------------------------------------------------------------------- */
extract_stationary AS (
  SELECT
      td.CORE_DATE,
      /* Carry through typical campaign dims if present in Stage 2; else NULL */
      td.OWNER_ID,
      td.MARKET_ID,
      td.FRAME_ID,

      /* Hour bin summary (same derivations as procedure) */
      DATE_PART(EPOCH_SECONDS, DATE_TRUNC(HOUR, TO_TIMESTAMP(td.DEVICE_TIME))) AS SUMMARY_TIME_BIN_HOUR,

      td.DEVICE_TIME,
      td.DEVICE_TIME_BIN_HOUR,

      td.DEVICE_HASH,
      DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(td.DEVICE_HASH) AS DEVICE_HASH_BIN,

      (td.VIEWABILITY = 'LIKELY_TO_SEE')                                         AS IS_LTS,
      (td.VIEWABILITY IN ('LIKELY_TO_SEE', 'OPPORTUNITY_TO_SEE'))                AS IS_OTS,

      td.DECAYED_VALUE,
      1.0                                                                        AS IMPACT_SCORE,

      td.H3I_7,
      td.H3I_9
  FROM DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES td
  JOIN geo_filtered_frames gf
    ON td.frame_id = gf.frame_id
  CROSS JOIN params p
  WHERE td.CORE_DATE BETWEEN p.window_start AND p.window_end
),
/* --------------------------------------------------------------------
   WEIGHTED HOURLY step (mirrors CAMPAIGN_DEVICES__WEIGHTED_HOURLY)
   Join multiplier by CORE_DATE, DEVICE_TIME_BIN_HOUR, H3I_7
--------------------------------------------------------------------- */
weighted_hourly AS (
  SELECT
      td.CORE_DATE,

      td.OWNER_ID,
      td.MARKET_ID,
      td.FRAME_ID,

      td.SUMMARY_TIME_BIN_HOUR,

      td.DEVICE_HASH,
      td.DEVICE_HASH_BIN,

      /* Device-level hourly frequencies */
      SUM(IFF(td.IS_LTS, 1, 0))::FLOAT AS DEVICE_FREQUENCY_LTS,
      SUM(IFF(td.IS_OTS, 1, 0))::FLOAT AS DEVICE_FREQUENCY_OTS,
      SUM(1)::FLOAT                    AS DEVICE_FREQUENCY_GROSS,

      /* Weighted by decayed value * multiplier * impact score (1.0) */
      SUM(IFF(td.IS_LTS, td.DECAYED_VALUE * COALESCE(mp.MULTIPLIER, 1) * td.IMPACT_SCORE, 0))::FLOAT AS CUMULATIVE_WEIGHT_LTS,
      SUM(IFF(td.IS_OTS, td.DECAYED_VALUE * COALESCE(mp.MULTIPLIER, 1) * td.IMPACT_SCORE, 0))::FLOAT AS CUMULATIVE_WEIGHT_OTS,
      SUM(       td.DECAYED_VALUE * COALESCE(mp.MULTIPLIER, 1) * td.IMPACT_SCORE)::FLOAT             AS CUMULATIVE_WEIGHT_GROSS

  FROM extract_stationary td
  LEFT JOIN DATALAKE.BRONZE.MULTIPLIER mp
    ON mp.CORE_DATE  = td.CORE_DATE
   AND mp.TIME_BIN_1H = td.DEVICE_TIME_BIN_HOUR
   AND mp.H3_INDEX    = td.H3I_7
  GROUP BY
      td.CORE_DATE,
      td.OWNER_ID, td.MARKET_ID, td.FRAME_ID,
      td.SUMMARY_TIME_BIN_HOUR,
      td.DEVICE_HASH, td.DEVICE_HASH_BIN
),
/* --------------------------------------------------------------------
   Roll up to HOURLY (per-frame) and add RELATIVE_DAY (± to event date)
--------------------------------------------------------------------- */
hourly_per_frame AS (
  SELECT
      wh.CORE_DATE,
      /* ± days from event date (negative=pre, zero=event day, positive=post) */
      DATEDIFF('day', (SELECT event_date FROM params), wh.CORE_DATE) AS RELATIVE_DAY,

      wh.FRAME_ID,
      wh.MARKET_ID,
      wh.OWNER_ID,

      wh.SUMMARY_TIME_BIN_HOUR,

      /* Aggregate device-hash rows to hourly per-frame */
      SUM(wh.DEVICE_FREQUENCY_LTS)  AS DEVICE_FREQ_LTS,
      SUM(wh.DEVICE_FREQUENCY_OTS)  AS DEVICE_FREQ_OTS,
      SUM(wh.DEVICE_FREQUENCY_GROSS) AS DEVICE_FREQ_GROSS,

      SUM(wh.CUMULATIVE_WEIGHT_LTS)  AS WEIGHT_LTS,
      SUM(wh.CUMULATIVE_WEIGHT_OTS)  AS WEIGHT_OTS,
      SUM(wh.CUMULATIVE_WEIGHT_GROSS) AS WEIGHT_GROSS
  FROM weighted_hourly wh
  GROUP BY
      wh.CORE_DATE, RELATIVE_DAY,
      wh.FRAME_ID, wh.MARKET_ID, wh.OWNER_ID,
      wh.SUMMARY_TIME_BIN_HOUR
)
SELECT * FROM hourly_per_frame
ORDER BY CORE_DATE, FRAME_ID, SUMMARY_TIME_BIN_HOUR;






