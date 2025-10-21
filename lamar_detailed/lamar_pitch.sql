select * from DOMAIN_STAGE.PLANNING.INVENTORY where date_range = 'Q1_25' and inventory_ref in ('1185858','1178794');

select TO_CHAR(count(*),'999,999,999,999') from datalake.bronze.stage_2__stationary_devices where frame_id in (2302791,2295900) AND core_date between '2025-06-01' and '2025-06-30';


/* Exposire Data */
CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES AS
SELECT 
    TD.*,
    DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(TD.DEVICE_HASH) AS DEVICE_HASH_BIN,
    COALESCE(MP.MULTIPLIER, 1) * TD.DECAYED_VALUE AS MP_VALUE
FROM 
    DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES TD
    LEFT JOIN DATALAKE.BRONZE.MULTIPLIER AS MP ON
        MP.CORE_DATE = TD.CORE_DATE
        AND MP.TIME_BIN_1H = TD.DEVICE_TIME_BIN_HOUR
        AND MP.H3_INDEX = TD.H3I_7
WHERE 
    TD.FRAME_ID IN (2302791, 2295900) 
    AND TD.CORE_DATE BETWEEN '2025-06-01' AND '2025-06-30';


/* Affinity Data */
select 
    ST_X(device_geog) as lon,
    ST_Y(device_geog) as lat,
    viewability,
    DA.AUDIENCE_ATTRIBUTE AS AFFINITY_ATTRIBUTE
 from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES exp
    JOIN DATALAKE.BRONZE.DEVICE_AFFINITIES DA ON
        exp.device_hash_bin = DA.DEVICE_HASH_BIN
        AND exp.device_hash = DA.DEVICE_HASH
    WHERE viewability <> 'DECAYED'
    and MATCH_INFO:MATCH_ANGLE_DEGREE < 65 AND MATCH_INFO:MATCH_ANGLE_DEGREE > -65
    and frame_id = 2295900;

/* Evening Location Data */
select 
    ST_X(device_geog) as lon,
    ST_Y(device_geog) as lat,
    viewability,
    CL.CDL_CITY,
    CL.CEL_CITY,
    CL.CEL_ZIP,
    CL.CDL_ZIP,
 from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES exp
    JOIN DATALAKE.BRONZE.DEVICE_COMMON_LOCATIONS CL ON
        exp.device_hash_bin = CL.DEVICE_HASH_BIN
        AND exp.device_hash = CL.DEVICE_HASH
    WHERE viewability <> 'DECAYED'
    and MATCH_INFO:MATCH_ANGLE_DEGREE < 65 AND MATCH_INFO:MATCH_ANGLE_DEGREE > -65
    and frame_id = 2295900;



/* POI Data */


CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.HIGH_ACTIVITY_DEVICES_SPOKANE AS
select 
    device_hash, 
    avg(ping_count) as avg_daily_pings
from (
    select 
        device_hash, 
        core_date, 
        count(*) as ping_count
    from DATALAKE.DEVICE.LOCATION
    where core_date between '2025-06-01' and '2025-06-30'
        AND DMA_CODE = 881
        AND device_hash in (select distinct device_hash from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES)
    group by device_hash, core_date
) t
group by device_hash
having avg(ping_count) > 10;

select count(*) from DS_SANDBOX.SUNAY.HIGH_ACTIVITY_DEVICES_SPOKANE;


CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.LAMAR_ORLANDO_POI_DATA AS
with sampled_devices as (
    select device_hash
    from (
        select distinct device_hash 
        from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES
    ) 
    qualify row_number() over (order by uniform(0,1,random())) <= 50
),
device_pings as (
    select 
        l.device_hash,
        ST_Y(l.device_geo) as device_lat,
        ST_X(l.device_geo) as device_lon,
        l.unix_ts as event_timestamp
    from DATALAKE.DEVICE.LOCATION l
    inner join sampled_devices d
        on l.device_hash = d.device_hash
    where l.core_date between '2025-06-01' and '2025-06-30'
        and l.dma_code = 534
    -- Optional: filter by possible relevant date range if needed
),
fl_orlando_pois as (
    select 
        fsq_place_id as poi_id,
        name as poi_name,
        latitude as poi_lat,
        longitude as poi_lon,
    from FSQ_OPEN_SOURCE_PLACES.FSQ_OS.PLACES
    where country = 'US'
      and region = 'FL'
      and locality = 'Orlando'
),
dev_poi_near as (
    select 
        d.device_hash,
        d.event_timestamp,
        f.poi_id,
        f.poi_name,
        d.device_lat,
        d.device_lon,
        f.poi_lat,
        f.poi_lon,
        ST_DISTANCE(ST_MAKEPOINT(d.device_lon, d.device_lat), ST_MAKEPOINT(f.poi_lon, f.poi_lat))::int as distance_meters
    from device_pings d
    join fl_orlando_pois f
      on 
        ST_DISTANCE(ST_MAKEPOINT(d.device_lon, d.device_lat), ST_MAKEPOINT(f.poi_lon, f.poi_lat)) <= 15
)
select 
    device_hash, 
    poi_id, 
    poi_name,
    avg(distance_meters) as avg_distance_meters,
    count(*) as num_nearby_pings
from dev_poi_near
group by device_hash, poi_id, poi_name
having num_nearby_pings >= 5
order by avg_distance_meters asc;

select * from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_POI_DATA;




with target_pois as (
    select fsq_place_id as poi_id, name as poi_name, latitude as poi_lat, longitude as poi_lon from FSQ_OPEN_SOURCE_PLACES.FSQ_OS.PLACES
    where 
    country = 'US'
      and region = 'FL'
      and locality = 'Orlando'
      and
    poi_id in (
        '5be8a9ade55d8b002cd1ba81', 
        '4c0bfaa4bbc676b0365e4cd5',
        '51a25df4498e6d1b8fdc6398',
        '4b840368f964a520331b31e3',
        '33a65ba16cfb414568be3e32'
    )
)
select * from target_pois;
exposed_near_poi as (
    select distinct
        l.device_hash,
        p.poi_name
    from DATALAKE.DEVICE.LOCATION l
    inner join DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES e
        on l.device_hash = e.device_hash
    inner join DS_SANDBOX.SUNAY.HIGH_ACTIVITY_DEVICES ha
        on l.device_hash = ha.device_hash
    inner join target_pois p
        on ST_DISTANCE(ST_MAKEPOINT(ST_X(l.device_geo), ST_Y(l.device_geo)), ST_MAKEPOINT(p.poi_lon, p.poi_lat)) <= 15
    where l.core_date between '2025-06-01' and '2025-06-30'
        and l.dma_code = 534
)
select
    l.device_hash,
    l.device_geo,
    l.unix_ts,
    e.poi_name
from DATALAKE.DEVICE.LOCATION l
inner join exposed_near_poi e
    on l.device_hash = e.device_hash
where l.core_date between '2025-06-01' and '2025-06-30'
    and l.dma_code = 534;

select * from datalake.operations.azira_accretive_market_rel where market_id = 195;

/* Boot Barns Data */

CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS AS
with poi as (
    select 'Boot Barn' as poi_name, 
    47.7109417 as poi_lat,
     -117.4091260 as poi_lon
),
poi_devices as (
    select
        l.device_hash,
        ST_Y(l.device_geo) as device_lat,
        ST_X(l.device_geo) as device_lon,
        unix_ts as device_ts,
        ST_DISTANCE(
            ST_MAKEPOINT(ST_X(l.device_geo), ST_Y(l.device_geo)),
            ST_MAKEPOINT((select poi_lon from poi), (select poi_lat from poi))
        ) as distance_meters
    from DATALAKE.DEVICE.LOCATION l
    where core_date between '2025-06-01' and '2025-06-30'
        and dma_code = 881
        and ST_DISTANCE(
            ST_MAKEPOINT(ST_X(l.device_geo), ST_Y(l.device_geo)),
            ST_MAKEPOINT((select poi_lon from poi), (select poi_lat from poi))
        ) <= 50
)
select * from poi_devices;



CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN AS
with poi as (
    select 'Boot Barn' as poi_name, 
    47.7109417 as poi_lat,
     -117.4091260 as poi_lon
),
billboards as (
    select any_value(geopath_id) as geopath_id, any_value(inventory_id) as frame_id, lat, lon
    from domain_stage.planning.inventory
    where date_range = 'Q1_25'
        and owner_id = 14
        and market_id = 195
        and inventory_type = 'billboards'
        and ST_DISTANCE(
                ST_MAKEPOINT(lon, lat),
                ST_MAKEPOINT((select poi_lon from poi), (select poi_lat from poi))
            ) >= 1609.34 -- 1 mile in meters
        and ST_DISTANCE(
                ST_MAKEPOINT(lon, lat),
                ST_MAKEPOINT((select poi_lon from poi), (select poi_lat from poi))
            ) < 4828.02  -- less than 3 miles in meters
        and geopath_id is not null
        group by lat, lon
),
top_10_billboards as (  
    select 
        ed.frame_id,
        bb.geopath_id,
        bb.lat,
        bb.lon,
        count(distinct ed.device_hash) as distinct_converted_devices
    from 
        DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES ed
    inner join 
        billboards bb
        on ed.frame_id = bb.frame_id
    inner join 
        DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS cd
        on ed.device_hash = cd.device_hash
    where 
        ed.core_date between '2025-06-01' and '2025-06-30'
        and ed.market_id = 195
        and ed.owner_id = 14
    group by 
        ed.frame_id,
        bb.geopath_id,
        bb.lat,
        bb.lon
    order by 
        distinct_converted_devices desc
    limit 10
)
select 
    ed.frame_id,
    tb.geopath_id,
    tb.lat,
    tb.lon,
    ed.device_hash,
    ed.device_time as exposed_ts,
    ed.viewability,
    ed.match_info,
    ed.h3i_7,
    ed.h3i_9
from 
    DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES ed
inner join top_10_billboards tb
    on ed.frame_id = tb.frame_id
where ed.core_date between '2025-06-01' and '2025-06-30'
    and ed.market_id = 195
    and ed.owner_id = 14;



CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.CONTROL_DEVICES_NEAR_BOOT_BARN AS
with 
control_candidates as (
    select distinct l.device_hash, l.h3i_7
    from DATALAKE.DEVICE.LOCATION l
    where l.core_date between '2025-06-01' and '2025-06-30'
        and l.dma_code = 881
        and l.h3i_9 in (select h3i_9 from DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN)
        and l.device_hash not in (select device_hash from DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN)
),
sampled_control_devices as (
    select device_hash
    from control_candidates
    qualify row_number() over (order by device_hash) <= (select count( distinct device_hash) from DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN)
)
select
    l.device_hash,
    ST_Y(l.device_geo) as lat,
    ST_X(l.device_geo) as lon,
    l.unix_ts,
    to_time(to_timestamp(l.unix_ts)) as time_only,
    to_timestamp(l.unix_ts) as full_timestamp,
    l.h3i_7
from DATALAKE.DEVICE.LOCATION l
inner join sampled_control_devices scd
    on l.device_hash = scd.device_hash
where l.core_date between '2025-06-01' and '2025-06-30'
    and l.dma_code = 881;


-- Control conversion rate
select 
count(distinct scd.device_hash)/10 as total_control_devices,
count(distinct bc.device_hash) as total_conversion_devices,
(total_conversion_devices / total_control_devices)*100 as conversion_rate
from DS_SANDBOX.SUNAY.CONTROL_DEVICES_NEAR_BOOT_BARN scd
left join DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS bc
    on scd.device_hash = bc.device_hash;


-- Exposed conversion rate
select 
count(distinct ed.device_hash)/10 as total_exposed_devices,
count(distinct bc.device_hash) as total_conversion_devices,
(total_conversion_devices / total_exposed_devices)*100 as conversion_rate
from DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN ed
left join DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS bc;


-- Exposed vConverted
CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.EXPOSED_VS_CONVERTED_BOOT_BARN AS
with base as (
select
    ed.device_hash,
    any_value(ed.match_info) as match_info,
    any_value(ed.viewability) as viewability,
    min_by(ed.exposed_ts, ed.exposed_ts) as exposed_ts,
    max_by(bc.device_ts, bc.device_ts) as converted_ts
from DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN ed
inner join DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS bc
    on ed.device_hash = bc.device_hash
group by ed.device_hash)
select
    device_hash,
    match_info:MATCH_ANGLE_DEGREE as match_angle_degree,
    match_info:MATCH_DISTANCE_METER as match_distance_meter,
    viewability,
    exposed_ts,
    converted_ts

from base;







-- with sampled_devices as (
--     select l.device_hash
--     from (
--         select distinct l.device_hash
--         from DATALAKE.DEVICE.LOCATION l
--         inner join DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS bbc
--             on l.device_hash = bbc.device_hash
--         inner join DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN ed
--             on l.device_hash = ed.device_hash
--         where l.core_date between '2025-06-01' and '2025-06-30'
--             and l.dma_code = 881
--     ) as l
--     sample (10 rows)
-- )
-- select
--     l.device_hash,
--     ST_Y(l.device_geo) as lat,
--     ST_X(l.device_geo) as lon,
--     l.unix_ts,
--     -- Extract time components (hour:minute:second) from timestamp
--     to_time(to_timestamp(l.unix_ts)) as time_only,
--     -- Keep original timestamp for sorting
--     to_timestamp(l.unix_ts) as full_timestamp
-- from DATALAKE.DEVICE.LOCATION l
-- inner join sampled_devices sd
--     on l.device_hash = sd.device_hash
-- inner join DS_SANDBOX.SUNAY.BOOT_BARN_CONVERSIONS bbc
--     on l.device_hash = bbc.device_hash
-- inner join DS_SANDBOX.SUNAY.EXPOSED_DEVICES_NEAR_BOOT_BARN ed
--     on l.device_hash = ed.device_hash
-- where l.core_date between '2025-06-01' and '2025-06-30'
--     and l.dma_code = 881;




      CALL TEST_APPLICATION_LAYER.PLANNING.FILTER_INVENTORY(
        OWNER_ID => 14,
    CORE_DATE_RANGE => 'Q1_25',
    BENCHMARK => 'NATIONAL',
    METRIC_TYPE => 'VIEWABLE',
    DURATION => 1,
    MARKET_IDS => ARRAY_CONSTRUCT(195),
    POSTCODES => ARRAY_CONSTRUCT(99208),
    INVENTORY_FILTERS => ARRAY_CONSTRUCT('billboards.bulletins', 'billboards.posters', 'billboards.juniorPosters', 'billboards.spectaculars', 'billboards.wallscapes'),
    AFFINITY_FILTERS => NULL,
    DEMOGRAPHIC_FILTERS => NULL,
    UFI_FILTERS => NULL,
    SEARCH_TEXT => ' Find me inventory in spokane washington billboards only, near the boot barn at 5640 N Division St, Spokane, WA 99208'
      );



      select inv.inventory_id, inv.ufi_data:"4" as ufi_1week,inv.affinity_data:MARKET as affinity_market_data
      from domain_stage.planning.inventory inv
      where inv.date_range = 'Q1_25' 
        and inv.owner_id = 14
        and inv.market_id = 195
        and inv.postcode = 99208
        and inv.inventory_type = 'billboards';


        select * from domain_stage.planning.inventory where date_range = 'Q1_25' and inventory_id in (1541423, 1541854, 1534647, 1541445, 1541453, 1541460, 1541452, 1530639, 1541466, 1541457);


        -- Comma separated list of INVENTORY_REF as requested:
        -- 21568,14358,21987,10067,21581,21560,21572,21567,21575,21538