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


CREATE OR REPLACE TABLE DS_SANDBOX.SUNAY.HIGH_ACTIVITY_DEVICES AS
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
        AND DMA_CODE = 534
        AND device_hash in (select distinct device_hash from DS_SANDBOX.SUNAY.LAMAR_ORLANDO_EXPOSURES)
    group by device_hash, core_date
) t
group by device_hash
having avg(ping_count) > 500;


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

