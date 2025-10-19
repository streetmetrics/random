/*======================================================================
  PING DIFFUSION — H3-11 ONLY (K=2)
  Author: Sunay + Nate

  What this does
  1) Build a simple baseline from the clean window (no HOD/HOW blending).
  2) Precompute per-source neighbor CDFs at H3-11 using GRID_DISK(K=2).
  3) Run N hops of inverse-CDF diffusion (center excluded by default).
  4) Persist per-iteration H3-11 densities for GIFs.
  5) Write out a parity-safe “smoothed” table with jittered coords.

  Notes
  - Lossless row count: every hop preserves 1:1 rows.
  - Sampling fallback uses max(CDF) per source cell to avoid nulls.
  - Cluster keys chosen on hot group-by keys to keep hops quick.
======================================================================*/

/*===========================  KNOBS  ============================*/
set FIX_DATE       = '2025-07-18';  -- day to fix
set BASE_START     = '2024-07-01';  -- clean window start
set BASE_END       = '2024-07-30';  -- clean window end
set DMA            = 803;           -- LA
set K              = 2;             -- GRID_DISK radius (2 = good spread, still tractable)
set EXCLUDE_CENTER = TRUE;          -- TRUE: forbid “stay” each hop
set TEMP = 0.2;                   -- temperature for the diffusion  
set EPS            = 1e-6;          -- tiny mass floor
set N_ITERS        = 10;            -- number of diffusion hops
set OUT_DBSCHEMA   = 'DS_SANDBOX.SUNAY';  -- where to snapshot per-iteration maps

/* Region: your 7 H3_7s */
create or replace temporary table _region_h3_7(n number);
insert into _region_h3_7(n) values
(608718334464098303),(608718334212440063),(608718334447321087),
(608718334027890687),(608718333994336255),(608718333977559039),
(608718334011113471);

/*=====================  1) STAGE SOURCE ROWS  =====================*/
create or replace temporary table _src_jul18 as
select
  l.*,
  l.h3i_7                                                       as h3_7,
  H3_LATLNG_TO_CELL(ST_Y(l.device_geo), ST_X(l.device_geo), 11) as src_h3_11,
  /* stable unique key for pass-through joins */
  hash(l.device_hash, l.ts, l.lat, l.lon, l.h3i_7)              as row_id
from datalake.device.location l
join _region_h3_7 r on r.n = l.h3i_7
where l.core_date = $FIX_DATE
  AND date(ingestion_time) = '2025-08-27'
  and provider = 'accretive'
  and l.dma_code  = $DMA;


/*===============  2) BASELINE WEIGHTS (SIMPLE, H3-11)  ===============*/
/* Counts from the “good” window, pooled over time */
create or replace temporary table _w11 as
select
  market_id,
  h3i_7                                                       as h3_7,
  H3_LATLNG_TO_CELL(ST_Y(device_geo), ST_X(device_geo), 11)   as h3_11,
  count(*) as w11
from datalake.device.location
where core_date between $BASE_START and $BASE_END
  and dma_code = $DMA
  and h3i_7 in (select n from _region_h3_7)
group by 1,2,3;

/* Convert to probabilities inside each (market_id, h3_7) */
create or replace temporary table _p11 as
with norm as (
  select market_id, h3_7, h3_11, w11,
         w11 / nullif(sum(w11) over (partition by market_id, h3_7), 0) as p
  from _w11
)
select * from norm;

/*===========  3) PER-SOURCE NEIGHBOR CDFs @ H3-11 (K=2)  ===========*/
/* Enumerate GRID_DISK(K) neighbors for every source cell seen in baseline */
create or replace temporary table _nbr11
cluster by (market_id, h3_7, src_h3_11) as
select
  s.market_id, s.h3_7, s.h3_11 as src_h3_11,
  f.value::number as nbr_h3_11
from (select distinct market_id, h3_7, h3_11 from _w11) s,
     lateral flatten(input => H3_GRID_DISK(s.h3_11, $K)) f
where $EXCLUDE_CENTER = FALSE
   or f.value::number <> s.h3_11;

/* Weight neighbors by global p11 (with EPS), renormalize over the neighbor set,
   then build a per-source CDF for inverse-CDF sampling. */
create or replace temporary table _nbr11_cdf
cluster by (market_id, h3_7, src_h3_11) as
with base as (
  /* pull baseline probs for each neighbor; add tiny floor */
  select
    n.market_id, n.h3_7, n.src_h3_11, n.nbr_h3_11,
    greatest(coalesce(p11.p, 0) + $EPS, $EPS) as p_base
  from _nbr11 n
  left join _p11 p11
    on p11.market_id = n.market_id
   and p11.h3_7      = n.h3_7
   and p11.h3_11     = n.nbr_h3_11
),
temped as (
  /* temperature flattening ONLY */
  select
    market_id, h3_7, src_h3_11, nbr_h3_11,
    power(p_base, $TEMP) as w_temp
  from base
),
norm as (
  /* normalize within each source cell's neighbor set */
  select
    market_id, h3_7, src_h3_11, nbr_h3_11,
    w_temp / nullif(sum(w_temp) over (partition by market_id, h3_7, src_h3_11), 0) as p
  from temped
)
select
  market_id, h3_7, src_h3_11, nbr_h3_11,
  /* inverse-CDF sampling target */
  sum(p) over (
    partition by market_id, h3_7, src_h3_11
    order by nbr_h3_11
    rows between unbounded preceding and current row
  ) as cdf
from norm;
/* Per-source max(CDF) for fallback when r > all */
create or replace temporary table _nbr11_cdf_max as
select market_id, h3_7, src_h3_11, max(cdf) as max_cdf
from _nbr11_cdf
group by 1,2,3;

/*================  4) DIFFUSION LOOP (H3-11 ONLY)  ================*/
/* Seed positions */
create or replace temporary table _iter11 as
select row_id, dma_code, h3_7, src_h3_11 as cur_h3_11
from _src_jul18;

/* Snapshot table for GIF frames (H3-11 density per iteration) */
create or replace table identifier('DS_SANDBOX.SUNAY.PING_DIFFUSION_STEP_R11_K2') (
  iter integer,
  h3_11 number,
  ping_count number
);

declare
  i integer default 0;
begin
  while (i < $N_ITERS) do

    /* Random target per row */
    create or replace temporary table _r11 as
    select row_id, dma_code, h3_7, cur_h3_11, uniform(0::FLOAT, 1::FLOAT, random()) as r
    from _iter11;

    /* Target CDF per row (min cdf ≥ r), fallback to max(cdf) per source */
    create or replace temporary table _t11 as
    select rr.row_id,
           coalesce(min(c.cdf), m.max_cdf) as target_cdf
    from _r11 rr
    left join datalake.device.nbr_h3_11_cdf c
      on c.dma_code=rr.dma_code
     and c.h3_7     =rr.h3_7
     and c.src_h3_11=rr.cur_h3_11
     and c.cdf      >=rr.r
    join datalake.device.nbr_h3_11_cdf_max m
      on m.dma_code=rr.dma_code
     and m.h3_7     =rr.h3_7
     and m.src_h3_11=rr.cur_h3_11
    group by rr.row_id, m.max_cdf;

    /* Pick smallest CDF ≥ target (inverse-CDF sample) */
    create or replace temporary table _next11 as
    select rr.row_id, rr.dma_code, rr.h3_7, c.nbr_h3_11 as cur_h3_11
    from _r11 rr
    join _t11 t on t.row_id = rr.row_id
    join datalake.device.nbr_h3_11_cdf c
      on c.dma_code=rr.dma_code
     and c.h3_7     =rr.h3_7
     and c.src_h3_11=rr.cur_h3_11
     and c.cdf      >=t.target_cdf
    qualify row_number() over (partition by rr.row_id order by c.cdf, c.nbr_h3_11)=1;

    /* Swap for next hop */
    drop table if exists _iter11;
    alter table _next11 rename to _iter11;

    /* Snapshot this iteration’s H3-11 density */
    insert into identifier('DS_SANDBOX.SUNAY.PING_DIFFUSION_STEP_R11_K2')
    select :i+1 as iter, cur_h3_11 as h3_11, count(*) as ping_count
    from _iter11
    group by 1,2;

    /* next */
    i := i + 1;
  end while;
end;
select iter,count(distinct h3_11) as cells_n from DS_SANDBOX.SUNAY.PING_DIFFUSION_STEP_R11_K2 group by iter order by iter;

/*==================  5) FINAL COORDS + WRITE OUT  ==================*/
/* Jitter each final H3-11 using a random H3-12 child centroid + noise */
-- Choose a finer child deterministically per row (no seed collisions)
set CHILD_RES = 14;

create or replace temporary table _moves_final as
select
  i.row_id,
  kids[idx]::number                as dest_h3_child,
  ST_Y(H3_CELL_TO_POINT(kids[idx]::NUMBER)) as new_lat,
  ST_X(H3_CELL_TO_POINT(kids[idx]::NUMBER)) as new_lon,
  H3_CELL_TO_POINT(kids[idx]::NUMBER) as new_geo
from (
  select
    i.*,
    H3_CELL_TO_CHILDREN(i.cur_h3_11, $CHILD_RES) as kids,
    /* IMPORTANT: use RANDOM() arithmetic; don't pass a seed to UNIFORM */
    cast(floor(UNIFORM(0::FLOAT,1::FLOAT,random()) * array_size(H3_CELL_TO_CHILDREN(i.cur_h3_11, $CHILD_RES))) as int) as idx
  from _iter11 i
) i;


/* Output table mirrors source schema; cluster on hot filters */
create or replace table ds_sandbox.sunay.location_smooth_jul18 like datalake.device.location;
alter table ds_sandbox.sunay.location_smooth_jul18 cluster by (core_date, market_id, h3i_7);

/* Parity-safe insert (coalesce moved coords, recompute H3s from them) */
insert into ds_sandbox.sunay.location_smooth_jul18 (
  core_date, device_hash, id_type, ts, unix_ts,
  lat, lon, device_geo,
  accuracy, device_ip, country_code, dma_name, dma_code, pub_id,
  h3i_9, h3i_7, h3i_5,
  ingestion_time, file_path, market_id, provider
)
select
  s.core_date, s.device_hash, s.id_type, s.ts, s.unix_ts,
  coalesce(mf.new_lat, s.lat) as lat,
  coalesce(mf.new_lon, s.lon) as lon,
  coalesce(mf.new_geo, s.device_geo) as device_geo,
  s.accuracy, s.device_ip, s.country_code, s.dma_name, s.dma_code, s.pub_id,
  H3_LATLNG_TO_CELL(coalesce(mf.new_lat, s.lat), coalesce(mf.new_lon, s.lon), 9) as h3i_9,
  H3_LATLNG_TO_CELL(coalesce(mf.new_lat, s.lat), coalesce(mf.new_lon, s.lon), 7) as h3i_7,
  H3_LATLNG_TO_CELL(coalesce(mf.new_lat, s.lat), coalesce(mf.new_lon, s.lon), 5) as h3i_5,
  s.ingestion_time, s.file_path, s.market_id, s.provider
from _src_jul18 s
left join _moves_final mf using (row_id);





/* ===========================================================
   RUN DIFFUSION MODELING PROCEDURE
   =========================================================== */


-- CALL DATALAKE.DEVICE.BUILD_LOCATION_WEIGHTS_AND_CDFS(
--     BASE_START => '2024-07-01',
--     BASE_END => '2024-07-14',
--     TEMP => 0.4,
--     K => 2
--     -- DMA_LIST => ARRAY_CONSTRUCT(803),
--     -- EXCLUDE_CENTER => TRUE,
--     -- EPS => 1e-6,
--     -- TEST_MODE => FALSE
-- );

-- select call_id, call_state, call_info, DATEDIFF(second, log_time, CURRENT_TIMESTAMP()) as seconds_since_log 
-- from DATALAKE.MONITOR.PROCEDURAL_LOG order by log_time desc limit 10;

with counts as (
select call_id,count(*) as counts from datalake.device.location_weights_h3 
group by call_id
)
select 
  call_id,
  TO_CHAR(counts, '999,999,999,999,999') as counts,
  call_info:CALL_ARGUMENTS:BASE_START as BASE_START,
  call_info:CALL_ARGUMENTS:BASE_END as BASE_END,
  call_info:CALL_ARGUMENTS:TEMP as TEMP,
  call_info:CALL_ARGUMENTS:K as K,
  call_info:CALL_ARGUMENTS:DMA_LIST as DMA_LIST,
  DATE(LOG_TIME) as run_date
from counts 
left join DATALAKE.MONITOR.PROCEDURAL_LOG USING (call_id) 
where call_state = 'EXIT';

-- select dma_code,count(*) from datalake.device.nbr_h3_11_cdf group by dma_code;

-- select dma_code,count(*) from datalake.device.nbr_h3_11_cdf_max group by dma_code;





-- CALL DATALAKE.DEVICE.DIFFUSE_LOCATION_DATA(
--     START_DATE => '2025-07-15',
--     END_DATE => '2025-08-18',
--     N_ITERS => 20,
--     DMA_LIST => ARRAY_CONSTRUCT(803),
--     BASE_CALL_ID => 10733
-- );






