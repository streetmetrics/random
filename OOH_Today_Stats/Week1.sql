



with top_market_exposures as (
    select device_hash, 'frame' as matchable_type, frame_id as matchable_id, market_id, core_date, viewability from DATALAKE.BRONZE.STAGE_2__STATIONARY_DEVICES
    where core_date between '2025-06-01' and '2025-06-30'
    and market_id in (26,97,91,150,30)

    UNION ALL

    select device_hash, 'asset' as matchable_type, asset_id as matchable_id, market_id, core_date, viewability from DATALAKE.BRONZE.STAGE_2__TRANSIT_DEVICES
    where core_date between '2025-06-01' and '2025-06-30'
    and market_id in (26,97,91,150,30)
),
affinities_join as (
select
    market_id,
    matchable_id,
    matchable_type,
    count(*) as total_exposures,
    count(case when aff.device_hash is not null then 1 end) as total_affinities
from top_market_exposures exp
    left join (
            select device_hash_bin, device_hash
            from DATALAKE.DEVICE.AFFINITIES
            WHERE audience_attribute = 'black_friday_shoppers'
    ) aff on aff.device_hash_bin = DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(exp.DEVICE_HASH)
        AND aff.device_hash = exp.DEVICE_HASH
group by market_id, matchable_id, matchable_type
),
inventory_join as (
    select
        aff.market_id,
        aff.matchable_id,
        case 
            when aff.matchable_type = 'frame' then f.inventorytype
            when aff.matchable_type = 'asset' then 'Transit'
            else 'Transit'
        end as inventory_type,
        case 
            when aff.matchable_type = 'frame' then f.inventorysubtype
            when aff.matchable_type = 'asset' then a.assettype
            else 'Unknown'
        end as inventory_subtype,
        DIV0(aff.total_affinities, aff.total_exposures) as pct_affinities
    from affinities_join aff
    left join SM_PROD_POSTGRESQL."selene"."Frame" f
        on aff.matchable_id = f.frameid
        and aff.matchable_type = 'frame'
    left join SM_PROD_POSTGRESQL."selene"."Asset" a
        on aff.matchable_id = a.assetid
        and aff.matchable_type = 'asset'
),
market_avg as (
    select 
        market_id,
        DIV0(count(case when aff.device_hash is not null then 1 end), count(distinct l.device_hash)) as pct_affinities
    from DATALAKE.DEVICE.LOCATION l
        left join (
            select device_hash_bin, device_hash
            from DATALAKE.DEVICE.AFFINITIES
            WHERE audience_attribute = 'black_friday_shoppers'
        ) aff
            on aff.device_hash_bin = DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(l.device_hash)
            and aff.device_hash = l.device_hash
    where core_date between '2025-06-01' and '2025-06-30'
    AND dma_code in (602,623,524,753,528)
    group by market_id
),
national_avg as (
    select 
        DIV0(count(case when aff.device_hash is not null then 1 end), count(distinct l.device_hash)) as pct_affinities
    from DATALAKE.DEVICE.LOCATION l
        left join (
            select device_hash_bin, device_hash
            from DATALAKE.DEVICE.AFFINITIES
            WHERE audience_attribute = 'black_friday_shoppers'
        ) aff
            on aff.device_hash_bin = DATALAKE.OPERATIONS.DEVICE_HASH_TO_BIN(l.device_hash)
            and aff.device_hash = l.device_hash
    where core_date between '2025-06-01' and '2025-06-30'
    AND dma_code in (602,623,524,753,528)
)
select 
    market_rel.marketname,
    i.inventory_type,
    round((avg(i.pct_affinities) * 100), 2) as avg_pct_affinities_inventory,
    DIV0(round((avg(i.pct_affinities) * 100), 2), ma.pct_affinities) as market_index,
    DIV0(round((avg(i.pct_affinities) * 100), 2), nat.pct_affinities) as national_index,
    count(distinct i.matchable_id) as total_inventory_items
from inventory_join i
    left join DATALAKE.OPERATIONS.AZIRA_ACCRETIVE_MARKET_REL market_rel
        on i.market_id = market_rel.market_id
    left join market_avg ma on i.market_id = ma.market_id
    left join national_avg nat on 1=1
group by 
    market_rel.marketname,
    i.inventory_type,
    ma.pct_affinities,
    nat.pct_affinities;


