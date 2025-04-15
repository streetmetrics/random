

/* Base Table with Multipliers at all Resolutions */
CREATE OR REPLACE TABLE DATASCIENCE.ANALYSIS.ACCRETIVE_Multi_Base (
    CORE_DATE DATE,
    RESOLUTION NUMBER(10,0),
    H3_INDEX NUMBER,
    TIME_BIN_START NUMBER(20,0),
    MULTIPLIER NUMBER(20, 4)
) CLUSTER BY (CORE_DATE, RESOLUTION, RESOLUTION, TIME_BIN_START);

/* Rollup Table with Multipliers at Resolution 7 */
CREATE OR REPLACE TABLE DATASCIENCE.ANALYSIS.ACCRETIVE_Multiplier (
    CORE_DATE DATE,
    H3_INDEX NUMBER,
    TIME_BIN_START NUMBER(20,0),
    TIME_BIN_END NUMBER(20,0),
    TIME_BIN_1H NUMBER(20,0),
    MULTIPLIER NUMBER(20, 4)
) CLUSTER BY (CORE_DATE, TIME_BIN_START);



/* Add Market ID to Accretive Market Data Table Using H3_9_to_DMA Table */
-- ALTER TABLE DATALAKE.BRONZE.ACCRETIVE_MARKET_DATA ADD COLUMN market_id STRING;

-- UPDATE DATALAKE.BRONZE.ACCRETIVE_MARKET_DATA AS amd
-- SET amd.market_id = h3_9_dma.MARKET_ID
-- FROM DATASCIENCE.OPERATIONS.H3_9_to_DMA AS h3_9_dma
-- WHERE amd.H3I_9 = h3_9_dma.H3_INDEX;


CREATE OR REPLACE PROCEDURE DATASCIENCE.ANALYSIS.CALC_ACCRETIVE_MULTIPLIER( -- TESTING: DS_SANDBOX.SUNAY.CALC_H3_MULTIPLIER_OPTIMIZED/ PRODUCTION: DATASCIENCE.PUBLIC.CALC_H3_MULTIPLIER
    "START_DATE" DATE DEFAULT DATE_ADDDAYSTODATE(-6, CURRENT_DATE()), 
    "END_DATE" DATE DEFAULT DATE_ADDDAYSTODATE(-6, CURRENT_DATE()), 
    "MARKET_HASHES" ARRAY DEFAULT null)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
COMMENT = 'Calculates H3 Multipliers for a given date range (task default is per day, 7 days ago)'
AS
$$
DECLARE
    start_ts BIGINT := DATE_PART(EPOCH_SECOND, TO_TIMESTAMP_NTZ(:start_date));
    end_ts BIGINT := DATE_PART(EPOCH_SECOND, TO_TIMESTAMP_NTZ(:end_date) + INTERVAL '1 day') - 1;
    time_bin_24hr number(38, 2) DEFAULT 86400;
    time_bin_4hr number(38, 2) DEFAULT 14400;
    time_bin_1hr number(38, 2) DEFAULT 3600;
BEGIN

    /* Calculate the H3 Base Multipliers for a given date range */

    CREATE OR REPLACE TEMPORARY TABLE DATASCIENCE.ANALYSIS.TEMP_H3_MULTI_BASE AS
    WITH device_pins AS ( -- Pull Device level data and pre-calculate all time bins at the device level
        SELECT 
            DEVICE_ID, LAT, LON, UNIX_TS, mi.marketid as market_id, H3I_5, H3I_7,
            -- Pre-calculate all time bins at the device level
            :start_ts + :time_bin_1hr * FLOOR(DIV0((UNIX_TS - :start_ts),:time_bin_1hr)) AS H3_7_TIME_BIN,
            :start_ts + :time_bin_4hr * FLOOR(DIV0((UNIX_TS - :start_ts),:time_bin_4hr)) AS H3_5_TIME_BIN,
            :start_ts + :time_bin_24hr * FLOOR(DIV0((UNIX_TS - :start_ts),:time_bin_24hr)) AS DMA_TIME_BIN
        FROM DATALAKE.BRONZE.ACCRETIVE_MARKET_DATA as amd
        LEFT JOIN DATALAKE.OPERATIONS.AZIRA_ACCRETIVE_MARKET_REL as m_rel
            ON amd.dma_code = m_rel.dma_code
        LEFT JOIN SM_PROD_POSTGRESQL."iris"."MarketInfo" mi ON m_rel.markethash = mi.markethash
        WHERE CORE_DATE BETWEEN :start_date AND :end_date
    )
    SELECT -- Calculate the H3-7 multipliers
        7 AS resolution,
        H3I_7 AS H3_INDEX,
        H3_7_TIME_BIN AS TIME_BIN_START,
        GREATEST(IFNULL(DIV0(pop.pop, COUNT(DISTINCT DEVICE_ID)::FLOAT), 1), 1) as multiplier,
        DATE(TO_TIMESTAMP(H3_7_TIME_BIN)) as core_date
    FROM device_pins
        LEFT JOIN DATASCIENCE.PUBLIC.H3_POPULATION as pop ON H3I_7 = pop.H3_INDEX
    GROUP BY H3I_7, H3_7_TIME_BIN, pop.pop

    UNION ALL

    SELECT -- Calculate the H3-5 multipliers
        5 AS resolution,
        H3I_5 AS H3_INDEX,
        H3_5_TIME_BIN AS TIME_BIN_START,
        GREATEST(IFNULL(DIV0(pop.pop, COUNT(DISTINCT DEVICE_ID)::FLOAT), 1), 1) as multiplier,
        DATE(TO_TIMESTAMP(H3_5_TIME_BIN)) as core_date
    FROM device_pins
        LEFT JOIN DATASCIENCE.PUBLIC.H3_POPULATION as pop ON H3I_5 = pop.H3_INDEX
    GROUP BY H3I_5, H3_5_TIME_BIN, pop.pop

    UNION ALL

    SELECT -- Calculate the DMA multipliers
        100 AS resolution,
        market_id AS H3_INDEX,
        DMA_TIME_BIN AS TIME_BIN_START,
        GREATEST(IFNULL(DIV0(mi.marketmetrics:marketpop, COUNT(DISTINCT DEVICE_ID)::FLOAT), 1), 1) as multiplier,
        DATE(TO_TIMESTAMP(DMA_TIME_BIN)) as core_date
    FROM device_pins
        LEFT JOIN SM_PROD_POSTGRESQL."iris"."MarketInfo" mi ON market_id = mi.marketid
    WHERE market_id is not null
    GROUP BY market_id, DMA_TIME_BIN, mi.marketmetrics:marketpop;

    /* Insert the results into the base table */

    MERGE INTO DATASCIENCE.ANALYSIS.ACCRETIVE_Multi_Base AS target -- TESTING: DS_SANDBOX.TESTING.H3_MULTI_BASE/ PRODUCTION: DATASCIENCE.PUBLIC.H3_Multi_Base
    USING DATASCIENCE.ANALYSIS.TEMP_H3_MULTI_BASE AS source
    ON target.RESOLUTION = source.RESOLUTION
        AND target.H3_INDEX = source.H3_INDEX
        AND target.TIME_BIN_START = source.TIME_BIN_START
    WHEN MATCHED THEN
        UPDATE SET
            target.MULTIPLIER = source.MULTIPLIER,
            target.core_date = source.core_date
    WHEN NOT MATCHED THEN
        INSERT (RESOLUTION, H3_INDEX, TIME_BIN_START, MULTIPLIER, core_date)
        VALUES (source.RESOLUTION, source.H3_INDEX, source.TIME_BIN_START, source.MULTIPLIER, source.core_date);

   /* Rollup the results into H3 7 Resolution (Avg multiplier) Using Temp Table Base */

    CREATE OR REPLACE TEMPORARY TABLE DATASCIENCE.ANALYSIS.TEMP_H3_MULTI AS
    with h3_7 as ( -- Pull H3 7 Resolution and Calculate the Time Bin End
        select *, TIME_BIN_START + :time_bin_1hr - 1 as TIME_BIN_END
        from DATASCIENCE.ANALYSIS.ACCRETIVE_Multi_Base
        where resolution = 7
    ),
    h3_5 as ( -- Pull H3 5 Resolution and Calculate the Time Bin End
        select *, TIME_BIN_START + :time_bin_4hr - 1 as TIME_BIN_END
        from DATASCIENCE.ANALYSIS.ACCRETIVE_Multi_Base
        where resolution = 5
    ),
    dma as ( -- Pull DMA Resolution and Calculate the Time Bin End
        select *, TIME_BIN_START + :time_bin_24hr - 1 as TIME_BIN_END
        from DATASCIENCE.ANALYSIS.ACCRETIVE_Multi_Base
        where resolution = 100
    )
    -- Calculate the H3 7 Multiplier (Avg multiplier) 
    select h3_7.CORE_DATE, h3_7.H3_INDEX, h3_7.TIME_BIN_START, h3_7.TIME_BIN_END, 
        floor(h3_7.TIME_BIN_START / 3600) AS TIME_BIN_1H,
        LEAST(
            (
                IFF(h3_7.multiplier IS NULL, 0, h3_7.multiplier) +
                IFF(h3_5.multiplier IS NULL, 0, h3_5.multiplier) +
                IFF(dma.multiplier IS NULL, 0, dma.multiplier)
            ) /
            (
                IFF(h3_7.multiplier IS NULL, 0, 1) +
                IFF(h3_5.multiplier IS NULL, 0, 1) +
                IFF(dma.multiplier IS NULL, 0, 1)
            ), 1000) as multiplier
    from h3_7
    left join h3_5 on H3_CELL_TO_PARENT(h3_7.H3_INDEX, 5) = h3_5.H3_INDEX
        AND h3_7.TIME_BIN_START >= h3_5.TIME_BIN_START
        AND h3_7.TIME_BIN_START <= h3_5.TIME_BIN_END
    left join DATASCIENCE.OPERATIONS.H3_9_to_DMA DMA_map
        on H3_CELL_TO_CHILDREN(h3_7.H3_INDEX,9)[0] = DMA_map.H3_INDEX
    left join dma on DMA_map.MARKET_ID = dma.H3_INDEX
        AND h3_7.TIME_BIN_START >= dma.TIME_BIN_START
        AND h3_7.TIME_BIN_START <= dma.TIME_BIN_END;

    /* Insert the results into the final table */

    MERGE INTO DATASCIENCE.ANALYSIS.ACCRETIVE_Multiplier AS target -- TESTING: DS_SANDBOX.TESTING.H3_7_MULTIPLIER/ PRODUCTION: DATASCIENCE.SHARED.H3_7_Multiplier
    USING DATASCIENCE.ANALYSIS.TEMP_H3_MULTI AS source
    ON target.CORE_DATE = source.CORE_DATE
        AND target.H3_INDEX = source.H3_INDEX
        AND target.TIME_BIN_START = source.TIME_BIN_START
    WHEN MATCHED THEN
        UPDATE SET
            target.MULTIPLIER = source.multiplier,
            target.TIME_BIN_1H = source.TIME_BIN_1H,
            target.TIME_BIN_END = source.TIME_BIN_END
    WHEN NOT MATCHED THEN
        INSERT (core_date, H3_INDEX, TIME_BIN_START, TIME_BIN_1H, TIME_BIN_END, MULTIPLIER)
        VALUES (source.core_date, source.H3_INDEX, source.TIME_BIN_START, source.TIME_BIN_1H, source.TIME_BIN_END, source.multiplier);

    RETURN 'H3 Multiplier Tables Calculated for' || :start_date || ' to ' || :end_date;
END;
$$;

-- CALL DATASCIENCE.ANALYSIS.CALC_ACCRETIVE_MULTIPLIER('2025-01-01', '2025-01-14');