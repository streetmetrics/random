
CALL DATASCIENCE.ANALYSIS.ACCRETIVE_MATCHING__STAGE_1__STATIONARY(
    START_DATE => '2025-01-02', 
    END_DATE => '2025-01-04',
    TASK_TYPE => 'TESTING', 
    TASK_PROXY => 'TESTING'
);


    select core_date, count(*) from DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_1__STATIONARY_DEVICES group by 1 order by 1 desc;


CALL DATASCIENCE.ANALYSIS.ACCRETIVE_MATCHING__STAGE_2__STATIONARY(
    START_DATE => '2025-01-02', 
    END_DATE => '2025-01-04'
);

select core_date, count(*) from DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES group by 1 order by 1 desc;



CREATE TABLE IF NOT EXISTS DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_1__STATIONARY_DEVICES CLUSTER BY (CORE_DATE, MARKET_ID, FRAME_ID, H3I_7, H3I_9) (
    CORE_DATE DATE,
    OWNER_ID INT,
    SITE_ID INT,
    FRAME_ID INT,
    FRAME_TYPE STRING,
    DEVICE_HASH STRING,
    DEVICE_TIME BIGINT,
    DEVICE_IP STRING,
    DEVICE_OS STRING,
    MATCH_DISTANCE_METER FLOAT,
    MATCH_ANGLE_DEGREE FLOAT,
    ABSOLUTE_ANGLE_DEGREE FLOAT,
    MATCH_INFO OBJECT,
    DEVICE_GEOG GEOGRAPHY,
    MARKET_ID INT,
    H3I_7 INT,
    H3I_9 INT,
    CALL_ID BIGINT,
    PRIMARY KEY (CORE_DATE, CALL_ID, FRAME_ID,H3I_7,H3I_9) -- Added primary key constraint for better data integrity
);


/*  Stationary Devices Stage 2 Table (Creative + Ad Plays) */
CREATE TABLE IF NOT EXISTS DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES CLUSTER BY (CORE_DATE, DEVICE_TIME_BIN_HOUR, MARKET_ID, CREATIVE_ID, FRAME_ID, H3I_7, H3I_9) (
    CORE_DATE DATE,
    OWNER_ID INT,
    SITE_ID INT,
    FRAME_ID INT,
    FRAME_TYPE STRING,
    CREATIVE_ID INT,
    AD_START_EPOCH BIGINT,
    AD_END_EPOCH BIGINT,
    DEVICE_HASH STRING,
    DEVICE_TIME BIGINT,
    DEVICE_TIME_BIN_HOUR INT,
    VIEWABILITY VARCHAR(16777216),
    DECAYED_VALUE FLOAT DEFAULT 1,
    DEVICE_IP STRING,
    DEVICE_OS STRING,
    MATCH_DISTANCE_METER FLOAT,
    MATCH_ANGLE_DEGREE FLOAT,
    ABSOLUTE_ANGLE_DEGREE FLOAT,
    DEVICE_GEOG GEOGRAPHY,
    MATCH_INFO OBJECT,
    MARKET_ID INT,
    H3I_7 INT,
    H3I_9 INT,
    CALL_ID BIGINT,
    PRIMARY KEY (CORE_DATE, CALL_ID, FRAME_ID, H3I_7, H3I_9) -- Added primary key constraint for better data integrity
);



--------------------------------------------
/*
    ************************** 
    *       Procedures       *
    ************************** 
*/

/* ****************** */
/*  Stage 1 Procedure */
/* ****************** */

CREATE OR REPLACE PROCEDURE DATASCIENCE.ANALYSIS.ACCRETIVE_MATCHING__STAGE_1__STATIONARY(
    START_DATE DATE,
    END_DATE DATE,
    CUSTOMER_IDS ARRAY DEFAULT NULL,
    MARKET_IDS ARRAY DEFAULT NULL,
    FRAME_IDS ARRAY DEFAULT NULL,
    MIN_EXPOSURE_ANGLE INT DEFAULT 180,
    FRAME_RADIUS_MULTIPLIER INT DEFAULT 3,
    TASK_TYPE STRING DEFAULT 'REGULAR',
    TASK_PROXY STRING DEFAULT 'DAILY_SCHEDULED'
)
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$    
    BEGIN

        /*  Step 1: Site Frame Proximity */

        -- Create the Site Table
        CREATE OR REPLACE TEMP TABLE SITE_FRAME_PROXIMITY AS (
            SELECT
                CSFR.CUSTOMER_ID AS OWNER_ID,
                CSFR.SITE_ID,
                CSFR.FRAME_ID,
                CSFR.FRAME_EXPOSITION_ANGLE,
                CSFR.FRAME_EXPOSITION_WIDTH,
                CSFR.FRAME_ELEVATION,
                CSFR.FRAME_HEIGHT,
                CSFR.FRAME_WIDTH,
                CSFR.MARKET_ID,
                CSFR.FRAME_TYPE,
                GREATEST(
                    DATALAKE.OPERATIONS.MATCH_STATIONARY__CALCULATE_MAX_RADIUS(
                    CSFR.FRAME_WIDTH,
                    CSFR.FRAME_HEIGHT,
                    'EXPOSED',
                    PM.DIMENSION_UNIT,
                    :FRAME_RADIUS_MULTIPLIER, -- TODO: Modify to PM.EXPOSED_VIEWSHED_SCALE, System Default 3x for now
                    0.0,
                    PM.MIN_NOTICING_RADIAL
                    ),
                    150 -- For Places, ideally remove this
                ) AS FRAME_MATCH_RADIUS,
                H3_POINT_TO_CELL(ST_MAKEPOINT(CSFR.SITE_LON, CSFR.SITE_LAT), 9) AS H3_BIN,
                OPERATIONS.GET_BIN__NUMERIC_ID(CSFR.SITE_ID, 100) AS SITE_BIN,
                ST_MAKEPOINT(CSFR.SITE_LON, CSFR.SITE_LAT) AS SITE_GEOG
            FROM DATALAKE.SILVER.STATIONARY__CUSTOMER_SITE_FRAME_REL_VIEW CSFR
                LEFT JOIN DATALAKE.BRONZE.STATIONARY__PROCESS_META AS PM USING (PROCESS_ID)
            WHERE
                (:CUSTOMER_IDS IS NULL OR CSFR.CUSTOMER_ID IN (SELECT VALUE FROM TABLE(FLATTEN(:CUSTOMER_IDS))))
                AND
                (:MARKET_IDS IS NULL OR CSFR.MARKET_ID IN (SELECT VALUE FROM TABLE(FLATTEN(:MARKET_IDS))))
                AND
                (:FRAME_IDS IS NULL OR CSFR.FRAME_ID IN (SELECT VALUE FROM TABLE(FLATTEN(:FRAME_IDS))))
        );
    
        /*  Step 2: H3 Cell Site Coverage */
        -- Create the Site H3 Table
        CREATE OR REPLACE TEMP TABLE SITE_H3 AS (
            SELECT
                SITE_FRAME_PROXIMITY.*,
                H3_C.H3I_9
            FROM
                SITE_FRAME_PROXIMITY
                JOIN ( -- Dynamic Site H3 Coverage ToDO: Move a Dynamic Table
                    WITH MAX_SITE_RADIUS AS (
                        SELECT 
                            SITE_ID,
                            MAX(
                                GREATEST(
                                    DATALAKE.OPERATIONS.MATCH_STATIONARY__CALCULATE_MAX_RADIUS(
                                        FRAME_WIDTH,
                                        FRAME_HEIGHT,
                                        'EXPOSED',
                                        PM.DIMENSION_UNIT,
                                        :FRAME_RADIUS_MULTIPLIER, -- TODO: Modify to PM.EXPOSED_VIEWSHED_SCALE, System Default 3x for now
                                        0.0,
                                        PM.MIN_NOTICING_RADIAL
                                    ),
                                    150 -- For Places, ideally remove this
                                )
                            ) AS MAX_FRAME_MATCH_RADIUS
                        FROM DATALAKE.SILVER.STATIONARY__CUSTOMER_SITE_FRAME_REL_VIEW CSFR
                        LEFT JOIN DATALAKE.BRONZE.STATIONARY__PROCESS_META AS PM USING (PROCESS_ID)
                        GROUP BY SITE_ID
                    )
                    SELECT
                        H3_POINT_TO_CELL(ST_MAKEPOINT(ST.SITE_LON, ST.SITE_LAT), 9) AS H3_BIN,
                        DATALAKE_TEST.OPERATIONS.GET_BIN__NUMERIC_ID(ST.SITE_ID, 100) AS SITE_BIN,
                        ST.SITE_ID,
                        ST_DISTANCE(ST_MAKEPOINT(ST.SITE_LON, ST.SITE_LAT), H3_CELL_TO_POINT(_H3I_9.VALUE::INT)) AS CELL_DISTANCE,
                        (FLOOR(CELL_DISTANCE / 10) * 10)::INT AS PROXIMITY_BIN_MIN,
                        (CEIL((CELL_DISTANCE + SQRT(ST_AREA(H3_CELL_TO_BOUNDARY(_H3I_9.VALUE::INT)) / PI())) / 10) * 10)::INT AS PROXIMITY_BIN_MAX,
                        H3_CELL_TO_PARENT(_H3I_9.VALUE::INT, 7) AS H3I_7,
                        _H3I_9.VALUE::INT AS H3I_9,
                        MSR.MAX_FRAME_MATCH_RADIUS
                    FROM
                        DATALAKE.BRONZE.STATIONARY__SITES AS ST
                        JOIN MAX_SITE_RADIUS MSR ON ST.SITE_ID = MSR.SITE_ID,
                        TABLE (
                        FLATTEN(
                            H3_TRY_COVERAGE(
                                TRY_TO_GEOGRAPHY(
                                    ST_BUFFER(
                                        TO_GEOMETRY(ST_MAKEPOINT(ST.SITE_LON, ST.SITE_LAT), 4326),
                                        DATALAKE_TEST.OPERATIONS.DISTANCE_TO_DEGREE(
                                            ST.SITE_LAT,
                                            MSR.MAX_FRAME_MATCH_RADIUS::FLOAT * 1.2 -- Use dynamic max radius with 1.2x factor
                                        )::FLOAT
                                    ),
                                    TRUE
                                ),
                                9
                            )
                        )
                        ) AS _H3I_9
                    WHERE
                        H3I_9 IS NOT NULL
                    ORDER BY
                        H3_BIN, PROXIMITY_BIN_MIN, SITE_BIN, ST.SITE_ID
                ) AS H3_C ON
                -- SITE_FRAME_PROXIMITY.H3_BIN = H3_C.H3_BIN
                -- AND
                -- SITE_FRAME_PROXIMITY.SITE_BIN = H3_C.SITE_BIN
                -- AND
                SITE_FRAME_PROXIMITY.SITE_ID = H3_C.SITE_ID
            ORDER BY
                H3_C.H3I_9
        );
      
        /*  Step 3: H3 Location Device Coverage */
        -- Create the Device H3 Pre-Filter Table
        CREATE OR REPLACE TEMP TABLE DEVICE_H3_PRE_FILTER AS (
            SELECT
                _MD.CORE_DATE,
                _MD.DEVICE_ID as DEVICE_HASH,
                _MD.DEVICE_IP,
                FLOOR(_MD.UNIX_TS / 3600.0) AS DEVICE_TIME_BIN_HOUR,
                _MD.UNIX_TS AS DEVICE_TIME,
                _MD.TS AS DEVICE_TIME_TS,
                TO_GEOGRAPHY(ST_SETSRID(_MD.DEVICE_GEO, 4326)) AS DEVICE_GEOG,
                NULL AS DEVICE_OS,
                _MD.H3I_7,
                _MD.H3I_9
            FROM
                DATALAKE.BRONZE.ACCRETIVE_MARKET_DATA AS _MD
                JOIN
                (SELECT DISTINCT H3I_9 FROM SITE_H3) AS SITE_H3 USING (H3I_9)
            WHERE
                _MD.CORE_DATE BETWEEN :START_DATE AND :END_DATE
            ORDER BY
                _MD.CORE_DATE, SITE_H3.H3I_9
        );
      
        /*  Step 4 Site/Frame H3 Device Matching */
        CREATE OR REPLACE TEMP TABLE SITE_FRAME_H3_DEVICE_MATCH AS (
            SELECT
                SITE.OWNER_ID,               
                MD.CORE_DATE,
                SITE.SITE_ID,
                SITE.FRAME_ID,
                DATALAKE.OPERATIONS.ST_AZIMUTH(SITE.SITE_GEOG, MD.DEVICE_GEOG) AS ABSOLUTE_ANGLE_DEGREE,
                LEAST(
                    MOD((ABSOLUTE_ANGLE_DEGREE - SITE.FRAME_EXPOSITION_ANGLE + 360), 360),
                    MOD((SITE.FRAME_EXPOSITION_ANGLE - ABSOLUTE_ANGLE_DEGREE + 360), 360)
                ) as MATCH_ANGLE_DEGREE,
                ST_DISTANCE(
                    SITE.SITE_GEOG,
                    MD.DEVICE_GEOG
                ) AS MATCH_DISTANCE_METER,
                MD.DEVICE_HASH,
                MD.DEVICE_IP,
                MD.DEVICE_TIME,
                MD.DEVICE_OS,
                SITE.FRAME_TYPE,
                SITE.MARKET_ID,
                MD.DEVICE_GEOG,
                OBJECT_CONSTRUCT(
                    'SITE_LAT', ST_Y(SITE.SITE_GEOG),
                    'SITE_LON', ST_X(SITE.SITE_GEOG),
                    'FRAME_MATCH_RADIUS', SITE.FRAME_MATCH_RADIUS,
                    'FRAME_EXPOSITION_ANGLE', SITE.FRAME_EXPOSITION_ANGLE,
                    'FRAME_EXPOSITION_WIDTH', SITE.FRAME_EXPOSITION_WIDTH
                ) AS MATCH_INFO,
                MD.H3I_7,
                MD.H3I_9
            FROM
                SITE_H3 AS SITE
                JOIN
                DEVICE_H3_PRE_FILTER AS MD ON
                    SITE.H3I_9 = MD.H3I_9
            WHERE
                ST_DWITHIN( -- Distance within the site proximity bin
                    SITE.SITE_GEOG,
                    MD.DEVICE_GEOG,
                    SITE.FRAME_MATCH_RADIUS
                )
                AND
                LEAST( -- Angle within the frame exposition width
                    MOD((DATALAKE.OPERATIONS.ST_AZIMUTH(SITE.SITE_GEOG, MD.DEVICE_GEOG) - SITE.FRAME_EXPOSITION_ANGLE + 360), 360),
                    MOD((SITE.FRAME_EXPOSITION_ANGLE - DATALAKE.OPERATIONS.ST_AZIMUTH(SITE.SITE_GEOG, MD.DEVICE_GEOG) + 360), 360)
                ) <= (GREATEST(SITE.FRAME_EXPOSITION_WIDTH, :MIN_EXPOSURE_ANGLE) / 2)
        );
    
        /*  Step 5: Dedupe STATIC Frames */

        CREATE OR REPLACE TEMP TABLE STATIONARY_FRAME_DEDUPED AS
        WITH static_matches AS (
            SELECT *
            FROM SITE_FRAME_H3_DEVICE_MATCH
            WHERE FRAME_TYPE = 'STATIC'
        ),
        -- Flag a row as starting a new group if it is the first in the partition or if the gap
        -- from the previous row (by DEVICE_TIME) is at least 3600 seconds (1 hour)
        flagged AS (
            SELECT 
                *,
                CASE 
                    WHEN LAG(DEVICE_TIME) OVER (PARTITION BY DEVICE_HASH, FRAME_ID ORDER BY DEVICE_TIME) IS NULL 
                        OR DEVICE_TIME - LAG(DEVICE_TIME) OVER (PARTITION BY DEVICE_HASH, FRAME_ID ORDER BY DEVICE_TIME) >= 3600
                    THEN 1
                    ELSE 0
                END AS new_group_flag
            FROM static_matches
        ),
        -- Compute a grouping identifier (i.e. an "island" number) by taking a running total
        grouped AS (
            SELECT 
                *,
                SUM(new_group_flag) OVER (PARTITION BY DEVICE_HASH, FRAME_ID ORDER BY DEVICE_TIME ROWS UNBOUNDED PRECEDING) AS grp
            FROM flagged
        ),
        -- For each group (i.e. consecutive rows within the same 1-hr window), select only
        -- the first row as the "kept" match.
        deduped AS (
            SELECT *
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY DEVICE_HASH, FRAME_ID, grp ORDER BY DEVICE_TIME) AS rn
                FROM grouped
            ) t
            WHERE rn = 1
        )
        SELECT * EXCLUDE (new_group_flag, grp, rn) FROM deduped
        UNION ALL
        SELECT * FROM SITE_FRAME_H3_DEVICE_MATCH
        WHERE FRAME_TYPE <> 'STATIC'
        ;

    
        /*  Step 6: Insert Matching Results */


        INSERT INTO DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_1__STATIONARY_DEVICES (
            SELECT 
                CORE_DATE,
                OWNER_ID,
                SITE_ID,
                FRAME_ID,
                FRAME_TYPE,
                DEVICE_HASH,
                DEVICE_TIME,
                DEVICE_IP,
                DEVICE_OS,
                MATCH_DISTANCE_METER,
                MATCH_ANGLE_DEGREE,
                ABSOLUTE_ANGLE_DEGREE,
                MATCH_INFO,
                DEVICE_GEOG,
                MARKET_ID,
                H3I_7,
                H3I_9,
                0 AS CALL_ID
             FROM STATIONARY_FRAME_DEDUPED
             ORDER BY (CORE_DATE, MARKET_ID, FRAME_ID, H3I_7, H3I_9)
        );

     
        RETURN TRUE;
    END;
$$;


/* ****************** */
/*  Stage 2 Procedure */
/* ****************** */

CREATE OR REPLACE PROCEDURE DATASCIENCE.ANALYSIS.ACCRETIVE_MATCHING__STAGE_2__STATIONARY(
    START_DATE DATE,
    END_DATE DATE,
    CUSTOMER_IDS ARRAY DEFAULT NULL,
    MARKET_IDS ARRAY DEFAULT NULL,
    FRAME_IDS ARRAY DEFAULT NULL,
    TIME_BUFFER_SECONDS INT DEFAULT 30
)
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
    BEGIN


        /*  Step 1: Creative/Ad Play Matching */

        -- Create the Creative/Ad Play Matching Table
        CREATE OR REPLACE TEMP TABLE STATIONARY_AD_JOINED AS (

            SELECT
                ST1_SD.CORE_DATE,
                ST1_SD.OWNER_ID,
                ST1_SD.MARKET_ID,
                ST1_SD.SITE_ID,
                ST1_SD.FRAME_ID,
                ST1_SD.FRAME_TYPE,
                AD.CREATIVE_ID,
                AD.ADPLAY_START_EPOCH,
                AD.ADPLAY_END_EPOCH,
                ST1_SD.DEVICE_TIME,
                ST1_SD.DEVICE_HASH,
                ST1_SD.DEVICE_IP,
                ST1_SD.DEVICE_OS,
                ST1_SD.MATCH_DISTANCE_METER,
                ST1_SD.MATCH_ANGLE_DEGREE,
                ST1_SD.ABSOLUTE_ANGLE_DEGREE,
                ST1_SD.MATCH_INFO,
                ST1_SD.DEVICE_GEOG,
                FLOOR(ST1_SD.DEVICE_TIME / 3600.0) AS DEVICE_TIME_BIN_HOUR,
                ST1_SD.H3I_7,
                ST1_SD.H3I_9
            FROM
                DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_1__STATIONARY_DEVICES AS ST1_SD
                LEFT JOIN
                    (SELECT * FROM DATALAKE.BRONZE.STATIONARY_ADPLAYS  
                    WHERE ADPLAY_START_DATE <= :START_DATE AND ADPLAY_END_DATE >= :END_DATE) AS AD
                    ON ST1_SD.SITE_ID = AD.SITE_ID
                        AND ST1_SD.FRAME_ID = AD.FRAME_ID
                        AND
                        FLOOR(ST1_SD.DEVICE_TIME / 3600.0)
                            BETWEEN FLOOR((AD.ADPLAY_START_EPOCH - :TIME_BUFFER_SECONDS) / 3600.0)
                                AND FLOOR((AD.ADPLAY_END_EPOCH + :TIME_BUFFER_SECONDS) / 3600.0)
                        AND
                        ST1_SD.DEVICE_TIME
                            BETWEEN (AD.ADPLAY_START_EPOCH - :TIME_BUFFER_SECONDS)
                                AND (AD.ADPLAY_END_EPOCH + :TIME_BUFFER_SECONDS)
            WHERE
                ST1_SD.CORE_DATE BETWEEN :START_DATE AND :END_DATE -- ToDo: Add customer IDs
                AND
                (:MARKET_IDS IS NULL OR ST1_SD.MARKET_ID IN (SELECT VALUE FROM TABLE(FLATTEN(:MARKET_IDS))))
                AND
                (:FRAME_IDS IS NULL OR ST1_SD.FRAME_ID IN (SELECT VALUE FROM TABLE(FLATTEN(:FRAME_IDS))))
        );


        /*  Step 2: Digital Deduplication */

        -- Create the Digital Deduplication Table
        CREATE OR REPLACE TEMP TABLE STATIONARY_AD_JOINED_DEDUPED AS
        WITH digital_matches AS (
             SELECT * FROM STATIONARY_AD_JOINED
             WHERE FRAME_TYPE <> 'STATIC'
        ),
        non_digital_matches AS (
             SELECT * FROM STATIONARY_AD_JOINED
             WHERE FRAME_TYPE = 'STATIC'
        ),
        dedup_digital AS ( --  compares the current DEVICE_TIME with the previous one in the same partition. If the previous record is NULL (i.e. the first event) or if the time gap is at least 300 seconds (i.e. 5 minutes), we flag the row as starting a new group
            SELECT
                *,
                CASE 
                    WHEN LAG(DEVICE_TIME) OVER (PARTITION BY DEVICE_HASH, CREATIVE_ID, FRAME_ID ORDER BY DEVICE_TIME) IS NULL
                         OR DEVICE_TIME - LAG(DEVICE_TIME) OVER (PARTITION BY DEVICE_HASH, CREATIVE_ID, FRAME_ID ORDER BY DEVICE_TIME) >= 300
                    THEN 1
                    ELSE 0
                END AS new_group_flag
            FROM digital_matches
        ),
        grouped_digital AS ( -- groups the rows into groups based on the new_group_flag
            SELECT
                *,
                SUM(new_group_flag) OVER (PARTITION BY DEVICE_HASH, CREATIVE_ID, FRAME_ID ORDER BY DEVICE_TIME ROWS UNBOUNDED PRECEDING) AS grp
            FROM dedup_digital
        ),
        deduped_digital AS ( -- selects the first row in each group
            SELECT * 
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY DEVICE_HASH, CREATIVE_ID, FRAME_ID, grp ORDER BY DEVICE_TIME) AS rn
                FROM grouped_digital
            ) t
            WHERE rn = 1
        )
        SELECT * EXCLUDE (new_group_flag, grp, rn) FROM deduped_digital
        UNION ALL
        SELECT * FROM non_digital_matches
        ;
  

        /*  Step 3: Decay Function and Viewability Binning */
        -- Create the Decay Function Table
        CREATE OR REPLACE TEMP TABLE STATIONARY_MATCHED_DEVICES AS (
            SELECT
                CORE_DATE,
                OWNER_ID,
                SITE_ID,
                FRAME_ID,
                FRAME_TYPE,
                CREATIVE_ID,
                ADPLAY_START_EPOCH as AD_START_EPOCH,
                ADPLAY_END_EPOCH as AD_END_EPOCH,
                DEVICE_HASH,
                DEVICE_TIME,
                DEVICE_TIME_BIN_HOUR,
                CASE
                    WHEN MATCH_DISTANCE_METER < 0.2 * SD_AD.MATCH_INFO:FRAME_MATCH_RADIUS THEN 'LIKELY_TO_SEE'
                    WHEN MATCH_DISTANCE_METER < 0.375 * SD_AD.MATCH_INFO:FRAME_MATCH_RADIUS THEN 'OPPORTUNITY_TO_SEE'
                    WHEN MATCH_DISTANCE_METER < 0.5 * SD_AD.MATCH_INFO:FRAME_MATCH_RADIUS THEN 'CHANCE_TO_SEE'
                    ELSE 'DECAYED'
                END AS VIEWABILITY,
                DATALAKE.OPERATIONS.ROLLUP_STATIONARY__MULTIVARIATE_DECAY(
                    MATCH_DISTANCE_METER,
                    SD_AD.MATCH_INFO:FRAME_MATCH_RADIUS / 2,
                    SD_AD.MATCH_INFO:FRAME_MATCH_RADIUS,
                    ABSOLUTE_ANGLE_DEGREE,
                    SD_AD.MATCH_INFO:FRAME_EXPOSITION_ANGLE,
                    SD_AD.MATCH_INFO:FRAME_EXPOSITION_WIDTH,
                    180,
                    FALSE,
                    FALSE
                ) AS DECAYED_VALUE,
                DEVICE_IP,
                DEVICE_OS,
                MATCH_DISTANCE_METER,
                MATCH_ANGLE_DEGREE,
                ABSOLUTE_ANGLE_DEGREE,
                DEVICE_GEOG,
                MATCH_INFO,
                MARKET_ID,
                H3I_7,
                H3I_9,
                0 AS CALL_ID
            from DATALAKE_TEST.MATCHING.STATIONARY_AD_JOINED_DEDUPED SD_AD
            ORDER BY (CORE_DATE, DEVICE_TIME_BIN_HOUR, MARKET_ID, CREATIVE_ID, FRAME_ID, H3I_7, H3I_9)
        );


        /*  Step 3: Insert into the Stationary Matching Table */
        -- Insert the Stationary Matching Table
        INSERT INTO DATASCIENCE.ANALYSIS.ACCRETIVE_STAGE_2__STATIONARY_DEVICES
        SELECT * 
        FROM STATIONARY_MATCHED_DEVICES
        ORDER BY (CORE_DATE, DEVICE_TIME_BIN_HOUR, MARKET_ID, CREATIVE_ID, FRAME_ID, H3I_7, H3I_9);

        RETURN TRUE;
    END;
$$;
