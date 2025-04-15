
CREATE OR REPLACE TEMPORARY TABLE DATASCIENCE.ANALYSIS.DEMO_ENRICHMENT_SUMMARY AS
SELECT
    DEVICE_HASH,
    CATEGORY,
    MAX(prior_probability) as max_prior_probability,
    MAX(posterior_probability) as max_posterior_probability,
    MIN(prior_probability) as min_prior_probability,
    MIN(posterior_probability) as min_posterior_probability,
    -- -- Calculate entropy for prior probabilities
    SUM(-prior_probability * LOG(2, GREATEST(NULLIF(prior_probability, 0), 1e-10))) AS prior_entropy,
    -- -- Calculate entropy for posterior probabilities
    SUM(-posterior_probability * LOG(2, GREATEST(NULLIF(posterior_probability, 0), 1e-10))) AS posterior_entropy
from DATALAKE.SILVER.DEVICE_DEMOGRAPHICS
group by 1,2;

CREATE OR REPLACE TABLE DATASCIENCE.ANALYSIS.DEMO_ENRICHMENT_SUMMARY AS SELECT * FROM DATASCIENCE.ANALYSIS.DEMO_ENRICHMENT_SUMMARY;


-- Overall percentile analysis of max probability changes across the entire dataset
WITH overall_percentiles AS (
    SELECT
        AVG(max_posterior_probability) as max_posterior_probability,
        AVG(max_prior_probability) as max_prior_probability,
        AVG(prior_entropy) as prior_entropy,
        AVG(posterior_entropy) as posterior_entropy,
        AVG(max_posterior_probability - max_prior_probability) AS probability_change,
        AVG(prior_entropy - posterior_entropy) as entropy_reduction
    FROM 
        DATASCIENCE.ANALYSIS.DEMO_ENRICHMENT_SUMMARY
),

-- Percentile analysis by category
category_percentiles AS (
    SELECT
        CATEGORY,
        AVG(max_posterior_probability) as max_posterior_probability,
        AVG(max_prior_probability) as max_prior_probability,
        AVG(prior_entropy) as prior_entropy,
        AVG(posterior_entropy) as posterior_entropy,
        AVG(max_posterior_probability - max_prior_probability) AS probability_change,
        AVG(prior_entropy - posterior_entropy) as entropy_reduction,
    FROM 
        DATASCIENCE.ANALYSIS.DEMO_ENRICHMENT_SUMMARY
    GROUP BY 
        CATEGORY
)

-- Combine the results
SELECT 
    'OVERALL' AS category,
    TO_VARCHAR(max_posterior_probability * 100, '990.00') || '%' AS max_posterior_probability,
    TO_VARCHAR(max_prior_probability * 100, '990.00') || '%' AS max_prior_probability,
    TO_VARCHAR(prior_entropy, '990.000') AS prior_entropy,
    TO_VARCHAR(posterior_entropy, '990.000') AS posterior_entropy,
    TO_VARCHAR(probability_change * 100, '990.00') || '%' AS probability_change,
    TO_VARCHAR(entropy_reduction, '990.000') AS entropy_reduction
FROM 
    overall_percentiles

UNION ALL

SELECT 
    CATEGORY,
    TO_VARCHAR(max_posterior_probability * 100, '990.00') || '%' AS max_posterior_probability,
    TO_VARCHAR(max_prior_probability * 100, '990.00') || '%' AS max_prior_probability,
    TO_VARCHAR(prior_entropy, '990.000') AS prior_entropy,
    TO_VARCHAR(posterior_entropy, '990.000') AS posterior_entropy,
    TO_VARCHAR(probability_change * 100, '990.00') || '%' AS probability_change,
    TO_VARCHAR(entropy_reduction, '990.000') AS entropy_reduction
FROM 
    category_percentiles
ORDER BY 
    CASE WHEN category = 'OVERALL' THEN 0 ELSE 1 END,
    probability_change DESC;



with base as (
    select device_hash, count(audience_attribute) as num_attributes
    from datalake.BRONZE.device_affinities
    where audience_attribute is not null
    group by 1
)

select 
max(num_attributes),
min(num_attributes),
avg(num_attributes),
stddev(num_attributes),
percentile_cont(0.05) within group (order by num_attributes) as p5_num_attributes,
percentile_cont(0.25) within group (order by num_attributes) as p25_num_attributes,
percentile_cont(0.5) within group (order by num_attributes) as p50_num_attributes,
percentile_cont(0.75) within group (order by num_attributes) as p75_num_attributes,
percentile_cont(0.95) within group (order by num_attributes) as p95_num_attributes,
from base;
