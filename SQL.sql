
-- Creatting Database
-- ======================================================
DROP DATABASE IF EXISTS Sustainability_db;
CREATE DATABASE IF NOT EXISTS Sustainability_db;
USE Sustainability_db;

-- ======================================================
-- Create raw_data table
-- ======================================================
DROP TABLE IF EXISTS raw_data;
CREATE TABLE IF NOT EXISTS raw_data (
    sustainability_rating        FLOAT,
    eco_friendly_manufacturing   VARCHAR(255),
    carbon_footprint             FLOAT,
    water_usage                  INT,
    waste_production             FLOAT,
    recycling_programs           VARCHAR(255),
    average_price                FLOAT,
    product_nameproduct_category VARCHAR(255),
    sustainability_status        VARCHAR(255),
    eco_index                    FLOAT,
    sustainability_level         VARCHAR(255),
    brand_name                   VARCHAR(255),
    brand_category               VARCHAR(255),
    country_name                 VARCHAR(255),
    region                       VARCHAR(255),
    year                         YEAR,
    material_type                VARCHAR(255),
    renewable                    VARCHAR(255),
    product_line                 VARCHAR(255),
    target_audience              VARCHAR(255),
    market_trend                 VARCHAR(255),
    trend_score                  VARCHAR(255),
    certification                VARCHAR(255),
    cert_type                    VARCHAR(255)
);

-- Check local_infile setting 
SHOW GLOBAL VARIABLES LIKE 'local_infile';
-- ======================================================
-- Load raw CSV into raw_data
-- ======================================================
LOAD DATA LOCAL INFILE 'D:/Dina Projects/Data Analysis Projects/Data sets/Sustainability_Raw_Data.csv'
INTO TABLE raw_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Showing raw_data table
SELECT * FROM raw_data ;

 -- CLEAN raw_data: Standardiztion
-- ========================================================
--  Trim and capitalize each word for qualitaive columns
UPDATE raw_data
SET brand_name = CONCAT(UPPER(LEFT(TRIM(brand_name), 1)), LOWER(SUBSTRING(TRIM(brand_name), 2))),
    brand_category = CONCAT(UPPER(LEFT(TRIM(brand_category), 1)), LOWER(SUBSTRING(TRIM(brand_category), 2))),
    certification = CONCAT(UPPER(LEFT(TRIM(certification), 1)), LOWER(SUBSTRING(TRIM(certification), 2))),
    cert_type = CONCAT(UPPER(LEFT(TRIM(cert_type), 1)), LOWER(SUBSTRING(TRIM(cert_type), 2))),
    country_name = CONCAT(UPPER(LEFT(TRIM(country_name), 1)), LOWER(SUBSTRING(TRIM(country_name), 2))),
    region = CONCAT(UPPER(LEFT(TRIM(region), 1)), LOWER(SUBSTRING(TRIM(region), 2))),
    product_nameproduct_category = CONCAT(UPPER(LEFT(TRIM(product_nameproduct_category), 1)), LOWER(SUBSTRING(TRIM(product_nameproduct_category), 2))),
    product_line = CONCAT(UPPER(LEFT(TRIM(product_line), 1)), LOWER(SUBSTRING(TRIM(product_line), 2))),
    target_audience = CONCAT(UPPER(LEFT(TRIM(target_audience), 1)), LOWER(SUBSTRING(TRIM(target_audience), 2))),
    sustainability_status = CONCAT(UPPER(LEFT(TRIM(sustainability_status), 1)), LOWER(SUBSTRING(TRIM(sustainability_status), 2))),
    sustainability_level = CONCAT(UPPER(LEFT(TRIM(sustainability_level), 1)), LOWER(SUBSTRING(TRIM(sustainability_level), 2))),
    recycling_programs = CONCAT(UPPER(LEFT(TRIM(recycling_programs), 1)), LOWER(SUBSTRING(TRIM(recycling_programs), 2))),
    eco_friendly_manufacturing = CONCAT(UPPER(LEFT(TRIM(eco_friendly_manufacturing), 1)), LOWER(SUBSTRING(TRIM(eco_friendly_manufacturing), 2))),
    material_type = CONCAT(UPPER(LEFT(TRIM(material_type), 1)), LOWER(SUBSTRING(TRIM(material_type), 2))),
    renewable = CONCAT(UPPER(LEFT(TRIM(renewable), 1)), LOWER(SUBSTRING(TRIM(renewable), 2))),
    market_trend = CONCAT(UPPER(LEFT(TRIM(market_trend), 1)), LOWER(SUBSTRING(TRIM(market_trend), 2))),
    trend_score = CONCAT(UPPER(LEFT(TRIM(trend_score), 1)), LOWER(SUBSTRING(TRIM(trend_score), 2)));

-- ========================================================================================================

 -- Standardize Yes/No fields (eco_friendly_manufacturing, recycling_programs)
UPDATE raw_data
SET eco_friendly_manufacturing =
  CASE
    WHEN eco_friendly_manufacturing IN ('1','yes','y','true','t') THEN 'Yes'
    WHEN eco_friendly_manufacturing IN ('0','no','n','false','f') THEN 'No'
    WHEN eco_friendly_manufacturing IS NULL OR eco_friendly_manufacturing = '' THEN NULL
    ELSE CONCAT(UPPER(LEFT(eco_friendly_manufacturing,1)),LOWER(SUBSTRING(eco_friendly_manufacturing,2)))
  END;
  -- ==============================================================================
  
UPDATE raw_data
SET recycling_programs =
  CASE
    WHEN recycling_programs IN ('1','yes','y','true','t') THEN 'Yes'
    WHEN recycling_programs IN ('0','no','n','false','f') THEN 'No'
    WHEN recycling_programs IS NULL OR recycling_programs = '' THEN NULL
    ELSE CONCAT(UPPER(LEFT(recycling_programs,1)),LOWER(SUBSTRING(recycling_programs,2)))
  END;
-- ====================================================================================

-- 3.4 Standardize renewable values
UPDATE raw_data
SET renewable =
  CASE
    WHEN renewable IN ('1','yes','y','fully renewable','fully') THEN 'Fully Renewable'
    WHEN renewable LIKE '%part%' THEN 'Partially Renewable'
    WHEN renewable IN ('0','no','n','not','not renewable') THEN 'Not Renewable'
    WHEN renewable IS NULL OR renewable = '' THEN NULL
    ELSE CONCAT(UPPER(LEFT(renewable,1)),LOWER(SUBSTRING(renewable,2)))
  END;

  ALTER TABLE raw_data
  ADD COLUMN raw_id INT PRIMARY KEY AUTO_INCREMENT;
  
  ALTER TABLE raw_data
  Drop COLUMN eco_index  ;
  
  
  --  Detect duplicates in raw_data 
-- ========================================================
SELECT raw_id ,COUNT(*) AS duplicate_count
FROM raw_data
WHERE raw_id IS NOT NULL
GROUP BY raw_id
HAVING COUNT(*) > 1
order by duplicate_count desc ;  -- There are no dublicates in our data 

  -- ========================================================
  --  Calculating MEAN and Median OF THE NUMERIC COLUMNS to check Skewness of the data
	-- MEAN OF THE NUMERIC COLUMNS 
  SELECT
	ROUND( AVG(water_usage) )AS mean_water_usage,
	ROUND(AVG(waste_production) )AS mean_waste_production,
    ROUND(AVG(carbon_footprint) )AS mean_carbon_footprint
  FROM raw_data ;
    -- MEDIAN OF NUMERIC COLUMNS
WITH RankedData AS (
    SELECT
        water_usage,
        waste_production,
        carbon_footprint,
        -- Assign rank based on water usage
        ROW_NUMBER() OVER (ORDER BY water_usage) AS rn_water,
        -- Assign rank based on waste production
        ROW_NUMBER() OVER (ORDER BY waste_production) AS rn_waste,
        -- Assign rank based on carbon footprint
        ROW_NUMBER() OVER (ORDER BY carbon_footprint) AS rn_carbon,
        -- Get the total number of rows
        COUNT(*) OVER () AS total_count
    FROM
        raw_data
)
SELECT
    -- Median for water_usage, rounded to 2 decimal places
    ROUND(
        AVG(CASE WHEN rn_water IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2)) THEN water_usage END)
    ) AS median_water_usage,

    -- Median for waste_production, rounded to 2 decimal places
    ROUND(
        AVG(CASE WHEN rn_waste IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2)) THEN waste_production END)
    ) AS median_waste_production,

    -- Median for carbon_footprint, rounded to 2 decimal places
    ROUND(
        AVG(CASE WHEN rn_carbon IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2)) THEN carbon_footprint END)
    ) AS median_carbon_footprint
FROM
    RankedData
WHERE
    -- Filter to include only the row(s) corresponding to the middle rank(s) for the AVG calculation
    rn_water IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2)) OR
    rn_waste IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2)) OR
    rn_carbon IN (FLOOR((total_count + 1) / 2), CEIL((total_count + 1) / 2));

-- OUR DATA IS NOT PERFECTLY NORMAL DISTRIBUTION (has little bit skewness),SO WE WILL USE IQR METHOD FOR DETECTING AND HANDLING OUTLIERS -- 

-- =======================================================================
--  Detecting & Handling Outliers
-- =======================================================================
-- BACKUP (CREATE A SAFE COPY)
-- =======================================================================
SET @ts := DATE_FORMAT(NOW(), '%Y%m%d_%H%i%s');
SET @backup_name := CONCAT('raw_data_backup_', @ts);
SET @sql := CONCAT('CREATE TABLE ', @backup_name, ' AS SELECT * FROM raw_data;');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
SELECT CONCAT('Backup created: ', @backup_name) AS info;

-- ============================================
-- ======================================================
-- STEP 0: Ensure water_usage and waste_production are DECIMAL to avoid floating point issues
-- ======================================================
ALTER TABLE raw_data 
    MODIFY water_usage DECIMAL(12,4),
    MODIFY waste_production DECIMAL(12,4);

-- ======================================================
-- STEP 1: Compute total counts for quartile positions
-- ======================================================
SET @total_sus = (SELECT COUNT(*) FROM raw_data WHERE sustainability_rating IS NOT NULL);
SET @q1_sus_pos = FLOOR(@total_sus * 0.25);
SET @q3_sus_pos = FLOOR(@total_sus * 0.75);

SET @total_price = (SELECT COUNT(*) FROM raw_data WHERE average_price IS NOT NULL);
SET @q1_price_pos = FLOOR(@total_price * 0.25);
SET @q3_price_pos = FLOOR(@total_price * 0.75);

SET @total_carbon = (SELECT COUNT(*) FROM raw_data WHERE carbon_footprint IS NOT NULL);
SET @q1_carbon_pos = FLOOR(@total_carbon * 0.25);
SET @q3_carbon_pos = FLOOR(@total_carbon * 0.75);

SET @total_water = (SELECT COUNT(*) FROM raw_data WHERE water_usage IS NOT NULL);
SET @q1_water_pos = FLOOR(@total_water * 0.25);
SET @q3_water_pos = FLOOR(@total_water * 0.75);

SET @total_waste = (SELECT COUNT(*) FROM raw_data WHERE waste_production IS NOT NULL);
SET @q1_waste_pos = FLOOR(@total_waste * 0.25);
SET @q3_waste_pos = FLOOR(@total_waste * 0.75);

-- ======================================================
-- STEP 2: Compute Q1 and Q3 using PREPARE statements
-- ======================================================
-- sustainability_rating
SET @sql = CONCAT(
  'SELECT sustainability_rating INTO @Q1_sus FROM raw_data ',
  'WHERE sustainability_rating IS NOT NULL ORDER BY sustainability_rating LIMIT ', @q1_sus_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'SELECT sustainability_rating INTO @Q3_sus FROM raw_data ',
  'WHERE sustainability_rating IS NOT NULL ORDER BY sustainability_rating LIMIT ', @q3_sus_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- average_price
SET @sql = CONCAT(
  'SELECT average_price INTO @Q1_price FROM raw_data ',
  'WHERE average_price IS NOT NULL ORDER BY average_price LIMIT ', @q1_price_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'SELECT average_price INTO @Q3_price FROM raw_data ',
  'WHERE average_price IS NOT NULL ORDER BY average_price LIMIT ', @q3_price_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- carbon_footprint
SET @sql = CONCAT(
  'SELECT carbon_footprint INTO @Q1_carbon FROM raw_data ',
  'WHERE carbon_footprint IS NOT NULL ORDER BY carbon_footprint LIMIT ', @q1_carbon_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'SELECT carbon_footprint INTO @Q3_carbon FROM raw_data ',
  'WHERE carbon_footprint IS NOT NULL ORDER BY carbon_footprint LIMIT ', @q3_carbon_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- water_usage
SET @sql = CONCAT(
  'SELECT water_usage INTO @Q1_water FROM raw_data ',
  'WHERE water_usage IS NOT NULL ORDER BY water_usage LIMIT ', @q1_water_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'SELECT water_usage INTO @Q3_water FROM raw_data ',
  'WHERE water_usage IS NOT NULL ORDER BY water_usage LIMIT ', @q3_water_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- waste_production
SET @sql = CONCAT(
  'SELECT waste_production INTO @Q1_waste FROM raw_data ',
  'WHERE waste_production IS NOT NULL ORDER BY waste_production LIMIT ', @q1_waste_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT(
  'SELECT waste_production INTO @Q3_waste FROM raw_data ',
  'WHERE waste_production IS NOT NULL ORDER BY waste_production LIMIT ', @q3_waste_pos, ',1'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- ======================================================
-- STEP 3: Compute lower and upper bounds
-- ======================================================
SET @sus_lower = ROUND(@Q1_sus - 1.5*(@Q3_sus-@Q1_sus), 4);
SET @sus_upper = ROUND(@Q3_sus + 1.5*(@Q3_sus-@Q1_sus), 4);

SET @price_lower = ROUND(@Q1_price - 1.5*(@Q3_price-@Q1_price), 4);
SET @price_upper = ROUND(@Q3_price + 1.5*(@Q3_price-@Q1_price), 4);

SET @carbon_lower = ROUND(@Q1_carbon - 1.5*(@Q3_carbon-@Q1_carbon), 4);
SET @carbon_upper = ROUND(@Q3_carbon + 1.5*(@Q3_carbon-@Q1_carbon), 4);

SET @water_lower = ROUND(@Q1_water - 1.5*(@Q3_water-@Q1_water), 4);
SET @water_upper = ROUND(@Q3_water + 1.5*(@Q3_water-@Q1_water), 4);

SET @waste_lower = ROUND(@Q1_waste - 1.5*(@Q3_waste-@Q1_waste), 4);
SET @waste_upper = ROUND(@Q3_waste + 1.5*(@Q3_waste-@Q1_waste), 4);

-- ======================================================
-- STEP 4: Detect outliers before Winsorizing
-- ======================================================
DROP TEMPORARY TABLE IF EXISTS outliers_before;
CREATE TEMPORARY TABLE outliers_before AS
SELECT *,
  CASE WHEN sustainability_rating IS NOT NULL AND (ROUND(sustainability_rating,4) < @sus_lower OR ROUND(sustainability_rating,4) > @sus_upper) THEN 1 ELSE 0 END AS sus_outlier,
  CASE WHEN average_price IS NOT NULL AND (ROUND(average_price,4) < @price_lower OR ROUND(average_price,4) > @price_upper) THEN 1 ELSE 0 END AS price_outlier,
  CASE WHEN carbon_footprint IS NOT NULL AND (ROUND(carbon_footprint,4) < @carbon_lower OR ROUND(carbon_footprint,4) > @carbon_upper) THEN 1 ELSE 0 END AS carbon_outlier,
  CASE WHEN water_usage IS NOT NULL AND (ROUND(water_usage,4) < @water_lower OR ROUND(water_usage,4) > @water_upper) THEN 1 ELSE 0 END AS water_outlier,
  CASE WHEN waste_production IS NOT NULL AND (ROUND(waste_production,4) < @waste_lower OR ROUND(waste_production,4) > @waste_upper) THEN 1 ELSE 0 END AS waste_outlier
FROM raw_data;

-- ======================================================
-- STEP 5: Round all numeric columns to 4 decimals
-- ======================================================
UPDATE raw_data
SET sustainability_rating = ROUND(sustainability_rating,4),
    average_price = ROUND(average_price,4),
    carbon_footprint = ROUND(carbon_footprint,4),
    water_usage = ROUND(water_usage,4),
    waste_production = ROUND(waste_production,4);

-- ======================================================
-- STEP 6: Winsorize (cap) all numeric columns
-- ======================================================
UPDATE raw_data
SET
  sustainability_rating = LEAST(GREATEST(sustainability_rating, @sus_lower), @sus_upper),
  average_price = LEAST(GREATEST(average_price, @price_lower), @price_upper),
  carbon_footprint = LEAST(GREATEST(carbon_footprint, @carbon_lower), @carbon_upper),
  water_usage = LEAST(GREATEST(water_usage, @water_lower), @water_upper),
  waste_production = LEAST(GREATEST(waste_production, @waste_lower), @waste_upper);

-- ======================================================
-- STEP 7: Detect outliers after Winsorizing
-- ======================================================
DROP TEMPORARY TABLE IF EXISTS outliers_after;
CREATE TEMPORARY TABLE outliers_after AS
SELECT *,
  CASE WHEN ROUND(sustainability_rating,4) < @sus_lower OR ROUND(sustainability_rating,4) > @sus_upper THEN 1 ELSE 0 END AS sus_outlier,
  CASE WHEN ROUND(average_price,4) < @price_lower OR ROUND(average_price,4) > @price_upper THEN 1 ELSE 0 END AS price_outlier,
  CASE WHEN ROUND(carbon_footprint,4) < @carbon_lower OR ROUND(carbon_footprint,4) > @carbon_upper THEN 1 ELSE 0 END AS carbon_outlier,
  CASE WHEN ROUND(water_usage,4) < @water_lower OR ROUND(water_usage,4) > @water_upper THEN 1 ELSE 0 END AS water_outlier,
  CASE WHEN ROUND(waste_production,4) < @waste_lower OR ROUND(waste_production,4) > @waste_upper THEN 1 ELSE 0 END AS waste_outlier
FROM raw_data;

-- ======================================================
-- STEP 8: Verification summary  Before Vs After
-- ======================================================
SELECT 
  SUM(sus_outlier) AS sus_outliers_before, 
  SUM(price_outlier) AS price_outliers_before,
  SUM(carbon_outlier) AS carbon_outliers_before,
  SUM(water_outlier) AS water_outliers_before,
  SUM(waste_outlier) AS waste_outliers_before
FROM outliers_before;

-- Checking for handling outliers
SELECT 
  SUM(sus_outlier) AS sus_outliers_after, 
  SUM(price_outlier) AS price_outliers_after,
  SUM(carbon_outlier) AS carbon_outliers_after,
  SUM(water_outlier) AS water_outliers_after,
  SUM(waste_outlier) AS waste_outliers_after
FROM outliers_after;

 -- ================================================================
 -- Checking for missing values
 -- ================================================================
 SELECT 
    
    SUM(CASE WHEN sustainability_rating IS NULL THEN 1 ELSE 0 END) AS missing_sustainability_rating,
    SUM(CASE WHEN eco_friendly_manufacturing IS NULL THEN 1 ELSE 0 END) AS missing_eco_friendly_manufacturing,
    SUM(CASE WHEN carbon_footprint IS NULL THEN 1 ELSE 0 END) AS missing_carbon_footprint,
    SUM(CASE WHEN water_usage IS NULL THEN 1 ELSE 0 END) AS missing_water_usage,
    SUM(CASE WHEN waste_production IS NULL THEN 1 ELSE 0 END) AS missing_waste_production,
    SUM(CASE WHEN recycling_programs IS NULL THEN 1 ELSE 0 END) AS missing_recycling_programs,
    SUM(CASE WHEN average_price IS NULL THEN 1 ELSE 0 END) AS missing_average_price,
    SUM(CASE WHEN product_nameproduct_category IS NULL THEN 1 ELSE 0 END) AS missing_product_nameproduct_category,
    SUM(CASE WHEN sustainability_status IS NULL THEN 1 ELSE 0 END) AS missing_sustainability_status,
    SUM(CASE WHEN sustainability_level IS NULL THEN 1 ELSE 0 END) AS missing_sustainability_level,
    SUM(CASE WHEN brand_name IS NULL THEN 1 ELSE 0 END) AS missing_brand_name,
    SUM(CASE WHEN brand_category IS NULL THEN 1 ELSE 0 END) AS missing_brand_category,
    SUM(CASE WHEN country_name IS NULL THEN 1 ELSE 0 END) AS missing_country_name,
    SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS missing_region,
    SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS missing_year,
    SUM(CASE WHEN material_type IS NULL THEN 1 ELSE 0 END) AS missing_material_type,
    SUM(CASE WHEN renewable IS NULL THEN 1 ELSE 0 END) AS missing_renewable,
    SUM(CASE WHEN product_line IS NULL THEN 1 ELSE 0 END) AS missing_product_line,
    SUM(CASE WHEN target_audience IS NULL THEN 1 ELSE 0 END) AS missing_target_audience,
    SUM(CASE WHEN market_trend IS NULL THEN 1 ELSE 0 END) AS missing_market_trend,
    SUM(CASE WHEN trend_score IS NULL THEN 1 ELSE 0 END) AS missing_trend_score,
    SUM(CASE WHEN certification IS NULL THEN 1 ELSE 0 END) AS missing_certification,
    SUM(CASE WHEN cert_type IS NULL THEN 1 ELSE 0 END) AS missing_cert_type
FROM raw_data;

 
 
 -- ====================================================
-- Create dimension tables
-- ======================================================

-- brand_dim
DROP TABLE IF EXISTS brand_dim;
CREATE TABLE IF NOT EXISTS brand_dim (
    brand_id     INT PRIMARY KEY AUTO_INCREMENT,
    brand_name   VARCHAR(255),
    brand_category VARCHAR(255)
);

-- certification_dim
DROP TABLE IF EXISTS certification_dim;
CREATE TABLE IF NOT EXISTS certification_dim (
    certification_id INT PRIMARY KEY AUTO_INCREMENT,
    certification    VARCHAR(255),
    cert_type        VARCHAR(255)
);

-- country_dim
DROP TABLE IF EXISTS country_dim;
CREATE TABLE IF NOT EXISTS country_dim (
    country_id   INT PRIMARY KEY AUTO_INCREMENT,
    country_name VARCHAR(255),
    region       VARCHAR(255)
);

-- market_trend_dim
DROP TABLE IF EXISTS market_trend_dim;
CREATE TABLE IF NOT EXISTS market_trend_dim (
    market_trend_id INT PRIMARY KEY AUTO_INCREMENT,
    market_trend    VARCHAR(255),
    trend_score     VARCHAR(255)
);

-- material_dim
DROP TABLE IF EXISTS material_dim;
CREATE TABLE IF NOT EXISTS material_dim (
    material_id   INT PRIMARY KEY AUTO_INCREMENT,
    material_type VARCHAR(255),
    renewable     VARCHAR(255)
);

-- product_category_dim
DROP TABLE IF EXISTS product_category_dim;
CREATE TABLE IF NOT EXISTS product_category_dim (
    product_category_id INT PRIMARY KEY AUTO_INCREMENT,
    product_line         VARCHAR(255),
    target_audience      VARCHAR(255)
);

-- year_dim
DROP TABLE IF EXISTS year_dim;
CREATE TABLE IF NOT EXISTS year_dim (
    year_id INT PRIMARY KEY AUTO_INCREMENT,
    year    YEAR
);

-- ======================================================
-- Create fact table with foreign keys
-- ======================================================
DROP TABLE IF EXISTS sustainability_fact;
CREATE TABLE IF NOT EXISTS sustainability_fact (
    fact_id                    INT PRIMARY KEY AUTO_INCREMENT,
    sustainability_rating      FLOAT,
    eco_friendly_manufacturing VARCHAR(255),
    carbon_footprint           FLOAT,
    water_usage                INT,
    waste_production           FLOAT,
    recycling_programs         VARCHAR(255),
    average_price              FLOAT,
    product_line               VARCHAR(255),
    sustainability_status      VARCHAR(255),
    sustainability_level       VARCHAR(255),

    -- Foreign key columns
    brand_id           INT,
    country_id         INT,
    year_id            INT,
    material_id        INT,
    product_category_id INT,
    certification_id   INT,
    market_trend_id    INT,

    -- Foreign key constraints
    FOREIGN KEY (brand_id) REFERENCES brand_dim (brand_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (country_id) REFERENCES country_dim (country_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (year_id) REFERENCES year_dim (year_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (material_id) REFERENCES material_dim (material_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (product_category_id) REFERENCES product_category_dim (product_category_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (certification_id) REFERENCES certification_dim (certification_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (market_trend_id) REFERENCES market_trend_dim (market_trend_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- ======================================================
-- Populate dimension tables from raw_data
-- ======================================================

-- brand_dim
INSERT INTO brand_dim (brand_name, brand_category)
SELECT DISTINCT brand_name, brand_category
FROM raw_data
WHERE brand_name IS NOT NULL;

-- certification_dim
INSERT INTO certification_dim (certification, cert_type)
SELECT DISTINCT certification, cert_type
FROM raw_data
WHERE certification IS NOT NULL;

-- country_dim
INSERT INTO country_dim (country_name, region)
SELECT DISTINCT country_name, region
FROM raw_data
WHERE country_name IS NOT NULL;

-- market_trend_dim
INSERT INTO market_trend_dim (market_trend, trend_score)
SELECT DISTINCT market_trend, trend_score
FROM raw_data
WHERE market_trend IS NOT NULL;

-- material_dim
INSERT INTO material_dim (material_type, renewable)
SELECT DISTINCT material_type, renewable
FROM raw_data
WHERE material_type IS NOT NULL;

-- product_category_dim
INSERT INTO product_category_dim (product_line, target_audience)
SELECT DISTINCT product_line, target_audience
FROM raw_data
WHERE product_line IS NOT NULL;

-- year_dim
INSERT INTO year_dim (year)
SELECT DISTINCT year
FROM raw_data
WHERE year IS NOT NULL;

-- ======================================================
-- Populate fact table by joining raw_data to dimension rows
-- ======================================================
INSERT INTO sustainability_fact (
    sustainability_rating,
    eco_friendly_manufacturing,
    carbon_footprint,
    water_usage,
    waste_production,
    recycling_programs,
    average_price,
    product_line,
    sustainability_status,
    sustainability_level,

    brand_id,
    country_id,
    year_id,
    material_id,
    product_category_id,
    certification_id,
    market_trend_id
)
SELECT DISTINCT
    r.sustainability_rating,
    r.eco_friendly_manufacturing,
    r.carbon_footprint,
    r.water_usage,
    r.waste_production,
    r.recycling_programs,
    r.average_price,
    r.product_line,
    r.sustainability_status,
    r.sustainability_level,

    b.brand_id,
    c.country_id,
    y.year_id,
    m.material_id,
    p.product_category_id,
    cert.certification_id,
    mt.market_trend_id
FROM raw_data r
LEFT JOIN brand_dim b ON r.brand_name = b.brand_name
LEFT JOIN country_dim c ON r.country_name = c.country_name
LEFT JOIN year_dim y ON r.year = y.year
LEFT JOIN material_dim m ON r.material_type = m.material_type
LEFT JOIN product_category_dim p ON r.product_line = p.product_line
LEFT JOIN certification_dim cert ON r.certification = cert.certification
LEFT JOIN market_trend_dim mt ON r.market_trend = mt.market_trend
WHERE r.sustainability_rating IS NOT NULL;


-- ======================================================
--  preprocessing steps 
-- ======================================================

-- Rename column 'renewable' to 'material_status' in material_dim
ALTER TABLE material_dim
    CHANGE COLUMN renewable material_status VARCHAR(255);


-- Rename 'product_line' to 'product_category' in product_category_dim
ALTER TABLE product_category_dim
    CHANGE COLUMN product_line product_category VARCHAR(255);

-- Fix region naming: 'Americas' -> 'America' in country_dim
UPDATE country_dim
SET region = 'America'
WHERE region = 'Americas';

-- Replace 'None' with 'Not Certified' in certification_dim
UPDATE certification_dim
SET certification = 'Not Certified',
    cert_type = 'Not Certified'
WHERE certification = 'None'
  AND cert_type = 'None';

-- ==========================================================================
-- Statistics (Finding Correlation between
--  Sustainability_rating ,
--  carbon_footprint,
--  waste_production,
--  water_usge,
--  average_price ) using (Pearson rule)
-- ==========================================================================
SELECT 'sustainability_rating vs carbon_footprint' AS pair,
       ROUND(
         (AVG(sustainability_rating * carbon_footprint) - AVG(sustainability_rating) * AVG(carbon_footprint))
         / (STD(sustainability_rating) * STD(carbon_footprint)), 2
       ) AS correlation
FROM raw_data

UNION ALL
SELECT 'sustainability_rating vs waste_production',
       ROUND(
         (AVG(sustainability_rating * waste_production) - AVG(sustainability_rating) * AVG(waste_production))
         / (STD(sustainability_rating) * STD(waste_production)), 2
       )
FROM raw_data

UNION ALL
SELECT 'sustainability_rating vs water_usage',
       ROUND(
         (AVG(sustainability_rating * water_usage) - AVG(sustainability_rating) * AVG(water_usage))
         / (STD(sustainability_rating) * STD(water_usage)), 2
       )
FROM raw_data

UNION ALL
SELECT 'sustainability_rating vs average_price',
       ROUND(
         (AVG(sustainability_rating * average_price) - AVG(sustainability_rating) * AVG(average_price))
         / (STD(sustainability_rating) * STD(average_price)), 2
       )
FROM raw_data;


-- ======================================================
-- ======================== KPIs =========================
CREATE VIEW KPIs AS
SELECT
    --  Averages
    ROUND(AVG(average_price)) AS average_price,
    ROUND(AVG(carbon_footprint)) AS average_footprint,
    ROUND(AVG(water_usage)) AS average_water_usage,
    ROUND(AVG(waste_production)) AS average_waste_production,

    --  Min & Max sustainability rating
    MIN(sustainability_rating) AS min_sustainability_rating,
    MAX(sustainability_rating) AS max_sustainability_rating,

    --  Number of eco-friendly manufacturing brands
    (SELECT COUNT(*) 
     FROM sustainability_fact 
     WHERE eco_friendly_manufacturing = 'yes') AS number_of_eco_manufacturing_brands,

    --  Number of non eco-friendly manufacturing brands
    (SELECT COUNT(*) 
     FROM sustainability_fact 
     WHERE eco_friendly_manufacturing = 'no') AS number_of_non_eco_manufacturing_brands,

    --  Number of brands that have recycling programs
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE recycling_programs = 'yes') AS brands_have_recycling_programs,

    --  Number of brands that do not have recycling programs
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE recycling_programs = 'no') AS brands_have_no_recycling_programs,

    --  Number of fully sustainable brands
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_status = 'fully sustainable') AS number_of_brands_who_are_fully_sustainable,

    --  Number of partially sustainable brands
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_status = 'partially sustainable') AS number_of_brands_who_are_partially_sustainable,

    --  Number of not sustainable brands
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_status = 'not sustainable') AS number_of_brands_who_are_not_sustainable,

    --  Sustainability level: High
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_level = 'high') AS number_of_high_sustainability_level_brands,

    --  Sustainability level: Medium
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_level = 'medium') AS number_of_medium_sustainability_level_brands,

    --  Sustainability level: Low
    (SELECT COUNT(*)
     FROM sustainability_fact
     WHERE sustainability_level = 'low') AS number_of_low_sustainability_level_brands
FROM sustainability_fact;

-- Showing KPIs in one table 
SELECT * FROM KPIs;


-- =========================================================================
-- Analysis Queries
-- =========================================================================
-- Product categories with max sustainability rating (sustainability_rating = 5)
SELECT
    pdm.product_category,
    ROUND(AVG(sf.sustainability_rating),3) AS avg_rating
FROM product_category_dim AS pdm
LEFT JOIN sustainability_fact AS sf
    ON pdm.product_category_id = sf.product_category_id
WHERE sf.sustainability_rating = 5
GROUP BY pdm.product_category
ORDER BY avg_rating DESC;

-- ====================================================

-- Product categories where AVG water_usage < 10000
SELECT
    product_category,
    ROUND(AVG(water_usage),2) AS average_water_usage
FROM product_category_dim AS pdm
LEFT JOIN sustainability_fact AS sf
    ON pdm.product_category_id = sf.product_category_id
GROUP BY product_category
HAVING  ROUND(AVG(water_usage),2) < 10000  -- average = 833.93
ORDER BY average_water_usage DESC;

-- ======================================================
-- Product categories where AVG waste_production < 7
SELECT
    product_category,
    ROUND(AVG(waste_production),2) AS average_waste_production
FROM product_category_dim AS pdm
LEFT JOIN sustainability_fact AS sf
    ON pdm.product_category_id = sf.product_category_id
GROUP BY product_category
HAVING ROUND(AVG(waste_production),2) < 7  -- average = 6.71
ORDER BY average_waste_production DESC;

-- ==========================================================

-- Product categories where AVG price < 30
SELECT
    product_category,
    ROUND(AVG(average_price),2) AS average_price
FROM product_category_dim AS pdm
LEFT JOIN sustainability_fact AS sf
    ON pdm.product_category_id = sf.product_category_id
GROUP BY product_category
HAVING ROUND(AVG(average_price),2) < 30  -- average = 30.56
ORDER BY average_price DESC;

-- ===========================================================
-- Product categories where AVG carbon_footprint < 34
SELECT
    product_category,
    ROUND(AVG(carbon_footprint),2) AS average_carbon_footprint
FROM product_category_dim AS pdm
LEFT JOIN sustainability_fact AS sf
    ON pdm.product_category_id = sf.product_category_id
GROUP BY product_category
HAVING ROUND(AVG(carbon_footprint),2) < 34  -- average = 34.59
ORDER BY average_carbon_footprint;

-- ============================================================

-- Top 10 sustainable brands (highest avg rating)
SELECT
    b.brand_name,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN brand_dim b
    ON f.brand_id = b.brand_id
GROUP BY b.brand_name
ORDER BY avg_rating DESC
LIMIT 10;

-- =============================================================
-- Bottom 5 brands (lowest avg rating)
SELECT
    b.brand_name,
    ROUND(AVG(sf.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact AS sf
JOIN brand_dim b
    ON sf.brand_id = b.brand_id
GROUP BY b.brand_name
ORDER BY avg_rating ASC
LIMIT 5;

-- =============================================================
-- Top 5 product categories in terms of sustainability
SELECT
    p.product_category,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN product_category_dim p
    ON f.product_category_id = p.product_category_id
GROUP BY p.product_category
ORDER BY avg_rating DESC
LIMIT 5;

-- ==============================================================

-- Top 5 brands with recycling programs (distinct)
SELECT DISTINCT
    brand_name,
    recycling_programs
FROM sustainability_fact AS sf
INNER JOIN brand_dim AS b
    ON sf.brand_id = b.brand_id
WHERE recycling_programs = 'Yes'
  AND sustainability_rating = 5
LIMIT 5;
 -- ============================================================
-- Top 5 countries by average sustainability rating
SELECT DISTINCT
    c.country_name,
    ROUND(AVG(sustainability_rating), 3) AS average_sustainability
FROM sustainability_fact AS sf
INNER JOIN country_dim AS c
    ON sf.country_id = c.country_id
GROUP BY country_name
ORDER BY average_sustainability DESC
LIMIT 5;

-- ==============================================================

-- Countries with highest sustainability score (avg)
SELECT
    c.country_name,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN country_dim c
    ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_rating DESC;

-- =============================================================

-- Top 3 regions by average sustainability
SELECT DISTINCT
    region,
    ROUND(AVG(sustainability_rating), 3) AS average_sustianability
FROM sustainability_fact AS sf
INNER JOIN country_dim AS c
    ON sf.country_id = c.country_id
GROUP BY region
ORDER BY average_sustianability DESC
LIMIT 3;
-- =============================================================
-- Material types that are fully renewable
SELECT material_type
FROM material_dim
WHERE material_status = 'Fully Renewable';
-- =============================================================
-- Material types that are partially renewable
SELECT material_type
FROM material_dim
WHERE material_status = 'Partially Renewable';
-- ==============================================================
-- Material types that are not renewable
SELECT material_type
FROM material_dim
WHERE material_status = 'Not Renewable';
-- ==================================================================
-- Brand category average sustainability
SELECT
    brand_category,
    ROUND(AVG(sustainability_rating), 3) AS average_sustainability_rating
FROM sustainability_fact AS sf
INNER JOIN brand_dim AS b
    ON sf.brand_id = b.brand_id
GROUP BY brand_category;
-- ===================================================================
-- Number of certifications per brand category
SELECT
    brand_category,
    COUNT(DISTINCT certification) AS number_of_certifications
FROM brand_dim AS b
INNER JOIN sustainability_fact AS sf
    ON b.brand_id = sf.brand_id
INNER JOIN certification_dim AS c
    ON sf.certification_id = c.certification_id
GROUP BY brand_category;
 
 -- =====================================================================
-- Product categories and the certifications they have
SELECT DISTINCT
    product_category,
    COUNT(certification) AS num_certification
FROM product_category_dim AS pcd
INNER JOIN sustainability_fact AS sf
    ON pcd.product_category_id = sf.product_category_id
INNER JOIN certification_dim AS c
    ON sf.certification_id = c.certification_id
GROUP BY product_category
ORDER BY COUNT(certification) DESC;

-- =======================================================================
-- AVG waste production, water usage and carbon footprint per product category
SELECT
    product_category,
    ROUND(AVG(waste_production),2) AS average_waste_production,
    ROUND(AVG(water_usage),2) AS average_water_usage,
    ROUND(AVG(carbon_footprint),2) AS average_carbon_footprint
FROM sustainability_fact AS sf
INNER JOIN product_category_dim AS pcd
    ON sf.product_category_id = pcd.product_category_id
GROUP BY product_category
ORDER BY average_waste_production,
         average_water_usage,
         average_carbon_footprint DESC;
-- ==============================================================================
-- Sustainability performance over years (avg metrics)
SELECT
    year,
    ROUND(AVG(waste_production),2) AS average_waste_production,
    ROUND(AVG(water_usage),2)  AS average_water_usage,
    ROUND(AVG(carbon_footprint),2) AS average_carbon_footprint
FROM sustainability_fact AS sf
INNER JOIN year_dim AS y
    ON sf.year_id = y.year_id
GROUP BY year
ORDER BY year DESC;

-- ==============================================================================
-- Sustainability improvements over time (avg rating)
SELECT
    y.year,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN year_dim y
    ON f.year_id = y.year_id
GROUP BY y.year
ORDER BY y.year;

-- ================================================================================
-- Target audience with greatest carbon footprint (avg)
SELECT
    target_audience,
    ROUND(AVG(carbon_footprint),2) AS average_carbon_footprint
FROM sustainability_fact AS sf
INNER JOIN product_category_dim AS pcd
    ON sf.product_category_id = pcd.product_category_id
GROUP BY target_audience
ORDER BY average_carbon_footprint DESC;

-- ================================================================================
-- Sustainability by target audience (avg rating)
SELECT
    p.target_audience,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN product_category_dim p
    ON f.product_category_id = p.product_category_id
GROUP BY p.target_audience
ORDER BY avg_rating DESC;
-- =================================================================================
-- Countries with highest total water usage (top 10)
SELECT
    c.country_name,
    SUM(f.water_usage) AS total_water
FROM sustainability_fact f
JOIN country_dim c
    ON f.country_id = c.country_id
GROUP BY c.country_name
ORDER BY total_water DESC
LIMIT 10;
-- =================================================================================
-- Renewable vs Unrenewable (avg rating by material status)
SELECT
    m.material_status,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN material_dim m
    ON f.material_id = m.material_id
GROUP BY m.material_status;
 -- ================================================================================
-- Waste production for every material type (avg)
SELECT
    m.material_type,
    ROUND(AVG(f.waste_production),2) AS avg_waste
FROM sustainability_fact f
JOIN material_dim m
    ON f.material_id = m.material_id
GROUP BY m.material_type
ORDER BY avg_waste ASC;
-- ================================================================================
-- Which brand in each country is most sustainable?
SELECT
    c.country_name,
    b.brand_name,
    ROUND(AVG(f.sustainability_rating),3) AS avg_rating
FROM sustainability_fact f
JOIN brand_dim b
    ON f.brand_id = b.brand_id
JOIN country_dim c
    ON f.country_id = c.country_id
GROUP BY c.country_name, b.brand_name
ORDER BY c.country_name, avg_rating DESC;
 
 -- ================================================================================
-- Trend score impact on sustainability (avg rating by trend_score)
SELECT
    mt.trend_score,
    ROUND(AVG(f.sustainability_rating),3) AS avg_rating
FROM sustainability_fact f
JOIN market_trend_dim mt
    ON f.market_trend_id = mt.market_trend_id
GROUP BY mt.trend_score
ORDER BY avg_rating DESC;

-- ===============================================================================
-- Does higher price mean higher sustainability? (price ranges)
SELECT
    CASE
        WHEN average_price < 50 THEN 'Low Price'
        WHEN average_price BETWEEN 50 AND 150 THEN 'Medium Price'
        ELSE 'High Price'
    END AS price_range,
    ROUND(AVG(sustainability_rating), 3) AS avg_rating
FROM sustainability_fact
GROUP BY price_range
ORDER BY avg_rating DESC;
 
 
 -- ============================================================================
-- Recycling programs vs waste (avg waste by recycling_programs)
SELECT
    f.recycling_programs,
    ROUND(AVG(f.waste_production),2) AS avg_waste
FROM sustainability_fact f
GROUP BY f.recycling_programs
ORDER BY avg_waste ASC;

-- =============================================================================
-- Does certification improve sustainability? (avg rating by certification)
SELECT
    c.certification,
    ROUND(AVG(f.sustainability_rating), 3) AS avg_rating
FROM sustainability_fact f
JOIN certification_dim c
    ON f.certification_id = c.certification_id
GROUP BY c.certification
ORDER BY avg_rating DESC;

-- =============================================================================

-- Waste production by material type (avg)
SELECT
    m.material_type,
    ROUND(AVG(f.waste_production),2) AS avg_waste
FROM sustainability_fact f
JOIN material_dim m
    ON f.material_id = m.material_id
GROUP BY m.material_type
ORDER BY avg_waste ASC;
