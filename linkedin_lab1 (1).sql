-- ============================================================
-- PROJET : Analyse des offres d'emploi LinkedIn avec Snowflake
-- Architecture Medallion : Bronze / Silver / Gold
-- ============================================================

-- ============================================================
-- 0. DATABASE + SCHEMAS
-- ============================================================
CREATE DATABASE IF NOT EXISTS linkedin_lab;
USE DATABASE linkedin_lab;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. STAGE + FILE FORMATS
-- ============================================================
USE SCHEMA bronze;

CREATE OR REPLACE STAGE linkedin_stage
  URL = 's3://snowflake-lab-bucket/'
  COMMENT = 'Stage S3 public contenant les fichiers LinkedIn';

LIST @linkedin_stage;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', 'N/A', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  COMMENT = 'Format CSV standard';

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE
  COMMENT = 'Format JSON avec tableau racine';

-- ============================================================
-- 2. BRONZE = DONNEES BRUTES
-- Données chargées telles quelles depuis le stage S3
-- ============================================================

-- ------------------------------------------------------------
-- 2.1 BRONZE.JOB_POSTINGS
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_postings (
   job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    med_salary STRING,
    min_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    original_listed_time STRING,
    remote_allowed STRING,
    views STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

COPY INTO bronze.job_postings
FROM @bronze.linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);
-- ------------------------------------------------------------
-- 2.2 BRONZE.BENEFITS
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.benefits (
  job_id   VARCHAR,
  inferred BOOLEAN,
  type     VARCHAR
);

COPY INTO bronze.benefits
FROM @bronze.linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);

-- ------------------------------------------------------------
-- 2.3 BRONZE.JOB_SKILLS
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_skills (
  job_id    VARCHAR,
  skill_abr VARCHAR
);

COPY INTO bronze.job_skills
FROM @bronze.linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format);


-- ------------------------------------------------------------
-- 2.4 BRONZE.JOB_INDUSTRIES
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_industries (
  job_id      VARCHAR,
  industry_id VARCHAR
);

COPY INTO bronze.job_industries
FROM (
    SELECT
        $1:job_id::VARCHAR,
        $1:industry_id::VARCHAR
    FROM @bronze.linkedin_stage/job_industries.json
    (FILE_FORMAT => bronze.json_format)
);


-- ============================================================
-- 3. SILVER = DONNEES NETTOYEES
-- Nettoyage, standardisation, suppression des doublons,
-- filtrage des lignes incomplètes et contrôle de cohérence
-- ============================================================
USE SCHEMA silver;
-- ------------------------------------------------------------
-- 3.1 SILVER.JOB_POSTINGS
-- Nettoyage :
-- - suppression des doublons sur job_id
-- - suppression des lignes avec clés vides
-- - trim sur les textes
-- - standardisation de certaines colonnes
-- - remplacement de valeurs nulles
-- - contrôle de cohérence métier
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_postings AS
SELECT
    TRIM(job_id) AS job_id,
    COALESCE(NULLIF(TRIM(company_name), ''), 'Non spécifiée') AS company_name,
    UPPER(TRIM(title)) AS title,
    TRIM(description) AS description,
    max_salary,
    med_salary,
    min_salary,
    UPPER(TRIM(pay_period)) AS pay_period,
    TRIM(formatted_work_type) AS formatted_work_type,
    TRIM(location) AS location,
    COALESCE(applies, 0) AS applies,
    original_listed_time,
    COALESCE(remote_allowed, FALSE) AS remote_allowed,
    COALESCE(views, 0) AS views,
    TRIM(job_posting_url) AS job_posting_url,
    TRIM(application_url) AS application_url,
    TRIM(application_type) AS application_type,
    expiry,
    closed_time,
    TRIM(formatted_experience_level) AS formatted_experience_level,
    TRIM(skills_desc) AS skills_desc,
    listed_time,
    TRIM(posting_domain) AS posting_domain,
    COALESCE(sponsored, FALSE) AS sponsored,
    TRIM(work_type) AS work_type,
    UPPER(TRIM(currency)) AS currency,
    TRIM(compensation_type) AS compensation_type
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY job_id
               ORDER BY listed_time DESC NULLS LAST
           ) AS rn
    FROM bronze.job_postings
    WHERE job_id IS NOT NULL
      AND TRIM(job_id) <> ''
      AND title IS NOT NULL
      AND TRIM(title) <> ''
      AND location IS NOT NULL
      AND TRIM(location) <> ''
)
WHERE rn = 1
  AND (min_salary IS NULL OR min_salary >= 0)
  AND (med_salary IS NULL OR med_salary >= 0)
  AND (max_salary IS NULL OR max_salary >= 0)
  AND (
        min_salary IS NULL
        OR max_salary IS NULL
        OR min_salary <= max_salary
      );
  -- ------------------------------------------------------------
-- 3.2 SILVER.BENEFITS
-- Nettoyage :
-- - suppression des lignes vides
-- - trim
-- - remplacement de inferred null par FALSE
-- - suppression des doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.benefits AS
SELECT DISTINCT
    TRIM(job_id) AS job_id,
    COALESCE(inferred, FALSE) AS inferred,
    TRIM(type) AS type
FROM bronze.benefits
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND type IS NOT NULL
  AND TRIM(type) <> '';

-- ------------------------------------------------------------
-- 3.3 SILVER.JOB_SKILLS
-- Nettoyage :
-- - suppression des lignes vides
-- - trim
-- - standardisation skill_abr
-- - suppression des doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_skills AS
SELECT DISTINCT
    TRIM(job_id) AS job_id,
    UPPER(TRIM(skill_abr)) AS skill_abr
FROM bronze.job_skills
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND skill_abr IS NOT NULL
  AND TRIM(skill_abr) <> '';

-- ------------------------------------------------------------
-- 3.4 SILVER.JOB_INDUSTRIES
-- Nettoyage :
-- - suppression des lignes vides
-- - trim
-- - suppression des doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_industries AS
SELECT DISTINCT
    TRIM(job_id) AS job_id,
    TRIM(industry_id) AS industry_id
FROM bronze.job_industries
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND industry_id IS NOT NULL
  AND TRIM(industry_id) <> '';

-- ============================================================
-- 4. GOLD = DONNEES PRETES POUR ANALYSE
-- Tables enrichies et orientées métier
-- ============================================================
USE SCHEMA gold;

-- ------------------------------------------------------------
-- 4.1 GOLD.JOB_POSTINGS
-- Table métier principale
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_postings AS
SELECT
    job_id,
    company_name,
    title,
    max_salary,
    med_salary,
    min_salary,
    pay_period,
    formatted_work_type,
    location,
    applies,
    views,
    job_posting_url,
    application_url,
    application_type,
    formatted_experience_level,
    skills_desc,
    listed_time,
    work_type,
    currency,
    compensation_type,
    remote_allowed,
    sponsored
FROM silver.job_postings
WHERE title IS NOT NULL
  AND TRIM(title) <> ''
  AND location IS NOT NULL
  AND TRIM(location) <> '';

-- ------------------------------------------------------------
-- 4.2 GOLD.BENEFITS
-- Table enrichie avec le titre et l'entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.benefits AS
SELECT
    b.job_id,
    jp.title,
    jp.company_name,
    b.inferred,
    b.type AS benefit_type
FROM silver.benefits b
JOIN silver.job_postings jp
    ON b.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.3 GOLD.JOB_SKILLS
-- Table enrichie avec le titre, entreprise et localisation
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_skills AS
SELECT
    js.job_id,
    jp.title,
    jp.company_name,
    jp.location,
    jp.formatted_work_type,
    js.skill_abr
FROM silver.job_skills js
JOIN silver.job_postings jp
    ON js.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.4 GOLD.JOB_INDUSTRIES
-- Table enrichie avec le titre, entreprise et localisation
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_industries AS
SELECT
    ji.job_id,
    jp.title,
    jp.company_name,
    jp.location,
    jp.formatted_work_type,
    ji.industry_id
FROM silver.job_industries ji
JOIN silver.job_postings jp
    ON ji.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.5 GOLD.JOB_POSTINGS_FULL
-- Table enrichie complète
-- Attention : une offre peut avoir plusieurs compétences,
-- plusieurs avantages et plusieurs industries
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_postings_full AS
SELECT
    jp.job_id,
    jp.company_name,
    jp.title,
    jp.location,
    jp.formatted_work_type,
    jp.max_salary,
    jp.med_salary,
    jp.min_salary,
    jp.pay_period,
    jp.applies,
    jp.views,
    jp.remote_allowed,
    jp.sponsored,
    ji.industry_id,
    js.skill_abr,
    gb.benefit_type,
    gb.inferred AS benefit_inferred
FROM gold.job_postings jp
LEFT JOIN gold.job_industries ji
    ON jp.job_id = ji.job_id
LEFT JOIN gold.job_skills js
    ON jp.job_id = js.job_id
LEFT JOIN gold.benefits gb
    ON jp.job_id = gb.job_id;

-- ============================================================
-- 5. CONTROLES DE VOLUMETRIE
-- Vérification du nombre de lignes à chaque couche
-- ============================================================
SELECT * FROM bronze.job_postings;
SELECT * FROM bronze.benefits;
SELECT * FROM bronze.job_skills;
SELECT * FROM bronze.job_industries;

SELECT * FROM silver.job_postings;
SELECT * FROM silver.benefits;
SELECT * FROM silver.job_skills;
SELECT * FROM silver.job_industries;



SELECT * FROM gold.job_postings;
SELECT * FROM gold.benefits;
SELECT * FROM gold.job_skills;
SELECT * FROM gold.job_industries;




SELECT COUNT(*) AS bronze_job_postings_count FROM bronze.job_postings;
SELECT COUNT(*) AS silver_job_postings_count FROM silver.job_postings;
SELECT COUNT(*) AS gold_job_postings_count   FROM gold.job_postings;

SELECT COUNT(*) AS bronze_benefits_count FROM bronze.benefits;
SELECT COUNT(*) AS silver_benefits_count FROM silver.benefits;
SELECT COUNT(*) AS gold_benefits_count   FROM gold.benefits;

SELECT COUNT(*) AS bronze_job_skills_count FROM bronze.job_skills;
SELECT COUNT(*) AS silver_job_skills_count FROM silver.job_skills;
SELECT COUNT(*) AS gold_job_skills_count   FROM gold.job_skills;

SELECT COUNT(*) AS bronze_job_industries_count FROM bronze.job_industries;
SELECT COUNT(*) AS silver_job_industries_count FROM silver.job_industries;
SELECT COUNT(*) AS gold_job_industries_count   FROM gold.job_industries;

-- ============================================================
-- 6. CONTROLES QUALITE - UNICITE / DOUBLONS EXACTS
-- ============================================================

-- 6.1 job_postings : unicité de job_id
SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT job_id) AS total_job_id_distincts
FROM linkedin_lab.silver.job_postings;

SELECT
    job_id,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_postings
GROUP BY job_id
HAVING COUNT(*) > 1;

-- 6.2 benefits : unicité de (job_id, type)
SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', type)) AS total_job_benefit_distincts
FROM linkedin_lab.silver.benefits;

SELECT
    job_id,
    type,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.benefits
GROUP BY job_id, type
HAVING COUNT(*) > 1;

-- 6.3 job_skills : unicité de (job_id, skill_abr)
SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', skill_abr)) AS total_job_skill_distincts
FROM linkedin_lab.silver.job_skills;

SELECT
    job_id,
    skill_abr,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_skills
GROUP BY job_id, skill_abr
HAVING COUNT(*) > 1;

-- 6.4 job_industries : unicité de (job_id, industry_id)
SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', industry_id)) AS total_job_industry_distincts
FROM linkedin_lab.silver.job_industries;

SELECT
    job_id,
    industry_id,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_industries
GROUP BY job_id, industry_id
HAVING COUNT(*) > 1;

-- ============================================================
-- 7. CONTROLES QUALITE - CHAINES VIDES
-- ============================================================

-- 7.1 job_postings
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR title IS NULL OR TRIM(title) = ''
   OR location IS NULL OR TRIM(location) = '';

-- 7.2 benefits
SELECT *
FROM linkedin_lab.silver.benefits
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR type IS NULL OR TRIM(type) = '';

-- 7.3 job_skills
SELECT *
FROM linkedin_lab.silver.job_skills
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR skill_abr IS NULL OR TRIM(skill_abr) = '';

-- 7.4 job_industries
SELECT *
FROM linkedin_lab.silver.job_industries
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR industry_id IS NULL OR TRIM(industry_id) = '';

-- ============================================================
-- 8. CONTROLES QUALITE - COHERENCE METIER JOB_POSTINGS
-- ============================================================

-- 8.1 salaires incohérents
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE min_salary IS NOT NULL
  AND max_salary IS NOT NULL
  AND min_salary > max_salary;

-- 8.2 dates incohérentes
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE listed_time IS NOT NULL
  AND expiry IS NOT NULL
  AND listed_time > expiry;

-- 8.3 valeurs négatives
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE (views < 0)
   OR (applies < 0)
   OR (min_salary < 0)
   OR (med_salary < 0)
   OR (max_salary < 0);

-- ============================================================
-- 9. CONTROLES QUALITE - VALEURS ABERRANTES
-- Seuil simple à ajuster selon le contexte
-- ============================================================

SELECT *
FROM linkedin_lab.silver.job_postings
WHERE max_salary > 1000000
   OR med_salary > 1000000
   OR min_salary > 1000000;

-- ============================================================
-- 10. CONTROLES QUALITE - TABLES GOLD
-- ============================================================

SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT job_id) AS total_job_id_distincts
FROM linkedin_lab.gold.job_postings;

SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', benefit_type)) AS total_job_benefit_distincts
FROM linkedin_lab.gold.benefits;

SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', skill_abr)) AS total_job_skill_distincts
FROM linkedin_lab.gold.job_skills;

SELECT
    COUNT(*) AS total_lignes,
    COUNT(DISTINCT CONCAT(job_id, '|', industry_id)) AS total_job_industry_distincts
FROM linkedin_lab.gold.job_industries;

-- ============================================================
-- 11. EXEMPLES DE REQUETES D'ANALYSE
-- ============================================================

-- 11.1 Top 10 des titres de postes les plus publiés par industrie
SELECT
    industry_id,
    title,
    COUNT(*) AS nb_offres,
    ROW_NUMBER() OVER (
        PARTITION BY industry_id
        ORDER BY COUNT(*) DESC
    ) AS rang
FROM linkedin_lab.gold.job_industries
GROUP BY industry_id, title
QUALIFY rang <= 10
ORDER BY industry_id, rang;

-- 11.2 Top 10 des postes les mieux rémunérés par industrie
SELECT
    ji.industry_id,
    jp.title,
    ROUND(AVG(jp.max_salary), 2) AS avg_max_salary,
    ROW_NUMBER() OVER (
        PARTITION BY ji.industry_id
        ORDER BY AVG(jp.max_salary) DESC
    ) AS rang
FROM linkedin_lab.gold.job_postings jp
JOIN linkedin_lab.gold.job_industries ji
    ON jp.job_id = ji.job_id
WHERE jp.max_salary IS NOT NULL
  AND jp.pay_period = 'YEARLY'
GROUP BY ji.industry_id, jp.title
QUALIFY rang <= 10
ORDER BY ji.industry_id, rang;

-- 11.3 Répartition des offres d’emploi par type d’emploi
SELECT
    formatted_work_type AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings
GROUP BY formatted_work_type
ORDER BY nb_offres DESC;

-- 11.4 Compétences les plus demandées
SELECT
    skill_abr,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.job_skills
GROUP BY skill_abr
ORDER BY nb_offres DESC
LIMIT 10;

-- 11.5 Avantages les plus fréquents
SELECT
    benefit_type,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.benefits
GROUP BY benefit_type
ORDER BY nb_offres DESC
LIMIT 10;



