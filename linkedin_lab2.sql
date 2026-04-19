-- ============================================================
-- PROJET : Analyse des offres d'emploi LinkedIn avec Snowflake
-- Architecture Medallion : Bronze / Silver / Gold
-- Binôme : Partie 1 (job_postings, benefits, job_skills, job_industries)
--          Partie 2 (companies, employee_counts, company_industries, company_specialities)
-- ============================================================

-- ============================================================
-- 0. DATABASE + SCHEMAS
-- ============================================================

-- Création de la base de données principale du projet
CREATE DATABASE IF NOT EXISTS linkedin_lab;
-- Sélection de la base de données pour toutes les opérations suivantes
USE DATABASE linkedin_lab;

-- Création des trois schémas de l'architecture Medallion
CREATE SCHEMA IF NOT EXISTS bronze;  -- Données brutes chargées depuis S3
CREATE SCHEMA IF NOT EXISTS silver;  -- Données nettoyées et standardisées
CREATE SCHEMA IF NOT EXISTS gold;    -- Données enrichies prêtes pour l'analyse

-- Sélection du warehouse de calcul qui va exécuter les requêtes
USE WAREHOUSE COMPUTE_WH;

-- ============================================================
-- 1. STAGE + FILE FORMATS
-- Configuration de l'accès au bucket S3 et des formats de fichiers
-- ============================================================

USE SCHEMA bronze;

-- Création du stage externe pointant vers le bucket S3 public
-- Le stage permet à Snowflake de lire les fichiers directement depuis S3
CREATE OR REPLACE STAGE linkedin_stage
  URL = 's3://snowflake-lab-bucket/'
  COMMENT = 'Stage S3 public contenant les fichiers LinkedIn';

-- Liste les fichiers disponibles dans le stage pour vérification
LIST @linkedin_stage;

-- Format CSV : pour les fichiers tabulaires (job_postings, benefits, job_skills, employee_counts)
-- FIELD_OPTIONALLY_ENCLOSED_BY : gère les champs entre guillemets
-- SKIP_HEADER = 1 : ignore la première ligne (en-têtes de colonnes)
-- NULL_IF : convertit ces valeurs en NULL lors du chargement
-- EMPTY_FIELD_AS_NULL : les champs vides deviennent NULL
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

-- Format JSON : pour les fichiers structurés (companies, job_industries, company_industries, company_specialities)
-- STRIP_OUTER_ARRAY = TRUE : retire le tableau racine [] pour charger chaque objet séparément
CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE
  COMMENT = 'Format JSON avec tableau racine';

-- ============================================================
-- 2. BRONZE : CHARGEMENT DES DONNEES BRUTES
-- Les données sont chargées telles quelles depuis S3, sans transformation
-- ============================================================

-- ------------------------------------------------------------
-- 2.1 BRONZE.JOB_POSTINGS
-- Contient les informations détaillées sur chaque offre d'emploi
-- Note : la colonne company_name contient en réalité des IDs numériques
--        elle sera renommée company_id dans silver après découverte
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_postings (
  job_id                     VARCHAR,  -- Identifiant unique de l'offre d'emploi
  company_id                 VARCHAR,  -- ID de l'entreprise (renommé depuis company_name car le CSV contient des IDs)
  title                      VARCHAR,  -- Titre du poste
  description                TEXT,     -- Description détaillée du poste
  max_salary                 FLOAT,    -- Salaire maximum proposé
  med_salary                 FLOAT,    -- Salaire médian proposé
  min_salary                 FLOAT,    -- Salaire minimum proposé
  pay_period                 VARCHAR,  -- Période de paiement (horaire, mensuel, annuel)
  formatted_work_type        VARCHAR,  -- Type de contrat formaté (temps plein, partiel, etc.)
  location                   VARCHAR,  -- Localisation du poste
  applies                    INT,      -- Nombre de candidatures reçues
  original_listed_time       BIGINT,   -- Timestamp de la première publication
  remote_allowed             BOOLEAN,  -- Indique si le télétravail est autorisé
  views                      INT,      -- Nombre de vues de l'offre
  job_posting_url            VARCHAR,  -- URL de l'offre sur LinkedIn
  application_url            VARCHAR,  -- URL pour postuler
  application_type           VARCHAR,  -- Type de candidature (sur site ou externe)
  expiry                     BIGINT,   -- Timestamp d'expiration de l'offre
  closed_time                BIGINT,   -- Timestamp de fermeture de l'offre
  formatted_experience_level VARCHAR,  -- Niveau d'expérience requis
  skills_desc                TEXT,     -- Description des compétences requises
  listed_time                BIGINT,   -- Timestamp de mise en ligne
  posting_domain             VARCHAR,  -- Domaine du site de candidature
  sponsored                  BOOLEAN,  -- Indique si l'offre est sponsorisée
  work_type                  VARCHAR,  -- Type de travail
  currency                   VARCHAR,  -- Devise du salaire
  compensation_type          VARCHAR   -- Type de rémunération
);

-- Chargement depuis le fichier CSV
COPY INTO bronze.job_postings
FROM @bronze.linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.2 BRONZE.BENEFITS
-- Contient les avantages associés à chaque offre d'emploi
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.benefits (
  job_id   VARCHAR,  -- Identifiant de l'offre d'emploi
  inferred BOOLEAN,  -- TRUE si l'avantage a été inféré par LinkedIn, FALSE si déclaré
  type     VARCHAR   -- Type d'avantage (401K, assurance médicale, etc.)
);

-- Chargement depuis le fichier CSV
COPY INTO bronze.benefits
FROM @bronze.linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.3 BRONZE.JOB_SKILLS
-- Contient les compétences associées à chaque offre d'emploi
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_skills (
  job_id    VARCHAR,  -- Identifiant de l'offre d'emploi
  skill_abr VARCHAR   -- Abréviation de la compétence requise
);

-- Chargement depuis le fichier CSV
COPY INTO bronze.job_skills
FROM @bronze.linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.4 BRONZE.JOB_INDUSTRIES
-- Contient les secteurs d'activité associés à chaque offre d'emploi
-- Source JSON : extraction des champs avec la notation $1:champ
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.job_industries (
  job_id      VARCHAR,  -- Identifiant de l'offre d'emploi
  industry_id VARCHAR   -- Identifiant du secteur d'activité
);

-- Chargement depuis le fichier JSON avec extraction des deux champs
COPY INTO bronze.job_industries
FROM (
    SELECT
        $1:job_id::VARCHAR,
        $1:industry_id::VARCHAR
    FROM @bronze.linkedin_stage/job_industries.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.5 BRONZE.COMPANIES
-- Contient les informations détaillées sur chaque entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.companies (
  company_id   VARCHAR,  -- Identifiant unique de l'entreprise sur LinkedIn
  name         VARCHAR,  -- Nom de l'entreprise
  description  TEXT,     -- Description de l'entreprise
  company_size INT,      -- Taille de l'entreprise (valeur numérique de 0 à 7)
  state        VARCHAR,  -- État du siège social
  country      VARCHAR,  -- Pays du siège social
  city         VARCHAR,  -- Ville du siège social
  zip_code     VARCHAR,  -- Code postal du siège social
  address      VARCHAR,  -- Adresse complète du siège social
  url          VARCHAR   -- Lien vers la page LinkedIn de l'entreprise
);

-- Chargement depuis le fichier JSON
-- $1 désigne la colonne VARIANT (JSON brut), on extrait chaque champ avec ::TYPE
COPY INTO bronze.companies
FROM (
    SELECT
        $1:company_id::VARCHAR,
        $1:name::VARCHAR,
        $1:description::VARCHAR,
        $1:company_size::INT,
        $1:state::VARCHAR,
        $1:country::VARCHAR,
        $1:city::VARCHAR,
        $1:zip_code::VARCHAR,
        $1:address::VARCHAR,
        $1:url::VARCHAR
    FROM @bronze.linkedin_stage/companies.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.6 BRONZE.EMPLOYEE_COUNTS
-- Contient le nombre d'employés et de followers par entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.employee_counts (
  company_id     VARCHAR,  -- Identifiant de l'entreprise
  employee_count INT,      -- Nombre d'employés à la date de collecte
  follower_count INT,      -- Nombre de followers LinkedIn à la date de collecte
  time_recorded  BIGINT    -- Timestamp Unix de la date de collecte
);

-- Chargement depuis le fichier CSV
COPY INTO bronze.employee_counts
FROM @bronze.linkedin_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.7 BRONZE.COMPANY_INDUSTRIES
-- Contient les secteurs d'activité associés à chaque entreprise
-- Une entreprise peut avoir plusieurs secteurs d'activité
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.company_industries (
  company_id VARCHAR,  -- Identifiant de l'entreprise
  industry   VARCHAR   -- Secteur d'activité associé à l'entreprise
);

-- Chargement depuis le fichier JSON
COPY INTO bronze.company_industries
FROM (
    SELECT
        $1:company_id::VARCHAR,
        $1:industry::VARCHAR
    FROM @bronze.linkedin_stage/company_industries.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';

-- ------------------------------------------------------------
-- 2.8 BRONZE.COMPANY_SPECIALITIES
-- Contient les spécialités déclarées par chaque entreprise
-- Une entreprise peut avoir plusieurs spécialités
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE bronze.company_specialities (
  company_id VARCHAR,  -- Identifiant de l'entreprise
  speciality VARCHAR   -- Spécialité déclarée par l'entreprise
);

-- Chargement depuis le fichier JSON
COPY INTO bronze.company_specialities
FROM (
    SELECT
        $1:company_id::VARCHAR,
        $1:speciality::VARCHAR
    FROM @bronze.linkedin_stage/company_specialities.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';

-- ============================================================
-- 3. SILVER : NETTOYAGE DES DONNEES
-- Standardisation, suppression des doublons et contrôles qualité
-- ============================================================

USE SCHEMA silver;

-- ------------------------------------------------------------
-- 3.1 SILVER.JOB_POSTINGS
-- Nettoyage :
-- - Suppression des doublons sur job_id via ROW_NUMBER()
-- - Suppression des lignes avec job_id, title ou location vides
-- - TRIM() sur tous les champs texte
-- - UPPER() pour standardiser title, pay_period, currency
-- - COALESCE() pour remplacer les NULL par des valeurs par défaut
-- - Contrôles de cohérence sur les salaires et les dates
-- - TO_VARCHAR(TRY_CAST(... AS BIGINT)) pour nettoyer le .0 des company_id
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_postings AS
SELECT
    TRIM(job_id)                                              AS job_id,
    -- Supprime le .0 du float : le CSV charge les IDs comme 3570660.0 → 3570660
    TO_VARCHAR(TRY_CAST(TRIM(company_id) AS BIGINT))          AS company_id,
    UPPER(TRIM(title))                                        AS title,
    TRIM(description)                                         AS description,
    max_salary,
    med_salary,
    min_salary,
    UPPER(TRIM(pay_period))                                   AS pay_period,
    TRIM(formatted_work_type)                                 AS formatted_work_type,
    TRIM(location)                                            AS location,
    COALESCE(applies, 0)                                      AS applies,         -- NULL → 0
    original_listed_time,
    COALESCE(remote_allowed, FALSE)                           AS remote_allowed,  -- NULL → FALSE
    COALESCE(views, 0)                                        AS views,           -- NULL → 0
    TRIM(job_posting_url)                                     AS job_posting_url,
    TRIM(application_url)                                     AS application_url,
    TRIM(application_type)                                    AS application_type,
    expiry,
    closed_time,
    TRIM(formatted_experience_level)                          AS formatted_experience_level,
    TRIM(skills_desc)                                         AS skills_desc,
    listed_time,
    TRIM(posting_domain)                                      AS posting_domain,
    COALESCE(sponsored, FALSE)                                AS sponsored,       -- NULL → FALSE
    TRIM(work_type)                                           AS work_type,
    UPPER(TRIM(currency))                                     AS currency,
    TRIM(compensation_type)                                   AS compensation_type
FROM (
    SELECT *,
           -- ROW_NUMBER() numérote les lignes par job_id
           -- On garde seulement rn = 1 pour éliminer les doublons (le plus récent en premier)
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
  AND (min_salary IS NULL OR min_salary >= 0)          -- Salaires positifs uniquement
  AND (med_salary IS NULL OR med_salary >= 0)
  AND (max_salary IS NULL OR max_salary >= 0)
  AND (views >= 0)
  AND (applies >= 0)
  AND (min_salary IS NULL OR max_salary IS NULL OR min_salary <= max_salary)  -- Min <= Max
  AND (listed_time IS NULL OR expiry IS NULL OR listed_time <= expiry);       -- Date publication <= expiration

-- ------------------------------------------------------------
-- 3.2 SILVER.BENEFITS
-- Nettoyage :
-- - Suppression des lignes vides
-- - TRIM() sur les champs texte
-- - COALESCE(inferred, FALSE) pour remplacer les NULL
-- - DISTINCT pour supprimer les doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.benefits AS
SELECT DISTINCT
    TRIM(job_id)           AS job_id,
    COALESCE(inferred, FALSE) AS inferred,  -- NULL → FALSE
    TRIM(type)             AS type
FROM bronze.benefits
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND type IS NOT NULL
  AND TRIM(type) <> '';

-- ------------------------------------------------------------
-- 3.3 SILVER.JOB_SKILLS
-- Nettoyage :
-- - Suppression des lignes vides
-- - UPPER() pour standardiser les abréviations de compétences
-- - DISTINCT pour supprimer les doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_skills AS
SELECT DISTINCT
    TRIM(job_id)           AS job_id,
    UPPER(TRIM(skill_abr)) AS skill_abr  -- Standardisation en majuscules
FROM bronze.job_skills
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND skill_abr IS NOT NULL
  AND TRIM(skill_abr) <> '';

-- ------------------------------------------------------------
-- 3.4 SILVER.JOB_INDUSTRIES
-- Nettoyage :
-- - Suppression des lignes vides
-- - TRIM() sur les champs
-- - DISTINCT pour supprimer les doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.job_industries AS
SELECT DISTINCT
    TRIM(job_id)      AS job_id,
    TRIM(industry_id) AS industry_id
FROM bronze.job_industries
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND industry_id IS NOT NULL
  AND TRIM(industry_id) <> '';

-- ------------------------------------------------------------
-- 3.5 SILVER.COMPANIES
-- Nettoyage :
-- - Suppression des doublons sur company_id via ROW_NUMBER()
-- - Suppression des lignes avec company_id ou name vides
-- - TRIM() sur tous les champs texte
-- - UPPER() sur state et country pour standardiser la casse
-- - COALESCE(company_size, -1) : -1 signifie taille inconnue
-- - Filtre sur company_size BETWEEN 0 AND 7
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.companies AS
SELECT
    TRIM(company_id)           AS company_id,
    TRIM(name)                 AS name,
    TRIM(description)          AS description,
    COALESCE(company_size, -1) AS company_size,  -- -1 = taille inconnue
    UPPER(TRIM(state))         AS state,
    UPPER(TRIM(country))       AS country,
    TRIM(city)                 AS city,
    TRIM(zip_code)             AS zip_code,
    TRIM(address)              AS address,
    TRIM(url)                  AS url
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company_id
               ORDER BY company_id
           ) AS rn
    FROM bronze.companies
    WHERE company_id IS NOT NULL
      AND TRIM(company_id) <> ''
      AND name IS NOT NULL
      AND TRIM(name) <> ''
)
WHERE rn = 1
  AND (company_size IS NULL OR company_size BETWEEN 0 AND 7);

-- ------------------------------------------------------------
-- 3.6 SILVER.EMPLOYEE_COUNTS
-- Nettoyage :
-- - Suppression des lignes vides
-- - Exclusion des valeurs négatives
-- - ROW_NUMBER() pour garder uniquement l'enregistrement le plus récent par entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.employee_counts AS
SELECT
    TRIM(company_id) AS company_id,
    employee_count,
    follower_count,
    time_recorded
FROM (
    SELECT *,
           -- Tri par time_recorded DESC pour avoir le plus récent en premier
           ROW_NUMBER() OVER (
               PARTITION BY TRIM(company_id)
               ORDER BY time_recorded DESC NULLS LAST
           ) AS rn
    FROM bronze.employee_counts
    WHERE company_id IS NOT NULL
      AND TRIM(company_id) <> ''
      AND employee_count >= 0
      AND follower_count >= 0
)
WHERE rn = 1;  -- Garde uniquement l'enregistrement le plus récent par entreprise

-- ------------------------------------------------------------
-- 3.7 SILVER.COMPANY_INDUSTRIES
-- Nettoyage :
-- - Suppression des lignes vides
-- - TRIM() sur les champs
-- - DISTINCT pour supprimer les doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.company_industries AS
SELECT DISTINCT
    TRIM(company_id) AS company_id,
    TRIM(industry)   AS industry
FROM bronze.company_industries
WHERE company_id IS NOT NULL
  AND TRIM(company_id) <> ''
  AND industry IS NOT NULL
  AND TRIM(industry) <> '';

-- ------------------------------------------------------------
-- 3.8 SILVER.COMPANY_SPECIALITIES
-- Nettoyage :
-- - Suppression des lignes vides
-- - TRIM() sur les champs
-- - DISTINCT pour supprimer les doublons exacts
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver.company_specialities AS
SELECT DISTINCT
    TRIM(company_id) AS company_id,
    TRIM(speciality) AS speciality
FROM bronze.company_specialities
WHERE company_id IS NOT NULL
  AND TRIM(company_id) <> ''
  AND speciality IS NOT NULL
  AND TRIM(speciality) <> '';

-- ============================================================
-- 4. GOLD : TABLES ENRICHIES POUR L'ANALYSE
-- Tables orientées métier combinant les données Silver
-- ============================================================

USE SCHEMA gold;

-- ------------------------------------------------------------
-- 4.1 GOLD.JOB_POSTINGS
-- Table métier principale des offres enrichie avec les infos entreprise
-- Jointure sur company_id pour récupérer la taille de l'entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_postings AS
SELECT
    jp.job_id,
    jp.company_id,
    c.name              AS company_name,    -- Nom réel depuis gold.companies
    c.company_size,
    -- Conversion du code numérique en libellé lisible
    CASE c.company_size
        WHEN 0 THEN '1 (Très petite)'
        WHEN 1 THEN '2-10'
        WHEN 2 THEN '11-50'
        WHEN 3 THEN '51-200'
        WHEN 4 THEN '201-500'
        WHEN 5 THEN '501-1000'
        WHEN 6 THEN '1001-5000'
        WHEN 7 THEN '5000+'
        ELSE 'Non spécifié'
    END                 AS company_size_label,
    jp.title,
    jp.max_salary,
    jp.med_salary,
    jp.min_salary,
    jp.pay_period,
    jp.formatted_work_type,
    jp.location,
    jp.applies,
    jp.views,
    jp.job_posting_url,
    jp.application_url,
    jp.application_type,
    jp.formatted_experience_level,
    jp.skills_desc,
    jp.listed_time,
    jp.work_type,
    jp.currency,
    jp.compensation_type,
    jp.remote_allowed,
    jp.sponsored
FROM silver.job_postings jp
-- LEFT JOIN pour garder toutes les offres même si l'entreprise n'est pas dans companies
LEFT JOIN silver.companies c ON jp.company_id = c.company_id
WHERE jp.title IS NOT NULL
  AND TRIM(jp.title) <> ''
  AND jp.location IS NOT NULL
  AND TRIM(jp.location) <> '';

-- ------------------------------------------------------------
-- 4.2 GOLD.BENEFITS
-- Table enrichie avec le titre et le nom de l'entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.benefits AS
SELECT
    b.job_id,
    jp.title,
    jp.company_name,
    b.inferred,
    b.type AS benefit_type
FROM silver.benefits b
JOIN silver.job_postings jp ON b.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.3 GOLD.JOB_SKILLS
-- Table enrichie avec le titre, entreprise et localisation
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_skills AS
SELECT
    js.job_id,
    jp.title,
    jp.company_id,
    jp.location,
    jp.formatted_work_type,
    js.skill_abr
FROM silver.job_skills js
JOIN silver.job_postings jp ON js.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.4 GOLD.JOB_INDUSTRIES
-- Table enrichie avec le titre, entreprise et localisation
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_industries AS
SELECT
    ji.job_id,
    jp.title,
    jp.company_id,
    jp.location,
    jp.formatted_work_type,
    ji.industry_id
FROM silver.job_industries ji
JOIN silver.job_postings jp ON ji.job_id = jp.job_id;

-- ------------------------------------------------------------
-- 4.5 GOLD.COMPANIES
-- Table métier principale des entreprises
-- Ajout du libellé de taille (company_size_label)
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.companies AS
SELECT
    company_id,
    name,
    description,
    company_size,
    CASE company_size
        WHEN 0 THEN '1 (Très petite)'
        WHEN 1 THEN '2-10'
        WHEN 2 THEN '11-50'
        WHEN 3 THEN '51-200'
        WHEN 4 THEN '201-500'
        WHEN 5 THEN '501-1000'
        WHEN 6 THEN '1001-5000'
        WHEN 7 THEN '5000+'
        ELSE 'Non spécifié'
    END AS company_size_label,
    state,
    country,
    city,
    zip_code,
    address,
    url
FROM silver.companies
WHERE name IS NOT NULL
  AND TRIM(name) <> '';

-- ------------------------------------------------------------
-- 4.6 GOLD.EMPLOYEE_COUNTS
-- Table enrichie des effectifs avec les infos entreprise
-- Garde uniquement l'enregistrement le plus récent par entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.employee_counts AS
SELECT
    ec.company_id,
    c.name             AS company_name,
    c.country,
    c.company_size,
    c.company_size_label,
    ec.employee_count,
    ec.follower_count,
    ec.time_recorded
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company_id
               ORDER BY time_recorded DESC NULLS LAST
           ) AS rn
    FROM silver.employee_counts
) ec
JOIN gold.companies c ON ec.company_id = c.company_id
WHERE ec.rn = 1;

-- ------------------------------------------------------------
-- 4.7 GOLD.COMPANY_INDUSTRIES
-- Table enrichie des secteurs d'activité avec les infos entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.company_industries AS
SELECT
    ci.company_id,
    c.name             AS company_name,
    c.country,
    c.company_size,
    c.company_size_label,
    ci.industry
FROM silver.company_industries ci
JOIN gold.companies c ON ci.company_id = c.company_id;

-- ------------------------------------------------------------
-- 4.8 GOLD.COMPANY_SPECIALITIES
-- Table enrichie des spécialités avec les infos entreprise
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.company_specialities AS
SELECT
    cs.company_id,
    c.name             AS company_name,
    c.country,
    c.company_size,
    c.company_size_label,
    cs.speciality
FROM silver.company_specialities cs
JOIN gold.companies c ON cs.company_id = c.company_id;

-- ------------------------------------------------------------
-- 4.9 GOLD.COMPANIES_FULL
-- Table enrichie complète : entreprise + effectifs + industries + spécialités
-- LEFT JOIN pour conserver toutes les entreprises
-- Attention : génère plusieurs lignes par entreprise si elle a plusieurs industries/spécialités
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.companies_full AS
SELECT
    c.company_id,
    c.name              AS company_name,
    c.description,
    c.company_size,
    c.company_size_label,
    c.state,
    c.country,
    c.city,
    c.url,
    ec.employee_count,
    ec.follower_count,
    ci.industry,
    cs.speciality
FROM gold.companies c
LEFT JOIN gold.employee_counts ec      ON c.company_id = ec.company_id
LEFT JOIN gold.company_industries ci   ON c.company_id = ci.company_id
LEFT JOIN gold.company_specialities cs ON c.company_id = cs.company_id;

-- ------------------------------------------------------------
-- 4.10 GOLD.JOB_POSTINGS_FULL
-- Table enrichie complète : offres + entreprises + industries + compétences + avantages
-- Utilisée directement par le Streamlit pour les visualisations
-- LEFT JOIN pour conserver toutes les offres
-- Attention : génère plusieurs lignes par offre si elle a plusieurs industries/compétences/avantages
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE gold.job_postings_full AS
SELECT
    jp.job_id,
    jp.company_id,
    jp.company_name,
    jp.company_size,
    jp.company_size_label,
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
    ji.industry_id,    -- Secteur d'activité de l'offre
    js.skill_abr,      -- Compétence requise
    gb.benefit_type,   -- Avantage proposé
    gb.inferred AS benefit_inferred  -- TRUE si avantage inféré, FALSE si déclaré
FROM gold.job_postings jp
LEFT JOIN gold.job_industries ji ON jp.job_id = ji.job_id
LEFT JOIN gold.job_skills js     ON jp.job_id = js.job_id
LEFT JOIN gold.benefits gb       ON jp.job_id = gb.job_id;

-- ============================================================
-- 5. CONTROLES DE VOLUMETRIE
-- Vérification du nombre de lignes à chaque couche Bronze/Silver/Gold
-- ============================================================

SELECT COUNT(*) AS bronze_job_postings_count     FROM bronze.job_postings;
SELECT COUNT(*) AS silver_job_postings_count     FROM silver.job_postings;
SELECT COUNT(*) AS gold_job_postings_count       FROM gold.job_postings;

SELECT COUNT(*) AS bronze_benefits_count         FROM bronze.benefits;
SELECT COUNT(*) AS silver_benefits_count         FROM silver.benefits;
SELECT COUNT(*) AS gold_benefits_count           FROM gold.benefits;

SELECT COUNT(*) AS bronze_job_skills_count       FROM bronze.job_skills;
SELECT COUNT(*) AS silver_job_skills_count       FROM silver.job_skills;
SELECT COUNT(*) AS gold_job_skills_count         FROM gold.job_skills;

SELECT COUNT(*) AS bronze_job_industries_count   FROM bronze.job_industries;
SELECT COUNT(*) AS silver_job_industries_count   FROM silver.job_industries;
SELECT COUNT(*) AS gold_job_industries_count     FROM gold.job_industries;

SELECT COUNT(*) AS bronze_companies_count        FROM bronze.companies;
SELECT COUNT(*) AS silver_companies_count        FROM silver.companies;
SELECT COUNT(*) AS gold_companies_count          FROM gold.companies;

SELECT COUNT(*) AS bronze_employee_counts_count  FROM bronze.employee_counts;
SELECT COUNT(*) AS silver_employee_counts_count  FROM silver.employee_counts;
SELECT COUNT(*) AS gold_employee_counts_count    FROM gold.employee_counts;

SELECT COUNT(*) AS bronze_company_industries_count FROM bronze.company_industries;
SELECT COUNT(*) AS silver_company_industries_count FROM silver.company_industries;
SELECT COUNT(*) AS gold_company_industries_count   FROM gold.company_industries;

SELECT COUNT(*) AS bronze_company_specialities_count FROM bronze.company_specialities;
SELECT COUNT(*) AS silver_company_specialities_count FROM silver.company_specialities;
SELECT COUNT(*) AS gold_company_specialities_count   FROM gold.company_specialities;

-- ============================================================
-- 6. CONTROLES QUALITE - UNICITE / DOUBLONS
-- Vérifie qu'il n'y a pas de doublons sur les clés primaires
-- Un résultat vide = pas de doublon = données propres
-- ============================================================

-- job_postings : unicité de job_id
SELECT COUNT(*) AS total_lignes, COUNT(DISTINCT job_id) AS total_job_id_distincts
FROM linkedin_lab.silver.job_postings;

SELECT job_id, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_postings
GROUP BY job_id HAVING COUNT(*) > 1;

-- benefits : unicité de (job_id, type)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', type)) AS total_job_benefit_distincts
FROM linkedin_lab.silver.benefits;

SELECT job_id, type, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.benefits
GROUP BY job_id, type HAVING COUNT(*) > 1;

-- job_skills : unicité de (job_id, skill_abr)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', skill_abr)) AS total_job_skill_distincts
FROM linkedin_lab.silver.job_skills;

SELECT job_id, skill_abr, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_skills
GROUP BY job_id, skill_abr HAVING COUNT(*) > 1;

-- job_industries : unicité de (job_id, industry_id)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', industry_id)) AS total_job_industry_distincts
FROM linkedin_lab.silver.job_industries;

SELECT job_id, industry_id, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_industries
GROUP BY job_id, industry_id HAVING COUNT(*) > 1;

-- companies : unicité de company_id
SELECT COUNT(*) AS total_lignes, COUNT(DISTINCT company_id) AS total_company_id_distincts
FROM linkedin_lab.silver.companies;

SELECT company_id, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.companies
GROUP BY company_id HAVING COUNT(*) > 1;

-- employee_counts : unicité de (company_id, time_recorded)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(company_id, '|', time_recorded)) AS total_distincts
FROM linkedin_lab.silver.employee_counts;

-- company_industries : unicité de (company_id, industry)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(company_id, '|', industry)) AS total_distincts
FROM linkedin_lab.silver.company_industries;

SELECT company_id, industry, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_industries
GROUP BY company_id, industry HAVING COUNT(*) > 1;

-- company_specialities : unicité de (company_id, speciality)
SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(company_id, '|', speciality)) AS total_distincts
FROM linkedin_lab.silver.company_specialities;

SELECT company_id, speciality, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_specialities
GROUP BY company_id, speciality HAVING COUNT(*) > 1;

-- ============================================================
-- 7. CONTROLES QUALITE - CHAINES VIDES
-- Vérifie qu'aucune valeur clé n'est vide après nettoyage
-- ============================================================

SELECT * FROM linkedin_lab.silver.job_postings
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR title IS NULL OR TRIM(title) = ''
   OR location IS NULL OR TRIM(location) = '';

SELECT * FROM linkedin_lab.silver.benefits
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR type IS NULL OR TRIM(type) = '';

SELECT * FROM linkedin_lab.silver.job_skills
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR skill_abr IS NULL OR TRIM(skill_abr) = '';

SELECT * FROM linkedin_lab.silver.job_industries
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR industry_id IS NULL OR TRIM(industry_id) = '';

SELECT * FROM linkedin_lab.silver.companies
WHERE company_id IS NULL OR TRIM(company_id) = ''
   OR name IS NULL OR TRIM(name) = '';

SELECT * FROM linkedin_lab.silver.employee_counts
WHERE company_id IS NULL OR TRIM(company_id) = '';

SELECT * FROM linkedin_lab.silver.company_industries
WHERE company_id IS NULL OR TRIM(company_id) = ''
   OR industry IS NULL OR TRIM(industry) = '';

SELECT * FROM linkedin_lab.silver.company_specialities
WHERE company_id IS NULL OR TRIM(company_id) = ''
   OR speciality IS NULL OR TRIM(speciality) = '';

-- ============================================================
-- 8. CONTROLES QUALITE - COHERENCE METIER
-- ============================================================

-- Salaires incohérents (min > max)
SELECT * FROM linkedin_lab.silver.job_postings
WHERE min_salary IS NOT NULL AND max_salary IS NOT NULL AND min_salary > max_salary;

-- Dates incohérentes (publication > expiration)
SELECT * FROM linkedin_lab.silver.job_postings
WHERE listed_time IS NOT NULL AND expiry IS NOT NULL AND listed_time > expiry;

-- Valeurs négatives dans job_postings
SELECT * FROM linkedin_lab.silver.job_postings
WHERE (views < 0) OR (applies < 0) OR (min_salary < 0) OR (med_salary < 0) OR (max_salary < 0);

-- Salaires aberrants (> 1 million)
SELECT * FROM linkedin_lab.silver.job_postings
WHERE max_salary > 1000000 OR med_salary > 1000000 OR min_salary > 1000000;

-- company_size hors plage [0-7] (on exclut -1 qui est notre valeur pour taille inconnue)
SELECT * FROM linkedin_lab.silver.companies
WHERE company_size NOT BETWEEN 0 AND 7 AND company_size != -1;

-- Effectifs ou followers négatifs
SELECT * FROM linkedin_lab.silver.employee_counts
WHERE employee_count < 0 OR follower_count < 0;

-- Contrôle croisé : les grandes entreprises doivent avoir plus d'employés en moyenne
SELECT
    c.company_size_label,
    ROUND(AVG(ec.employee_count), 0) AS avg_employees,
    COUNT(*) AS nb_entreprises
FROM linkedin_lab.gold.employee_counts ec
JOIN linkedin_lab.gold.companies c ON ec.company_id = c.company_id
GROUP BY c.company_size, c.company_size_label
ORDER BY c.company_size;

-- ============================================================
-- 9. CONTROLES QUALITE - TABLES GOLD
-- ============================================================

SELECT COUNT(*) AS total_lignes, COUNT(DISTINCT job_id) AS total_job_id_distincts
FROM linkedin_lab.gold.job_postings;

SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', benefit_type)) AS total_job_benefit_distincts
FROM linkedin_lab.gold.benefits;

SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', skill_abr)) AS total_job_skill_distincts
FROM linkedin_lab.gold.job_skills;

SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(job_id, '|', industry_id)) AS total_job_industry_distincts
FROM linkedin_lab.gold.job_industries;

SELECT COUNT(*) AS total_lignes, COUNT(DISTINCT company_id) AS total_company_id_distincts
FROM linkedin_lab.gold.companies;

SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(company_id, '|', industry)) AS total_company_industry_distincts
FROM linkedin_lab.gold.company_industries;

SELECT COUNT(*) AS total_lignes,
       COUNT(DISTINCT CONCAT(company_id, '|', speciality)) AS total_company_speciality_distincts
FROM linkedin_lab.gold.company_specialities;

-- ============================================================
-- 10. REQUETES D'ANALYSE
-- ============================================================

-- 10.1 Top 10 des titres de postes les plus publiés par industrie
SELECT industry_id, title, nb_offres, rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (PARTITION BY ji.industry_id ORDER BY COUNT(*) DESC) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp ON ji.job_id = jp.job_id
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;

-- 10.2 Top 10 des postes les mieux rémunérés par industrie
SELECT industry_id, title, avg_max_salary, rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        ROUND(AVG(jp.max_salary), 2) AS avg_max_salary,
        ROW_NUMBER() OVER (PARTITION BY ji.industry_id ORDER BY AVG(jp.max_salary) DESC) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp ON ji.job_id = jp.job_id
    WHERE jp.max_salary IS NOT NULL AND jp.pay_period = 'YEARLY'
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;

-- 10.3 Répartition des offres par taille d'entreprise
SELECT
    c.company_size_label,
    COUNT(DISTINCT jp.job_id) AS nb_offres,
    ROUND(100 * COUNT(DISTINCT jp.job_id) / SUM(COUNT(DISTINCT jp.job_id)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings jp
JOIN linkedin_lab.gold.companies c ON jp.company_id = c.company_id
GROUP BY c.company_size, c.company_size_label
ORDER BY c.company_size;

-- 10.4 Répartition des offres par secteur d'activité
SELECT
    industry_id,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_industries
GROUP BY industry_id
ORDER BY nb_offres DESC;

-- 10.5 Répartition des offres par type d'emploi
SELECT
    formatted_work_type AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings
WHERE formatted_work_type IS NOT NULL AND TRIM(formatted_work_type) <> ''
GROUP BY formatted_work_type
ORDER BY nb_offres DESC;

-- 10.6 Top 10 des industries avec le plus d'entreprises
SELECT industry, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_industries
GROUP BY industry
ORDER BY nb_entreprises DESC
LIMIT 10;

-- 10.7 Top 10 des spécialités les plus représentées
SELECT speciality, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_specialities
GROUP BY speciality
ORDER BY nb_entreprises DESC
LIMIT 10;

-- 10.8 Top 10 des entreprises avec le plus de followers
SELECT company_name, follower_count, employee_count, company_size_label, country
FROM linkedin_lab.gold.employee_counts
ORDER BY follower_count DESC
LIMIT 10;

-- 10.9 Répartition des entreprises par pays (top 20)
SELECT country, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.companies
WHERE country IS NOT NULL AND TRIM(country) <> ''
GROUP BY country
ORDER BY nb_entreprises DESC
LIMIT 20;
