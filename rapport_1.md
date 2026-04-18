# Linkedinlab1
# Projet : Analyse des Offres d’Emploi LinkedIn avec Snowflake

## Présentation

Ce projet a pour objectif d’analyser un jeu de données LinkedIn dans **Snowflake** afin d’étudier le marché de l’emploi à partir de plusieurs tables CSV et JSON disponibles dans un bucket S3 public.

Le travail a été organisé avec une architecture **Medallion** :

- **Bronze** : données brutes
- **Silver** : données nettoyées
- **Gold** : données prêtes pour l’analyse

Les tables exploitées dans cette version sont :

- `job_postings.csv`
- `benefits.csv`
- `job_skills.csv`
- `job_industries.json`

---

## Objectifs

Les analyses demandées sont :

1. Top 10 des titres de postes les plus publiés par industrie  
2. Top 10 des postes les mieux rémunérés par industrie  
3. Répartition des offres d’emploi par taille d’entreprise  
4. Répartition des offres d’emploi par secteur d’activité  
5. Répartition des offres d’emploi par type d’emploi  

---

## Technologies utilisées

- **Snowflake**
- **SQL**
- **Streamlit in Snowflake**
- **GitHub**

---

## Structure du projet

```text
linkedin_lab
├── bronze
│   ├── job_postings
│   ├── benefits
│   ├── job_skills
│   └── job_industries
├── silver
│   ├── job_postings
│   ├── benefits
│   ├── job_skills
│   └── job_industries
└── gold
    ├── job_postings
    ├── benefits
    ├── job_skills
    ├── job_industries
    └── job_postings_full
````
---

## 5. Étapes de réalisation

### 5.1 Création de la base et des schémas

```sql
CREATE DATABASE IF NOT EXISTS linkedin_lab;
USE DATABASE linkedin_lab;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```
### 5.2 Sélection du warehouse

Avant toute opération de chargement ou de transformation, il est nécessaire de sélectionner un **warehouse** actif dans Snowflake.  
Le warehouse représente la ressource de calcul utilisée pour exécuter les requêtes SQL.

Sans cette instruction, certaines commandes comme `COPY INTO`, `CREATE TABLE AS SELECT` ou d’autres traitements peuvent échouer.

```sql
USE WAREHOUSE COMPUTE_WH;
```
### 5.3 Création du stage S3 et des formats de fichiers

Cette étape consiste à configurer l’accès aux fichiers sources stockés dans le bucket S3 public fourni pour le projet.  
Un **stage externe** est créé afin de référencer ce bucket dans Snowflake.

Ensuite, deux **formats de fichiers** sont définis :

- un format **CSV** pour les fichiers tabulaires,
- un format **JSON** pour les fichiers structurés en tableau JSON.

```sql
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
  TIMESTAMP_FORMAT = 'AUTO';

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE;
```
## 6. Couche Bronze : chargement des données brutes

La couche **Bronze** contient les données brutes, c’est-à-dire les données importées directement depuis les fichiers du bucket S3, sans transformation avancée.

### 6.1 Table `bronze.job_postings`

Cette table contient les informations détaillées sur les offres d’emploi LinkedIn.

```sql
CREATE OR REPLACE TABLE bronze.job_postings (
  job_id                     VARCHAR,
  company_name               VARCHAR,
  title                      VARCHAR,
  description                TEXT,
  max_salary                 FLOAT,
  med_salary                 FLOAT,
  min_salary                 FLOAT,
  pay_period                 VARCHAR,
  formatted_work_type        VARCHAR,
  location                   VARCHAR,
  applies                    INT,
  original_listed_time       BIGINT,
  remote_allowed             BOOLEAN,
  views                      INT,
  job_posting_url            VARCHAR,
  application_url            VARCHAR,
  application_type           VARCHAR,
  expiry                     BIGINT,
  closed_time                BIGINT,
  formatted_experience_level VARCHAR,
  skills_desc                TEXT,
  listed_time                BIGINT,
  posting_domain             VARCHAR,
  sponsored                  BOOLEAN,
  work_type                  VARCHAR,
  currency                   VARCHAR,
  compensation_type          VARCHAR
);

COPY INTO bronze.job_postings
FROM @bronze.linkedin_stage/job_postings.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';
```
### 6.2 Table `bronze.benefits`

Cette table contient les avantages proposés dans les offres d’emploi.

```sql
CREATE OR REPLACE TABLE bronze.benefits (
  job_id   VARCHAR,
  inferred BOOLEAN,
  type     VARCHAR
);

COPY INTO bronze.benefits
FROM @bronze.linkedin_stage/benefits.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';
```
### 6.3 Table `bronze.job_skills`

Cette table contient les compétences associées à chaque offre d’emploi.

```sql
CREATE OR REPLACE TABLE bronze.job_skills (
  job_id    VARCHAR,
  skill_abr VARCHAR
);

COPY INTO bronze.job_skills
FROM @bronze.linkedin_stage/job_skills.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';
```
### 6.4 Table `bronze.job_industries`

Cette table contient les secteurs d’activité associés à chaque offre d’emploi.

```sql
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
)
ON_ERROR = 'CONTINUE';
```
## 7. Couche Silver : nettoyage des données

La couche **Silver** contient les données nettoyées et standardisées.  
Les opérations principales effectuées dans cette couche sont :

- suppression des doublons,
- suppression des lignes vides,
- nettoyage des chaînes de caractères,
- remplacement de certaines valeurs nulles,
- contrôle de cohérence métier.

### 7.1 Table `silver.job_postings`

Cette table est construite à partir de `bronze.job_postings` en appliquant un nettoyage plus rigoureux :

- suppression des lignes avec `job_id`, `title` ou `location` vides,
- suppression des doublons avec `ROW_NUMBER()`,
- nettoyage des espaces avec `TRIM()`,
- mise en majuscule de certaines colonnes,
- remplacement de certaines valeurs nulles avec `COALESCE()`,
- contrôle de cohérence sur les salaires et les dates.

```sql
USE SCHEMA silver;
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
  AND (views >= 0)
  AND (applies >= 0)
  AND (min_salary IS NULL OR max_salary IS NULL OR min_salary <= max_salary)
  AND (listed_time IS NULL OR expiry IS NULL OR listed_time <= expiry);
```
### 7.2 Table `silver.benefits`

Cette table est construite à partir de `bronze.benefits`.  
Le nettoyage appliqué consiste à :

- supprimer les lignes où `job_id` est vide,
- supprimer les lignes où le type d’avantage est vide,
- supprimer les espaces inutiles avec `TRIM()`,
- remplacer les valeurs nulles de `inferred` par `FALSE`,
- supprimer les doublons exacts avec `DISTINCT`.

```sql
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
```


### 7.3 Table `silver.job_skills`

Cette table est construite à partir de `bronze.job_skills`.  
Le nettoyage appliqué consiste à :

- supprimer les lignes où `job_id` est vide,
- supprimer les lignes où la compétence est vide,
- supprimer les espaces inutiles avec `TRIM()`,
- uniformiser les abréviations de compétences en majuscule avec `UPPER()`,
- supprimer les doublons exacts avec `DISTINCT`.

```sql
CREATE OR REPLACE TABLE silver.job_skills AS
SELECT DISTINCT
    TRIM(job_id) AS job_id,
    UPPER(TRIM(skill_abr)) AS skill_abr
FROM bronze.job_skills
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND skill_abr IS NOT NULL
  AND TRIM(skill_abr) <> '';
```


### 7.4 Table `silver.job_industries`

Cette table est construite à partir de `bronze.job_industries`.  
Le nettoyage appliqué consiste à :

- supprimer les lignes où `job_id` est vide,
- supprimer les lignes où `industry_id` est vide,
- supprimer les espaces inutiles avec `TRIM()`,
- supprimer les doublons exacts avec `DISTINCT`.

```sql
CREATE OR REPLACE TABLE silver.job_industries AS
SELECT DISTINCT
    TRIM(job_id) AS job_id,
    TRIM(industry_id) AS industry_id
FROM bronze.job_industries
WHERE job_id IS NOT NULL
  AND TRIM(job_id) <> ''
  AND industry_id IS NOT NULL
  AND TRIM(industry_id) <> '';
```

---


## 8. Couche Gold : tables prêtes pour l’analyse
### 8.1 Table `gold.job_postings`

Cette table constitue la table métier principale.  
Elle regroupe les colonnes essentielles des offres d’emploi après nettoyage.

```sql
USE SCHEMA gold;
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
```

### 8.2 Table `gold.benefits`

Cette table enrichit les avantages avec le titre du poste et le nom de l’entreprise.

```sql
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
```
### 8.3 Table `gold.job_skills`

Cette table enrichit les compétences avec des informations métier sur l’offre d’emploi.

```sql
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
```

### 8.4 Table `gold.job_industries`

Cette table enrichit les secteurs d’activité avec les informations de l’offre correspondante.

```sql
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
```
### 8.5 Table `gold.job_postings_full`

Cette table regroupe dans une seule structure les offres, les industries, les compétences et les avantages.  
Elle est utile pour certaines analyses transversales.

```sql
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
```

---
## 9. Contrôles qualité
## 9.1 Vérification du contenu des tables

Les requêtes suivantes permettent d’afficher le contenu des tables des couches Bronze, Silver et Gold afin de vérifier que les données ont bien été chargées, nettoyées et transformées.

```sql
-- Vérification des tables Bronze
SELECT * FROM bronze.job_postings;
SELECT * FROM bronze.benefits;
SELECT * FROM bronze.job_skills;
SELECT * FROM bronze.job_industries;

-- Vérification des tables Silver
SELECT * FROM silver.job_postings;
SELECT * FROM silver.benefits;
SELECT * FROM silver.job_skills;
SELECT * FROM silver.job_industries;

-- Vérification des tables Gold
SELECT * FROM gold.job_postings;
SELECT * FROM gold.benefits;
SELECT * FROM gold.job_skills;
SELECT * FROM gold.job_industries;
```
### 9.2 Vérification des volumes

Cette étape permet de comparer le nombre de lignes entre les couches Bronze, Silver et Gold.

```sql
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
```
### 9.3 Vérification des doublons

Cette vérification permet de s’assurer qu’il n’existe pas de doublons exacts dans les tables Silver.

#### `job_postings`

```sql
SELECT
    job_id,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_postings
GROUP BY job_id
HAVING COUNT(*) > 1;
```
#### `benefits`

```sql
SELECT
    job_id,
    type,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.benefits
GROUP BY job_id, type
HAVING COUNT(*) > 1;
```
#### `job_skills`

```sql
SELECT
    job_id,
    skill_abr,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_skills
GROUP BY job_id, skill_abr
HAVING COUNT(*) > 1;
```
#### `job_industries`

```sql
SELECT
    job_id,
    industry_id,
    COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.job_industries
GROUP BY job_id, industry_id
HAVING COUNT(*) > 1;
```
### 9.4 Vérification des chaînes vides

Cette étape permet d’identifier les lignes contenant des valeurs nulles ou des chaînes vides dans les colonnes clés.

```sql
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR title IS NULL OR TRIM(title) = ''
   OR location IS NULL OR TRIM(location) = '';

SELECT *
FROM linkedin_lab.silver.benefits
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR type IS NULL OR TRIM(type) = '';

SELECT *
FROM linkedin_lab.silver.job_skills
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR skill_abr IS NULL OR TRIM(skill_abr) = '';

SELECT *
FROM linkedin_lab.silver.job_industries
WHERE job_id IS NULL OR TRIM(job_id) = ''
   OR industry_id IS NULL OR TRIM(industry_id) = '';
```
### 9.5 Vérification de la cohérence métier

Cette étape permet de détecter certaines incohérences métier, notamment :

- un salaire minimum supérieur au salaire maximum,
- une date de publication postérieure à la date d’expiration,
- des valeurs négatives dans des colonnes numériques.

```sql
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE min_salary IS NOT NULL
  AND max_salary IS NOT NULL
  AND min_salary > max_salary;

SELECT *
FROM linkedin_lab.silver.job_postings
WHERE listed_time IS NOT NULL
  AND expiry IS NOT NULL
  AND listed_time > expiry;

SELECT *
FROM linkedin_lab.silver.job_postings
WHERE (views < 0)
   OR (applies < 0)
   OR (min_salary < 0)
   OR (med_salary < 0)
   OR (max_salary < 0);

```
### 9.6 Vérification des valeurs aberrantes

Cette étape permet de repérer les salaires anormalement élevés qui pourraient correspondre à des anomalies dans les données.

```sql
SELECT *
FROM linkedin_lab.silver.job_postings
WHERE max_salary > 1000000
   OR med_salary > 1000000
   OR min_salary > 1000000;
```
---
## 10. Problèmes rencontrés et solutions

### 10.1 Le chargement des données ne fonctionnait pas

**Problème :** certaines commandes échouaient au moment du chargement.  
**Cause :** aucun warehouse n’était sélectionné.

**Solution :**

```sql
USE WAREHOUSE COMPUTE_WH;
```
### 10.3 Erreur avec la suppression des doublons

**Problème :** l’utilisation de `DELETE` avec `WITH` et `ROW_NUMBER()` n’était pas acceptée comme prévu.

**Exemple problématique :**

```sql
WITH duplicates AS (
  SELECT job_id,
         ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_id) AS row_num
  FROM job_postings
)
DELETE FROM job_postings
WHERE job_id IN (
  SELECT job_id
  FROM duplicates
  WHERE row_num > 1
);
```
**Solution :** gérer les doublons directement dans la couche Silver avec `ROW_NUMBER()` puis `WHERE rn = 1`.

### 10.4 Certaines lignes n’étaient pas chargées

**Problème :** Snowflake affichait par exemple :

- `rows_parsed = 15886`
- `rows_loaded = 13546`

**Explication :** l’option suivante avait été utilisée :

```sql
ON_ERROR = 'CONTINUE'
```
Cela signifie que Snowflake ignore les lignes en erreur et poursuit le chargement des lignes valides.

**Solution adoptée :**

- accepter dans un premier temps un chargement partiel pour ne pas bloquer l’ingestion,
- identifier et documenter le nombre de lignes rejetées,
- réaliser ensuite des contrôles qualité sur les données effectivement chargées,
- effectuer le nettoyage dans la couche Silver afin d’obtenir des tables cohérentes et exploitables.
  
### 10.5 `ON_ERROR = 'CONTINUE'` ne nettoie pas les données

**Problème :** cette option ne corrige pas les erreurs, elle ignore simplement les lignes problématiques.

**Solution :**

- vérifier les doublons,
- vérifier les chaînes vides,
- vérifier la cohérence des salaires,
- créer une couche Silver propre.

### 10.6 Erreur `invalid identifier`

**Problème :** certaines colonnes renommées dans Gold n’étaient plus accessibles avec leur ancien nom.  
Exemple : `type` devient `benefit_type`.

**Solution :** vérifier la structure de la table :

```sql
DESC TABLE linkedin_lab.gold.benefits;
```
---
## 11. Analyses réalisées
### 11.1 Top 10 des titres de postes les plus publiés par industrie

```sql
SELECT
    industry_id,
    title,
    nb_offres,
    rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id
            ORDER BY COUNT(*) DESC
        ) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp
        ON ji.job_id = jp.job_id
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;
```

### 11.2 Top 10 des postes les mieux rémunérés par industrie

```sql
SELECT
    industry_id,
    title,
    avg_max_salary,
    rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        ROUND(AVG(jp.max_salary), 2) AS avg_max_salary,
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id
            ORDER BY AVG(jp.max_salary) DESC
        ) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp
        ON ji.job_id = jp.job_id
    WHERE jp.max_salary IS NOT NULL
      AND jp.pay_period = 'YEARLY'
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;
```

### 11.3 Répartition des offres d’emploi par secteur d’activité

```sql
SELECT
    industry_id,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_industries
GROUP BY industry_id
ORDER BY nb_offres DESC;
```
### 11.4 Répartition des offres d’emploi par type d’emploi

```sql
SELECT
    formatted_work_type AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings
WHERE formatted_work_type IS NOT NULL
  AND TRIM(formatted_work_type) <> ''
GROUP BY formatted_work_type
ORDER BY nb_offres DESC;
```
### 11.5 Compétences les plus demandées

```sql
SELECT
    skill_abr,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.job_skills
GROUP BY skill_abr
ORDER BY nb_offres DESC
LIMIT 10;
```

### 11.6 Avantages les plus fréquents

```sql
SELECT
    benefit_type,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.benefits
GROUP BY benefit_type
ORDER BY nb_offres DESC
LIMIT 10;
```


---
## 12. Partie Streamlit

Les visualisations ont été réalisées avec **Streamlit in Snowflake** à partir des tables Gold.  
L’application permet d’exécuter les requêtes SQL directement depuis Snowflake, de convertir les résultats en DataFrame Pandas, puis d’afficher les graphiques avec **Altair**.

---

### 12.1 Import des bibliothèques et configuration de l’application

Dans un premier temps, les bibliothèques nécessaires sont importées :

- `streamlit` pour l’interface,
- `pandas` pour la manipulation des données,
- `altair` pour les graphiques,
- `get_active_session` pour récupérer la session Snowflake active.

Ensuite, la page est configurée avec un titre et une mise en page large.

```
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
```
```
#### CONFIGURATION

st.set_page_config(page_title="Analyse des offres LinkedIn", layout="wide")
st.title("Analyse des Offres d'Emploi LinkedIn")
st.markdown("Visualisations construites à partir des tables Gold dans Snowflake.")
session = get_active_session()
```
### 12.2 Requêtes SQL utilisées dans Streamlit

L’application Streamlit repose sur quatre requêtes SQL principales, chacune correspondant à une analyse spécifique réalisée sur les tables de la couche Gold :

- le **top 10 des titres de postes les plus publiés par industrie**,
- le **top 10 des postes les mieux rémunérés par industrie**,
- la **répartition des offres d’emploi par secteur d’activité**,
- la **répartition des offres d’emploi par type d’emploi**.

Ces requêtes sont exécutées directement depuis Snowflake, puis leurs résultats sont convertis en DataFrames Pandas pour être affichés dans l’interface Streamlit.

```
query_top_titles = """
SELECT
    industry_id,
    title,
    nb_offres,
    rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id
            ORDER BY COUNT(*) DESC
        ) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp
        ON ji.job_id = jp.job_id
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;
"""

query_top_salary = """
SELECT
    industry_id,
    title,
    avg_max_salary,
    rang
FROM (
    SELECT
        ji.industry_id,
        jp.title,
        ROUND(AVG(jp.max_salary), 2) AS avg_max_salary,
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id
            ORDER BY AVG(jp.max_salary) DESC
        ) AS rang
    FROM linkedin_lab.gold.job_industries ji
    JOIN linkedin_lab.gold.job_postings jp
        ON ji.job_id = jp.job_id
    WHERE jp.max_salary IS NOT NULL
      AND jp.pay_period = 'YEARLY'
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang;
"""

query_industry_distribution = """
SELECT
    industry_id,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_industries
GROUP BY industry_id
ORDER BY nb_offres DESC;
"""

query_work_type_distribution = """
SELECT
    formatted_work_type AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings
WHERE formatted_work_type IS NOT NULL
  AND TRIM(formatted_work_type) <> ''
GROUP BY formatted_work_type
ORDER BY nb_offres DESC;
"""
```
### 12.3 Chargement des données depuis Snowflake

Après la définition des requêtes SQL, les résultats sont récupérés à l’aide de la session Snowflake active.  
Chaque requête est exécutée avec `session.sql(...)`, puis convertie en DataFrame Pandas avec `.to_pandas()`.
```
# CHARGEMENT DES DONNEES

df_top_titles = session.sql(query_top_titles).to_pandas()
df_top_salary = session.sql(query_top_salary).to_pandas()
df_industry_distribution = session.sql(query_industry_distribution).to_pandas()
df_work_type_distribution = session.sql(query_work_type_distribution).to_pandas()
````

### 12.4 Analyse 1 : Top 10 des titres de postes les plus publiés par industrie

Cette première partie de l’application permet à l’utilisateur de sélectionner une industrie, puis d’afficher les 10 titres de postes les plus publiés dans cette industrie.

L’affichage comprend :

- une liste déroulante pour choisir l’industrie,
- un histogramme en barres,
- un tableau des résultats.

```
# ANALYSE 1 : TOP 10 DES TITRES LES PLUS PUBLIES PAR INDUSTRIE

st.header("1. Top 10 des titres de postes les plus publiés par industrie")

if not df_top_titles.empty:
    industries_titles = sorted(df_top_titles["INDUSTRY_ID"].astype(str).unique())
    selected_industry_titles = st.selectbox(
        "Choisir une industrie pour les titres les plus publiés",
        industries_titles,
        key="titles_industry"
    )

    filtered_titles = df_top_titles[
        df_top_titles["INDUSTRY_ID"].astype(str) == selected_industry_titles
    ].copy()

    chart_titles = (
        alt.Chart(filtered_titles)
        .mark_bar()
        .encode(
            x=alt.X("TITLE:N", sort="-x", title="Titre du poste"),
            y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
            tooltip=["INDUSTRY_ID", "TITLE", "NB_OFFRES", "RANG"]
        )
        .properties(height=450)
    )

    st.altair_chart(chart_titles, use_container_width=True)
    st.dataframe(filtered_titles, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 1.")
```

<img width="798" height="525" alt="TOP_10_industrie" src="https://github.com/user-attachments/assets/cffa0231-153c-4bab-800f-e3b08161cb37" />


### 12.5 Analyse 2 : Top 10 des postes les mieux rémunérés par industrie


Cette deuxième partie permet à l’utilisateur de sélectionner une industrie et d’afficher les 10 postes les mieux rémunérés, selon le salaire maximum moyen.

L’affichage comprend :

- une liste déroulante pour choisir l’industrie,
- un histogramme en barres,
- un tableau récapitulatif.

```
# ANALYSE 2 : TOP 10 DES POSTES LES MIEUX REMUNERES PAR INDUSTRIE

st.header("2. Top 10 des postes les mieux rémunérés par industrie")

if not df_top_salary.empty:
    industries_salary = sorted(df_top_salary["INDUSTRY_ID"].astype(str).unique())
    selected_industry_salary = st.selectbox(
        "Choisir une industrie pour les postes les mieux rémunérés",
        industries_salary,
        key="salary_industry"
    )

    filtered_salary = df_top_salary[
        df_top_salary["INDUSTRY_ID"].astype(str) == selected_industry_salary
    ].copy()

    chart_salary = (
        alt.Chart(filtered_salary)
        .mark_bar()
        .encode(
            x=alt.X("TITLE:N", sort="-x", title="Titre du poste"),
            y=alt.Y("AVG_MAX_SALARY:Q", title="Salaire maximum moyen"),
            
            tooltip=["INDUSTRY_ID", "TITLE", "AVG_MAX_SALARY", "RANG"]
        )
        .properties(height=450)
    )

    st.altair_chart(chart_salary, use_container_width=True)
    st.dataframe(filtered_salary, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 2.")

```

<img width="776" height="454" alt="salaire_max_moy" src="https://github.com/user-attachments/assets/0ba2a1ef-7f08-4074-8134-80c0f109373b" />


### 12.6 Analyse 3 : Répartition des offres d’emploi par secteur d’activité

Cette visualisation montre la répartition des offres d’emploi selon les secteurs d’activité.

L’affichage comprend :

- un histogramme en barres,
- un tableau des effectifs et pourcentages par secteur.

```
# ANALYSE 4 : REPARTITION DES OFFRES PAR SECTEUR D'ACTIVITE

st.header("4. Répartition des offres d'emploi par secteur d'activité")

if not df_industry_distribution.empty:
    chart_industry_distribution = (
        alt.Chart(df_industry_distribution)
        .mark_bar()
        .encode(
            x=alt.X("INDUSTRY_ID:N", sort="-y", title="Secteur d'activité"),
            y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
            tooltip=["INDUSTRY_ID", "NB_OFFRES", "POURCENTAGE"]
        )
        .properties(height=500)
    )

    st.altair_chart(chart_industry_distribution, use_container_width=True)
    st.dataframe(df_industry_distribution, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 3.")
```
<img width="764" height="459" alt="image" src="https://github.com/user-attachments/assets/2d0a133a-4888-46e0-922f-afb96817bd59" />


###  12.7   Analyse 5 : Répartition des offres d’emploi par type d’emploi

Cette dernière visualisation présente la répartition des offres par type d’emploi.

Deux représentations graphiques sont proposées :

- un histogramme pour comparer les effectifs,
- un diagramme en donut pour représenter visuellement la répartition.

Un tableau récapitulatif est également affiché.

```
# ANALYSE 5 : REPARTITION DES OFFRES PAR TYPE D'EMPLOI

st.header("5. Répartition des offres d'emploi par type d'emploi")

if not df_work_type_distribution.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Histogramme des types d'emploi")

        chart_work_type_distribution = (
            alt.Chart(df_work_type_distribution)
            .mark_bar()
            .encode(
                x=alt.X("TYPE_EMPLOI:N", sort="-y", title="Type d'emploi"),
                y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
                tooltip=["TYPE_EMPLOI", "NB_OFFRES", "POURCENTAGE"]
            )
            .properties(height=450)
        )

        st.altair_chart(chart_work_type_distribution, use_container_width=True)

    with col2:
        st.subheader("Répartition en donut chart")

        donut_chart = (
            alt.Chart(df_work_type_distribution)
            .mark_arc(innerRadius=70)
            .encode(
                theta=alt.Theta("NB_OFFRES:Q", title="Nombre d'offres"),
                color=alt.Color("TYPE_EMPLOI:N", title="Type d'emploi"),
                tooltip=["TYPE_EMPLOI", "NB_OFFRES", "POURCENTAGE"]
            )
            .properties(height=450)
        )

        st.altair_chart(donut_chart, use_container_width=True)

    st.subheader("Table des résultats")
    st.dataframe(df_work_type_distribution, use_container_width=True)

else:
    st.warning("Aucune donnée disponible pour l'analyse 5.")
```

<img width="783" height="427" alt="image" src="https://github.com/user-attachments/assets/fee50481-9e2a-47e9-9d98-7ef8a590dfc3" />


###  12.8 Analyse 6 : Compétences les plus demandées

Cette visualisation met en évidence les compétences les plus demandées dans les offres d’emploi.
L’affichage comprend :

-un histogramme des compétences les plus fréquentes,
-un diagramme en donut,
-un tableau récapitulatif des résultats.
```
query_top_skills = """
SELECT
    skill_abr,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.job_skills
GROUP BY skill_abr
ORDER BY nb_offres DESC
LIMIT 10;
"""

df_top_skills = session.sql(query_top_skills).to_pandas()

st.header("6. Compétences les plus demandées")

if not df_top_skills.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Histogramme des compétences")

        chart_top_skills = (
            alt.Chart(df_top_skills)
            .mark_bar()
            .encode(
                x=alt.X("NB_OFFRES:Q", title="Nombre d'offres"),
                y=alt.Y("SKILL_ABR:N", sort="-x", title="Compétence"),
                tooltip=["SKILL_ABR", "NB_OFFRES"]
            )
            .properties(height=450)
        )

        st.altair_chart(chart_top_skills, use_container_width=True)

    with col2:
        st.subheader("Répartition en donut chart")

        donut_skills = (
            alt.Chart(df_top_skills)
            .mark_arc(innerRadius=70)
            .encode(
                theta=alt.Theta("NB_OFFRES:Q", title="Nombre d'offres"),
                color=alt.Color("SKILL_ABR:N", title="Compétence"),
                tooltip=["SKILL_ABR", "NB_OFFRES"]
            )
            .properties(height=450)
        )

        st.altair_chart(donut_skills, use_container_width=True)

    st.subheader("Table des résultats")
    st.dataframe(df_top_skills, use_container_width=True)

else:
    st.warning("Aucune donnée disponible pour l'analyse 6.")
```
<img width="610" height="466" alt="image" src="https://github.com/user-attachments/assets/0895bb18-2317-4bc7-a96d-05e237c6a05a" />

12.9 Analyse 7 : Avantages les plus fréquents

Cette visualisation permet d’identifier les avantages les plus fréquemment proposés dans les offres d’emploi.
L’affichage comprend :

-un histogramme des avantages les plus fréquents,
-un diagramme en donut,
-un tableau récapitulatif des résultats.
```
12.7 Analyse 7 : Avantages les plus fréquents

Cette visualisation permet d’identifier les avantages les plus fréquemment proposés dans les offres d’emploi.
L’affichage comprend :

un histogramme des avantages les plus fréquents,
un diagramme en donut,
un tableau récapitulatif des résultats.
```
<img width="539" height="472" alt="image" src="https://github.com/user-attachments/assets/12526391-08d9-45eb-9093-516e1a9e5c7f" />

### 12.10 Résumé de la partie Streamlit

La partie Streamlit complète le projet en ajoutant une couche de visualisation interactive au-dessus des tables Gold.

L’application permet :

- d’exécuter des requêtes SQL directement sur Snowflake,
- de transformer les résultats en DataFrames Pandas,
- d’afficher des graphiques interactifs avec Altair,
- de proposer des filtres dynamiques pour certaines analyses.

Cette interface rend les résultats plus lisibles et facilite l’exploration des données pour l’utilisateur final.
---

## 13. Conclusion

Ce projet a permis de :

- charger des fichiers CSV et JSON dans Snowflake,
- structurer les données selon une architecture Medallion,
- nettoyer les données dans la couche Silver,
- préparer des tables analytiques dans la couche Gold,
- réaliser plusieurs analyses sur les offres d’emploi LinkedIn,
- visualiser les résultats avec Streamlit.

Les principaux apprentissages concernent :

- la gestion des erreurs de chargement,
- le nettoyage des données,
- la structuration des pipelines de données,
- l’importance de séparer données brutes, données nettoyées et données analytiques.
