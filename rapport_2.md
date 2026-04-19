# Rapport Binôme 2 — Analyse des Entreprises LinkedIn avec Snowflake

## Présentation

Ce rapport détaille la partie réalisée par le Binôme 2 dans le cadre du projet d'analyse des offres d'emploi LinkedIn avec Snowflake.

Les fichiers pris en charge sont :

- `companies.json`
- `employee_counts.csv`
- `company_industries.json`
- `company_specialities.json`

---

## Objectifs

Les analyses réalisées dans cette partie sont :

1. Répartition des offres d'emploi par taille d'entreprise
2. Top 10 des industries les plus représentées parmi les entreprises
3. Top 10 des spécialités les plus représentées parmi les entreprises
4. Top 10 des entreprises avec le plus de followers
5. Répartition des entreprises par pays

---

## Technologies utilisées

- **Snowflake**
- **SQL**
- **Streamlit in Snowflake**
- **GitHub**

---

## Structure des tables

```text
linkedin_lab
├── bronze
│   ├── companies
│   ├── employee_counts
│   ├── company_industries
│   └── company_specialities
├── silver
│   ├── companies
│   ├── employee_counts
│   ├── company_industries
│   └── company_specialities
└── gold
    ├── companies
    ├── employee_counts
    ├── company_industries
    ├── company_specialities
    └── companies_full
```

---

## 1. Étapes de réalisation

### 1.1 Création de la base et des schémas

```sql
CREATE DATABASE IF NOT EXISTS linkedin_lab;
USE DATABASE linkedin_lab;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### 1.2 Sélection du warehouse

Avant toute opération de chargement ou de transformation, il est nécessaire de sélectionner un **warehouse** actif dans Snowflake.  
Le warehouse représente la ressource de calcul utilisée pour exécuter les requêtes SQL.

```sql
USE WAREHOUSE COMPUTE_WH;
```

### 1.3 Création du stage S3 et des formats de fichiers

Un **stage externe** est créé afin de référencer le bucket S3 public.  
Deux **formats de fichiers** sont définis : CSV pour les fichiers tabulaires, JSON pour les fichiers structurés.

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

---

## 2. Couche Bronze : chargement des données brutes

La couche **Bronze** contient les données brutes importées directement depuis les fichiers du bucket S3, sans transformation avancée.

### 2.1 Table `bronze.companies`

Cette table contient les informations détaillées sur chaque entreprise.

```sql
CREATE OR REPLACE TABLE bronze.companies (
  company_id   VARCHAR,
  name         VARCHAR,
  description  TEXT,
  company_size INT,
  state        VARCHAR,
  country      VARCHAR,
  city         VARCHAR,
  zip_code     VARCHAR,
  address      VARCHAR,
  url          VARCHAR
);

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
```

### 2.2 Table `bronze.employee_counts`

Cette table contient le nombre d'employés et de followers pour chaque entreprise.

```sql
CREATE OR REPLACE TABLE bronze.employee_counts (
  company_id     VARCHAR,
  employee_count INT,
  follower_count INT,
  time_recorded  BIGINT
);

COPY INTO bronze.employee_counts
FROM @bronze.linkedin_stage/employee_counts.csv
FILE_FORMAT = (FORMAT_NAME = bronze.csv_format)
ON_ERROR = 'CONTINUE';
```

### 2.3 Table `bronze.company_industries`

Cette table répertorie les secteurs d'activité associés à chaque entreprise.

```sql
CREATE OR REPLACE TABLE bronze.company_industries (
  company_id VARCHAR,
  industry   VARCHAR
);

COPY INTO bronze.company_industries
FROM (
    SELECT
        $1:company_id::VARCHAR,
        $1:industry::VARCHAR
    FROM @bronze.linkedin_stage/company_industries.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';
```

### 2.4 Table `bronze.company_specialities`

Cette table liste les spécialités associées à chaque entreprise.

```sql
CREATE OR REPLACE TABLE bronze.company_specialities (
  company_id VARCHAR,
  speciality VARCHAR
);

COPY INTO bronze.company_specialities
FROM (
    SELECT
        $1:company_id::VARCHAR,
        $1:speciality::VARCHAR
    FROM @bronze.linkedin_stage/company_specialities.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';
```

---

## 3. Couche Silver : nettoyage des données

La couche **Silver** contient les données nettoyées et standardisées.

### 3.1 Table `silver.companies`

- suppression des lignes avec `company_id` ou `name` vides,
- suppression des doublons avec `ROW_NUMBER()`,
- nettoyage avec `TRIM()` et `UPPER()`,
- remplacement des `company_size` nulles par `-1`,
- filtre sur `company_size` entre 0 et 7.

```sql
USE SCHEMA silver;

CREATE OR REPLACE TABLE silver.companies AS
SELECT
    TRIM(company_id)           AS company_id,
    TRIM(name)                 AS name,
    TRIM(description)          AS description,
    COALESCE(company_size, -1) AS company_size,
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
```

### 3.2 Table `silver.employee_counts`

- suppression des lignes vides,
- exclusion des valeurs négatives,
- garde uniquement l'enregistrement le plus récent par entreprise.

```sql
CREATE OR REPLACE TABLE silver.employee_counts AS
SELECT
    TRIM(company_id) AS company_id,
    employee_count,
    follower_count,
    time_recorded
FROM (
    SELECT *,
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
WHERE rn = 1;
```

### 3.3 Table `silver.company_industries`

- suppression des lignes vides,
- suppression des doublons exacts avec `DISTINCT`.

```sql
CREATE OR REPLACE TABLE silver.company_industries AS
SELECT DISTINCT
    TRIM(company_id) AS company_id,
    TRIM(industry)   AS industry
FROM bronze.company_industries
WHERE company_id IS NOT NULL
  AND TRIM(company_id) <> ''
  AND industry IS NOT NULL
  AND TRIM(industry) <> '';
```

### 3.4 Table `silver.company_specialities`

- suppression des lignes vides,
- suppression des doublons exacts avec `DISTINCT`.

```sql
CREATE OR REPLACE TABLE silver.company_specialities AS
SELECT DISTINCT
    TRIM(company_id) AS company_id,
    TRIM(speciality) AS speciality
FROM bronze.company_specialities
WHERE company_id IS NOT NULL
  AND TRIM(company_id) <> ''
  AND speciality IS NOT NULL
  AND TRIM(speciality) <> '';
```

---

## 4. Couche Gold : tables prêtes pour l'analyse

### 4.1 Table `gold.companies`

Table métier principale avec ajout du libellé `company_size_label`.

```sql
USE SCHEMA gold;

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
```

### 4.2 Table `gold.employee_counts`

Table enrichie avec les infos entreprise, garde uniquement l'enregistrement le plus récent.

```sql
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
```

### 4.3 Table `gold.company_industries`

Table enrichie avec les infos entreprise.

```sql
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
```

### 4.4 Table `gold.company_specialities`

Table enrichie avec les infos entreprise.

```sql
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
```

### 4.5 Table `gold.companies_full`

Table complète regroupant entreprises, effectifs, industries et spécialités.

```sql
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
```

---

## 5. Contrôles qualité

### 5.1 Vérification des volumes

```sql
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
```

### 5.2 Vérification des doublons

#### `companies`

```sql
SELECT company_id, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.companies
GROUP BY company_id
HAVING COUNT(*) > 1;
```

#### `company_industries`

```sql
SELECT company_id, industry, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_industries
GROUP BY company_id, industry
HAVING COUNT(*) > 1;
```

#### `company_specialities`

```sql
SELECT company_id, speciality, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_specialities
GROUP BY company_id, speciality
HAVING COUNT(*) > 1;
```

### 5.3 Vérification des chaînes vides

```sql
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
```

### 5.4 Vérification de la cohérence métier

```sql
-- company_size hors plage [0-7]
SELECT * FROM linkedin_lab.silver.companies
WHERE company_size NOT BETWEEN 0 AND 7
  AND company_size != -1;

-- Effectifs ou followers négatifs
SELECT * FROM linkedin_lab.silver.employee_counts
WHERE employee_count < 0 OR follower_count < 0;

-- Contrôle croisé taille vs effectifs
SELECT
    c.company_size_label,
    ROUND(AVG(ec.employee_count), 0) AS avg_employees,
    COUNT(*) AS nb_entreprises
FROM linkedin_lab.gold.employee_counts ec
JOIN linkedin_lab.gold.companies c ON ec.company_id = c.company_id
GROUP BY c.company_size, c.company_size_label
ORDER BY c.company_size;
```

---

## 6. Problèmes rencontrés et solutions

### 6.1 La jointure entre job_postings et companies ne fonctionnait pas

**Problème :** la colonne `company_name` de `job_postings.csv` contenait des valeurs numériques comme `3570660.0` au lieu de noms d'entreprises. La jointure retournait 0 match sur 7077 lignes.

**Solution :** renommer la colonne en `company_id` dès le Bronze, puis nettoyer le `.0` dans Silver.

```sql
TO_VARCHAR(TRY_CAST(TRIM(company_id) AS BIGINT)) AS company_id
```

### 6.2 Les IDs étaient chargés comme des floats

**Problème :** le CSV chargeait les IDs avec un `.0` (ex: `3570660.0`), empêchant la jointure avec les IDs entiers de `companies` (ex: `3570660`).

**Solution :** `TO_VARCHAR(TRY_CAST(... AS BIGINT))` pour convertir proprement le float en entier puis en chaîne.

### 6.3 Erreur `invalid identifier 'C.COMPANY_SIZE_LABEL'`

**Problème :** `company_size_label` est une colonne calculée dans `gold.companies` mais absente de `silver.companies`. La jointure échouait.

**Solution :** recalculer le `CASE company_size` directement dans la requête de création de `gold.job_postings`.

---

## 7. Analyses réalisées

### 7.1 Répartition des offres d'emploi par taille d'entreprise

```sql
SELECT
    c.company_size_label,
    COUNT(DISTINCT jp.job_id) AS nb_offres,
    ROUND(100 * COUNT(DISTINCT jp.job_id) / SUM(COUNT(DISTINCT jp.job_id)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings jp
JOIN linkedin_lab.gold.companies c ON jp.company_id = c.company_id
GROUP BY c.company_size, c.company_size_label
ORDER BY c.company_size;
```

### 7.2 Top 10 des industries les plus représentées

```sql
SELECT industry, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_industries
GROUP BY industry
ORDER BY nb_entreprises DESC
LIMIT 10;
```

### 7.3 Top 10 des spécialités les plus représentées

```sql
SELECT speciality, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_specialities
GROUP BY speciality
ORDER BY nb_entreprises DESC
LIMIT 10;
```

### 7.4 Top 10 des entreprises avec le plus de followers

```sql
SELECT company_name, follower_count, employee_count, company_size_label, country
FROM linkedin_lab.gold.employee_counts
ORDER BY follower_count DESC
LIMIT 10;
```

### 7.5 Répartition des entreprises par pays

```sql
SELECT country, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.companies
WHERE country IS NOT NULL AND TRIM(country) <> ''
GROUP BY country
ORDER BY nb_entreprises DESC
LIMIT 20;
```

---

## 8. Partie Streamlit

Les visualisations ont été réalisées avec **Streamlit in Snowflake** à partir des tables Gold.

### 8.1 Import des bibliothèques et configuration

```python
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Analyse des entreprises LinkedIn", layout="wide")
st.title("Analyse des Entreprises LinkedIn")
st.markdown("Visualisations construites à partir des tables Gold dans Snowflake.")
session = get_active_session()
```

### 8.2 Analyse 3 : Répartition des offres par taille d'entreprise

Histogramme + donut chart côte à côte + tableau.

```python
query_company_size = """
SELECT
    company_size                AS company_size_order,
    company_size_label,
    COUNT(DISTINCT job_id)      AS nb_offres,
    ROUND(
        100.0 * COUNT(DISTINCT job_id)
        / SUM(COUNT(DISTINCT job_id)) OVER (),
        2
    ) AS pourcentage
FROM linkedin_lab.gold.job_postings_full
WHERE company_size_label IS NOT NULL
  AND company_size_label <> 'Non spécifié'
GROUP BY company_size, company_size_label
ORDER BY company_size
"""

st.header("3. Répartition des offres d'emploi par taille d'entreprise")

if not df_company_size.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Histogramme par taille d'entreprise")
        chart_size_bar = (
            alt.Chart(df_company_size)
            .mark_bar()
            .encode(
                x=alt.X(
                    "COMPANY_SIZE_LABEL:N",
                    sort=alt.SortField(field="COMPANY_SIZE_ORDER", order="ascending"),
                    title="Taille de l'entreprise"
                ),
                y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
                tooltip=[
                    alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                    alt.Tooltip("NB_OFFRES:Q", title="Nombre d'offres"),
                    alt.Tooltip("POURCENTAGE:Q", title="Pourcentage (%)")
                ]
            )
            .properties(height=450)
        )
        st.altair_chart(chart_size_bar, use_container_width=True)

    with col2:
        st.subheader("Répartition en donut chart")
        donut_size = (
            alt.Chart(df_company_size)
            .mark_arc(innerRadius=70)
            .encode(
                theta=alt.Theta("NB_OFFRES:Q", title="Nombre d'offres"),
                color=alt.Color("COMPANY_SIZE_LABEL:N", title="Taille"),
                tooltip=[
                    alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                    alt.Tooltip("NB_OFFRES:Q", title="Nombre d'offres"),
                    alt.Tooltip("POURCENTAGE:Q", title="Pourcentage (%)")
                ]
            )
            .properties(height=450)
        )
        st.altair_chart(donut_size, use_container_width=True)

    st.subheader("Table des résultats")
    st.dataframe(df_company_size, use_container_width=True)
```

> 📸 *Ajoute ici une capture d'écran de l'analyse 3 depuis Snowflake Streamlit*

### 8.3 Analyse 6 : Top 10 des industries les plus représentées

```python
query_company_industries = """
SELECT
    industry,
    COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_industries
GROUP BY industry
ORDER BY nb_entreprises DESC
LIMIT 10
"""

st.header("6. Top 10 des industries les plus représentées parmi les entreprises")

if not df_company_industries.empty:
    chart_industries = (
        alt.Chart(df_company_industries)
        .mark_bar()
        .encode(
            x=alt.X("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
            y=alt.Y("INDUSTRY:N", sort="-x", title="Secteur d'activité"),
            tooltip=[
                alt.Tooltip("INDUSTRY:N", title="Industrie"),
                alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")
            ]
        )
        .properties(height=400)
    )
    st.altair_chart(chart_industries, use_container_width=True)
    st.dataframe(df_company_industries, use_container_width=True)
```

> 📸 *Ajoute ici une capture d'écran de l'analyse 6 depuis Snowflake Streamlit*

### 8.4 Analyse 7 : Top 10 des spécialités les plus représentées

```python
query_company_specialities = """
SELECT
    speciality,
    COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_specialities
GROUP BY speciality
ORDER BY nb_entreprises DESC
LIMIT 10
"""

st.header("7. Top 10 des spécialités les plus représentées parmi les entreprises")

if not df_company_specialities.empty:
    chart_specialities = (
        alt.Chart(df_company_specialities)
        .mark_bar()
        .encode(
            x=alt.X("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
            y=alt.Y("SPECIALITY:N", sort="-x", title="Spécialité"),
            tooltip=[
                alt.Tooltip("SPECIALITY:N", title="Spécialité"),
                alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")
            ]
        )
        .properties(height=400)
    )
    st.altair_chart(chart_specialities, use_container_width=True)
    st.dataframe(df_company_specialities, use_container_width=True)
```

> 📸 *Ajoute ici une capture d'écran de l'analyse 7 depuis Snowflake Streamlit*

### 8.5 Analyse 8 : Top 10 des entreprises avec le plus de followers

```python
query_top_followers = """
SELECT
    company_name,
    follower_count,
    employee_count,
    company_size_label,
    country
FROM linkedin_lab.gold.employee_counts
ORDER BY follower_count DESC
LIMIT 10
"""

st.header("8. Top 10 des entreprises avec le plus de followers")

if not df_top_followers.empty:
    chart_followers = (
        alt.Chart(df_top_followers)
        .mark_bar()
        .encode(
            x=alt.X("FOLLOWER_COUNT:Q", title="Nombre de followers"),
            y=alt.Y("COMPANY_NAME:N", sort="-x", title="Entreprise"),
            color=alt.Color("COMPANY_SIZE_LABEL:N", title="Taille"),
            tooltip=[
                alt.Tooltip("COMPANY_NAME:N", title="Entreprise"),
                alt.Tooltip("FOLLOWER_COUNT:Q", title="Followers"),
                alt.Tooltip("EMPLOYEE_COUNT:Q", title="Employés"),
                alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                alt.Tooltip("COUNTRY:N", title="Pays")
            ]
        )
        .properties(height=400)
    )
    st.altair_chart(chart_followers, use_container_width=True)
    st.dataframe(df_top_followers, use_container_width=True)
```

> 📸 *Ajoute ici une capture d'écran de l'analyse 8 depuis Snowflake Streamlit*

### 8.6 Analyse 9 : Répartition des entreprises par pays

```python
query_companies_by_country = """
SELECT
    country,
    COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.companies
WHERE country IS NOT NULL
  AND TRIM(country) <> ''
GROUP BY country
ORDER BY nb_entreprises DESC
LIMIT 15
"""

st.header("9. Répartition des entreprises par pays (Top 15)")

if not df_companies_by_country.empty:
    chart_country = (
        alt.Chart(df_companies_by_country)
        .mark_bar()
        .encode(
            x=alt.X("COUNTRY:N", sort="-y", title="Pays"),
            y=alt.Y("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
            tooltip=[
                alt.Tooltip("COUNTRY:N", title="Pays"),
                alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")
            ]
        )
        .properties(height=450)
    )
    st.altair_chart(chart_country, use_container_width=True)
    st.dataframe(df_companies_by_country, use_container_width=True)
```

> 📸 *Ajoute ici une capture d'écran de l'analyse 9 depuis Snowflake Streamlit*

### 8.7 Résumé de la partie Streamlit

L'application permet :

- d'exécuter des requêtes SQL directement sur Snowflake,
- de transformer les résultats en DataFrames Pandas,
- d'afficher des graphiques interactifs avec Altair,
- de visualiser les données entreprises sous différents angles.

---

## 9. Conclusion

Cette partie du projet a permis de :

- charger les fichiers entreprises dans Snowflake depuis un bucket S3,
- structurer les données selon l'architecture Medallion,
- nettoyer les données dans la couche Silver,
- préparer les tables analytiques dans la couche Gold,
- résoudre le problème de jointure dû aux IDs chargés comme floats,
- réaliser les analyses sur les tailles, industries, spécialités et géographie des entreprises,
- visualiser les résultats avec Streamlit.
