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

```sql
USE WAREHOUSE COMPUTE_WH;
```

### 1.3 Création du stage S3 et des formats de fichiers

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

### 2.1 Table `bronze.companies`

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
        $1:company_id::VARCHAR, $1:name::VARCHAR, $1:description::VARCHAR,
        $1:company_size::INT, $1:state::VARCHAR, $1:country::VARCHAR,
        $1:city::VARCHAR, $1:zip_code::VARCHAR, $1:address::VARCHAR, $1:url::VARCHAR
    FROM @bronze.linkedin_stage/companies.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';
```

### 2.2 Table `bronze.employee_counts`

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

```sql
CREATE OR REPLACE TABLE bronze.company_industries (
  company_id VARCHAR,
  industry   VARCHAR
);

COPY INTO bronze.company_industries
FROM (
    SELECT $1:company_id::VARCHAR, $1:industry::VARCHAR
    FROM @bronze.linkedin_stage/company_industries.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';
```

### 2.4 Table `bronze.company_specialities`

```sql
CREATE OR REPLACE TABLE bronze.company_specialities (
  company_id VARCHAR,
  speciality VARCHAR
);

COPY INTO bronze.company_specialities
FROM (
    SELECT $1:company_id::VARCHAR, $1:speciality::VARCHAR
    FROM @bronze.linkedin_stage/company_specialities.json
    (FILE_FORMAT => bronze.json_format)
)
ON_ERROR = 'CONTINUE';
```

---

## 3. Couche Silver : nettoyage des données

### 3.1 Table `silver.companies`

- suppression des doublons avec `ROW_NUMBER()`,
- nettoyage avec `TRIM()` et `UPPER()`,
- `company_size` null remplacé par `-1`,
- filtre sur `company_size` entre 0 et 7.

```sql
USE SCHEMA silver;

CREATE OR REPLACE TABLE silver.companies AS
SELECT
    TRIM(company_id) AS company_id, TRIM(name) AS name, TRIM(description) AS description,
    COALESCE(company_size, -1) AS company_size,
    UPPER(TRIM(state)) AS state, UPPER(TRIM(country)) AS country,
    TRIM(city) AS city, TRIM(zip_code) AS zip_code, TRIM(address) AS address, TRIM(url) AS url
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY company_id) AS rn
    FROM bronze.companies
    WHERE company_id IS NOT NULL AND TRIM(company_id) <> ''
      AND name IS NOT NULL AND TRIM(name) <> ''
)
WHERE rn = 1
  AND (company_size IS NULL OR company_size BETWEEN 0 AND 7);
```

### 3.2 Table `silver.employee_counts`

- exclusion des valeurs négatives,
- garde uniquement l'enregistrement le plus récent par entreprise.

```sql
CREATE OR REPLACE TABLE silver.employee_counts AS
SELECT TRIM(company_id) AS company_id, employee_count, follower_count, time_recorded
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY TRIM(company_id)
               ORDER BY time_recorded DESC NULLS LAST
           ) AS rn
    FROM bronze.employee_counts
    WHERE company_id IS NOT NULL AND TRIM(company_id) <> ''
      AND employee_count >= 0 AND follower_count >= 0
)
WHERE rn = 1;
```

### 3.3 Table `silver.company_industries`

```sql
CREATE OR REPLACE TABLE silver.company_industries AS
SELECT DISTINCT TRIM(company_id) AS company_id, TRIM(industry) AS industry
FROM bronze.company_industries
WHERE company_id IS NOT NULL AND TRIM(company_id) <> ''
  AND industry IS NOT NULL AND TRIM(industry) <> '';
```

### 3.4 Table `silver.company_specialities`

```sql
CREATE OR REPLACE TABLE silver.company_specialities AS
SELECT DISTINCT TRIM(company_id) AS company_id, TRIM(speciality) AS speciality
FROM bronze.company_specialities
WHERE company_id IS NOT NULL AND TRIM(company_id) <> ''
  AND speciality IS NOT NULL AND TRIM(speciality) <> '';
```

---

## 4. Couche Gold : tables prêtes pour l'analyse

### 4.1 Table `gold.companies`

Ajout du libellé lisible `company_size_label`.

```sql
USE SCHEMA gold;

CREATE OR REPLACE TABLE gold.companies AS
SELECT
    company_id, name, description, company_size,
    CASE company_size
        WHEN 0 THEN '1 (Très petite)' WHEN 1 THEN '2-10'
        WHEN 2 THEN '11-50'           WHEN 3 THEN '51-200'
        WHEN 4 THEN '201-500'         WHEN 5 THEN '501-1000'
        WHEN 6 THEN '1001-5000'       WHEN 7 THEN '5000+'
        ELSE 'Non spécifié'
    END AS company_size_label,
    state, country, city, zip_code, address, url
FROM silver.companies
WHERE name IS NOT NULL AND TRIM(name) <> '';
```

### 4.2 Table `gold.employee_counts`

```sql
CREATE OR REPLACE TABLE gold.employee_counts AS
SELECT
    ec.company_id, c.name AS company_name, c.country,
    c.company_size, c.company_size_label,
    ec.employee_count, ec.follower_count, ec.time_recorded
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY time_recorded DESC NULLS LAST) AS rn
    FROM silver.employee_counts
) ec
JOIN gold.companies c ON ec.company_id = c.company_id
WHERE ec.rn = 1;
```

### 4.3 Table `gold.company_industries`

```sql
CREATE OR REPLACE TABLE gold.company_industries AS
SELECT ci.company_id, c.name AS company_name, c.country, c.company_size, c.company_size_label, ci.industry
FROM silver.company_industries ci
JOIN gold.companies c ON ci.company_id = c.company_id;
```

### 4.4 Table `gold.company_specialities`

```sql
CREATE OR REPLACE TABLE gold.company_specialities AS
SELECT cs.company_id, c.name AS company_name, c.country, c.company_size, c.company_size_label, cs.speciality
FROM silver.company_specialities cs
JOIN gold.companies c ON cs.company_id = c.company_id;
```

### 4.5 Table `gold.companies_full`

```sql
CREATE OR REPLACE TABLE gold.companies_full AS
SELECT
    c.company_id, c.name AS company_name, c.description, c.company_size, c.company_size_label,
    c.state, c.country, c.city, c.url,
    ec.employee_count, ec.follower_count, ci.industry, cs.speciality
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

```sql
SELECT company_id, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.companies
GROUP BY company_id HAVING COUNT(*) > 1;

SELECT company_id, industry, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_industries
GROUP BY company_id, industry HAVING COUNT(*) > 1;

SELECT company_id, speciality, COUNT(*) AS nb_lignes
FROM linkedin_lab.silver.company_specialities
GROUP BY company_id, speciality HAVING COUNT(*) > 1;
```

### 5.3 Vérification des chaînes vides

```sql
SELECT * FROM linkedin_lab.silver.companies
WHERE company_id IS NULL OR TRIM(company_id) = ''
   OR name IS NULL OR TRIM(name) = '';

SELECT * FROM linkedin_lab.silver.employee_counts
WHERE company_id IS NULL OR TRIM(company_id) = '';
```

### 5.4 Vérification de la cohérence métier

```sql
-- company_size hors plage [0-7]
SELECT * FROM linkedin_lab.silver.companies
WHERE company_size NOT BETWEEN 0 AND 7 AND company_size != -1;

-- Effectifs ou followers négatifs
SELECT * FROM linkedin_lab.silver.employee_counts
WHERE employee_count < 0 OR follower_count < 0;

-- Contrôle croisé taille vs effectifs
SELECT c.company_size_label, ROUND(AVG(ec.employee_count), 0) AS avg_employees, COUNT(*) AS nb_entreprises
FROM linkedin_lab.gold.employee_counts ec
JOIN linkedin_lab.gold.companies c ON ec.company_id = c.company_id
GROUP BY c.company_size, c.company_size_label
ORDER BY c.company_size;
```

---

## 6. Problèmes rencontrés et solutions

### 6.1 La jointure entre job_postings et companies ne fonctionnait pas

**Problème :** la colonne `company_name` de `job_postings.csv` contenait des valeurs numériques comme `3570660.0` — 0 match sur 7077 lignes.

**Solution :** renommer en `company_id` dès le Bronze et nettoyer le `.0` dans Silver :

```sql
TO_VARCHAR(TRY_CAST(TRIM(company_id) AS BIGINT)) AS company_id
```

### 6.2 Les IDs étaient chargés comme des floats

**Problème :** `3570660.0` ≠ `3570660` → jointure impossible.  
**Solution :** `TO_VARCHAR(TRY_CAST(... AS BIGINT))` pour convertir float → entier → chaîne.

### 6.3 Erreur `invalid identifier 'C.COMPANY_SIZE_LABEL'`

**Problème :** `company_size_label` absent de `silver.companies`.  
**Solution :** recalculer le `CASE company_size` directement dans `gold.job_postings`.

---

## 7. Partie Streamlit

### 7.1 Analyse 3 : Répartition des offres par taille d'entreprise

Histogramme + donut chart côte à côte + tableau.

```python
query_company_size = """
SELECT
    company_size AS company_size_order, company_size_label,
    COUNT(DISTINCT job_id) AS nb_offres,
    ROUND(100.0 * COUNT(DISTINCT job_id) / SUM(COUNT(DISTINCT job_id)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings_full
WHERE company_size_label IS NOT NULL AND company_size_label <> 'Non spécifié'
GROUP BY company_size, company_size_label
ORDER BY company_size
"""

st.header("3. Répartition des offres d'emploi par taille d'entreprise")

col1, col2 = st.columns(2)
with col1:
    st.subheader("Histogramme par taille d'entreprise")
    chart_size_bar = (
        alt.Chart(df_company_size).mark_bar()
        .encode(
            x=alt.X("COMPANY_SIZE_LABEL:N",
                    sort=alt.SortField(field="COMPANY_SIZE_ORDER", order="ascending"),
                    title="Taille de l'entreprise"),
            y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
            tooltip=[alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                     alt.Tooltip("NB_OFFRES:Q", title="Nombre d'offres"),
                     alt.Tooltip("POURCENTAGE:Q", title="Pourcentage (%)")]
        ).properties(height=450)
    )
    st.altair_chart(chart_size_bar, use_container_width=True)

with col2:
    st.subheader("Répartition en donut chart")
    donut_size = (
        alt.Chart(df_company_size).mark_arc(innerRadius=70)
        .encode(
            theta=alt.Theta("NB_OFFRES:Q"),
            color=alt.Color("COMPANY_SIZE_LABEL:N", title="Taille"),
            tooltip=[alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                     alt.Tooltip("NB_OFFRES:Q", title="Nombre d'offres"),
                     alt.Tooltip("POURCENTAGE:Q", title="Pourcentage (%)")]
        ).properties(height=450)
    )
    st.altair_chart(donut_size, use_container_width=True)

st.subheader("Table des résultats")
st.dataframe(df_company_size, use_container_width=True)
```

<img width="1638" height="562" alt="Capture d&#39;écran 2026-04-19 022607" src="https://github.com/user-attachments/assets/8d81b67d-8490-43e4-b6c3-416e77feb3fe" />


---

### 7.2 Analyse 6 : Top 10 des industries les plus représentées

Histogramme horizontal + tableau.

```python
query_company_industries = """
SELECT industry, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_industries
GROUP BY industry
ORDER BY nb_entreprises DESC
LIMIT 10
"""

st.header("6. Top 10 des industries les plus représentées parmi les entreprises")

chart_industries = (
    alt.Chart(df_company_industries).mark_bar()
    .encode(
        x=alt.X("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
        y=alt.Y("INDUSTRY:N", sort="-x", title="Secteur d'activité"),
        tooltip=[alt.Tooltip("INDUSTRY:N", title="Industrie"),
                 alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")]
    ).properties(height=400)
)
st.altair_chart(chart_industries, use_container_width=True)
st.dataframe(df_company_industries, use_container_width=True)
```

<img width="1659" height="535" alt="Capture d&#39;écran 2026-04-19 022701" src="https://github.com/user-attachments/assets/2e90cb8f-f090-4d3c-9a28-f0198dab1981" />


---

### 7.3 Analyse 7 : Top 10 des spécialités les plus représentées

Histogramme horizontal + tableau.

```python
query_company_specialities = """
SELECT speciality, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_specialities
GROUP BY speciality
ORDER BY nb_entreprises DESC
LIMIT 10
"""

st.header("7. Top 10 des spécialités les plus représentées parmi les entreprises")

chart_specialities = (
    alt.Chart(df_company_specialities).mark_bar()
    .encode(
        x=alt.X("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
        y=alt.Y("SPECIALITY:N", sort="-x", title="Spécialité"),
        tooltip=[alt.Tooltip("SPECIALITY:N", title="Spécialité"),
                 alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")]
    ).properties(height=400)
)
st.altair_chart(chart_specialities, use_container_width=True)
st.dataframe(df_company_specialities, use_container_width=True)
```

<img width="1647" height="537" alt="Capture d&#39;écran 2026-04-19 022718" src="https://github.com/user-attachments/assets/58995b2d-5f20-472b-88e8-8b7c410880b3" />


---

### 7.4 Analyse 8 : Top 10 des entreprises avec le plus de followers

Histogramme horizontal coloré par taille + tableau.

```python
query_top_followers = """
SELECT company_name, follower_count, employee_count, company_size_label, country
FROM linkedin_lab.gold.employee_counts
ORDER BY follower_count DESC
LIMIT 10
"""

st.header("8. Top 10 des entreprises avec le plus de followers")

chart_followers = (
    alt.Chart(df_top_followers).mark_bar()
    .encode(
        x=alt.X("FOLLOWER_COUNT:Q", title="Nombre de followers"),
        y=alt.Y("COMPANY_NAME:N", sort="-x", title="Entreprise"),
        color=alt.Color("COMPANY_SIZE_LABEL:N", title="Taille"),
        tooltip=[alt.Tooltip("COMPANY_NAME:N", title="Entreprise"),
                 alt.Tooltip("FOLLOWER_COUNT:Q", title="Followers"),
                 alt.Tooltip("EMPLOYEE_COUNT:Q", title="Employés"),
                 alt.Tooltip("COMPANY_SIZE_LABEL:N", title="Taille"),
                 alt.Tooltip("COUNTRY:N", title="Pays")]
    ).properties(height=400)
)
st.altair_chart(chart_followers, use_container_width=True)
st.dataframe(df_top_followers, use_container_width=True)
```

<img width="1650" height="519" alt="Capture d&#39;écran 2026-04-19 022736" src="https://github.com/user-attachments/assets/015ba006-f760-4f26-8a3e-82a2bf454a64" />


---

### 7.5 Analyse 9 : Répartition des entreprises par pays

Histogramme vertical + tableau.

```python
query_companies_by_country = """
SELECT country, COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.companies
WHERE country IS NOT NULL AND TRIM(country) <> ''
GROUP BY country
ORDER BY nb_entreprises DESC
LIMIT 15
"""

st.header("9. Répartition des entreprises par pays (Top 15)")

chart_country = (
    alt.Chart(df_companies_by_country).mark_bar()
    .encode(
        x=alt.X("COUNTRY:N", sort="-y", title="Pays"),
        y=alt.Y("NB_ENTREPRISES:Q", title="Nombre d'entreprises"),
        tooltip=[alt.Tooltip("COUNTRY:N", title="Pays"),
                 alt.Tooltip("NB_ENTREPRISES:Q", title="Nombre d'entreprises")]
    ).properties(height=450)
)
st.altair_chart(chart_country, use_container_width=True)
st.dataframe(df_companies_by_country, use_container_width=True)
```

<img width="1639" height="551" alt="Capture d&#39;écran 2026-04-19 022748" src="https://github.com/user-attachments/assets/abe37c78-b6f8-4c9f-ac68-13cdf3583a2d" />


---

## 8. Conclusion

Cette partie du projet a permis de charger les fichiers entreprises dans Snowflake, de structurer les données selon l'architecture Medallion, de résoudre le problème de jointure lié aux IDs chargés comme floats, de préparer les tables Gold, et de réaliser les analyses sur les tailles, industries, spécialités et géographie des entreprises avec Streamlit.
