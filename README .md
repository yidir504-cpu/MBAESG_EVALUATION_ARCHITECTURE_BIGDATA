# Projet : Analyse des Offres d'Emploi LinkedIn avec Snowflake

## Présentation

Ce projet a pour objectif d'analyser un jeu de données LinkedIn dans **Snowflake** afin d'étudier le marché de l'emploi à partir de plusieurs tables CSV et JSON disponibles dans un bucket S3 public.

Le travail a été organisé avec une architecture **Medallion** :

- **Bronze** : données brutes chargées depuis S3
- **Silver** : données nettoyées et standardisées
- **Gold** : données enrichies prêtes pour l'analyse

---

## Répartition des tâches

| Fichier | Responsable |
|---|---|
| `job_postings.csv` | Binôme 1 |
| `benefits.csv` | Binôme 1 |
| `job_skills.csv` | Binôme 1 |
| `job_industries.json` | Binôme 1 |
| `companies.json` | Binôme 2 |
| `employee_counts.csv` | Binôme 2 |
| `company_industries.json` | Binôme 2 |
| `company_specialities.json` | Binôme 2 |

---

## Objectifs

Les analyses réalisées sont :

1. Top 10 des titres de postes les plus publiés par industrie
2. Top 10 des postes les mieux rémunérés par industrie
3. Répartition des offres d'emploi par taille d'entreprise
4. Répartition des offres d'emploi par secteur d'activité
5. Répartition des offres d'emploi par type d'emploi

Analyses bonus :

6. Top 10 des industries les plus représentées parmi les entreprises
7. Top 10 des spécialités les plus représentées parmi les entreprises
8. Top 10 des entreprises avec le plus de followers
9. Répartition des entreprises par pays

---

## Technologies utilisées

- **Snowflake**
- **SQL**
- **Streamlit in Snowflake**
- **GitHub**

---

## Structure du projet

```
linkedin_lab/
├── linkedin_lab.sql      # Script SQL complet (Bronze → Silver → Gold + contrôles qualité)
├── streamlit_app.py      # Application Streamlit complète (9 visualisations)
└── README.md             # Documentation du projet
```

### Structure des tables dans Snowflake

```
linkedin_lab
├── bronze
│   ├── job_postings
│   ├── benefits
│   ├── job_skills
│   ├── job_industries
│   ├── companies
│   ├── employee_counts
│   ├── company_industries
│   └── company_specialities
├── silver
│   ├── job_postings
│   ├── benefits
│   ├── job_skills
│   ├── job_industries
│   ├── companies
│   ├── employee_counts
│   ├── company_industries
│   └── company_specialities
└── gold
    ├── job_postings
    ├── benefits
    ├── job_skills
    ├── job_industries
    ├── job_postings_full
    ├── companies
    ├── employee_counts
    ├── company_industries
    ├── company_specialities
    └── companies_full
```

---

## Étapes de réalisation

### 1. Création de la base et des schémas

Création de la base de données `linkedin_lab` et des trois schémas de l'architecture Medallion.

```sql
CREATE DATABASE IF NOT EXISTS linkedin_lab;
USE DATABASE linkedin_lab;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

### 2. Sélection du warehouse

Le warehouse représente la ressource de calcul utilisée pour exécuter les requêtes SQL. Sans cette instruction, les commandes comme `COPY INTO` ou `CREATE TABLE AS SELECT` peuvent échouer.

```sql
USE WAREHOUSE COMPUTE_WH;
```

### 3. Création du stage S3 et des formats de fichiers

Un **stage externe** est créé pour référencer le bucket S3 public. Deux formats de fichiers sont définis : CSV pour les fichiers tabulaires, JSON pour les fichiers structurés.

```sql
CREATE OR REPLACE STAGE linkedin_stage
  URL = 's3://snowflake-lab-bucket/';

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', 'N/A', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE;

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE;
```

### 4. Couche Bronze : chargement des données brutes

Les données sont chargées telles quelles depuis S3 sans transformation. Chaque fichier correspond à une table Bronze.

**Point important découvert lors du projet** : la colonne `company_name` du fichier `job_postings.csv` contient en réalité des IDs numériques (ex: `3570660.0`). Elle a été renommée `company_id` dès le Bronze pour permettre la jointure avec la table `companies`.

### 5. Couche Silver : nettoyage des données

Les opérations appliquées dans cette couche sont :

- Suppression des doublons avec `ROW_NUMBER() OVER (PARTITION BY ...)`
- Suppression des lignes avec champs clés vides
- Nettoyage des espaces avec `TRIM()`
- Standardisation de la casse avec `UPPER()`
- Remplacement des valeurs nulles avec `COALESCE()`
- Contrôles de cohérence métier (salaires, dates, valeurs négatives)
- Nettoyage du `.0` des company_id avec `TO_VARCHAR(TRY_CAST(... AS BIGINT))`

### 6. Couche Gold : tables enrichies

Les tables Gold combinent les données Silver pour créer des tables orientées métier :

- `gold.job_postings` : offres enrichies avec les infos entreprise (taille, nom)
- `gold.job_postings_full` : table complète offres + entreprises + industries + compétences + avantages
- `gold.companies` : entreprises avec libellé de taille lisible (`company_size_label`)
- `gold.companies_full` : table complète entreprises + effectifs + industries + spécialités

### 7. Contrôles qualité

Plusieurs niveaux de contrôle sont effectués :

- **Volumétrie** : comparaison du nombre de lignes Bronze / Silver / Gold
- **Unicité** : vérification qu'il n'y a pas de doublons sur les clés primaires
- **Chaînes vides** : vérification que les champs obligatoires ne sont pas vides
- **Cohérence métier** : salaires positifs, dates cohérentes, tailles d'entreprise valides

---

## Problèmes rencontrés et solutions

### Problème 1 : company_name contient des IDs numériques

**Problème** : La colonne `company_name` de `job_postings.csv` contenait des valeurs numériques comme `3570660.0` au lieu de noms d'entreprises. La jointure avec `companies` ne fonctionnait donc pas.

**Solution** : Renommer la colonne en `company_id` dès le Bronze, et nettoyer le `.0` dans Silver avec `TO_VARCHAR(TRY_CAST(TRIM(company_id) AS BIGINT))`.

### Problème 2 : IDs chargés comme floats

**Problème** : Le CSV chargeait les IDs numériques avec un `.0` (ex: `3570660.0`), ce qui empêchait la jointure avec les IDs entiers de `companies` (ex: `3570660`).

**Solution** : Utiliser `TO_VARCHAR(TRY_CAST(... AS BIGINT))` dans Silver pour convertir proprement le float en entier puis en chaîne.

---

## Visualisations Streamlit

L'application Streamlit (`streamlit_app.py`) contient 9 visualisations :

| # | Analyse | Type de graphique |
|---|---|---|
| 1 | Top 10 titres par industrie | Histogramme + filtre |
| 2 | Top 10 postes mieux rémunérés | Histogramme + filtre |
| 3 | Répartition par taille d'entreprise | Histogramme + Donut |
| 4 | Répartition par secteur d'activité | Histogramme |
| 5 | Répartition par type d'emploi | Histogramme + Donut |
| 6 | Top 10 industries entreprises | Histogramme |
| 7 | Top 10 spécialités entreprises | Histogramme |
| 8 | Top 10 entreprises par followers | Histogramme |
| 9 | Répartition entreprises par pays | Histogramme |

---

## Conclusion

Ce projet a permis de :

- Charger des fichiers CSV et JSON dans Snowflake depuis un bucket S3
- Structurer les données selon une architecture Medallion Bronze / Silver / Gold
- Nettoyer et standardiser les données dans la couche Silver
- Préparer des tables analytiques enrichies dans la couche Gold
- Réaliser plusieurs analyses sur les offres d'emploi LinkedIn
- Visualiser les résultats avec Streamlit in Snowflake

Les principaux apprentissages concernent :

- La gestion des erreurs de chargement et des types de données inattendus
- L'importance de vérifier le contenu réel des colonnes avant de les utiliser
- La structuration des pipelines de données en couches séparées
- La jointure entre tables via des clés techniques plutôt que des libellés texte
