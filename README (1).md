# Projet : Analyse des Offres d'Emploi LinkedIn avec Snowflake

## Présentation

Ce projet a pour objectif d'analyser un jeu de données LinkedIn dans **Snowflake** afin d'étudier le marché de l'emploi à partir de plusieurs tables CSV et JSON disponibles dans un bucket S3 public.

Le travail a été réalisé en binôme et organisé avec une architecture **Medallion** :

- **Bronze** : données brutes chargées depuis S3
- **Silver** : données nettoyées et standardisées
- **Gold** : données enrichies prêtes pour l'analyse

---

## Répartition des tâches

| Fichier source | Responsable | Tables créées |
|---|---|---|
| `job_postings.csv` | Binôme 1 | `bronze/silver/gold.job_postings` |
| `benefits.csv` | Binôme 1 | `bronze/silver/gold.benefits` |
| `job_skills.csv` | Binôme 1 | `bronze/silver/gold.job_skills` |
| `job_industries.json` | Binôme 1 | `bronze/silver/gold.job_industries` |
| `companies.json` | Binôme 2 | `bronze/silver/gold.companies` |
| `employee_counts.csv` | Binôme 2 | `bronze/silver/gold.employee_counts` |
| `company_industries.json` | Binôme 2 | `bronze/silver/gold.company_industries` |
| `company_specialities.json` | Binôme 2 | `bronze/silver/gold.company_specialities` |

---

## Objectifs

Les analyses demandées et réalisées sont :

1. Top 10 des titres de postes les plus publiés par industrie
2. Top 10 des postes les mieux rémunérés par industrie
3. Répartition des offres d'emploi par taille d'entreprise
4. Répartition des offres d'emploi par secteur d'activité
5. Répartition des offres d'emploi par type d'emploi

Analyses bonus réalisées :

6. Top 10 des industries les plus représentées parmi les entreprises
7. Top 10 des spécialités les plus représentées parmi les entreprises
8. Top 10 des entreprises avec le plus de followers
9. Répartition des entreprises par pays

---

## Technologies utilisées

- **Snowflake** — entrepôt de données cloud
- **SQL** — manipulation et transformation des données
- **Streamlit in Snowflake** — visualisation interactive
- **GitHub** — versionnement et livraison du projet
- **Amazon S3** — stockage des fichiers sources

---

## Structure du projet GitHub

```
MBAESG_EVALUATION_ARCHITECTURE_BIGDATA/
├── README.md              # Ce fichier — présentation générale du projet
├── rapport_1.md           # Rapport détaillé partie Binôme 1
├── rapport_2.md           # Rapport détaillé partie Binôme 2
├── linkedin_lab1.sql      # Script SQL complet Binôme 1
├── linkedin_lab2.sql      # Script SQL complet Binôme 2
└── streamlit_app.py       # Application Streamlit unique (9 visualisations)
```

---

## Structure des tables dans Snowflake

```
linkedin_lab
├── bronze                        ← Données brutes chargées depuis S3
│   ├── job_postings              (Binôme 1)
│   ├── benefits                  (Binôme 1)
│   ├── job_skills                (Binôme 1)
│   ├── job_industries            (Binôme 1)
│   ├── companies                 (Binôme 2)
│   ├── employee_counts           (Binôme 2)
│   ├── company_industries        (Binôme 2)
│   └── company_specialities      (Binôme 2)
├── silver                        ← Données nettoyées et standardisées
│   ├── job_postings              (Binôme 1)
│   ├── benefits                  (Binôme 1)
│   ├── job_skills                (Binôme 1)
│   ├── job_industries            (Binôme 1)
│   ├── companies                 (Binôme 2)
│   ├── employee_counts           (Binôme 2)
│   ├── company_industries        (Binôme 2)
│   └── company_specialities      (Binôme 2)
└── gold                          ← Données enrichies prêtes pour l'analyse
    ├── job_postings              (Binôme 1 + jointure Binôme 2)
    ├── benefits                  (Binôme 1)
    ├── job_skills                (Binôme 1)
    ├── job_industries            (Binôme 1)
    ├── job_postings_full         (table centrale — Binôme 1 + Binôme 2)
    ├── companies                 (Binôme 2)
    ├── employee_counts           (Binôme 2)
    ├── company_industries        (Binôme 2)
    ├── company_specialities      (Binôme 2)
    └── companies_full            (Binôme 2)
```

---

## Architecture Medallion

### Couche Bronze
Les données sont chargées **telles quelles** depuis le bucket S3 public, sans aucune transformation.

- Les fichiers CSV sont chargés avec `COPY INTO` et le format `csv_format`
- Les fichiers JSON sont chargés avec extraction des champs via la notation `$1:champ::TYPE`
- L'option `ON_ERROR = 'CONTINUE'` permet d'ignorer les lignes en erreur sans bloquer le chargement

### Couche Silver
Les données sont **nettoyées et standardisées** :

- Suppression des doublons avec `ROW_NUMBER() OVER (PARTITION BY ...)`
- Suppression des lignes avec champs clés vides
- Nettoyage des espaces avec `TRIM()`
- Standardisation de la casse avec `UPPER()`
- Remplacement des valeurs nulles avec `COALESCE()`
- Contrôles de cohérence métier (salaires positifs, dates cohérentes, tailles valides)
- Correction du problème de `company_id` chargé comme float : `TO_VARCHAR(TRY_CAST(... AS BIGINT))`

### Couche Gold
Les données sont **enrichies et orientées métier** :

- `gold.job_postings` : offres enrichies avec les infos entreprise via jointure sur `company_id`
- `gold.job_postings_full` : table centrale combinant offres + entreprises + industries + compétences + avantages
- `gold.companies` : entreprises avec libellé lisible de taille (`company_size_label`)
- `gold.companies_full` : table entreprises complète avec effectifs, industries et spécialités

---

## Contrôles qualité

| Contrôle | Description |
|---|---|
| Volumétrie | Comparaison du nombre de lignes Bronze / Silver / Gold |
| Unicité | Vérification des doublons sur les clés primaires |
| Chaînes vides | Vérification des champs obligatoires |
| Cohérence métier | Salaires positifs, min ≤ max, dates cohérentes |
| Valeurs aberrantes | Salaires > 1 million, tailles hors plage [0-7] |

---

## Problèmes rencontrés et solutions

### Problème 1 : company_name contient des IDs numériques
**Problème :** La colonne `company_name` de `job_postings.csv` contenait des valeurs numériques comme `3570660.0` au lieu de noms d'entreprises.  
**Solution :** Renommer la colonne en `company_id` dès le Bronze et nettoyer le `.0` dans Silver avec `TO_VARCHAR(TRY_CAST(TRIM(company_id) AS BIGINT))`.

### Problème 2 : IDs chargés comme floats
**Problème :** Le CSV chargeait les IDs avec un `.0`, empêchant la jointure. Résultat : 0 match sur 7077 lignes.  
**Solution :** `TO_VARCHAR(TRY_CAST(... AS BIGINT))` pour convertir proprement le float en entier puis en chaîne.

### Problème 3 : Erreur `invalid identifier 'C.COMPANY_SIZE_LABEL'`
**Problème :** `company_size_label` est une colonne calculée dans `gold.companies` mais absente de `silver.companies`.  
**Solution :** Recalculer le `CASE company_size` directement dans la requête de création de `gold.job_postings`.

---

## Visualisations Streamlit

| # | Analyse | Type de graphique | Source |
|---|---|---|---|
| 1 | Top 10 titres par industrie | Histogramme + filtre déroulant | Binôme 1 |
| 2 | Top 10 postes mieux rémunérés | Histogramme + filtre déroulant | Binôme 1 |
| 3 | Répartition par taille d'entreprise | Histogramme + Donut | Binôme 2 |
| 4 | Répartition par secteur d'activité | Histogramme | Binôme 1 |
| 5 | Répartition par type d'emploi | Histogramme + Donut | Binôme 1 |
| 6 | Top 10 industries entreprises | Histogramme | Binôme 2 |
| 7 | Top 10 spécialités entreprises | Histogramme | Binôme 2 |
| 8 | Top 10 entreprises par followers | Histogramme coloré | Binôme 2 |
| 9 | Répartition entreprises par pays | Histogramme | Binôme 2 |

---

## Conclusion

Ce projet a permis de charger des fichiers CSV et JSON dans Snowflake depuis un bucket S3 public, de structurer les données selon une architecture Medallion Bronze / Silver / Gold, de nettoyer et standardiser les données, de préparer des tables analytiques enrichies, et de visualiser les résultats avec Streamlit in Snowflake.

Les principaux apprentissages concernent la gestion des types de données inattendus, l'importance de vérifier le contenu réel des colonnes avant de les utiliser en jointure, et la structuration des pipelines de données en couches séparées.
