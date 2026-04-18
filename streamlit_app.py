import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ============================================================
# CONFIGURATION DE LA PAGE
# ============================================================
st.set_page_config(page_title="Analyse des offres LinkedIn", layout="wide")
st.title("Analyse des Offres d'Emploi LinkedIn")
st.markdown("Visualisations construites à partir des tables Gold dans Snowflake.")

# Initialisation de la session Snowflake active
session = get_active_session()

# ============================================================
# REQUETES SQL
# ============================================================

# Analyse 1 : Top 10 des titres les plus publiés par industrie
query_top_titles = """
SELECT industry_id, title, nb_offres, rang
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
    JOIN linkedin_lab.gold.job_postings jp ON ji.job_id = jp.job_id
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang
"""

# Analyse 2 : Top 10 des postes les mieux rémunérés par industrie
query_top_salary = """
SELECT industry_id, title, avg_max_salary, rang
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
    JOIN linkedin_lab.gold.job_postings jp ON ji.job_id = jp.job_id
    WHERE jp.max_salary IS NOT NULL
      AND jp.pay_period = 'YEARLY'
    GROUP BY ji.industry_id, jp.title
)
WHERE rang <= 10
ORDER BY industry_id, rang
"""

# Analyse 3 : Répartition des offres par taille d'entreprise
# Utilise job_postings_full qui contient déjà company_size et company_size_label
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

# Analyse 4 : Répartition des offres par secteur d'activité
query_industry_distribution = """
SELECT
    industry_id,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_industries
GROUP BY industry_id
ORDER BY nb_offres DESC
"""

# Analyse 5 : Répartition des offres par type d'emploi
query_work_type_distribution = """
SELECT
    formatted_work_type AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pourcentage
FROM linkedin_lab.gold.job_postings
WHERE formatted_work_type IS NOT NULL
  AND TRIM(formatted_work_type) <> ''
GROUP BY formatted_work_type
ORDER BY nb_offres DESC
"""

# Analyse bonus : Top 10 des industries avec le plus d'entreprises
query_company_industries = """
SELECT
    industry,
    COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_industries
GROUP BY industry
ORDER BY nb_entreprises DESC
LIMIT 10
"""

# Analyse bonus : Top 10 des spécialités les plus représentées
query_company_specialities = """
SELECT
    speciality,
    COUNT(DISTINCT company_id) AS nb_entreprises
FROM linkedin_lab.gold.company_specialities
GROUP BY speciality
ORDER BY nb_entreprises DESC
LIMIT 10
"""

# Analyse bonus : Top 10 des entreprises par nombre de followers
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

# Analyse bonus : Répartition des entreprises par pays (top 15)
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

# ============================================================
# CHARGEMENT DES DONNEES
# Chaque requête est exécutée via la session Snowflake
# et convertie en DataFrame Pandas avec .to_pandas()
# ============================================================
try:
    df_top_titles             = session.sql(query_top_titles).to_pandas()
    df_top_salary             = session.sql(query_top_salary).to_pandas()
    df_company_size           = session.sql(query_company_size).to_pandas()
    df_industry_distribution  = session.sql(query_industry_distribution).to_pandas()
    df_work_type_distribution = session.sql(query_work_type_distribution).to_pandas()
    df_company_industries     = session.sql(query_company_industries).to_pandas()
    df_company_specialities   = session.sql(query_company_specialities).to_pandas()
    df_top_followers          = session.sql(query_top_followers).to_pandas()
    df_companies_by_country   = session.sql(query_companies_by_country).to_pandas()
except Exception as e:
    st.error(f"Erreur lors du chargement des données : {e}")
    st.stop()

# ============================================================
# ANALYSE 1 : TOP 10 DES TITRES LES PLUS PUBLIES PAR INDUSTRIE
# Sélecteur d'industrie + histogramme + tableau
# ============================================================
st.header("1. Top 10 des titres de postes les plus publiés par industrie")

if not df_top_titles.empty:
    # Liste déroulante pour filtrer par industrie
    industries_titles = sorted(df_top_titles["INDUSTRY_ID"].astype(str).unique())
    selected_industry_titles = st.selectbox(
        "Choisir une industrie pour les titres les plus publiés",
        industries_titles,
        key="titles_industry"
    )

    # Filtrage du DataFrame selon l'industrie sélectionnée
    filtered_titles = df_top_titles[
        df_top_titles["INDUSTRY_ID"].astype(str) == selected_industry_titles
    ].copy()

    chart_titles = (
        alt.Chart(filtered_titles)
        .mark_bar()
        .encode(
            x=alt.X("NB_OFFRES:Q", title="Nombre d'offres"),
            y=alt.Y("TITLE:N", sort="-x", title="Titre du poste"),
            tooltip=["INDUSTRY_ID", "TITLE", "NB_OFFRES", "RANG"]
        )
        .properties(height=450)
    )

    st.altair_chart(chart_titles, use_container_width=True)
    st.dataframe(filtered_titles, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 1.")

# ============================================================
# ANALYSE 2 : TOP 10 DES POSTES LES MIEUX REMUNERES PAR INDUSTRIE
# Sélecteur d'industrie + histogramme + tableau
# ============================================================
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
            x=alt.X("AVG_MAX_SALARY:Q", title="Salaire maximum moyen"),
            y=alt.Y("TITLE:N", sort="-x", title="Titre du poste"),
            tooltip=["INDUSTRY_ID", "TITLE", "AVG_MAX_SALARY", "RANG"]
        )
        .properties(height=450)
    )

    st.altair_chart(chart_salary, use_container_width=True)
    st.dataframe(filtered_salary, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 2.")

# ============================================================
# ANALYSE 3 : REPARTITION DES OFFRES PAR TAILLE D'ENTREPRISE
# Histogramme + donut chart côte à côte + tableau
# ============================================================
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
                    # Tri par company_size_order (0→7) pour respecter l'ordre croissant
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
else:
    st.warning("Aucune donnée disponible pour l'analyse 3.")

# ============================================================
# ANALYSE 4 : REPARTITION DES OFFRES PAR SECTEUR D'ACTIVITE
# Histogramme + tableau
# ============================================================
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
    st.warning("Aucune donnée disponible pour l'analyse 4.")

# ============================================================
# ANALYSE 5 : REPARTITION DES OFFRES PAR TYPE D'EMPLOI
# Histogramme + donut chart côte à côte + tableau
# ============================================================
st.header("5. Répartition des offres d'emploi par type d'emploi")

if not df_work_type_distribution.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Histogramme des types d'emploi")

        chart_work_type = (
            alt.Chart(df_work_type_distribution)
            .mark_bar()
            .encode(
                x=alt.X("TYPE_EMPLOI:N", sort="-y", title="Type d'emploi"),
                y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
                tooltip=["TYPE_EMPLOI", "NB_OFFRES", "POURCENTAGE"]
            )
            .properties(height=450)
        )

        st.altair_chart(chart_work_type, use_container_width=True)

    with col2:
        st.subheader("Répartition en donut chart")

        donut_work_type = (
            alt.Chart(df_work_type_distribution)
            .mark_arc(innerRadius=70)
            .encode(
                theta=alt.Theta("NB_OFFRES:Q", title="Nombre d'offres"),
                color=alt.Color("TYPE_EMPLOI:N", title="Type d'emploi"),
                tooltip=["TYPE_EMPLOI", "NB_OFFRES", "POURCENTAGE"]
            )
            .properties(height=450)
        )

        st.altair_chart(donut_work_type, use_container_width=True)

    st.subheader("Table des résultats")
    st.dataframe(df_work_type_distribution, use_container_width=True)
else:
    st.warning("Aucune donnée disponible pour l'analyse 5.")

# ============================================================
# ANALYSE BONUS 1 : TOP 10 DES INDUSTRIES PAR NB D'ENTREPRISES
# ============================================================
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
else:
    st.warning("Aucune donnée disponible pour l'analyse des industries.")

# ============================================================
# ANALYSE BONUS 2 : TOP 10 DES SPECIALITES
# ============================================================
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
else:
    st.warning("Aucune donnée disponible pour l'analyse des spécialités.")

# ============================================================
# ANALYSE BONUS 3 : TOP 10 ENTREPRISES PAR FOLLOWERS
# ============================================================
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
else:
    st.warning("Aucune donnée disponible pour l'analyse des followers.")

# ============================================================
# ANALYSE BONUS 4 : REPARTITION DES ENTREPRISES PAR PAYS
# ============================================================
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
else:
    st.warning("Aucune donnée disponible pour l'analyse par pays.")
