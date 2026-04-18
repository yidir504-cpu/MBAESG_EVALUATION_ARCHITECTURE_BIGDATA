import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session

# ============================================================
# CONFIGURATION
# ============================================================
st.set_page_config(page_title="Analyse des offres LinkedIn", layout="wide")
st.title("Analyse des Offres d'Emploi LinkedIn")
st.markdown("Visualisations construites à partir des tables Gold dans Snowflake.")

session = get_active_session()

# ============================================================
# REQUETES SQL
# ============================================================
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
query_top_skills = """
SELECT
    skill_abr,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.job_skills
GROUP BY skill_abr
ORDER BY nb_offres DESC
LIMIT 10;
"""

query_top_benefits = """
SELECT
    benefit_type,
    COUNT(*) AS nb_offres
FROM linkedin_lab.gold.benefits
GROUP BY benefit_type
ORDER BY nb_offres DESC
LIMIT 10;
"""
# ============================================================
# CHARGEMENT DES DONNEES
# ============================================================
df_top_titles = session.sql(query_top_titles).to_pandas()
df_top_salary = session.sql(query_top_salary).to_pandas()
df_industry_distribution = session.sql(query_industry_distribution).to_pandas()
df_work_type_distribution = session.sql(query_work_type_distribution).to_pandas()
df_top_skills = session.sql(query_top_skills).to_pandas()
df_top_benefits = session.sql(query_top_benefits).to_pandas()

# ============================================================
# ANALYSE 1 : TOP 10 DES TITRES LES PLUS PUBLIES PAR INDUSTRIE
# ============================================================


st.header("1. Top 10 des titres de postes les plus publiés par industrie")

if not df_top_titles.empty:
    industries = sorted(df_top_titles["INDUSTRY_ID"].astype(str).unique())

    selected_industry = st.selectbox(
        "Choisir une industrie",
        industries
    )

    filtered_df = df_top_titles[
        df_top_titles["INDUSTRY_ID"].astype(str) == selected_industry
    ].copy()

    st.subheader(f"Industrie sélectionnée : {selected_industry}")

    chart = (
        alt.Chart(filtered_df)
        .mark_bar()
        .encode(
            x=alt.X("NB_OFFRES:Q", title="Nombre d'offres"),
            y=alt.Y("TITLE:N", sort="-x", title="Titre du poste"),
            tooltip=["INDUSTRY_ID", "TITLE", "NB_OFFRES", "RANG"]
        )
        .properties(height=450)
    )

    st.altair_chart(chart, use_container_width=True)
    st.dataframe(filtered_df, use_container_width=True)

else:
    st.warning("Aucune donnée disponible pour l'analyse 1.")

# ============================================================
# ANALYSE 2 : TOP 10 DES POSTES LES MIEUX REMUNERES PAR INDUSTRIE
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

# ============================================================
# ANALYSE 4 : REPARTITION DES OFFRES PAR SECTEUR D'ACTIVITE
# ============================================================
st.header("3. Répartition des offres d'emploi par secteur d'activité")

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


# ============================================================
# ANALYSE 5 : REPARTITION DES OFFRES PAR TYPE D'EMPLOI
# ============================================================
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



# ============================================================
# ANALYSE 6 : COMPETENCES LES PLUS DEMANDEES
# ============================================================
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

else:
    st.warning("Aucune donnée disponible pour l'analyse 6.")

# ============================================================
# ANALYSE 7 : AVANTAGES LES PLUS FREQUENTS
# ============================================================
st.header("7. Avantages les plus fréquents")

if not df_top_benefits.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Histogramme des avantages")

        chart_top_benefits = (
            alt.Chart(df_top_benefits)
            .mark_bar()
            .encode(
                x=alt.X("BENEFIT_TYPE:N", sort="-x", title="Avantage"),
                y=alt.Y("NB_OFFRES:Q", title="Nombre d'offres"),
                
                tooltip=["BENEFIT_TYPE", "NB_OFFRES"]
            )
            .properties(height=450)
        )

        st.altair_chart(chart_top_benefits, use_container_width=True)

    
else:
    st.warning("Aucune donnée disponible pour l'analyse 7.")
