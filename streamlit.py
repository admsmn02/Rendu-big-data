import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Allow imports from flows
flows_path = Path(__file__).parent / "flows"
sys.path.insert(0, str(flows_path))

from config import BUCKET_GOLD, get_minio_client


st.set_page_config(page_title="Gold KPIs Dashboard", layout="wide")
st.title("Gold KPIs Dashboard")
st.caption("Exploration rapide des données de la couche Gold (MinIO)")


@st.cache_data(show_spinner=False)
def list_gold_objects() -> list[str]:
    client = get_minio_client()
    return [obj.object_name for obj in client.list_objects(BUCKET_GOLD)]


@st.cache_data(show_spinner=False)
def load_gold_csv(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(BytesIO(data))


def plot_if_available(df: pd.DataFrame) -> None:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        st.info("Aucune colonne numérique disponible pour le graphique.")
        return

    x_col = st.selectbox("Axe X", options=df.columns, index=0)
    y_col = st.selectbox("Axe Y", options=numeric_cols, index=0)
    chart_type = st.selectbox("Type de graphique", ["line", "bar", "scatter"])

    if chart_type == "line":
        fig = px.line(df, x=x_col, y=y_col)
    elif chart_type == "bar":
        fig = px.bar(df, x=x_col, y=y_col)
    else:
        fig = px.scatter(df, x=x_col, y=y_col)

    st.plotly_chart(fig, use_container_width=True)


def render_kpi_section() -> None:
    st.header("KPIs prêts à l'emploi")
    col1, col2 = st.columns(2)

    with col1:
        try:
            monthly_df = load_gold_csv("kpis_monthly.csv")
            if "year_month" in monthly_df.columns and "ca_total" in monthly_df.columns:
                st.subheader("CA mensuel")
                fig = px.line(monthly_df, x="year_month", y="ca_total")
                st.plotly_chart(fig, use_container_width=True)
            if "ca_growth_rate" in monthly_df.columns:
                st.subheader("Croissance CA MoM")
                fig = px.bar(monthly_df, x="year_month", y="ca_growth_rate")
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.info(f"KPIs mensuels indisponibles: {exc}")

    with col2:
        try:
            weekly_df = load_gold_csv("kpis_weekly.csv")
            if "year_week" in weekly_df.columns and "ca_total" in weekly_df.columns:
                st.subheader("CA hebdomadaire")
                fig = px.line(weekly_df, x="year_week", y="ca_total")
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.info(f"KPIs hebdo indisponibles: {exc}")

    col3, col4 = st.columns(2)

    with col3:
        try:
            country_df = load_gold_csv("kpis_by_country.csv")
            if "country" in country_df.columns and "ca_total" in country_df.columns:
                st.subheader("CA par pays")
                fig = px.bar(country_df, x="country", y="ca_total")
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.info(f"KPIs par pays indisponibles: {exc}")

    with col4:
        try:
            product_df = load_gold_csv("kpis_by_product.csv")
            if "produit" in product_df.columns and "ca_total" in product_df.columns:
                st.subheader("CA par produit")
                fig = px.bar(product_df, x="produit", y="ca_total")
                st.plotly_chart(fig, use_container_width=True)
        except Exception as exc:
            st.info(f"KPIs par produit indisponibles: {exc}")

    st.subheader("Segments clients")
    try:
        segments_df = load_gold_csv("client_segmentation.csv")
        if "segment" in segments_df.columns:
            seg_counts = segments_df["segment"].value_counts().reset_index()
            seg_counts.columns = ["segment", "count"]
            fig = px.pie(seg_counts, names="segment", values="count")
            st.plotly_chart(fig, use_container_width=True)
    except Exception as exc:
        st.info(f"Segmentation indisponible: {exc}")


def render_advanced_section() -> None:
    st.header("Visualisations avancées")

    with st.expander("3D: valeur vs ancienneté client vs mois", expanded=True):
        try:
            fact_df = load_gold_csv("fact_achats.csv")
            required = {"client_age_months", "montant", "month"}
            if required.issubset(fact_df.columns):
                sample = fact_df.dropna(subset=list(required)).copy()
                if len(sample) > 2000:
                    sample = sample.sample(2000, random_state=42)
                fig = px.scatter_3d(
                    sample,
                    x="client_age_months",
                    y="montant",
                    z="month",
                    color="country" if "country" in sample.columns else None,
                    opacity=0.7,
                )
                fig.update_traces(marker=dict(size=3))
                fig.update_layout(
                    scene=dict(
                        xaxis_title="Ancienneté (mois)",
                        yaxis_title="Montant",
                        zaxis_title="Mois",
                    )
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Colonnes manquantes pour le nuage 3D.")
        except Exception as exc:
            st.info(f"Nuage 3D indisponible: {exc}")

    col1, col2 = st.columns(2)

    with col1:
        with st.expander("Surface 3D des cohortes", expanded=False):
            try:
                cohort_df = load_gold_csv("cohort_analysis.csv")
                needed = {"cohort", "year_month", "ca_total"}
                if needed.issubset(cohort_df.columns):
                    pivot = cohort_df.pivot(index="cohort", columns="year_month", values="ca_total")
                    pivot = pivot.fillna(0)
                    fig = go.Figure(
                        data=[
                            go.Surface(
                                z=pivot.values,
                                x=list(pivot.columns),
                                y=list(pivot.index),
                                colorscale="Viridis",
                            )
                        ]
                    )
                    fig.update_layout(
                        scene=dict(
                            xaxis_title="Mois achat",
                            yaxis_title="Cohorte",
                            zaxis_title="CA",
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonnes manquantes pour la surface cohortes.")
            except Exception as exc:
                st.info(f"Surface cohortes indisponible: {exc}")

    with col2:
        with st.expander("Treemap produits (CA)", expanded=False):
            try:
                product_df = load_gold_csv("kpis_by_product.csv")
                if {"produit", "ca_total"}.issubset(product_df.columns):
                    fig = px.treemap(product_df, path=["produit"], values="ca_total")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonnes manquantes pour la treemap.")
            except Exception as exc:
                st.info(f"Treemap produits indisponible: {exc}")

    col3, col4 = st.columns(2)

    with col3:
        with st.expander("Bubble chart: pays (CA vs volume)", expanded=False):
            try:
                country_df = load_gold_csv("kpis_by_country.csv")
                needed = {"country", "ca_total", "volume_achats"}
                if needed.issubset(country_df.columns):
                    fig = px.scatter(
                        country_df,
                        x="volume_achats",
                        y="ca_total",
                        size="market_share" if "market_share" in country_df.columns else None,
                        color="country",
                        hover_name="country",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonnes manquantes pour le bubble chart.")
            except Exception as exc:
                st.info(f"Bubble chart indisponible: {exc}")

    with col4:
        with st.expander("Carte monde: CA par pays", expanded=False):
            try:
                country_df = load_gold_csv("kpis_by_country.csv")
                if {"country", "ca_total"}.issubset(country_df.columns):
                    fig = px.scatter_geo(
                        country_df,
                        locations="country",
                        locationmode="country names",
                        size="ca_total",
                        color="ca_total",
                        projection="natural earth",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonnes manquantes pour la carte monde.")
            except Exception as exc:
                st.info(f"Carte monde indisponible: {exc}")

    col5, col6 = st.columns(2)

    with col5:
        with st.expander("Heatmap KPIs mensuels", expanded=False):
            try:
                monthly_df = load_gold_csv("kpis_monthly.csv")
                metrics = [
                    col
                    for col in ["ca_total", "panier_moyen", "volume_achats"]
                    if col in monthly_df.columns
                ]
                if "year_month" in monthly_df.columns and metrics:
                    heat_df = monthly_df.set_index("year_month")[metrics]
                    fig = px.imshow(heat_df.T, aspect="auto", color_continuous_scale="Blues")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonnes manquantes pour la heatmap.")
            except Exception as exc:
                st.info(f"Heatmap indisponible: {exc}")

    with col6:
        with st.expander("Distribution des montants", expanded=False):
            try:
                fact_df = load_gold_csv("fact_achats.csv")
                if "montant" in fact_df.columns:
                    fig = px.violin(fact_df, y="montant", box=True, points="all")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Colonne montant manquante pour la distribution.")
            except Exception as exc:
                st.info(f"Distribution indisponible: {exc}")

    with st.expander("Parallèle: segmentation RFM", expanded=False):
        try:
            segments_df = load_gold_csv("client_segmentation.csv")
            required = {"recency_days", "frequency", "monetary_value", "segment"}
            if required.issubset(segments_df.columns):
                sample = segments_df.dropna(subset=list(required)).copy()
                if len(sample) > 1500:
                    sample = sample.sample(1500, random_state=42)
                fig = px.parallel_coordinates(
                    sample,
                    dimensions=["recency_days", "frequency", "monetary_value"],
                    color="recency_days",
                    color_continuous_scale=px.colors.sequential.Teal,
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Colonnes manquantes pour le parallèle RFM.")
        except Exception as exc:
            st.info(f"Parallèle RFM indisponible: {exc}")


def main() -> None:
    try:
        objects = list_gold_objects()
    except Exception as exc:
        st.error(f"Impossible de lister les objets Gold: {exc}")
        st.stop()

    if not objects:
        st.warning("Aucun fichier dans le bucket gold. Lance d'abord le pipeline Gold.")
        st.stop()

    st.sidebar.header("Sélection")
    selected = st.sidebar.selectbox("Fichier Gold", objects)

    st.subheader(f"Aperçu: {selected}")

    try:
        df = load_gold_csv(selected)
    except Exception as exc:
        st.error(f"Impossible de charger {selected}: {exc}")
        st.stop()

    st.write(f"Lignes: {len(df):,} | Colonnes: {len(df.columns)}")
    st.dataframe(df, use_container_width=True)

    st.subheader("Visualisation rapide")
    plot_if_available(df)

    st.divider()
    render_kpi_section()

    st.divider()
    render_advanced_section()


if __name__ == "__main__":
    main()
