import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import plotly.express as px
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


if __name__ == "__main__":
    main()
