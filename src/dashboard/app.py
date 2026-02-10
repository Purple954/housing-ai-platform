"""Housing Retrofit AI — Streamlit dashboard."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

from src.api.predictor import get_predictor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH      = PROJECT_ROOT / "data" / "processed" / "housing.duckdb"

st.set_page_config(
    page_title="Housing Retrofit AI",
    page_icon="🏠",
    layout="wide",
)


@st.cache_resource
def get_db():
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data
def load_summary():
    con = get_db()
    return con.execute("""
        SELECT
            COUNT(*)                                              AS total,
            ROUND(AVG(current_efficiency), 1)                   AS avg_efficiency,
            ROUND(AVG(retrofit_score), 1)                       AS avg_retrofit_score,
            SUM(CASE WHEN retrofit_priority='High' THEN 1 END)  AS high_count,
            ROUND(SUM(annual_savings_potential) / 1e6, 2)       AS total_savings_m,
            ROUND(SUM(co2_saving_tonnes))                       AS total_co2
        FROM gold.epc_features
    """).fetchone()


@st.cache_data
def load_priority_dist():
    return get_db().execute("""
        SELECT retrofit_priority, COUNT(*) AS n
        FROM gold.epc_features
        GROUP BY retrofit_priority
        ORDER BY n DESC
    """).df()


@st.cache_data
def load_efficiency_by_type():
    return get_db().execute("""
        SELECT property_type,
               ROUND(AVG(current_efficiency), 1)  AS avg_current,
               ROUND(AVG(potential_efficiency), 1) AS avg_potential
        FROM gold.epc_features
        GROUP BY property_type
        ORDER BY avg_current
    """).df()


@st.cache_data
def load_score_by_age():
    return get_db().execute("""
        SELECT construction_age_band,
               ROUND(AVG(retrofit_score), 1) AS avg_score,
               COUNT(*) AS n
        FROM gold.epc_features
        WHERE construction_age_band IS NOT NULL
        GROUP BY construction_age_band
        ORDER BY avg_score DESC
    """).df()


@st.cache_data
def load_savings_by_type():
    return get_db().execute("""
        SELECT property_type,
               ROUND(AVG(annual_savings_potential), 0) AS avg_savings
        FROM gold.epc_features
        GROUP BY property_type
        ORDER BY avg_savings DESC
    """).df()


@st.cache_data
def load_top_properties(priority: str, limit: int):
    where = f"WHERE retrofit_priority = '{priority}'" if priority != "All" else ""
    return get_db().execute(f"""
        SELECT postcode, property_type, built_form, construction_age_band,
               current_rating, current_efficiency, potential_rating, potential_efficiency,
               retrofit_score, retrofit_priority,
               ROUND(annual_savings_potential, 0) AS annual_savings_gbp,
               ROUND(co2_saving_tonnes, 1) AS co2_saving_tonnes,
               main_fuel, data_quality_score
        FROM gold.epc_features
        {where}
        ORDER BY retrofit_score DESC
        LIMIT {limit}
    """).df()


@st.cache_data
def load_efficiency_scatter(n: int = 3000):
    return get_db().execute(f"""
        SELECT current_efficiency, potential_efficiency,
               retrofit_score, property_type, retrofit_priority
        FROM gold.epc_features
        USING SAMPLE {n}
    """).df()


# ── Sidebar navigation ────────────────────────────────────────────────────────

st.sidebar.title("Housing Retrofit AI")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Portfolio Explorer", "Top Properties", "Predict"],
)

# ── PAGE: Overview ─────────────────────────────────────────────────────────────

if page == "Overview":
    st.title("Housing Retrofit AI — Salford EPC Analysis")
    st.caption("171k+ domestic properties · Medallion data pipeline · Multimodal AI")

    total, avg_eff, avg_score, high, savings_m, co2 = load_summary()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Properties", f"{total:,}")
    c2.metric("High Retrofit Priority", f"{high:,}", f"{high/total*100:.1f}% of stock")
    c3.metric("Total Savings Potential", f"£{savings_m}M / yr")
    c4.metric("CO2 Saving Potential", f"{int(co2):,} tonnes")

    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Retrofit Priority Distribution")
        df_pri = load_priority_dist()
        colours = {"High": "#e74c3c", "Medium": "#f39c12", "Low": "#2ecc71"}
        fig = px.bar(
            df_pri, x="retrofit_priority", y="n",
            color="retrofit_priority",
            color_discrete_map=colours,
            labels={"retrofit_priority": "Priority", "n": "Properties"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Current vs Potential Efficiency by Property Type")
        df_eff = load_efficiency_by_type()
        df_melt = df_eff.melt(
            id_vars="property_type",
            value_vars=["avg_current", "avg_potential"],
            var_name="State", value_name="Efficiency",
        )
        df_melt["State"] = df_melt["State"].map(
            {"avg_current": "Current", "avg_potential": "Potential"}
        )
        fig2 = px.bar(
            df_melt, x="property_type", y="Efficiency",
            color="State", barmode="group",
            color_discrete_map={"Current": "#3498db", "Potential": "#2ecc71"},
            labels={"property_type": "Property Type", "Efficiency": "Avg Score"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Efficiency: Current vs Potential (sample of 3,000 properties)")
    df_scatter = load_efficiency_scatter()
    fig3 = px.scatter(
        df_scatter,
        x="current_efficiency", y="potential_efficiency",
        color="retrofit_priority",
        color_discrete_map={"High": "#e74c3c", "Medium": "#f39c12", "Low": "#2ecc71"},
        opacity=0.4,
        labels={
            "current_efficiency": "Current Efficiency",
            "potential_efficiency": "Potential Efficiency",
            "retrofit_priority": "Priority",
        },
    )
    fig3.add_shape(type="line", x0=0, y0=0, x1=100, y1=100,
                   line=dict(color="grey", dash="dot"))
    st.plotly_chart(fig3, use_container_width=True)


# ── PAGE: Portfolio Explorer ───────────────────────────────────────────────────

elif page == "Portfolio Explorer":
    st.title("Portfolio Explorer")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Average Retrofit Score by Construction Era")
        df_age = load_score_by_age()
        fig = px.bar(
            df_age, x="avg_score", y="construction_age_band",
            orientation="h",
            color="avg_score",
            color_continuous_scale="RdYlGn_r",
            labels={"avg_score": "Avg Retrofit Score", "construction_age_band": ""},
            text="avg_score",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Average Annual Savings Potential by Property Type")
        df_sav = load_savings_by_type()
        fig2 = px.bar(
            df_sav, x="property_type", y="avg_savings",
            color="avg_savings",
            color_continuous_scale="Blues",
            labels={"property_type": "Property Type", "avg_savings": "Avg Savings (GBP/yr)"},
            text="avg_savings",
        )
        fig2.update_traces(textposition="outside")
        fig2.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Portfolio Aggregation Table")
    df_agg = get_db().execute("""
        SELECT property_type, construction_age_band, retrofit_priority,
               property_count, avg_retrofit_score, avg_annual_savings_gbp,
               total_co2_saving_tonnes, avg_current_efficiency
        FROM gold.portfolio_agg
        ORDER BY avg_retrofit_score DESC
    """).df()

    pri_filter = st.selectbox("Filter by priority", ["All", "High", "Medium", "Low"])
    if pri_filter != "All":
        df_agg = df_agg[df_agg["retrofit_priority"] == pri_filter]

    st.dataframe(df_agg, use_container_width=True, hide_index=True)


# ── PAGE: Top Properties ───────────────────────────────────────────────────────

elif page == "Top Properties":
    st.title("Top Retrofit Candidates")

    col1, col2 = st.columns(2)
    priority = col1.selectbox("Priority filter", ["All", "High", "Medium", "Low"])
    limit    = col2.slider("Number of properties", 10, 200, 50)

    df = load_top_properties(priority, limit)

    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Showing", f"{len(df):,} properties")
    col_b.metric("Avg Retrofit Score", f"{df['retrofit_score'].mean():.1f}")
    col_c.metric("Avg Annual Savings", f"£{df['annual_savings_gbp'].mean():,.0f}")

    st.dataframe(df, use_container_width=True, hide_index=True)

    st.download_button(
        "Download as CSV",
        df.to_csv(index=False),
        file_name="top_retrofit_properties.csv",
        mime="text/csv",
    )


# ── PAGE: Predict ──────────────────────────────────────────────────────────────

elif page == "Predict":
    st.title("Retrofit Score Predictor")
    st.caption("Enter property details to get an AI-predicted retrofit score.")

    with st.form("predict_form"):
        st.subheader("Property Description")
        text_summary = st.text_area(
            "Property description",
            height=120,
            value=(
                "This is a semi-detached house (semi-detached) built england and wales: "
                "1950-1966. It has an energy rating of D (efficiency score 58/100) and "
                "could reach B (82/100) with improvements. Walls: cavity wall, filled cavity. "
                "Roof: pitched, 250 mm loft insulation. Windows: fully double glazed. "
                "Heating: boiler and radiators, mains gas. Main fuel: mains gas (not community)."
            ),
        )

        st.subheader("Component Efficiency Scores (1 = Very Poor, 5 = Very Good)")
        c1, c2, c3 = st.columns(3)
        walls   = c1.slider("Walls",     1, 5, 3)
        roof    = c2.slider("Roof",      1, 5, 4)
        windows = c3.slider("Windows",   1, 5, 3)
        heating = c1.slider("Heating",   1, 5, 4)
        hot_water = c2.slider("Hot Water", 1, 5, 4)
        lighting  = c3.slider("Lighting",  1, 5, 5)

        st.subheader("Property Details")
        d1, d2 = st.columns(2)
        current_eff = d1.number_input("Current efficiency score (1–100)", 1, 100, 58)
        floor_area  = d2.number_input("Total floor area (m²)", 10.0, 500.0, 83.0)

        submitted = st.form_submit_button("Predict Retrofit Score", type="primary")

    if submitted:
        with st.spinner("Running model..."):
            p = get_predictor()
            result = p.predict(
                text_summary=text_summary,
                structured_fields={
                    "walls_eff_score":     float(walls),
                    "roof_eff_score":      float(roof),
                    "windows_eff_score":   float(windows),
                    "heating_eff_score":   float(heating),
                    "hot_water_eff_score": float(hot_water),
                    "lighting_eff_score":  float(lighting),
                    "current_efficiency":  float(current_eff),
                    "total_floor_area":    float(floor_area),
                },
            )

        score    = result["retrofit_score"]
        priority = result["retrofit_priority"]

        colour = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}.get(priority, "⚪")
        st.success(f"{colour} **Retrofit Priority: {priority}**")

        col1, col2 = st.columns(2)
        col1.metric("Predicted Retrofit Score", f"{score:.1f} / 100")
        col2.metric("Priority Band", priority)

        st.progress(int(min(score, 100)) / 100)

        if priority == "High":
            st.info(
                "This property has significant retrofit potential. "
                "Improvements to insulation, heating, and glazing could deliver "
                "major efficiency gains and cost savings."
            )
        elif priority == "Medium":
            st.info("Moderate retrofit potential. Targeted improvements recommended.")
        else:
            st.info("This property is already relatively efficient.")
