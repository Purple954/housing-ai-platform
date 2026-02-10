"""Housing Retrofit AI — FastAPI backend."""

from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager

from src.api.schemas import (
    PredictRequest, PredictResponse,
    PropertyDetail, PortfolioSegment, HealthResponse,
)
from src.api import db, predictor


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load model and DB connection once at startup
    predictor.get_predictor()
    db.get_connection()
    yield


app = FastAPI(
    title="Housing Retrofit AI",
    description="Multimodal AI platform for EPC retrofit prioritisation.",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    try:
        count = db.query("SELECT COUNT(*) FROM gold.epc_features")[0][0]
        db_ok = True
    except Exception:
        count = 0
        db_ok = False

    model_ok = predictor.get_predictor() is not None

    return HealthResponse(
        status="ok" if (db_ok and model_ok) else "degraded",
        model_loaded=model_ok,
        db_connected=db_ok,
        property_count=count,
    )


@app.post("/predict", response_model=PredictResponse, tags=["Prediction"])
def predict(body: PredictRequest):
    """Run the fusion model on submitted property features and return a retrofit score."""
    p = predictor.get_predictor()
    result = p.predict(
        text_summary=body.text_summary,
        structured_fields={
            "walls_eff_score":     body.walls_eff_score,
            "roof_eff_score":      body.roof_eff_score,
            "windows_eff_score":   body.windows_eff_score,
            "heating_eff_score":   body.heating_eff_score,
            "hot_water_eff_score": body.hot_water_eff_score,
            "lighting_eff_score":  body.lighting_eff_score,
            "current_efficiency":  body.current_efficiency,
            "total_floor_area":    body.total_floor_area,
        },
    )
    return PredictResponse(**result)


@app.get("/properties/{lmk_key}", response_model=PropertyDetail, tags=["Properties"])
def get_property(lmk_key: str):
    """Fetch a property from the gold layer by its LMK key."""
    rows = db.query(
        """
        SELECT lmk_key, postcode, property_type, built_form, construction_age_band,
               tenure, current_rating, current_efficiency, potential_rating,
               potential_efficiency, retrofit_score, retrofit_priority,
               annual_savings_potential, co2_saving_tonnes, total_floor_area,
               main_fuel, data_quality_score
        FROM gold.epc_features
        WHERE lmk_key = ?
        LIMIT 1
        """,
        [lmk_key],
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Property not found")

    cols = [
        "lmk_key", "postcode", "property_type", "built_form", "construction_age_band",
        "tenure", "current_rating", "current_efficiency", "potential_rating",
        "potential_efficiency", "retrofit_score", "retrofit_priority",
        "annual_savings_potential", "co2_saving_tonnes", "total_floor_area",
        "main_fuel", "data_quality_score",
    ]
    return PropertyDetail(**dict(zip(cols, rows[0])))


@app.get("/properties", response_model=list[PropertyDetail], tags=["Properties"])
def list_properties(
    priority: str | None = Query(None, description="Filter by retrofit_priority: High, Medium, Low"),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List properties from the gold layer, optionally filtered by retrofit priority."""
    where = "WHERE retrofit_priority = ?" if priority else ""
    params = [priority] if priority else []

    rows = db.query(
        f"""
        SELECT lmk_key, postcode, property_type, built_form, construction_age_band,
               tenure, current_rating, current_efficiency, potential_rating,
               potential_efficiency, retrofit_score, retrofit_priority,
               annual_savings_potential, co2_saving_tonnes, total_floor_area,
               main_fuel, data_quality_score
        FROM gold.epc_features
        {where}
        ORDER BY retrofit_score DESC
        LIMIT {limit} OFFSET {offset}
        """,
        params or None,
    )

    cols = [
        "lmk_key", "postcode", "property_type", "built_form", "construction_age_band",
        "tenure", "current_rating", "current_efficiency", "potential_rating",
        "potential_efficiency", "retrofit_score", "retrofit_priority",
        "annual_savings_potential", "co2_saving_tonnes", "total_floor_area",
        "main_fuel", "data_quality_score",
    ]
    return [PropertyDetail(**dict(zip(cols, row))) for row in rows]


@app.get("/portfolio", response_model=list[PortfolioSegment], tags=["Portfolio"])
def portfolio(
    property_type: str | None = Query(None),
    priority: str | None = Query(None),
):
    """Return aggregated portfolio statistics, optionally filtered."""
    conditions = []
    params = []
    if property_type:
        conditions.append("property_type = ?")
        params.append(property_type)
    if priority:
        conditions.append("retrofit_priority = ?")
        params.append(priority)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = db.query(
        f"""
        SELECT property_type, construction_age_band, retrofit_priority,
               property_count, avg_current_efficiency, avg_potential_efficiency,
               avg_retrofit_score, avg_annual_savings_gbp,
               total_co2_saving_tonnes, avg_floor_area_m2
        FROM gold.portfolio_agg
        {where}
        ORDER BY avg_retrofit_score DESC
        """,
        params or None,
    )

    cols = [
        "property_type", "construction_age_band", "retrofit_priority",
        "property_count", "avg_current_efficiency", "avg_potential_efficiency",
        "avg_retrofit_score", "avg_annual_savings_gbp",
        "total_co2_saving_tonnes", "avg_floor_area_m2",
    ]
    return [PortfolioSegment(**dict(zip(cols, row))) for row in rows]


@app.get("/stats/summary", tags=["Portfolio"])
def summary_stats():
    """High-level headline figures across the full dataset."""
    row = db.query("""
        SELECT
            COUNT(*)                                   AS total_properties,
            ROUND(AVG(current_efficiency), 1)          AS avg_current_efficiency,
            ROUND(AVG(retrofit_score), 1)              AS avg_retrofit_score,
            SUM(CASE WHEN retrofit_priority='High' THEN 1 ELSE 0 END)   AS high_priority_count,
            SUM(CASE WHEN retrofit_priority='Medium' THEN 1 ELSE 0 END) AS medium_priority_count,
            SUM(CASE WHEN retrofit_priority='Low' THEN 1 ELSE 0 END)    AS low_priority_count,
            ROUND(SUM(annual_savings_potential) / 1e6, 2) AS total_savings_potential_m_gbp,
            ROUND(SUM(co2_saving_tonnes), 0)           AS total_co2_saving_tonnes
        FROM gold.epc_features
    """)[0]

    return {
        "total_properties":            row[0],
        "avg_current_efficiency":      row[1],
        "avg_retrofit_score":          row[2],
        "high_priority_count":         row[3],
        "medium_priority_count":       row[4],
        "low_priority_count":          row[5],
        "total_savings_potential_m_gbp": row[6],
        "total_co2_saving_tonnes":     row[7],
    }
