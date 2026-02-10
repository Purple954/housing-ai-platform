"""Pydantic models for API request and response validation."""

from pydantic import BaseModel, Field
from typing import Optional


class PredictRequest(BaseModel):
    text_summary: str = Field(..., description="Natural language description of the property")
    current_efficiency: int = Field(..., ge=1, le=100)
    walls_eff_score: Optional[float] = Field(None, ge=1, le=5)
    roof_eff_score: Optional[float] = Field(None, ge=1, le=5)
    windows_eff_score: Optional[float] = Field(None, ge=1, le=5)
    heating_eff_score: Optional[float] = Field(None, ge=1, le=5)
    hot_water_eff_score: Optional[float] = Field(None, ge=1, le=5)
    lighting_eff_score: Optional[float] = Field(None, ge=1, le=5)
    total_floor_area: Optional[float] = Field(None, gt=0)

    model_config = {
        "json_schema_extra": {
            "example": {
                "text_summary": (
                    "This is a semi-detached house (semi-detached) built "
                    "england and wales: 1950-1966. It has an energy rating of D "
                    "(efficiency score 58/100) and could reach B (82/100) with improvements. "
                    "Walls: cavity wall, filled cavity. Roof: pitched, 250 mm loft insulation. "
                    "Windows: fully double glazed. Heating: boiler and radiators, mains gas. "
                    "Main fuel: mains gas (not community)."
                ),
                "current_efficiency": 58,
                "walls_eff_score": 3,
                "roof_eff_score": 4,
                "windows_eff_score": 3,
                "heating_eff_score": 4,
                "hot_water_eff_score": 4,
                "lighting_eff_score": 5,
                "total_floor_area": 83.0,
            }
        }
    }


class PredictResponse(BaseModel):
    retrofit_score: float
    retrofit_priority: str
    annual_savings_estimate_gbp: Optional[float] = None


class PropertyDetail(BaseModel):
    lmk_key: str
    postcode: Optional[str]
    property_type: Optional[str]
    built_form: Optional[str]
    construction_age_band: Optional[str]
    tenure: Optional[str]
    current_rating: Optional[str]
    current_efficiency: Optional[int]
    potential_rating: Optional[str]
    potential_efficiency: Optional[int]
    retrofit_score: Optional[float]
    retrofit_priority: Optional[str]
    annual_savings_potential: Optional[float]
    co2_saving_tonnes: Optional[float]
    total_floor_area: Optional[float]
    main_fuel: Optional[str]
    data_quality_score: Optional[float]


class PortfolioSegment(BaseModel):
    property_type: Optional[str]
    construction_age_band: Optional[str]
    retrofit_priority: Optional[str]
    property_count: int
    avg_current_efficiency: Optional[float]
    avg_potential_efficiency: Optional[float]
    avg_retrofit_score: Optional[float]
    avg_annual_savings_gbp: Optional[float]
    total_co2_saving_tonnes: Optional[float]
    avg_floor_area_m2: Optional[float]


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    db_connected: bool
    property_count: int
