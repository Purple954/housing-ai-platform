"""Loads the trained fusion model and runs inference for the API."""

import numpy as np
import torch
from pathlib import Path
from functools import lru_cache

from src.models.text_encoder import TextEncoder
from src.models.fusion import build_model, STRUCTURED_DIM
from src.models.image_encoder import EMBEDDING_DIM as IMAGE_DIM

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODELS_DIR   = PROJECT_ROOT / "models"

STRUCTURED_COLS = [
    "walls_eff_score", "roof_eff_score", "windows_eff_score",
    "heating_eff_score", "hot_water_eff_score", "lighting_eff_score",
    "current_efficiency", "total_floor_area",
]

# Fallback values used when a field is missing
DEFAULTS = {
    "walls_eff_score":     3.0,
    "roof_eff_score":      3.0,
    "windows_eff_score":   3.0,
    "heating_eff_score":   3.0,
    "hot_water_eff_score": 3.0,
    "lighting_eff_score":  3.0,
    "current_efficiency":  50.0,
    "total_floor_area":    80.0,
}


class Predictor:
    def __init__(self):
        self.text_encoder = TextEncoder()

        self.struct_mean = np.load(str(MODELS_DIR / "struct_mean.npy"))
        self.struct_std  = np.load(str(MODELS_DIR / "struct_std.npy"))

        self.model = build_model(structured_dim=STRUCTURED_DIM)
        self.model.load_state_dict(
            torch.load(str(MODELS_DIR / "fusion_model.pt"), map_location="cpu")
        )
        self.model.eval()

    def predict(self, text_summary: str, structured_fields: dict) -> dict:
        text_emb = self.text_encoder.encode_single(text_summary)
        image_emb = np.zeros(IMAGE_DIM, dtype=np.float32)

        raw = np.array(
            [structured_fields.get(col, DEFAULTS[col]) or DEFAULTS[col]
             for col in STRUCTURED_COLS],
            dtype=np.float32,
        )
        struct_norm = (raw - self.struct_mean) / self.struct_std

        text_t   = torch.tensor(text_emb,    dtype=torch.float32).unsqueeze(0)
        image_t  = torch.tensor(image_emb,   dtype=torch.float32).unsqueeze(0)
        struct_t = torch.tensor(struct_norm, dtype=torch.float32).unsqueeze(0)

        with torch.no_grad():
            score = self.model(text_t, image_t, struct_t).item()

        score = round(max(0.0, min(100.0, score)), 2)

        if score >= 20:
            priority = "High"
        elif score >= 10:
            priority = "Medium"
        else:
            priority = "Low"

        return {"retrofit_score": score, "retrofit_priority": priority}


@lru_cache(maxsize=1)
def get_predictor() -> Predictor:
    return Predictor()
