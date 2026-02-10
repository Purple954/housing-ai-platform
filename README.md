# housing-ai-platform
An AI-powered platform for housing 
analysis, built to demonstrate data engineering and machine learning capabilities 
relevant to the UK housing sector.

## What This Does

- Ingests and processes UK EPC (Energy Performance Certificate) open data
- Runs a multimodal AI model (text + image) to predict retrofit improvement potential
- Exposes predictions via a REST API (FastAPI)
- Provides an interactive portfolio dashboard (Streamlit)
- Uses a bronze → silver → gold medallion data pipeline with DuckDB

## Tech Stack

| Layer | Technology |
|---|---|
| Data storage | DuckDB + Parquet |
| ETL pipeline | Python + DVC |
| Text model | sentence-transformers (all-MiniLM-L6-v2) |
| Image model | CLIP (openai/clip-vit-base-patch32) |
| Multimodal fusion | PyTorch |
| API | FastAPI |
| Dashboard | Streamlit |
| Containerisation | Docker + Docker Compose |

## Data Sources

- **UK EPC Open Data** – Domestic Energy Performance Certificates  
  Download from: https://epc.opendatacommunities.org/ (free registration required)  
  Local authority used: Salford (E08000006) and Manchester (E08000003)

- **Property images** – Open Images V7 (residential buildings subset)  
  Downloaded via the `fiftyone` library (see setup instructions below)

## Project Structure

housing-ai-platform/
├── data/               # Not committed - see Data Setup below
├── src/
│   ├── etl/            # Bronze → Silver → Gold pipeline
│   ├── models/         # Text, image, and fusion models
│   ├── api/            # FastAPI backend
│   └── dashboard/      # Streamlit frontend
├── notebooks/          # Exploratory data analysis
├── dvc.yaml            # Pipeline orchestration
├── docker-compose.yml
└── requirements.txt



## Setup

### 1. Clone and install

```bash
git clone https://github.com/Purple954/housing-ai-platform.git
cd housing-ai-platform
pip install -r requirements.txt
2. Download EPC data
Register at https://epc.opendatacommunities.org/, download the Salford
domestic EPC dataset as a ZIP, extract certificates.csv into data/raw/.

3. Run the pipeline

dvc repro
4. Run the API

uvicorn src.api.main:app --reload
5. Run the dashboard

streamlit run src/dashboard/app.py
6. Run with Docker

docker-compose up
Verification

python check_setup.py
Key Features
Medallion architecture (bronze/silver/gold) with data quality metrics at each stage
Multimodal transformer model fusing text descriptions, images, and structured EPC features
Retrofit priority scoring to identify properties with the highest improvement potential
Portfolio-level aggregation by property type, construction era, and ward
Bias assessment for equity implications in housing data
Model card and data lineage documentation
