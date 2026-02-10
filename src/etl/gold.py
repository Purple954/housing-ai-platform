"""Gold layer — retrofit scoring, financial savings, and portfolio aggregation."""

import duckdb
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH      = PROJECT_ROOT / "data" / "processed" / "housing.duckdb"


def run(db_path: str = str(DB_PATH)) -> dict:
    print("[gold] Starting feature engineering...")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("DROP TABLE IF EXISTS gold.epc_features")
    con.execute("DROP TABLE IF EXISTS gold.portfolio_agg")

    con.execute("""
        CREATE TABLE gold.epc_features AS
        SELECT
            lmk_key,
            uprn,
            postcode,
            local_authority,
            constituency,
            property_type,
            built_form,
            construction_age_band,
            tenure,
            total_floor_area,
            num_habitable_rooms,
            main_fuel,
            mains_gas,
            solar_water_heating,
            solar_pv_supply_pct,
            current_rating,
            current_efficiency,
            potential_rating,
            potential_efficiency,

            (potential_efficiency - current_efficiency) AS retrofit_score,

            CASE
                WHEN (potential_efficiency - current_efficiency) >= 20 THEN 'High'
                WHEN (potential_efficiency - current_efficiency) >= 10 THEN 'Medium'
                ELSE 'Low'
            END AS retrofit_priority,

            COALESCE(heating_cost_current,    0)
            + COALESCE(hot_water_cost_current, 0)
            + COALESCE(lighting_cost_current,  0)  AS total_cost_current,

            COALESCE(heating_cost_potential,    0)
            + COALESCE(hot_water_cost_potential, 0)
            + COALESCE(lighting_cost_potential,  0) AS total_cost_potential,

            (COALESCE(heating_cost_current,    0) - COALESCE(heating_cost_potential,    0))
            + (COALESCE(hot_water_cost_current, 0) - COALESCE(hot_water_cost_potential, 0))
            + (COALESCE(lighting_cost_current,  0) - COALESCE(lighting_cost_potential,  0))
                                                    AS annual_savings_potential,

            ROUND(co2_current - co2_potential, 2)  AS co2_saving_tonnes,

            walls_eff_score,
            roof_eff_score,
            windows_eff_score,
            heating_eff_score,
            hot_water_eff_score,
            lighting_eff_score,
            walls_description,
            roof_description,
            windows_description,
            heating_description,
            hot_water_description,
            lighting_description,
            data_quality_score,

            -- Sentence-transformer input: structured description of each property
            CONCAT(
                'This is a ', LOWER(COALESCE(property_type, 'residential property')),
                ' (', LOWER(COALESCE(built_form, 'unknown form')), ')',
                ' built ', LOWER(COALESCE(construction_age_band, 'in an unknown period')), '.',
                ' It has an energy rating of ', COALESCE(current_rating, '?'),
                ' (efficiency score ', COALESCE(CAST(current_efficiency AS VARCHAR), '?'), '/100)',
                ' and could reach ', COALESCE(potential_rating, '?'),
                ' (', COALESCE(CAST(potential_efficiency AS VARCHAR), '?'), '/100) with improvements.',
                ' Walls: ', LOWER(COALESCE(walls_description, 'unknown')), '.',
                ' Roof: ', LOWER(COALESCE(roof_description, 'unknown')), '.',
                ' Windows: ', LOWER(COALESCE(windows_description, 'unknown')), '.',
                ' Heating: ', LOWER(COALESCE(heating_description, 'unknown')), '.',
                ' Main fuel: ', LOWER(COALESCE(main_fuel, 'unknown')), '.'
            ) AS text_summary,

            lodgement_date

        FROM silver.epc_clean
        WHERE current_efficiency   BETWEEN 1 AND 100
          AND potential_efficiency BETWEEN 1 AND 100
    """)

    con.execute("""
        CREATE TABLE gold.portfolio_agg AS
        SELECT
            property_type,
            construction_age_band,
            retrofit_priority,
            COUNT(*)                                AS property_count,
            ROUND(AVG(current_efficiency), 1)       AS avg_current_efficiency,
            ROUND(AVG(potential_efficiency), 1)     AS avg_potential_efficiency,
            ROUND(AVG(retrofit_score), 1)           AS avg_retrofit_score,
            ROUND(AVG(annual_savings_potential), 0) AS avg_annual_savings_gbp,
            ROUND(SUM(co2_saving_tonnes), 1)        AS total_co2_saving_tonnes,
            ROUND(AVG(total_floor_area), 1)         AS avg_floor_area_m2
        FROM gold.epc_features
        GROUP BY property_type, construction_age_band, retrofit_priority
        ORDER BY avg_retrofit_score DESC
    """)

    feat_count = con.execute("SELECT COUNT(*) FROM gold.epc_features").fetchone()[0]
    agg_count  = con.execute("SELECT COUNT(*) FROM gold.portfolio_agg").fetchone()[0]

    score_stats = con.execute("""
        SELECT
            retrofit_priority,
            COUNT(*) AS n,
            ROUND(AVG(retrofit_score), 1) AS avg_score,
            ROUND(AVG(annual_savings_potential), 0) AS avg_savings
        FROM gold.epc_features
        GROUP BY retrofit_priority
        ORDER BY avg_score DESC
    """).fetchall()

    con.close()

    print(f"[gold] Done -- {feat_count:,} feature rows, {agg_count} portfolio segments")
    print("[gold] Retrofit priority breakdown:")
    for row in score_stats:
        print(f"  {row[0]:<8} {row[1]:>7,} properties  |  avg score {row[2]}  |  avg savings GBP{row[3]:,}/yr")

    return {
        "layer":          "gold",
        "features_table": "gold.epc_features",
        "agg_table":      "gold.portfolio_agg",
        "feature_rows":   feat_count,
        "agg_segments":   agg_count,
        "db_path":        db_path,
        "completed":      datetime.now().isoformat(),
    }


if __name__ == "__main__":
    run()
