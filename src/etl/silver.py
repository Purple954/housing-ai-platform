"""Silver layer — type casting, deduplication, and data quality scoring."""

import duckdb
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH      = PROJECT_ROOT / "data" / "processed" / "housing.duckdb"


def run(db_path: str = str(DB_PATH)) -> dict:
    print("[silver] Starting cleaning...")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("DROP TABLE IF EXISTS silver.epc_clean")

    con.execute("""
        CREATE TABLE silver.epc_clean AS
        WITH deduped AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY LMK_KEY
                       ORDER BY LODGEMENT_DATETIME DESC
                   ) AS _rn
            FROM bronze.epc_raw
        ),
        base AS (
            SELECT
                LMK_KEY                                             AS lmk_key,
                BUILDING_REFERENCE_NUMBER                           AS building_ref,
                UPRN                                                AS uprn,
                ADDRESS1                                            AS address1,
                ADDRESS2                                            AS address2,
                POSTCODE                                            AS postcode,
                POSTTOWN                                            AS posttown,
                LOCAL_AUTHORITY                                     AS local_authority_code,
                LOCAL_AUTHORITY_LABEL                               AS local_authority,
                CONSTITUENCY_LABEL                                  AS constituency,
                PROPERTY_TYPE                                       AS property_type,
                BUILT_FORM                                          AS built_form,
                CONSTRUCTION_AGE_BAND                               AS construction_age_band,
                TENURE                                              AS tenure,
                TRY_CAST(TOTAL_FLOOR_AREA AS DOUBLE)               AS total_floor_area,
                TRY_CAST(NUMBER_HABITABLE_ROOMS AS INTEGER)        AS num_habitable_rooms,
                UPPER(TRIM(CURRENT_ENERGY_RATING))                  AS current_rating,
                UPPER(TRIM(POTENTIAL_ENERGY_RATING))                AS potential_rating,
                TRY_CAST(CURRENT_ENERGY_EFFICIENCY AS INTEGER)     AS current_efficiency,
                TRY_CAST(POTENTIAL_ENERGY_EFFICIENCY AS INTEGER)   AS potential_efficiency,
                TRY_CAST(CO2_EMISSIONS_CURRENT AS DOUBLE)          AS co2_current,
                TRY_CAST(CO2_EMISSIONS_POTENTIAL AS DOUBLE)        AS co2_potential,
                TRY_CAST(HEATING_COST_CURRENT AS DOUBLE)           AS heating_cost_current,
                TRY_CAST(HEATING_COST_POTENTIAL AS DOUBLE)         AS heating_cost_potential,
                TRY_CAST(HOT_WATER_COST_CURRENT AS DOUBLE)         AS hot_water_cost_current,
                TRY_CAST(HOT_WATER_COST_POTENTIAL AS DOUBLE)       AS hot_water_cost_potential,
                TRY_CAST(LIGHTING_COST_CURRENT AS DOUBLE)          AS lighting_cost_current,
                TRY_CAST(LIGHTING_COST_POTENTIAL AS DOUBLE)        AS lighting_cost_potential,
                WALLS_DESCRIPTION                                   AS walls_description,
                ROOF_DESCRIPTION                                    AS roof_description,
                WINDOWS_DESCRIPTION                                 AS windows_description,
                MAINHEAT_DESCRIPTION                                AS heating_description,
                HOTWATER_DESCRIPTION                                AS hot_water_description,
                LIGHTING_DESCRIPTION                                AS lighting_description,
                WALLS_ENERGY_EFF                                    AS walls_eff_label,
                ROOF_ENERGY_EFF                                     AS roof_eff_label,
                WINDOWS_ENERGY_EFF                                  AS windows_eff_label,
                MAINHEAT_ENERGY_EFF                                 AS heating_eff_label,
                HOT_WATER_ENERGY_EFF                                AS hot_water_eff_label,
                LIGHTING_ENERGY_EFF                                 AS lighting_eff_label,
                MAIN_FUEL                                           AS main_fuel,
                MAINS_GAS_FLAG                                      AS mains_gas,
                SOLAR_WATER_HEATING_FLAG                            AS solar_water_heating,
                TRY_CAST(PHOTO_SUPPLY AS DOUBLE)                   AS solar_pv_supply_pct,
                TRY_CAST(INSPECTION_DATE AS DATE)                  AS inspection_date,
                TRY_CAST(LODGEMENT_DATE AS DATE)                   AS lodgement_date,
                TRANSACTION_TYPE                                    AS transaction_type
            FROM deduped
            WHERE _rn = 1
              AND CURRENT_ENERGY_EFFICIENCY  IS NOT NULL
              AND POTENTIAL_ENERGY_EFFICIENCY IS NOT NULL
              AND PROPERTY_TYPE              IS NOT NULL
        )
        SELECT
            base.*,
            CASE walls_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS walls_eff_score,
            CASE roof_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS roof_eff_score,
            CASE windows_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS windows_eff_score,
            CASE heating_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS heating_eff_score,
            CASE hot_water_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS hot_water_eff_score,
            CASE lighting_eff_label
                WHEN 'Very Good' THEN 5 WHEN 'Good' THEN 4
                WHEN 'Average'   THEN 3 WHEN 'Poor' THEN 2
                WHEN 'Very Poor' THEN 1 ELSE NULL
            END AS lighting_eff_score,
            ROUND(
                100.0 * (
                    (walls_description     IS NOT NULL)::INT +
                    (roof_description      IS NOT NULL)::INT +
                    (windows_description   IS NOT NULL)::INT +
                    (heating_description   IS NOT NULL)::INT +
                    (total_floor_area      IS NOT NULL)::INT +
                    (construction_age_band IS NOT NULL)::INT +
                    (tenure                IS NOT NULL)::INT +
                    (main_fuel             IS NOT NULL)::INT
                ) / 8.0
            ) AS data_quality_score
        FROM base
    """)

    row_count = con.execute("SELECT COUNT(*) FROM silver.epc_clean").fetchone()[0]
    raw_count = con.execute("SELECT COUNT(*) FROM bronze.epc_raw").fetchone()[0]
    dropped   = raw_count - row_count

    con.close()

    print(f"[silver] Done -- {row_count:,} rows (dropped {dropped:,}) -> silver.epc_clean")
    return {
        "layer":     "silver",
        "table":     "silver.epc_clean",
        "rows":      row_count,
        "dropped":   dropped,
        "db_path":   db_path,
        "completed": datetime.now().isoformat(),
    }


if __name__ == "__main__":
    run()
