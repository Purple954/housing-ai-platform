"""Run the full ETL pipeline: bronze -> silver -> gold."""

import time
from src.etl import bronze, silver, gold


def run():
    print("\n" + "=" * 55)
    print("  HOUSING RETROFIT AI -- ETL PIPELINE")
    print("=" * 55 + "\n")

    start = time.time()

    b = bronze.run()
    s = silver.run()
    g = gold.run()

    elapsed = round(time.time() - start, 1)

    print("\n" + "=" * 55)
    print("  PIPELINE COMPLETE")
    print("=" * 55)
    print(f"  Bronze : {b['rows']:>8,} rows  -> {b['table']}")
    print(f"  Silver : {s['rows']:>8,} rows  -> {s['table']}  (dropped {s['dropped']:,})")
    print(f"  Gold   : {g['feature_rows']:>8,} rows  -> {g['features_table']}")
    print(f"  Took   : {elapsed}s")
    print(f"  DB     : {b['db_path']}")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    run()
