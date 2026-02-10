import sys
import importlib
import subprocess
import os
import urllib.request

REQUIRED_PACKAGES = [
    ("pandas", "pandas"),
    ("duckdb", "duckdb"),
    ("pyarrow", "pyarrow"),
    ("torch", "torch"),
    ("transformers", "transformers"),
    ("sentence_transformers", "sentence-transformers"),
    ("sklearn", "scikit-learn"),
    ("PIL", "Pillow"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("streamlit", "streamlit"),
    ("plotly", "plotly"),
    ("dvc", "dvc"),
    ("requests", "requests"),
    ("tqdm", "tqdm"),
    ("dotenv", "python-dotenv"),
]

DATA_FILES = [
    ("data/raw/certificates.csv", "EPC domestic certificates CSV"),
    ("data/images/", "Property images folder"),
]

print(f"\n{'='*55}")
print("  HOUSING RETROFIT AI - SETUP VERIFICATION")
print(f"{'='*55}")

pv = sys.version_info
status = "OK" if pv >= (3, 10) else "FAIL - need 3.10+"
print(f"\n[Python]  {pv.major}.{pv.minor}.{pv.micro}  ->  {status}")

try:
    r = subprocess.run(["git", "--version"], capture_output=True, text=True)
    print(f"[Git]     {r.stdout.strip()}  ->  OK")
except FileNotFoundError:
    print("[Git]     NOT FOUND  ->  FAIL")

try:
    r = subprocess.run(["docker", "--version"], capture_output=True, text=True)
    print(f"[Docker]  {r.stdout.strip()}  ->  OK")
except FileNotFoundError:
    print("[Docker]  NOT FOUND  ->  WARN")

print(f"\n{'-'*55}")
print("  PYTHON PACKAGES")
print(f"{'-'*55}")
missing = []
for import_name, pkg_name in REQUIRED_PACKAGES:
    try:
        mod = importlib.import_module(import_name)
        ver = getattr(mod, "__version__", "installed")
        print(f"  OK   {pkg_name:<30} {ver}")
    except ImportError:
        print(f"  FAIL {pkg_name:<30} NOT INSTALLED")
        missing.append(pkg_name)

print(f"\n{'-'*55}")
print("  DATA FILES")
print(f"{'-'*55}")
data_missing = []
for path, label in DATA_FILES:
    exists = os.path.exists(path)
    status = "OK" if exists else "MISSING"
    size = ""
    if exists and os.path.isfile(path):
        mb = os.path.getsize(path) / 1_048_576
        size = f"({mb:.1f} MB)"
    elif exists and os.path.isdir(path):
        count = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
        size = f"({count} files)"
    print(f"  {status:<6} {label:<35} {size}")
    if not exists:
        data_missing.append(path)

print(f"\n{'-'*55}")
print("  CONNECTIVITY")
print(f"{'-'*55}")
try:
    urllib.request.urlopen("https://epc.opendatacommunities.org", timeout=5)
    print("  OK   EPC Open Data Communities - reachable")
except Exception:
    print("  FAIL EPC Open Data Communities - not reachable")

print(f"\n{'='*55}")
if missing:
    print(f"  MISSING PACKAGES: pip install {' '.join(missing)}")
if data_missing:
    print(f"  DATA NOT YET DOWNLOADED - follow README steps")
if not missing and not data_missing:
    print("  ALL CHECKS PASSED - ready to build")
elif not missing:
    print("  PACKAGES OK - still need to download data")
print(f"{'='*55}\n")
