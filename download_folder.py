from google.cloud import storage
import os
import subprocess, shlex
from IPython.display import FileLink, display

# ---------- SETTINGS ----------
BUCKET = "us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket"
BASE_PREFIX = "dags/pdi-ingestion-gcp/Dev"
SUBFOLDERS = ["config", "scripts"]     # only these two
LOCAL_ROOT = "/home/jupyter/Dev_pickup"
ZIP_PATH = "/home/jupyter/Dev_config_scripts.zip"
# -------------------------------

# Create storage client
client = storage.Client()
bucket = client.bucket(BUCKET)

# Ensure local root exists
os.makedirs(LOCAL_ROOT, exist_ok=True)

# Download each folder
for sub in SUBFOLDERS:
    prefix = f"{BASE_PREFIX}/{sub}/"
    print(f"Downloading {prefix} ...")
    
    for blob in bucket.list_blobs(prefix=prefix):
        rel = blob.name[len(prefix):]  # relative path inside subfolder
        if not rel:  # skip folder placeholder
            continue
        
        dest = os.path.join(LOCAL_ROOT, sub, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        
        blob.download_to_filename(dest)
        print(f"  ✓ {blob.name} -> {dest}")

# Zip everything
if os.path.exists(ZIP_PATH):
    os.remove(ZIP_PATH)  # overwrite if exists

subprocess.check_call(shlex.split(f'zip -r {ZIP_PATH} {LOCAL_ROOT}'))

print(f"\nZipped to {ZIP_PATH}")

# Show clickable download link
display(FileLink(ZIP_PATH))
