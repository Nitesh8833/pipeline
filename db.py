
import sys
import json
from google.cloud import storage

def load_config_from_gcs(gcs_path: str) -> dict:
    """Load JSON config file from GCS given a gs:// path."""
    if not gcs_path.startswith("gs://"):
        raise ValueError("Config path must start with gs://")

    # remove "gs://" prefix
    path_no_prefix = gcs_path[5:]
    bucket_name, blob_path = path_no_prefix.split("/", 1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    config_text = blob.download_as_text()
    return json.loads(config_text)

if __name__ == "__main__":
    # Airflow passes JSON_CONFIG_PATH here
    config_path = sys.argv[1]
    print(f"Config path received: {config_path}")

    cfg = load_config_from_gcs(config_path)

    # Example: access values inside JSON
    print("Full config:", cfg)
    print("Send email?", cfg.get("send_email"))
    print("Database name:", cfg.get("database", {}).get("dbname"))

****************************************************
How do you read a CSV/Parquet/JSON file into a PySpark DataFrame?
How do you optimize a PySpark job that is running slow?
How do you perform window functions in PySpark? (ROW_NUMBER, RANK, SUM over partition, etc.)
How do you handle null values in PySpark (drop, fill, replace)?
How do you explode an array column into multiple rows?
How do you handle out-of-memory errors in PySpark executors?
You are given 500 GB of transaction data daily in CSV. How will you process and load it into BigQuery using Dataproc?
How do you handle production issues when a PySpark job fails in a critical SLA pipeline?

During the discussion, I was also asked questions 
related to the projects I had completed. 
When asked how much time it would take to convert the

output into a standard format using an LLM model such
 as ChatGPT or Copilot, I explained that under normal
 circumstances it would take approximately 10 to 15
 minutes. In case of any issues or complexities,
 the entire process would still be completed within a
 maximum of 30 minutes.
# ---------------- ENV → config file selection ----------------
def resolve_env(env_raw: str) -> str:
    env = (env_raw or "DEV").upper()
    logging.info(f"Environment: {env}")
    print(f"Environment: {env}")
    if env not in ["DEV", "QA", "PROD"]:
        raise ValueError(f"Invalid environment: {env}. Expected DEV, QA, or PROD")
    return env

# Priority: Airflow ENV var → fallback default "DEV"
ENV = resolve_env(os.environ.get("ENV"))

# Map ENV to config filename
CONFIG_FILES = {
    "DEV": "dags/pdi-ingestion-gcp/Dev/config/conformance_load_config_dev.json",
    "QA": "dags/pdi-ingestion-gcp/QA/config/conformance_load_config_QA.json",
    "PROD": "dags/pdi-ingestion-gcp/Prod/config/conformance_load_config_prod.json",
}

config_blob = CONFIG_FILES[ENV]

# ---------------- Load config from GCS ----------------
cfg_all = load_config_from_gcs(
    "us-east4-cmp-dev-pdi-ink-05-5e69530c-bucket",  # same bucket
    config_blob
)

***************************************
def resolve_db_name(env_raw: str) -> str:
    env = (env_raw or "").upper()
    logging.info(f"Environment: {env}")
    print(f"Environment: {env}")

    mapping = {
        "DEV":  "pdigppsd1_db",
        "QA":   "pdigppgsql_db",
        "PROD": "pdigppsp1_db",
    }
    try:
        return mapping[env]
    except KeyError:
        raise ValueError(f"Invalid environment: {env}. Expected one of {list(mapping)}")

# Priority: AIRFLOW env var ENV → config ENVIRONMENT → default DEV
ENV = (os.environ.get("ENV") or cfg.get("ENVIRONMENT") or "DEV").upper()
DB_NAME = resolve_db_name(ENV)
