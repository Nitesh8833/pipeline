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
