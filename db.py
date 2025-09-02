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
