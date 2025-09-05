from __future__ import annotations
import os, json, logging
from datetime import datetime
import pendulum

from google.cloud import storage, secretmanager
from google.auth import default as gauth_default

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.models import Variable

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)

# -----------------------------------------
# Timezone and Logging
# -----------------------------------------
local_tz = pendulum.timezone("US/Eastern")
logging.basicConfig(level=logging.INFO)

# -----------------------------------------
# Helper Functions
# -----------------------------------------
def load_config_from_gcs(bucket_name: str, blob_name: str) -> dict:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    return json.loads(blob.download_as_text())

def resolve_env(env: str | None = None) -> str:
    env = env or Variable.get("ENV", os.environ.get("ENV", "DEV")).upper()
    if env not in ("DEV", "QA", "PROD"):
        raise ValueError(f"Invalid environment: {env}. Expected DEV/QA/PROD")
    return env

fetch_env = Variable.get("ENV", default_var="DEV")
ENV = resolve_env(fetch_env)
print(f"Resolved ENV: {ENV}")

# Bucket + config mapping
BUCKET_NAME = Variable.get("BUCKET_NAME_ENV")
CONFIG_FILES = {
    "DEV":  "dags/prv_rstr_cnf_reports/Dev/config/conformance_load_config_dev.json",
    "QA":   "dags/prv_rstr_cnf_reports/QA/config/conformance_load_config_QA.json",
    "PROD": "dags/prv_rstr_cnf_reports/PROD/config/conformance_load_config_prod.json",
}
config_blob = CONFIG_FILES[ENV]

# Load config
cfg_all = load_config_from_gcs(BUCKET_NAME, config_blob)
cfg = cfg_all["config"]

# Cluster info (we are NOT creating/deleting it; it must already exist)
cluster_block    = cfg_all["cluster_config"]
GCE_CLUSTER_NAME = cluster_block["cluster_name"]
REGION           = cfg_all.get("region") or cfg["REGION"]

# Convenience getters
PROJECT_ID        = os.environ.get("GCP_PROJECT") or cfg["PROJECT_ID"]
OWNER_NAME        = cfg.get("OWNER_NAME", "owner")
DAG_ID            = cfg["DAG_ID"]
DAG_TAGS          = cfg["DAG_TAGS"]
CONNECT_SA        = cfg["CONNECT_SA"]
JSON_CONFIG_PATH  = cfg["JSON_CONFIG_PATH"]

# -----------------------------------------
# DB/env helpers
# -----------------------------------------
def resolve_db_name(env_raw: str) -> str:
    env = (env_raw or "").upper()
    mapping = {"DEV": "pdi_gcppsql1_db_dev", "QA": "pdi_gcppsql1_db_qa", "PROD": "pdi_gcppsql1_db"}
    try:
        return mapping[env]
    except KeyError:
        raise ValueError(f"Invalid environment: {env}. Expected one of {list(mapping)}")

ENV = (os.environ.get("ENV") or cfg.get("ENVIRONMENT", "DEV")).upper()
DB_NAME = resolve_db_name(ENV)

def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    resp = client.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user":     get_secret_value(project_id, "pdi_prvstrcnf_cloud_sql_user", creds),
        "password": get_secret_value(project_id, "pdi_prvstrcnf_cloud_sql_password", creds),
        "host":     get_secret_value(project_id, "pdi_prvstrcnf_cloud_sql_ip", creds),
    }

def get_curr_date(date_format_for: str) -> str:
    dt_now = pendulum.now("America/New_York")
    if date_format_for == "file":
        return dt_now.format("MMDDYYYY_HHmmss")
    return dt_now.format(date_format_for)

def final_status(**kwargs):
    ti = kwargs["ti"]
    for t in kwargs["dag"].tasks:
        ti_task = ti.get_previous_ti(task_id=t.task_id)
        if ti_task and ti_task.current_state() != State.SUCCESS:
            raise Exception(f"Task {t.task_id} failed. Failing this DAG run")

# Runtime cfg
creds, project_id = gauth_default()
db_creds = get_db_credentials(project_id, creds)

default_args = {
    "owner": OWNER_NAME,
    "retries": 0,
}

# -----------------------------------------
# Build Dataproc PySpark job payload (uses existing cluster)
# -----------------------------------------
def make_pyspark_job(main_uri: str, extra_args: list[str] | None = None) -> dict:
    base_args = [
        f"--ENV={cfg['ENVIRONMENT']}",
        f"--DB_NAME={cfg['DB_NAME']}",
        f"--TBL_NAME={cfg['TBL_NAME']}",
        f"--GCS_BUCKET={cfg['STG_STORAGE_BUCKET']}",
        f"--STORAGE_PROJECT_ID={cfg['STORAGE_PROJECT_ID']}",
        f"--BLOB_NAME={cfg['BLOB_NAME']}",
        f"--DB_USER={db_creds['user']}",
        f"--DB_PASSWORD={db_creds['password']}",
        f"--DB_INSTANCE={db_creds['host']}",
        f"--FROM_EMAIL={cfg['FROM_EMAIL']}",
        f"--TO_EMAIL={cfg['TO_EMAIL']}",
        f"--SMTP_SERVER={cfg['SMTP_SERVER']}",
        f"--JSON_CONFIG_PATH={cfg['JSON_CONFIG_PATH']}",
    ]
    if extra_args:
        base_args.extend(extra_args)

    return {
        "reference": {"project_id": PROJECT_ID},
        # IMPORTANT: placement points at the **existing** cluster
        "placement": {"cluster_name": GCE_CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": main_uri, "args": base_args},
    }

# Entry scripts
PY_MAIN1 = cfg["PYTHON_FILE_URIS_DAILY_1"]
PY_MAIN2 = cfg["PYTHON_FILE_URIS_DAILY_2"]

# -----------------------------------------
# DAG definition (no cluster create/delete)
# -----------------------------------------
dag = DAG(
    dag_id=DAG_ID,
    description="Runs PySpark jobs on an existing Dataproc cluster (no create/delete).",
    default_args=default_args,
    schedule_interval="0 5 * * *",  # daily 05:00 UTC
    start_date=timezone.datetime(2025, 9, 2, tzinfo=local_tz),
    catchup=False,
    tags=DAG_TAGS,
)

# Job 1
CER1 = DataprocSubmitJobOperator(
    task_id="job1_extract_and_enrich",
    job=make_pyspark_job(PY_MAIN1, extra_args=[f"--JSON_CONFIG_PATH={JSON_CONFIG_PATH}"]),
    region=REGION,
    project_id=PROJECT_ID,
    impersonation_chain=CONNECT_SA,
    dag=dag,
)

# Job 2
CER2 = DataprocSubmitJobOperator(
    task_id="job2_transform_and_load_to_gcp",
    job=make_pyspark_job(
        PY_MAIN2,
        extra_args=[f"--JSON_CONFIG_PATH={JSON_CONFIG_PATH}", "--STEP=publish_to_gcp"],
    ),
    region=REGION,
    project_id=PROJECT_ID,
    impersonation_chain=CONNECT_SA,
    dag=dag,
)

# Final status
final_status_task = PythonOperator(
    task_id="final_status",
    python_callable=final_status,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Dependencies: only jobs + final status
CER1 >> CER2 >> final_status_task
