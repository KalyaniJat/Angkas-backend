from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# GCP Clients
from google.cloud import dataplex_v1
from google.cloud import dlp_v2
from google.cloud import asset_v1

# Set project and region
PROJECT_ID = "practicebigdataanalytics"
LOCATION = "us-central1"
PARENT = "projects/practicebigdataanalytics/locations/us-central1"

# --- Task Functions ---

def capture_lineage():
    client = dataplex_v1.DataScanServiceClient()
    print(f"Capturing data lineage under: {PARENT}")
    scans = client.list_data_scans(parent=PARENT)
    for scan in scans:
        print(f"Found scan: {scan.name}")

def run_data_quality_checks():
    client = dataplex_v1.DataScanServiceClient()
    # scan_name = f"{PARENT}/dataScans/your-dq-scan-name"  # Replace with your actual Data Quality scan name
    # response = client.run_data_scan(name=scan_name)
    print(f"Triggered Data Quality Scan: ")

def tag_glossary_terms():
    print("Glossary tagging placeholder. Add metadata tagging logic here.")

def validate_compliance():
    client = dlp_v2.DlpServiceClient()
    info_types = [{"name": "PERSON_NAME"}, {"name": "EMAIL_ADDRESS"}, {"name": "PHONE_NUMBER"}]

    item = {"value": "Sample input containing john.doe@example.com"}
    response = client.inspect_content(
        request={
            "parent": f"projects/{PROJECT_ID}",
            "inspect_config": {"info_types": info_types, "include_quote": True},
            "item": item,
        }
    )
    for result in response.result.findings:
        print(f"Compliance check found: {result.info_type.name} -> {result.quote}")

def classify_data():
    print("Classifying data using DLP or metadata. Add logic based on your classification rules.")

def audit_access_controls():
    client = asset_v1.AssetServiceClient()
    scope = f"projects/{PROJECT_ID}"
    response = client.search_all_iam_policies(request={"scope": scope, "query": "policy:roles/viewer"})
    for result in response:
        print(f"IAM Policy found: {result}")

# --- DAG Definition ---

with DAG(
    dag_id="data_governance_pipeline",
    start_date=datetime(2025, 3, 27),
    schedule_interval="@daily",
    catchup=False,
    tags=["data_governance", "gcp"],
    description="DAG for managing data governance in GCP using Dataplex, DLP, and IAM.",
) as dag:

    start = DummyOperator(task_id="start")

    lineage = PythonOperator(
        task_id="capture_lineage",
        python_callable=capture_lineage,
    )

    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    glossary = PythonOperator(
        task_id="glossary_tagging",
        python_callable=tag_glossary_terms,
    )

    compliance = PythonOperator(
        task_id="compliance_validation",
        python_callable=validate_compliance,
    )

    classification = PythonOperator(
        task_id="data_classification",
        python_callable=classify_data,
    )

    access_control = PythonOperator(
        task_id="access_control_audit",
        python_callable=audit_access_controls,
    )

    end = DummyOperator(task_id="end")

    start >> [lineage, data_quality, glossary, compliance, classification, access_control] >> end
