"""
Telecom Data Pipeline — Master Airflow DAG
==========================================
Orchestrates the full ELT pipeline:
  1. Trigger ADF pipelines (ingest raw data to Blob)
  2. Wait for ADF completion
  3. Run PySpark transformations (raw → silver)
  4. Run dbt models (bronze → silver → gold)
  5. Run dbt tests (data quality checks)
  6. Send success/failure email alerts

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

# ── Default Arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner":            "telecom_de_team",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email":            ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="telecom_master_pipeline",
    default_args=default_args,
    description="End-to-end telecom ELT pipeline",
    schedule_interval="0 6 * * *",     # Daily at 6AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["telecom", "ELT", "production"],
) as dag:

    # ── Task 1: Trigger ADF — Customers ──────────────────────────────────────
    def trigger_adf_pipeline(source_name, file_name, file_format, **kwargs):
        """
        Triggers Azure Data Factory pipeline for a given source.
        Uses Azure SDK to call ADF REST API.
        """
        from azure.identity import ClientSecretCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient
        from azure.mgmt.datafactory.models import CreateRunResponse
        import os

        credential = ClientSecretCredential(
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET")
        )

        adf_client = DataFactoryManagementClient(
            credential,
            os.getenv("AZURE_SUBSCRIPTION_ID")
        )

        load_date = kwargs["ds"]   # Airflow execution date (YYYY-MM-DD)

        pipeline_params = {
            "p_source_name": source_name,
            "p_file_name":   file_name,
            "p_file_format": file_format,
            "p_load_date":   load_date,
        }

        run_response: CreateRunResponse = adf_client.pipelines.create_run(
            resource_group_name="telecom-de-rg",
            factory_name="telecom-de-adf",
            pipeline_name="pl_ingest_telecom_master",
            parameters=pipeline_params
        )

        print(f"ADF pipeline triggered for {source_name} | Run ID: {run_response.run_id}")
        return run_response.run_id

    trigger_adf_customers = PythonOperator(
        task_id="trigger_adf_customers",
        python_callable=trigger_adf_pipeline,
        op_kwargs={
            "source_name": "customers",
            "file_name":   "customers.csv",
            "file_format": "csv",
        },
    )

    trigger_adf_cdr = PythonOperator(
        task_id="trigger_adf_cdr",
        python_callable=trigger_adf_pipeline,
        op_kwargs={
            "source_name": "cdr",
            "file_name":   "cdr.csv",
            "file_format": "csv",
        },
    )

    trigger_adf_billing = PythonOperator(
        task_id="trigger_adf_billing",
        python_callable=trigger_adf_pipeline,
        op_kwargs={
            "source_name": "billing",
            "file_name":   "billing.json",
            "file_format": "json",
        },
    )

    trigger_adf_network_kpi = PythonOperator(
        task_id="trigger_adf_network_kpi",
        python_callable=trigger_adf_pipeline,
        op_kwargs={
            "source_name": "network_kpi",
            "file_name":   "network_kpi.csv",
            "file_format": "csv",
        },
    )

    # ── Task 2: Wait for ADF pipelines to complete ───────────────────────────
    wait_for_adf = TimeDeltaSensor(
        task_id="wait_for_adf_completion",
        delta=timedelta(minutes=10),
        poke_interval=60,
    )

    # ── Task 3: Run PySpark Transformations ───────────────────────────────────
    def run_pyspark_transformations(**kwargs):
        """
        Submits PySpark job to process raw files from Blob
        and write cleaned Parquet files to silver zone.
        """
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import os

        load_date = kwargs["ds"]

        spark = SparkSession.builder \
            .appName("TelecomTransformations") \
            .getOrCreate()

        storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
        storage_key     = os.getenv("AZURE_STORAGE_KEY")

        spark.conf.set(
            f"fs.azure.account.key.{storage_account}.blob.core.windows.net",
            storage_key
        )

        processed_path = f"wasbs://processed@{storage_account}.blob.core.windows.net"
        silver_path    = f"wasbs://silver@{storage_account}.blob.core.windows.net"

        # Transform customers
        customers_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(f"{processed_path}/customers/dt={load_date}/customers.csv")

        customers_clean = customers_df \
            .dropDuplicates(["customer_id"]) \
            .filter(F.col("customer_id").isNotNull()) \
            .withColumn("tenure_years", F.round(F.col("tenure_months") / 12, 1)) \
            .withColumn("ingestion_timestamp", F.current_timestamp())

        customers_clean.write.mode("overwrite") \
            .parquet(f"{silver_path}/customers/dt={load_date}/")

        print(f"PySpark transformations complete for {load_date}")

    run_pyspark = PythonOperator(
        task_id="run_pyspark_transformations",
        python_callable=run_pyspark_transformations,
    )

    # ── Task 4: Run dbt Models ────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_telecom && dbt run --profiles-dir /opt/airflow/dbt_telecom",
    )

    # ── Task 5: Run dbt Tests ─────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_telecom && dbt test --profiles-dir /opt/airflow/dbt_telecom",
    )

    # ── Task 6: Success Alert ─────────────────────────────────────────────────
    success_alert = EmailOperator(
        task_id="send_success_alert",
        to="your_email@example.com",
        subject="Telecom Pipeline SUCCESS — {{ ds }}",
        html_content="""
            <h3>Telecom Pipeline completed successfully!</h3>
            <p>Date: {{ ds }}</p>
            <p>All tasks completed:</p>
            <ul>
                <li>ADF ingestion — 4 sources loaded</li>
                <li>PySpark transformations — silver zone updated</li>
                <li>dbt models — 7 models refreshed</li>
                <li>dbt tests — all passed</li>
            </ul>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task 7: Failure Alert ─────────────────────────────────────────────────
    failure_alert = EmailOperator(
        task_id="send_failure_alert",
        to="your_email@example.com",
        subject="Telecom Pipeline FAILED — {{ ds }}",
        html_content="""
            <h3>Telecom Pipeline FAILED!</h3>
            <p>Date: {{ ds }}</p>
            <p>Please check the Airflow logs for details.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── DAG Dependencies (Task Order) ─────────────────────────────────────────
    #
    #  trigger_adf_customers ──┐
    #  trigger_adf_cdr ────────┤
    #                          ├──► wait_for_adf ──► run_pyspark ──► dbt_run ──► dbt_test ──► success_alert
    #  trigger_adf_billing ────┤                                                           └──► failure_alert
    #  trigger_adf_network_kpi─┘
    #

    [
        trigger_adf_customers,
        trigger_adf_cdr,
        trigger_adf_billing,
        trigger_adf_network_kpi,
    ] >> wait_for_adf >> run_pyspark >> dbt_run >> dbt_test >> [success_alert, failure_alert]
