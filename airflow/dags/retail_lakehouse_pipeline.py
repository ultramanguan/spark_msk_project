"""Production pipeline DAG: creates an ephemeral EMR cluster, runs the emr_jobs/ scripts as EMR
Steps in the same order resources/jobs.yml (the now-retired Databricks Job) used, then always
terminates the cluster.

Account-specific values come from Airflow Variables rather than Terraform state directly, since
this file is parsed by the Airflow scheduler independently of any `terraform apply`. Set these once
per environment (Airflow UI -> Admin -> Variables, or `airflow variables set`):
  - retail_lakehouse_bucket            (from `terraform output lakehouse_bucket_name`)
  - retail_lakehouse_subnet_id         (one of the subnet_ids passed to Terraform)
  - retail_lakehouse_base_path         (e.g. s3://<bucket>/data)
  - retail_lakehouse_emr_msk_client_sg (from `terraform output emr_msk_client_security_group_id`)
Optional, with defaults matching Plan 1's Terraform variable defaults:
  - retail_lakehouse_emr_instance_profile (default: retail-lakehouse-dev-emr-instance-profile)
  - retail_lakehouse_emr_service_role     (default: retail-lakehouse-dev-emr-service-role)
  - retail_lakehouse_emr_release_label    (default: emr-7.5.0)
  - retail_lakehouse_emr_instance_type    (default: m5.xlarge)
  - retail_lakehouse_schema               (default: retail_lakehouse)

Prerequisites this DAG does not automate:
  - The five emr_jobs/*.py scripts must be synced to S3 before this DAG can run, e.g.:
      aws s3 sync emr_jobs/ s3://<bucket>/emr_jobs/
    This isn't automated by this plan; it's expected to be handled by a later CI/CD plan.
  - The EMR subnet (retail_lakehouse_subnet_id) must have outbound internet access (a NAT
    gateway or equivalent) so that `--packages io.delta:...` can resolve from Maven Central
    during Ivy dependency resolution. If the configured subnet has none, every step will fail
    at submit time.
"""
import itertools
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

LAKEHOUSE_BUCKET = Variable.get("retail_lakehouse_bucket")
SUBNET_ID = Variable.get("retail_lakehouse_subnet_id")
BASE_PATH = Variable.get("retail_lakehouse_base_path")
EMR_MSK_CLIENT_SG = Variable.get("retail_lakehouse_emr_msk_client_sg")
EMR_INSTANCE_PROFILE = Variable.get(
    "retail_lakehouse_emr_instance_profile", default_var="retail-lakehouse-dev-emr-instance-profile"
)
EMR_SERVICE_ROLE = Variable.get(
    "retail_lakehouse_emr_service_role", default_var="retail-lakehouse-dev-emr-service-role"
)
EMR_RELEASE_LABEL = Variable.get("retail_lakehouse_emr_release_label", default_var="emr-7.5.0")
EMR_INSTANCE_TYPE = Variable.get("retail_lakehouse_emr_instance_type", default_var="m5.xlarge")
SCHEMA = Variable.get("retail_lakehouse_schema", default_var="retail_lakehouse")

WHEEL_S3_URI = f"s3://{LAKEHOUSE_BUCKET}/artifacts/retail_lakehouse-latest.whl"
BOOTSTRAP_S3_URI = f"s3://{LAKEHOUSE_BUCKET}/bootstrap/install_retail_lakehouse.sh"
SCRIPTS_S3_PREFIX = f"s3://{LAKEHOUSE_BUCKET}/emr_jobs"

# EMR doesn't ship Delta Lake built-in (unlike Databricks) -- every step needs this explicitly.
DELTA_SPARK_CONF = [
    "--packages", "io.delta:delta-spark_2.12:3.1.0",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "--conf", f"spark.sql.warehouse.dir={BASE_PATH}/tables",
]


def spark_step(name, script, extra_args=None):
    args = (
        ["spark-submit", "--deploy-mode", "cluster"]
        + DELTA_SPARK_CONF
        + [f"{SCRIPTS_S3_PREFIX}/{script}", "--schema", SCHEMA, "--base-path", BASE_PATH]
        + (extra_args or [])
    )
    return {
        "Name": name,
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": args},
    }


@task
def get_step_id(step_ids, i):
    return step_ids[i]


JOB_FLOW_OVERRIDES = {
    "Name": "retail-lakehouse-pipeline",
    "LogUri": f"s3://{LAKEHOUSE_BUCKET}/emr-logs/pipeline/",
    "ReleaseLabel": EMR_RELEASE_LABEL,
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Configurations": [
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "InstanceRole": "MASTER", "InstanceType": EMR_INSTANCE_TYPE, "InstanceCount": 1},
            {"Name": "Core", "InstanceRole": "CORE", "InstanceType": EMR_INSTANCE_TYPE, "InstanceCount": 2},
        ],
        "Ec2SubnetId": SUBNET_ID,
        "AdditionalMasterSecurityGroups": [EMR_MSK_CLIENT_SG],
        "AdditionalSlaveSecurityGroups": [EMR_MSK_CLIENT_SG],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "AutoTerminationPolicy": {"IdleTimeout": 3600},
    "BootstrapActions": [
        {"Name": "install-retail-lakehouse", "ScriptBootstrapAction": {"Path": BOOTSTRAP_S3_URI, "Args": [WHEEL_S3_URI]}},
    ],
    "JobFlowRole": EMR_INSTANCE_PROFILE,
    "ServiceRole": EMR_SERVICE_ROLE,
    "VisibleToAllUsers": True,
    "Tags": [
        {"Key": "Project", "Value": "retail-lakehouse"},
        {"Key": "Environment", "Value": "dev"},
    ],
}

# Same order as resources/jobs.yml's task graph: 00 -> 01 -> 07 (fallback) -> 03 -> 06.
STEPS = [
    spark_step("00_environment_setup", "00_environment_setup.py"),
    spark_step("01_batch_medallion", "01_batch_lakehouse_bronze_silver_gold.py"),
    spark_step("02_streaming_ingest_fallback", "07_file_rate_streaming_fallback.py"),
    spark_step(
        "03_streaming_silver_gold", "03_streaming_silver_gold_delta.py",
        extra_args=["--bronze-table", "bronze_clickstream_rate"],
    ),
    spark_step("04_capstone", "06_capstone_end_to_end.py"),
]

with DAG(
    dag_id="retail_lakehouse_pipeline",
    description="Retail lakehouse production pipeline on ephemeral EMR, orchestrated by MWAA",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["retail-lakehouse"],
    default_args={"execution_timeout": timedelta(hours=2)},
) as dag:
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        emr_conn_id=None,
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_cluster.output,
        steps=STEPS,
    )

    step_sensors = [
        EmrStepSensor(
            task_id=f"wait_for_{step['Name']}",
            job_flow_id=create_cluster.output,
            step_id=get_step_id.override(task_id=f"get_step_id_{step['Name']}")(add_steps.output, i),
        )
        for i, step in enumerate(STEPS)
    ]

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> add_steps >> step_sensors[0]
    for upstream, downstream in itertools.pairwise(step_sensors):
        upstream >> downstream
    step_sensors[-1] >> terminate_cluster
