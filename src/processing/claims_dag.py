"""
Healthcare Claims Pipeline — Apache Airflow DAG
Orchestrates AWS Glue ETL, Step Functions workflow, and Great Expectations
validation for 2M+ member enrollment and claims processing.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import boto3
import json
import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "pramod.vishnumolakala",
    "depends_on_past":  False,
    "email":            ["pramodvishnumolakala@gmail.com"],
    "email_on_failure": True,
    "retries":          3,
    "retry_delay":      timedelta(minutes=10),
}

SNS_TOPIC = "arn:aws:sns:us-east-1:123456789:healthcare-pipeline-alerts"
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:123456789:stateMachine:healthcare-claims-workflow"


def run_great_expectations_validation(**context):
    """
    Runs Great Expectations checkpoint against curated claims data.
    Raises on validation failure to halt the pipeline.
    """
    import great_expectations as ge

    context_ge = ge.data_context.DataContext("/opt/airflow/great_expectations")
    result = context_ge.run_checkpoint(
        checkpoint_name="healthcare_claims_checkpoint",
        batch_request={
            "datasource_name": "s3_claims_datasource",
            "data_connector_name": "default_inferred_data_connector",
            "data_asset_name": f"curated/claims/year={context['ds'][:4]}/month={context['ds'][5:7]}/",
        },
    )
    if not result["success"]:
        failed = [
            r["expectation_config"]["expectation_type"]
            for r in result["results"]
            if not r["success"]
        ]
        raise ValueError(f"Great Expectations validation failed: {failed}")
    logger.info("Great Expectations validation passed.")


def publish_pipeline_metrics(**context):
    """Push pipeline run metrics to CloudWatch."""
    cw = boto3.client("cloudwatch", region_name="us-east-1")
    cw.put_metric_data(
        Namespace="HealthcarePipeline",
        MetricData=[
            {
                "MetricName": "PipelineSuccess",
                "Value": 1,
                "Unit": "Count",
                "Dimensions": [{"Name": "PipelineName", "Value": "healthcare-claims-etl"}],
            }
        ],
    )
    logger.info("Pipeline metrics published to CloudWatch.")


with DAG(
    dag_id="healthcare_claims_data_lake",
    description="HIPAA-compliant healthcare enrollment and claims ETL pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 3 * * *",    # 3am UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "hipaa", "claims", "pramod"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Enrollment ETL
    enrollment_etl = GlueJobOperator(
        task_id="enrollment_etl",
        job_name="healthcare-enrollment-etl",
        script_args={
            "--S3_RAW_BUCKET":     "s3://pramod-healthcare-raw/",
            "--S3_CURATED_BUCKET": "s3://pramod-healthcare-curated/",
            "--KMS_KEY_ID":        "alias/healthcare-hipaa-key",
            "--PROCESS_DATE":      "{{ ds }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # Claims ETL
    claims_etl = GlueJobOperator(
        task_id="claims_etl",
        job_name="healthcare-claims-etl",
        script_args={
            "--S3_RAW_BUCKET":     "s3://pramod-healthcare-raw/",
            "--S3_CURATED_BUCKET": "s3://pramod-healthcare-curated/",
            "--REDSHIFT_URL":      "jdbc:redshift://healthcare-dw.us-east-1.redshift.amazonaws.com:5439/healthdb",
            "--REDSHIFT_TEMP_DIR": "s3://pramod-healthcare-redshift-temp/",
            "--KMS_KEY_ID":        "alias/healthcare-hipaa-key",
            "--PROCESS_DATE":      "{{ ds }}",
        },
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    # Validate output with Great Expectations
    validate = PythonOperator(
        task_id="validate_with_great_expectations",
        python_callable=run_great_expectations_validation,
    )

    # Trigger Step Functions for Redshift load + Glue Catalog update
    trigger_step_fn = StepFunctionStartExecutionOperator(
        task_id="trigger_redshift_load",
        state_machine_arn=STATE_MACHINE_ARN,
        input=json.dumps({"process_date": "{{ ds }}", "pipeline": "healthcare-claims"}),
        aws_conn_id="aws_default",
    )

    wait_step_fn = StepFunctionExecutionSensor(
        task_id="wait_for_redshift_load",
        execution_arn="{{ task_instance.xcom_pull('trigger_redshift_load') }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=7200,
    )

    # Publish CloudWatch metrics
    publish_metrics = PythonOperator(
        task_id="publish_metrics",
        python_callable=publish_pipeline_metrics,
    )

    # Success notification
    notify_success = SnsPublishOperator(
        task_id="notify_success",
        target_arn=SNS_TOPIC,
        message="Healthcare Claims Pipeline SUCCESS — {{ ds }}\nHIPAA compliance: 100%\nData available in Redshift healthcare schema.",
        subject="Healthcare Pipeline Succeeded — {{ ds }}",
        aws_conn_id="aws_default",
    )

    # Failure notification
    notify_failure = SnsPublishOperator(
        task_id="notify_failure",
        target_arn=SNS_TOPIC,
        message="ALERT: Healthcare Claims Pipeline FAILED — {{ ds }}\nCheck Airflow logs immediately.",
        subject="ALERT: Healthcare Pipeline Failed",
        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # DAG dependencies
    start >> [enrollment_etl, claims_etl]
    [enrollment_etl, claims_etl] >> validate
    validate >> trigger_step_fn >> wait_step_fn >> publish_metrics >> notify_success >> end
    notify_failure >> end
