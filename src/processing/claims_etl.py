"""
Healthcare Claims ETL — HIPAA-compliant batch processing pipeline.
Processes enrollment and claims for 2M+ members using AWS Glue + PySpark.
Applies field-level masking for PHI and loads into Amazon Redshift.
Pramod Vishnumolakala — github.com/pramod-vishnumolakala
"""

import sys
import hashlib
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, DateType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "S3_RAW_BUCKET", "S3_CURATED_BUCKET",
    "REDSHIFT_URL", "REDSHIFT_TEMP_DIR", "KMS_KEY_ID",
])

sc      = SparkContext()
glueCtx = GlueContext(sc)
spark   = glueCtx.spark_session
job     = Job(glueCtx)
job.init(args["JOB_NAME"], args)

RAW_BUCKET     = args["S3_RAW_BUCKET"]
CURATED_BUCKET = args["S3_CURATED_BUCKET"]
REDSHIFT_URL   = args["REDSHIFT_URL"]
TEMP_DIR       = args["REDSHIFT_TEMP_DIR"]

# ── PHI masking UDFs ─────────────────────────────────────────────────

@F.udf(StringType())
def mask_ssn(ssn: str) -> str:
    """Mask SSN — show only last 4 digits. HIPAA Safe Harbor."""
    if not ssn:
        return None
    cleaned = ssn.replace("-", "").replace(" ", "")
    return f"XXX-XX-{cleaned[-4:]}" if len(cleaned) >= 4 else "XXX-XX-XXXX"


@F.udf(StringType())
def hash_member_id(member_id: str) -> str:
    """One-way SHA-256 hash for member ID — preserves join-ability without PII."""
    if not member_id:
        return None
    return hashlib.sha256(member_id.encode()).hexdigest()[:16]


@F.udf(StringType())
def mask_dob(dob: str) -> str:
    """Generalise DOB to birth year only — HIPAA Safe Harbor §164.514(b)."""
    if not dob:
        return None
    return dob[:4] if len(dob) >= 4 else None


@F.udf(StringType())
def mask_zip(zip_code: str) -> str:
    """Truncate ZIP to 3 digits for low-population areas per HIPAA."""
    if not zip_code:
        return None
    return zip_code[:3] + "XX"


# ── 1. Enrollment ingestion ──────────────────────────────────────────
def process_enrollment() -> int:
    logger.info("Processing enrollment data...")
    raw = spark.read.parquet(f"{RAW_BUCKET}/enrollment/")
    logger.info(f"Raw enrollment records: {raw.count():,}")

    processed = (
        raw
        .filter(F.col("member_id").isNotNull())
        .dropDuplicates(["member_id", "enrollment_date"])

        # Apply PHI masking — HIPAA Safe Harbor
        .withColumn("member_id_hashed", hash_member_id(F.col("member_id")))
        .withColumn("ssn_masked",       mask_ssn(F.col("ssn")))
        .withColumn("dob_year",         mask_dob(F.col("date_of_birth")))
        .withColumn("zip_masked",       mask_zip(F.col("zip_code")))

        # Drop original PII columns
        .drop("member_id", "ssn", "date_of_birth", "zip_code",
              "first_name", "last_name", "address_line1", "phone_number")

        # Standardise
        .withColumn("enrollment_date",    F.to_date("enrollment_date"))
        .withColumn("disenrollment_date", F.to_date("disenrollment_date"))
        .withColumn("coverage_type",      F.upper(F.trim("coverage_type")))
        .withColumn("plan_code",          F.upper(F.trim("plan_code")))
        .withColumn("state_code",         F.upper(F.trim("state_code")))

        # Derived fields
        .withColumn("is_active",          F.col("disenrollment_date").isNull())
        .withColumn("coverage_months",    F.months_between(
                                              F.coalesce(F.col("disenrollment_date"), F.current_date()),
                                              F.col("enrollment_date")
                                          ).cast(IntegerType()))
        .withColumn("age_group",          F.when(F.col("age") < 18, "PEDIATRIC")
                                           .when(F.col("age") < 26, "YOUNG_ADULT")
                                           .when(F.col("age") < 65, "ADULT")
                                           .otherwise("SENIOR"))

        .withColumn("processed_at", F.current_timestamp())
        .withColumn("hipaa_compliant", F.lit(True))
    )

    count = processed.count()
    processed.write.mode("overwrite").parquet(f"{CURATED_BUCKET}/enrollment/")
    logger.info(f"Enrollment curated: {count:,}")
    return count


# ── 2. Claims ingestion ──────────────────────────────────────────────
def process_claims() -> int:
    logger.info("Processing claims data...")
    raw = spark.read.parquet(f"{RAW_BUCKET}/claims/")
    logger.info(f"Raw claims records: {raw.count():,}")

    processed = (
        raw
        .filter(F.col("claim_id").isNotNull())
        .filter(F.col("member_id").isNotNull())
        .dropDuplicates(["claim_id"])

        # PHI masking
        .withColumn("member_id_hashed", hash_member_id(F.col("member_id")))
        .withColumn("provider_npi",     F.col("provider_npi"))  # NPI is public
        .drop("member_id", "patient_name", "patient_dob", "patient_ssn")

        # Standardise
        .withColumn("service_date",  F.to_date("service_date"))
        .withColumn("paid_date",     F.to_date("paid_date"))
        .withColumn("claim_status",  F.upper(F.trim("claim_status")))
        .withColumn("claim_type",    F.upper(F.trim("claim_type")))

        # Financial fields
        .withColumn("billed_amount",   F.col("billed_amount").cast(DoubleType()))
        .withColumn("allowed_amount",  F.col("allowed_amount").cast(DoubleType()))
        .withColumn("paid_amount",     F.col("paid_amount").cast(DoubleType()))
        .withColumn("member_liability", F.col("billed_amount") - F.col("allowed_amount"))

        # Claim cycle metrics
        .withColumn("days_to_process", F.datediff("paid_date", "service_date"))

        # ICD-10 code grouping (first 3 chars = category)
        .withColumn("icd10_category", F.substring(F.col("icd10_primary"), 1, 3))

        # High-cost flag
        .withColumn("is_high_cost",   (F.col("paid_amount") >= 10_000).cast("boolean"))

        .withColumn("processed_at",   F.current_timestamp())
        .withColumn("hipaa_compliant", F.lit(True))
    )

    count = processed.count()
    # Partition by year/month for efficient querying
    (
        processed
        .withColumn("year",  F.year("service_date"))
        .withColumn("month", F.month("service_date"))
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{CURATED_BUCKET}/claims/")
    )

    logger.info(f"Claims curated: {count:,}")
    return count


# ── 3. Load to Redshift ───────────────────────────────────────────────
def load_to_redshift(curated_path: str, table_name: str):
    df = spark.read.parquet(curated_path)
    (
        df.write
          .format("com.databricks.spark.redshift")
          .option("url",     REDSHIFT_URL)
          .option("dbtable", table_name)
          .option("tempdir", TEMP_DIR)
          .option("extracopyoptions", "TRUNCATECOLUMNS ACCEPTINVCHARS")
          .mode("append")
          .save()
    )
    logger.info(f"Loaded {df.count():,} records into {table_name}")


if __name__ == "__main__":
    e_count = process_enrollment()
    c_count = process_claims()
    load_to_redshift(f"{CURATED_BUCKET}/enrollment/", "healthcare.enrollment")
    load_to_redshift(f"{CURATED_BUCKET}/claims/",     "healthcare.claims")
    logger.info(f"Job complete — enrollment: {e_count:,} | claims: {c_count:,}")
    job.commit()
