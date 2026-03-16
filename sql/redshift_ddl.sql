-- ============================================================
-- Healthcare Claims Data Lake — Redshift DDL & Analytics
-- Author: Pramod Vishnumolakala
-- ============================================================

CREATE SCHEMA IF NOT EXISTS healthcare;
CREATE SCHEMA IF NOT EXISTS healthcare_analytics;


-- ── Enrollment table ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare.enrollment (
    member_id_hashed     VARCHAR(16)   NOT NULL ENCODE zstd,
    coverage_type        VARCHAR(30)            ENCODE bytedict,
    plan_code            VARCHAR(20)            ENCODE bytedict,
    state_code           CHAR(2)                ENCODE bytedict,
    enrollment_date      DATE                   ENCODE az64,
    disenrollment_date   DATE                   ENCODE az64,
    is_active            BOOLEAN                ENCODE raw,
    coverage_months      INT                    ENCODE az64,
    age                  SMALLINT               ENCODE az64,
    age_group            VARCHAR(15)            ENCODE bytedict,
    dob_year             CHAR(4)                ENCODE az64,
    zip_masked           CHAR(5)                ENCODE zstd,
    gender_code          CHAR(1)                ENCODE bytedict,
    hipaa_compliant      BOOLEAN DEFAULT TRUE   ENCODE raw,
    processed_at         TIMESTAMP              ENCODE az64
)
DISTSTYLE KEY
DISTKEY (member_id_hashed)
SORTKEY (enrollment_date);


-- ── Claims table ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare.claims (
    claim_id             VARCHAR(36)   NOT NULL ENCODE zstd,
    member_id_hashed     VARCHAR(16)   NOT NULL ENCODE zstd,
    provider_npi         VARCHAR(10)            ENCODE zstd,
    service_date         DATE                   ENCODE az64,
    paid_date            DATE                   ENCODE az64,
    claim_status         VARCHAR(20)            ENCODE bytedict,
    claim_type           VARCHAR(20)            ENCODE bytedict,
    icd10_category       CHAR(3)                ENCODE zstd,
    icd10_primary        VARCHAR(10)            ENCODE zstd,
    cpt_code             VARCHAR(10)            ENCODE zstd,
    billed_amount        DECIMAL(18,2)          ENCODE az64,
    allowed_amount       DECIMAL(18,2)          ENCODE az64,
    paid_amount          DECIMAL(18,2)          ENCODE az64,
    member_liability     DECIMAL(18,2)          ENCODE az64,
    days_to_process      INT                    ENCODE az64,
    is_high_cost         BOOLEAN                ENCODE raw,
    siu_flag             BOOLEAN DEFAULT FALSE  ENCODE raw,
    hipaa_compliant      BOOLEAN DEFAULT TRUE   ENCODE raw,
    processed_at         TIMESTAMP              ENCODE az64,
    year                 SMALLINT               ENCODE az64,
    month                SMALLINT               ENCODE az64
)
DISTSTYLE KEY
DISTKEY (member_id_hashed)
SORTKEY (service_date, member_id_hashed);


-- ── Provider dimension ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS healthcare.providers (
    provider_npi         VARCHAR(10)   NOT NULL ENCODE zstd,
    provider_name        VARCHAR(100)           ENCODE zstd,
    specialty_code       VARCHAR(20)            ENCODE bytedict,
    specialty_desc       VARCHAR(100)           ENCODE zstd,
    state_code           CHAR(2)                ENCODE bytedict,
    network_status       VARCHAR(20)            ENCODE bytedict,
    provider_type        VARCHAR(30)            ENCODE bytedict
)
DISTSTYLE ALL
SORTKEY (provider_npi);


-- ============================================================
-- ANALYTICAL QUERIES
-- ============================================================

-- 1. Claims processing efficiency — daily SLA dashboard (400+ analysts)
SELECT
    service_date,
    claim_type,
    COUNT(claim_id)                             AS total_claims,
    ROUND(AVG(days_to_process), 1)              AS avg_days_to_process,
    SUM(CASE WHEN days_to_process <= 30 THEN 1 ELSE 0 END) AS within_30_days,
    ROUND(SUM(CASE WHEN days_to_process <= 30 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) AS pct_within_sla,
    SUM(billed_amount)                          AS total_billed,
    SUM(paid_amount)                            AS total_paid,
    ROUND(SUM(paid_amount) / NULLIF(SUM(billed_amount), 0) * 100, 1) AS payment_rate_pct
FROM healthcare.claims
WHERE service_date >= DATEADD('month', -3, GETDATE())
GROUP BY service_date, claim_type
ORDER BY service_date DESC, total_claims DESC;


-- 2. High-cost claimants analysis — actuarial reporting
SELECT
    c.member_id_hashed,
    e.age_group,
    e.plan_code,
    e.state_code,
    COUNT(c.claim_id)                AS claim_count,
    SUM(c.paid_amount)               AS total_paid,
    AVG(c.paid_amount)               AS avg_paid_per_claim,
    MAX(c.paid_amount)               AS max_single_claim,
    SUM(CASE WHEN c.is_high_cost THEN 1 ELSE 0 END) AS high_cost_claims
FROM healthcare.claims c
JOIN healthcare.enrollment e ON c.member_id_hashed = e.member_id_hashed
WHERE c.service_date >= DATEADD('year', -1, GETDATE())
GROUP BY 1, 2, 3, 4
HAVING SUM(c.paid_amount) > 50000
ORDER BY total_paid DESC
LIMIT 200;


-- 3. ICD-10 diagnosis frequency and cost (claims analytics)
SELECT
    c.icd10_category,
    COUNT(DISTINCT c.member_id_hashed) AS unique_members,
    COUNT(c.claim_id)                  AS claim_count,
    SUM(c.paid_amount)                 AS total_paid,
    ROUND(AVG(c.paid_amount), 2)       AS avg_paid,
    ROUND(AVG(c.days_to_process), 1)   AS avg_days_to_process
FROM healthcare.claims c
WHERE c.service_date >= DATEADD('year', -1, GETDATE())
  AND c.claim_status = 'PAID'
GROUP BY c.icd10_category
ORDER BY total_paid DESC
LIMIT 50;


-- 4. Provider performance scorecard
SELECT
    p.provider_npi,
    p.provider_name,
    p.specialty_desc,
    p.state_code,
    COUNT(c.claim_id)               AS total_claims,
    SUM(c.paid_amount)              AS total_paid,
    ROUND(AVG(c.days_to_process),1) AS avg_process_days,
    SUM(CASE WHEN c.siu_flag THEN 1 ELSE 0 END) AS siu_referrals,
    ROUND(SUM(c.paid_amount) / NULLIF(SUM(c.billed_amount),0) * 100, 1) AS avg_reimbursement_pct
FROM healthcare.claims c
JOIN healthcare.providers p ON c.provider_npi = p.provider_npi
WHERE c.service_date >= DATEADD('year', -1, GETDATE())
GROUP BY 1, 2, 3, 4
HAVING COUNT(c.claim_id) >= 50
ORDER BY siu_referrals DESC, total_paid DESC;


-- 5. Enrollment trend — active members by plan and month
SELECT
    DATE_TRUNC('month', enrollment_date) AS enrollment_month,
    plan_code,
    coverage_type,
    COUNT(*)                              AS new_enrollments,
    SUM(COUNT(*)) OVER (
        PARTITION BY plan_code
        ORDER BY DATE_TRUNC('month', enrollment_date)
        ROWS UNBOUNDED PRECEDING
    )                                     AS cumulative_members
FROM healthcare.enrollment
WHERE enrollment_date >= DATEADD('year', -2, GETDATE())
GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- 6. Storage cost audit — S3 lifecycle tier tracking
SELECT
    year,
    month,
    COUNT(claim_id)         AS claims_count,
    SUM(billed_amount)      AS total_billed,
    ROUND(COUNT(claim_id) * 0.023 / 1000.0, 4) AS est_s3_standard_cost_usd,
    ROUND(COUNT(claim_id) * 0.004 / 1000.0, 4) AS est_s3_glacier_cost_usd
FROM healthcare.claims
GROUP BY year, month
ORDER BY year DESC, month DESC;
