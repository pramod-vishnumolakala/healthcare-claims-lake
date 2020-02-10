# Healthcare Claims Data Lake

HIPAA-compliant data lake on AWS for healthcare enrollment and claims processing, serving **2M+ members** and **5M+ patient records**. Improved pipeline efficiency by **32%**, reduced claims backlog by **3 days**, and achieved **100% HIPAA compliance**.

## Architecture

```
Source Systems
┌────────────┐  ┌────────────┐  ┌─────────────┐
│ Enrollment │  │  Claims    │  │  Provider   │
│   System   │  │ Adjudication│  │  Directory  │
└─────┬──────┘  └─────┬──────┘  └──────┬──────┘
      │                │                │
      └────────────────┼────────────────┘
                       │
          ┌────────────▼─────────────┐
          │   AWS Glue / Step        │
          │   Functions              │  ← Orchestration
          └────────────┬─────────────┘
                       │
          ┌────────────▼─────────────┐
          │       Amazon S3          │
          │  raw/ → staging/ → curated/ │  ← Data lake zones
          └────────────┬─────────────┘
                       │
               ┌───────┴────────┐
               │                │
     ┌─────────▼──────┐  ┌──────▼──────────┐
     │  AWS Glue ETL  │  │  AWS Kinesis    │
     │  (batch claims)│  │  (real-time)    │
     └─────────┬──────┘  └──────┬──────────┘
               │                │
               └───────┬────────┘
                       │
          ┌────────────▼─────────────┐
          │    Amazon Redshift       │  ← Analytical warehouse
          │  (claims star schema)    │
          └────────────┬─────────────┘
                       │
          ┌────────────▼─────────────┐
          │   Power BI / QuickSight  │  ← 400+ analysts
          └──────────────────────────┘
```

## Key Features

- **HIPAA-compliant** - KMS encryption, field-level masking, CloudTrail audit logging
- **2M+ member** enrollment and claims processing
- **5M+ patient records** with S3 lifecycle archival (22% storage cost reduction)
- **Real-time claims** ingestion via Kinesis + Lambda (26% faster fraud detection)
- **100+ automated workflows** - 40% reduction in manual processing errors
- **5,000+ datasets** catalogued in AWS Glue Catalog (+58% discovery efficiency)
- **41% improvement** in Redshift query performance for 400+ analysts

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion (batch) | AWS Glue, AWS DMS |
| Ingestion (streaming) | AWS Kinesis, AWS Lambda |
| Storage | Amazon S3 (tiered), AWS Glue Catalog |
| Processing | PySpark, AWS Glue DataBrew |
| Warehouse | Amazon Redshift |
| Orchestration | AWS Step Functions, Apache Airflow |
| Security | AWS KMS, CloudTrail, IAM, Secrets Manager |
| Quality | Great Expectations, AWS Glue DataBrew |
| BI | Amazon QuickSight, Power BI |
| IaC | Terraform |

## HIPAA Compliance Controls

| Control | Implementation |
|---|---|
| Encryption at rest | AWS KMS CMK on all S3, Redshift, DynamoDB |
| Encryption in transit | TLS 1.2+ enforced on all endpoints |
| Field masking | Glue DataBrew masking for PII fields |
| Audit logging | CloudTrail enabled on all API calls |
| Access control | IAM least-privilege, no public S3 buckets |
| Data retention | S3 lifecycle: 7yr active, Glacier after 2yr |

## Author

**Pramod Vishnumolakala** - Senior Data Engineer  
[pramodvishnumolakala@gmail.com](mailto:pramodvishnumolakala@gmail.com) · [LinkedIn](https://linkedin.com/in/pramod-vishnumolakala)
