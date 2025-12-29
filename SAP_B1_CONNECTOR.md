# SAP Business One Connector for Comboi

## Overview

The SAP Business One (SAP B1) connector enables GDPR-compliant extraction of SAP B1 data from pre-extracted Parquet files stored in object storage (ADLS Gen2 or S3). This connector is designed for services companies that need to implement the **16 core tables** required for financial, operational, and customer analytics.

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  SAP Business   │     │  Object Storage  │     │     Comboi          │
│      One        │────▶│   (ADLS/S3)      │────▶│   Bronze Layer      │
│                 │     │                  │     │                     │
│  Data Services  │     │  Raw Parquet     │     │  GDPR-Compliant     │
│  File Export    │     │  Partitioned     │     │  Pseudonymized      │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
```

## Key Features

✅ **File-Based Extraction**: Reads from pre-extracted Parquet files instead of connecting directly to SAP B1
✅ **GDPR Compliance**: Automatic pseudonymization and PII exclusion
✅ **Incremental Loading**: Supports delta loading via UpdateDate/RefDate columns
✅ **Partitioned Data**: Handles both partitioned (dt=YYYY-MM-DD) and non-partitioned files
✅ **Checkpoint Management**: Tracks last successful extraction to avoid duplicates
✅ **16 Core Tables**: Pre-configured for essential services company KPIs

## Supported Tables

### Tier 1: Absolutely Essential (P0)

| Table | Description | Strategy | Frequency | GDPR |
|-------|-------------|----------|-----------|------|
| **OINV** | AR Invoice Headers | Incremental | Hourly | ❌ |
| **INV1** | AR Invoice Lines | Incremental | Hourly | ❌ |
| **OCRD** | Business Partners | Incremental | Daily | ✅ |
| **OPRJ** | Projects | Incremental | Daily | ✅ |
| **OITM** | Items/Services | Full Snapshot | Weekly | ❌ |
| **OACT** | Chart of Accounts | Full Snapshot | Weekly | ❌ |
| **ORCT** | Incoming Payments | Incremental | Hourly | ❌ |
| **JDT1** | Journal Lines | Incremental | Daily | ❌ |

### Tier 2: Important for Full Picture (P1)

| Table | Description | Strategy | Frequency | GDPR |
|-------|-------------|----------|-----------|------|
| **OPCH** | AP Invoice Headers | Incremental | Hourly | ❌ |
| **PCH1** | AP Invoice Lines | Incremental | Hourly | ❌ |
| **OVPM** | Outgoing Payments | Incremental | Hourly | ❌ |
| **OCRG** | BP Groups | Full Snapshot | Weekly | ❌ |
| **OITB** | Item Groups | Full Snapshot | Weekly | ❌ |
| **OOCR** | Cost Centers | Full Snapshot | Weekly | ❌ |
| **OSLP** | Sales Employees | Incremental | Daily | ✅ |
| **RCT2** | Payment Invoices | Incremental | Hourly | ❌ |

## GDPR Compliance

### Automatic Data Protection

The connector applies GDPR rules automatically when `apply_gdpr: true` (default):

#### REMOVED Columns (PII)
- Contact information: Phone, Email, Fax, Mobile
- Physical addresses: Address, ZipCode, City, Country
- Tax IDs: LicTradNum, VatIdUnCmp

#### PSEUDONYMIZED Columns (SHA-256 Hash)
- `OCRD.CardName` → `CardName_Hash`
- `OPRJ.PrjName` → `PrjName_Hash`
- `OSLP.SlpName` → `SlpName_Hash`

#### RETAINED As-Is
- All transaction amounts and dates
- All reference codes (CardCode, PrjCode, etc.)
- All financial metrics

### GDPR Configuration

Custom GDPR rules can be defined in [src/comboi/gdpr.py](src/comboi/gdpr.py):

```python
from comboi.gdpr import SAP_B1_GDPR_RULES

# Add custom table rules
SAP_B1_GDPR_RULES["CUSTOM_TABLE"] = {
    "exclude_columns": ["SensitiveField1", "SensitiveField2"],
    "pseudonymize": ["CustomerName"],
    "retain": ["ID", "Amount", "Date"],
    "hash_algorithm": "sha256"
}
```

## Configuration

### Prerequisites

1. **SAP B1 Data Export**: Data must be pre-extracted to object storage in Parquet format
2. **Storage Structure**:
   ```
   /raw/sap_b1/
   ├── ocrd/
   │   └── dt=2024-01-15/
   │       └── data.parquet
   ├── oinv/
   │   └── dt=2024-01-15/
   │       └── data.parquet
   └── ...
   ```
3. **Azure Key Vault**: For secure credential management
4. **ADLS Containers**: Bronze, Silver, Gold containers created

### Example Configuration

See [configs/sap_b1_example.yml](configs/sap_b1_example.yml) for a complete configuration example.

```yaml
sources:
  - name: sap_b1_services
    type: sap_b1
    connection:
      source_storage_path: "abfss://raw@yourstorage.dfs.core.windows.net/sap_b1"
      apply_gdpr: true
    checkpoint_key: "sap_b1_services"
    tables:
      # Incremental table
      - name: OINV
        query: "SELECT * FROM OINV"
        incremental_column: "UpdateDate"
        source_file_pattern: "**/*.parquet"

      # Full snapshot table
      - name: OITM
        query: "SELECT * FROM OITM"
        source_file_pattern: "*.parquet"
```

### Configuration Parameters

#### Connection
- `source_storage_path`: Base path to SAP B1 extracts (supports ADLS `abfss://` or S3 `s3://`)
- `apply_gdpr`: Enable/disable GDPR processing (default: `true`)
- `credential`: Optional Azure storage credential

#### Table Configuration
- `name`: SAP B1 table name (e.g., `OINV`, `OCRD`)
- `incremental_column`: Column for delta loading (`UpdateDate`, `RefDate`)
- `source_file_pattern`: File pattern (e.g., `**/*.parquet` for partitioned, `*.parquet` for flat)
- `checkpoint_key`: Optional override for checkpoint tracking

## Usage

### 1. Plan Execution

```bash
comboi plan --config configs/sap_b1_example.yml
```

### 2. Run Pipeline

```bash
# Run all stages
comboi run all --config configs/sap_b1_example.yml

# Run Bronze only
comboi run bronze --config configs/sap_b1_example.yml
```

### 3. Verify GDPR Compliance

```bash
# Check OCRD table for pseudonymization
duckdb -c "SELECT CardCode, CardName_Hash FROM read_parquet('data/bronze/sap_b1_services/OCRD.parquet') LIMIT 5"

# Verify no PII columns exist
duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/bronze/sap_b1_services/OCRD.parquet')"
# Should NOT contain: Phone1, Phone2, Email, Address, etc.
```

## KPI Mapping

### Revenue KPIs

| KPI | Formula | Tables |
|-----|---------|--------|
| **Total Revenue** | `SUM(OINV.DocTotal)` | OINV |
| **Revenue by Service** | `SUM(INV1.LineTotal) GROUP BY ItemCode` | INV1, OITM |
| **Revenue by Customer** | `SUM(OINV.DocTotal) GROUP BY CardCode` | OINV, OCRD |
| **Revenue by Project** | `SUM(INV1.LineTotal) GROUP BY Project` | INV1, OPRJ |

### Profitability KPIs

| KPI | Formula | Tables |
|-----|---------|--------|
| **Gross Margin** | `(Revenue - Costs) / Revenue` | OINV, OPCH |
| **Project Profitability** | `Revenue - Costs per Project` | INV1, PCH1, OPRJ |
| **Service Margin** | `By ItemCode` | INV1, PCH1, OITM |

### Cash Flow KPIs

| KPI | Formula | Tables |
|-----|---------|--------|
| **DSO** | `(AR / Revenue) × Days` | OINV, ORCT |
| **DPO** | `(AP / COGS) × Days` | OPCH, OVPM |
| **Cash Collected** | `SUM(ORCT.DocTotal)` | ORCT |

### Operational KPIs

| KPI | Formula | Tables |
|-----|---------|--------|
| **Active Projects** | `COUNT(OPRJ WHERE Active='Y')` | OPRJ |
| **Revenue per Employee** | `Revenue / Headcount` | OINV, OSLP |
| **Customer Concentration** | `Top 10 % of Revenue` | OINV, OCRD |

## Transformations

### Silver Layer Example

Create a contract-driven transformation for GDPR-compliant customer data:

```yaml
# transformations/contracts/sap_b1_customers.yml
version: "1.0.0"
dataset: "sap_b1_customers"
stage: "silver"
owner: "data-governance-team"
description: "GDPR-compliant customer master from SAP B1"

schema:
  columns:
    - name: CardCode
      type: VARCHAR
      nullable: false
      constraints:
        - unique: true
    - name: CardName_Hash
      type: VARCHAR
      description: "SHA-256 hash of customer name (GDPR)"
    - name: Balance
      type: DECIMAL
      nullable: true

quality_rules:
  - name: "no_duplicate_customers"
    type: "uniqueness"
    column: "CardCode"
    severity: "error"

privacy:
  classification: "PII"
  gdpr_applicable: true
  data_subject_rights: ["access", "erasure"]
```

### Gold Layer Example

```sql
-- transformations/sql/sap_b1_revenue_metrics.sql
-- Revenue metrics from SAP B1 invoices

SELECT
    DATE_TRUNC('month', i.DocDate) AS revenue_month,
    c.CardCode,
    c.CardName_Hash AS customer_hash,
    COUNT(DISTINCT i.DocNum) AS invoice_count,
    SUM(i.DocTotal) AS total_revenue,
    AVG(i.DocTotal) AS avg_invoice_value,
    SUM(CASE WHEN i.DocStatus = 'C' THEN i.DocTotal ELSE 0 END) AS closed_revenue,
    CURRENT_TIMESTAMP AS processed_at
FROM bronze_oinv i
LEFT JOIN bronze_ocrd c ON i.CardCode = c.CardCode
WHERE i.DocDate >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1, 2, 3
```

## Deployment

### Azure Functions

```bash
# 1. Deploy infrastructure
cd terraform
terraform apply -var="prefix=myproject" -var="environment=prod"

# 2. Embed comboi package
python tools/embed_comboi.py

# 3. Copy configurations
cp -r configs azure_functions/
cp -r transformations azure_functions/

# 4. Deploy to Azure
cd azure_functions
func azure functionapp publish myproject-comboi-prod
```

### Scheduled Extraction

Configure timer triggers in Azure Functions:

```json
// azure_functions/driver/function.json
{
  "schedule": "0 0 * * * *"  // Hourly for transaction tables
}
```

## Troubleshooting

### Issue: File Not Found

```bash
# Check source path exists
az storage fs file list \
  --file-system raw \
  --path sap_b1/ocrd \
  --account-name yourstorage
```

### Issue: GDPR Rules Not Applied

```python
# Verify GDPR configuration
from comboi.gdpr import get_sap_b1_table_config
config = get_sap_b1_table_config("OCRD")
print(config)
```

### Issue: Incremental Loading Not Working

```bash
# Check checkpoint file
cat checkpoints/sap_b1_bronze.json

# Reset checkpoint to force full reload
comboi reset-checkpoint --stage bronze --source sap_b1_services
```

## Cost Estimation

### Storage (ADLS Gen2)

| Layer | Tables | Monthly Size | Cost (Hot Tier) |
|-------|--------|--------------|-----------------|
| Bronze | 16 | ~500 MB | ~$0.01 |
| Silver | 16 | ~300 MB | ~$0.006 |
| Gold | 10 | ~100 MB | ~$0.002 |

### Databricks Compute

| Workload | Cluster | Hours/Day | Monthly DBU |
|----------|---------|-----------|-------------|
| Ingestion | Jobs Lite | 0.5 | ~15 DBU |
| Transformation | Jobs | 1.0 | ~30 DBU |

**Estimated Total**: $150-300/month for typical services company

## Expansion Path

```
Phase 1 (Now)          Phase 2 (+3mo)         Phase 3 (+6mo)
─────────────────────────────────────────────────────────────
16 core tables         + Time Tracking        + Quotes/Pipeline
Bronze + GDPR          + Multi-currency       + CRM Integration
                       + Aging Details        + Custom UDFs
```

### Additional Tables for Phase 2

- `OTSK`/`TSK1`: Time tracking for billable hours
- `ORTT`: Exchange rates for multi-currency
- `INV6`/`PCH6`: Payment terms and aging detail

### Additional Tables for Phase 3

- `OQUT`/`QUT1`: Quotations and opportunities
- `OCPR`: Contact persons (with GDPR)
- `ATC1`: Document attachments
- Custom User-Defined Fields (UDFs)

## References

- [SAP Business One Data Dictionary](https://help.sap.com/docs/SAP_BUSINESS_ONE)
- [Comboi Documentation](README.md)
- [GDPR Compliance Guide](https://gdpr.eu/)
- [Data Contracts Specification](transformations/contracts/README.md)

## Support

For issues or questions:
1. Check the [troubleshooting section](#troubleshooting)
2. Review logs at `logs/sap_b1_pipeline.log`
3. Open an issue on GitHub

---

**License**: See [LICENSE](LICENSE)
**Made in Berlin** | Contributions welcome!
