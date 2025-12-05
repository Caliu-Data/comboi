# Comboi - Serverless DuckDB ELT System in Azure

[![Repository](https://img.shields.io/badge/GitHub-Caliu--Data%2Fcomboi-blue)](https://github.com/Caliu-Data/comboi)

**Comboi** is a Python-based serverless ELT (Extract, Load, Transform) system on top of DuckDB and Azure Data Lake Storage. It runs using Azure Functions and is designed to be configuration-driven and easy to operate.

## 🚀 Key Features

- **Bronze Landing**: Incremental extracts from Azure SQL Database and PostgreSQL using DuckDB, persisted as Parquet in Azure Data Lake Storage (ADLS) with checkpointing to avoid full reloads
- **Silver Refinement**: Data cleansing using **Data Contracts** with quality checks and Splink-based deduplication, materialized back to ADLS
- **Gold Metrics**: Aggregations and business-ready metrics generated in DuckDB and delivered to ADLS
- **Contract-Driven SQL**: DuckDB SQL transformations automatically generated from data contracts
- **Data Contracts**: Declarative schema, quality rules, and SLA validation through YAML contracts
- **Serverless Execution**: Azure Functions with timer/queue triggers for automated pipeline orchestration
- **Secret Management**: Azure Key Vault integration for secure credential management
- **Structured Logging**: JSON logs using structlog for better observability
- **Infrastructure-as-Code**: Complete Azure infrastructure provisioned via Terraform
- **Contract Generator**: Automated contract and SQL generation from source schemas with build.py

## 📁 Repository Structure

```
.
├── configs/                    # Pipeline configuration files
│   ├── initial.yml            # Infrastructure and source configuration
│   ├── transformations.yml     # Transformation and quality check definitions
│   └── default.yml            # Example configuration
├── transformations/            # All transformation assets
│   ├── sql/                   # DuckDB SQL transformations
│   │   └── README.md          # SQL transformation guidelines
│   └── contracts/             # Data contract definitions (YAML)
│       ├── gdpr_customers.yml  # Sample GDPR-compliant contract
│       └── README.md          # Contract documentation
├── src/comboi/                # Main application code
│   ├── connectors/            # Source connectors (Azure SQL, PostgreSQL)
│   ├── contracts/             # Data contract validation
│   ├── io/                    # ADLS client
│   ├── pipeline/              # Pipeline orchestration
│   ├── cli.py                 # CLI entry point
│   └── runner.py              # Driver factory
├── azure_functions/           # Azure Functions entrypoints
│   ├── driver/                # Timer-triggered scheduler
│   ├── executor/              # Queue-triggered executor
│   └── shared_packages/       # Vendored comboi package
├── terraform/                 # Infrastructure as Code
├── tools/                     # Utility scripts
│   ├── embed_comboi.py        # Copy comboi to Azure Functions
│   └── scaffold_transformation.py  # Scaffold new transformations
└── build.py                   # Contract generator from source schemas
```

## 🏗️ Architecture

### Medallion Data Architecture

**Bronze (Landing Zone)**
- Extracts raw data from source systems (Azure SQL, PostgreSQL)
- Uses DuckDB extensions (`odbc`, `postgres_scanner`) for direct connection
- Incremental loads with checkpointing to avoid full reloads
- Persists as Parquet files in ADLS

**Silver (Curation Zone)**
- Reads Bronze Parquet files via DuckDB
- Applies contract-driven DuckDB SQL transformations
- Validates data using **Data Contracts** (schema, quality rules, SLAs)
- Deduplicates using Splink
- Materializes cleaned data back to ADLS

**Gold (Serving Zone)**
- Composes Silver datasets into analytical models using DuckDB SQL
- Generates business metrics and aggregations
- Exports as Parquet to ADLS for BI and downstream consumption

## 🚦 Quick Start

### Prerequisites

- **Python 3.9+** (for local development)
- **uv** (fast Python package installer - [install from astral.sh](https://github.com/astral-sh/uv))
- **Terraform >= 1.5** (for infrastructure deployment)
- **Azure CLI** (logged in with appropriate permissions)
- **Azure Functions Core Tools** (optional, for local testing)
- Access to source databases and Azure resources

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd comboi

# Install dependencies
uv pip install -e .
```

### Usage Flow

Comboi follows a three-step workflow:

**1. Configure the Source**
- Define your data sources in `configs/initial.yml`
- Configure connection strings, tables, and incremental load settings

**2. Create Plan**
The planning phase extracts metadata, generates pipelines, queues transformations, and prepares execution:
```bash
comboi plan --config configs/my-env.yml
```

This command will:
- Extract metadata from your configured sources
- Generate a DAG of transformation dependencies
- Queue transformations based on dependencies
- Display the execution plan

**3. Execute**
Run the planned pipeline:
```bash
comboi run all --config configs/my-env.yml
```

See the [End-to-End Guide](#-end-to-end-guide) below for detailed step-by-step instructions.

## 🚀 Deployment

### 1. Deploy Infrastructure with Terraform

```bash
cd terraform
terraform init
terraform apply -var="prefix=myproject" -var="environment=prod"
```

This provisions:
- Resource group
- Storage accounts (Functions + ADLS)
- Bronze/Silver/Gold Data Lake containers
- Azure Storage Queue (`comboi-tasks`)
- Key Vault with queue connection string secret
- Linux Consumption Function App with managed identity

### 2. Configure Secrets in Key Vault

Add the following secrets to Key Vault:
- `queue-connection-string` (auto-created by Terraform)
- `adls-storage-key` (auto-created by Terraform)
- `azure-sql-password` (your database password)
- `postgres-password` (your database password)

### 3. Deploy Azure Functions

```bash
# Embed comboi package into Azure Functions
python tools/embed_comboi.py

# Copy transformations, contracts, and configs to Azure Functions
cp -r transformations azure_functions/
cp -r configs azure_functions/

# Deploy to Azure
cd azure_functions
func azure functionapp publish <function_app_name>
```

**Note**: The `transformations/` and `configs/` directories must be accessible to Azure Functions at runtime. Include them in the deployment package or use a mounted file share.

### 4. Verify Execution

- Check Function App → Monitor for timer trigger execution
- Review logs (structured JSON via structlog)
- Verify queue-triggered executions complete for all stages
- Check log files at configured `log_path` for detailed execution logs

## 📖 End-to-End Guide

This guide walks you through building a complete data pipeline from source to analytics-ready datasets.

### Step 1: Configure the Source

Create your environment configuration:

```bash
# Copy the template
cp configs/initial.yml configs/production.yml
```

Edit `configs/production.yml` to define your data sources:

```yaml
# Configure Key Vault for secrets
key_vault:
  vault_url: "https://my-keyvault.vault.azure.net/"

# Configure source databases
sources:
  - name: crm_database
    type: azure_sql
    connection:
      dsn: "Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver.database.windows.net;Database=crm;Uid=myuser;Pwd={{keyvault:azure-sql-password}};Encrypt=yes;"
    checkpoint_key: "crm_database"
    tables:
      - name: customers
        query: "SELECT customer_id, email, country, created_at, updated_at FROM dbo.customers"
        incremental_column: "updated_at"
      - name: orders
        query: "SELECT order_id, customer_id, amount, order_date FROM dbo.orders"
        incremental_column: "order_date"

# Configure data lake stages
stages:
  bronze:
    checkpoint_path: "checkpoints/bronze.json"
    local_path: "data/bronze"
    data_lake:
      account_name: "{{env:DATA_LAKE_ACCOUNT_NAME}}"
      file_system: "bronze"
      credential: "{{keyvault:adls-storage-key}}"
    remote_path_template: "{stage}/{source}/{table}.parquet"

  silver:
    local_path: "data/silver"
    data_lake:
      account_name: "{{env:DATA_LAKE_ACCOUNT_NAME}}"
      file_system: "silver"
      credential: "{{keyvault:adls-storage-key}}"
    transformations_path: "transformations/sql"
    contracts_path: "transformations/contracts"

  gold:
    local_path: "data/gold"
    data_lake:
      account_name: "{{env:DATA_LAKE_ACCOUNT_NAME}}"
      file_system: "gold"
      credential: "{{keyvault:adls-storage-key}}"
    transformations_path: "transformations/sql"
```

### Step 2: Generate Data Contracts from Source Metadata

Use `build.py` to automatically generate contracts from your source schemas:

```bash
# First, extract a sample of your source data to Bronze
comboi run bronze --config configs/production.yml

# Generate contracts from the Bronze data
python build.py \
  --source data/bronze/crm_database/customers.parquet \
  --output transformations/contracts/customers_clean.yml \
  --dataset customers_clean \
  --stage silver \
  --owner data-team \
  --description "Cleansed customer data with GDPR compliance"
```

This will create a contract file like:

```yaml
version: "1.0.0"
dataset: "customers_clean"
stage: "silver"
owner: "data-team"
description: "Cleansed customer data with GDPR compliance"

schema:
  columns:
    - name: customer_id
      type: VARCHAR
      nullable: false
      constraints:
        - unique: true
        - not_null: true
    - name: email
      type: VARCHAR
      nullable: true
    - name: country
      type: VARCHAR
      nullable: false
    # ... more columns

quality_rules:
  - name: "no_null_customer_ids"
    type: "not_null"
    column: "customer_id"
    severity: "error"
  - name: "minimum_rows"
    type: "volume"
    min_rows: 1
    severity: "error"

sla:
  freshness:
    max_age_hours: 24
  completeness:
    min_row_count: 1
```

**Customize the generated contract:**
- Add business-specific quality rules
- Define allowed values for categorical columns
- Set appropriate freshness and completeness thresholds
- Add GDPR privacy metadata if handling personal data

### Step 3: Generate SQL Transformation from Contract

Use `build.py` to automatically generate DuckDB SQL transformation from the contract:

```bash
# Generate SQL transformation from contract
python build.py \
  --generate-sql \
  --contract transformations/contracts/customers_clean.yml \
  --output transformations/sql/customers_clean.sql
```

This will create `transformations/sql/customers_clean.sql`:

```sql
-- Generated from contract: customers_clean.yml
-- Customer Data Cleansing Transformation
--
-- This transformation:
-- - Removes duplicate customer records
-- - Pseudonymizes PII for GDPR compliance
-- - Normalizes country codes
-- - Applies contract schema and constraints

SELECT
    customer_id,
    SHA2(email, 256) AS email_hash,  -- Pseudonymize email (GDPR)
    UPPER(SUBSTR(country, 1, 2)) AS country_code,  -- Normalize country code
    created_at,
    updated_at,
    CURRENT_TIMESTAMP AS processed_at
FROM bronze_customers
WHERE
    customer_id IS NOT NULL  -- Contract constraint: not_null
    AND customer_id != ''    -- Contract constraint: not empty
-- Remove duplicates based on primary key
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
```

**Customize the generated SQL** (optional):
- Add business-specific transformations
- Include complex validation logic
- Add computed columns
- Implement custom aggregations

### Step 4: Configure Transformations Pipeline

Create `configs/production-transformations.yml`:

```yaml
# Silver layer transformations (contract-driven SQL)
silver:
  - name: customers_clean
    type: sql
    inputs:
      - alias: bronze_customers
        stage: bronze
        source_path: "crm_database/customers.parquet"
    quality_checks:
      - contract:customers_clean

  - name: orders_clean
    type: sql
    inputs:
      - alias: bronze_orders
        stage: bronze
        source_path: "crm_database/orders.parquet"
    quality_checks:
      - contract:orders_clean

# Gold layer aggregations (DuckDB SQL)
gold:
  - name: customer_metrics
    type: sql
    inputs:
      - alias: customers
        stage: silver
        source_path: "refined/customers_clean.parquet"
      - alias: orders
        stage: silver
        source_path: "refined/orders_clean.parquet"
    quality_checks:
      - contract:customer_metrics
```

### Step 5: Create and Review the Execution Plan

Generate the execution plan to see what will be executed:

```bash
comboi plan --config configs/production.yml
```

**Output example:**
```
🔍 Planning pipeline execution...

📊 Metadata Extraction:
  ✓ Found 2 sources (crm_database)
  ✓ Found 2 tables (customers, orders)
  ✓ Checkpoint: Last run 2024-12-05 10:30:00

📋 Pipeline DAG:
  Stage: Bronze
    ├─ crm_database/customers (incremental: updated_at > '2024-12-05 10:30:00')
    └─ crm_database/orders (incremental: order_date > '2024-12-05 10:30:00')

  Stage: Silver
    ├─ customers_clean (depends on: bronze/customers)
    │   └─ Contract: customers_clean.yml
    └─ orders_clean (depends on: bronze/orders)
        └─ Contract: orders_clean.yml

  Stage: Gold
    └─ customer_metrics (depends on: silver/customers_clean, silver/orders_clean)
        └─ Contract: customer_metrics.yml

⚡ Execution Queue:
  1. Extract bronze/crm_database/customers
  2. Extract bronze/crm_database/orders
  3. Transform silver/customers_clean
  4. Transform silver/orders_clean
  5. Transform gold/customer_metrics

📦 Estimated Data:
  Bronze: 2 tables, ~50MB
  Silver: 2 transformations
  Gold: 1 aggregation
```

**Review the plan:**
- Verify all dependencies are correct
- Check that incremental loads will run with correct filters
- Ensure quality checks are in place
- Confirm the execution order makes sense

### Step 6: Execute the Pipeline

Run the full pipeline:

```bash
# Run all stages
comboi run all --config configs/production.yml

# Or run stage by stage
comboi run bronze --config configs/production.yml
comboi run silver --config configs/production.yml
comboi run gold --config configs/production.yml
```

**Monitor execution:**

```
🚀 Starting pipeline execution...

📥 Stage: Bronze
  ✓ Extracting crm_database/customers (5,234 rows, incremental)
  ✓ Writing to data/bronze/crm_database/customers.parquet
  ✓ Extracting crm_database/orders (12,456 rows, incremental)
  ✓ Writing to data/bronze/crm_database/orders.parquet
  Bronze completed in 45s

🔧 Stage: Silver
  ⚙ Transforming customers_clean...
    ✓ Loaded 5,234 rows from Bronze
    ✓ Removed 12 duplicates
    ✓ Pseudonymized 5,222 emails
    ✓ Output: 5,222 rows
  ✅ Running quality checks (contract:customers_clean)...
    ✓ no_null_customer_ids: PASS
    ✓ no_duplicate_customers: PASS
    ✓ minimum_rows: PASS (5,222 rows)
  ✓ Writing to data/silver/refined/customers_clean.parquet

  ⚙ Transforming orders_clean...
    ✓ Loaded 12,456 rows from Bronze
    ✓ Output: 12,456 rows
  ✅ Running quality checks (contract:orders_clean)...
    ✓ no_null_order_ids: PASS
    ✓ valid_amounts: PASS
    ✓ minimum_rows: PASS (12,456 rows)
  ✓ Writing to data/silver/refined/orders_clean.parquet
  Silver completed in 23s

📊 Stage: Gold
  ⚙ Transforming customer_metrics...
    ✓ Loaded customers: 5,222 rows
    ✓ Loaded orders: 12,456 rows
    ✓ Aggregated: 5,222 customer metrics
  ✅ Running quality checks (contract:customer_metrics)...
    ✓ all_customers_have_metrics: PASS
  ✓ Writing to data/gold/metrics/customer_metrics.parquet
  Gold completed in 12s

✅ Pipeline completed successfully in 1m 20s
📊 Metrics written to logs/metrics.json
📝 Detailed logs at logs/pipeline.log
```

### Step 7: Verify Results

Check the output data:

```bash
# Inspect Silver data
duckdb -c "SELECT * FROM read_parquet('data/silver/refined/customers_clean.parquet') LIMIT 5"

# Check Gold metrics
duckdb -c "SELECT * FROM read_parquet('data/gold/metrics/customer_metrics.parquet') LIMIT 5"

# Review logs
cat logs/pipeline.log | jq '.'
```

### Step 8: Schedule Automated Runs

Deploy to Azure Functions for scheduled execution:

```bash
# Deploy infrastructure (one-time)
cd terraform
terraform apply -var="prefix=myproject" -var="environment=prod"

# Deploy code
cd ..
python tools/embed_comboi.py
cp -r transformations azure_functions/
cp -r configs azure_functions/
cd azure_functions
func azure functionapp publish myproject-comboi-prod
```

The pipeline will now run automatically on the configured schedule (default: hourly).

### Troubleshooting

**Contract validation failures:**
```bash
# Test a specific contract
comboi validate-contract --contract transformations/contracts/customers_clean.yml --data data/silver/refined/customers_clean.parquet
```

**Transformation errors:**
```bash
# Run with verbose logging
comboi run silver --config configs/production.yml --log-level DEBUG
```

**Incremental load issues:**
```bash
# Reset checkpoint to force full reload
comboi reset-checkpoint --stage bronze --source crm_database
```

## 📝 Data Contracts

Data contracts provide declarative validation for your datasets. See [transformations/contracts/README.md](transformations/contracts/README.md) for complete documentation.

**Quick Example:**
```yaml
# transformations/contracts/gdpr_customers.yml
version: "1.0.0"
dataset: "gdpr_customers"
stage: "silver"
owner: "data-governance-team"
description: "GDPR-compliant customer data with privacy controls"

schema:
  columns:
    - name: customer_id
      type: VARCHAR
      nullable: false
      constraints:
        - unique: true
        - not_null: true
    - name: email_hash
      type: VARCHAR
      nullable: true
      description: "SHA-256 hash of email (pseudonymized)"
    - name: consent_marketing
      type: BOOLEAN
      nullable: false
      description: "Marketing consent flag (GDPR Article 6)"

quality_rules:
  - name: "no_duplicate_customers"
    type: "uniqueness"
    column: "customer_id"
    severity: "error"
  - name: "consent_timestamp_when_given"
    type: "custom_sql"
    query: "SELECT COUNT(*) FROM {dataset} WHERE consent_marketing = true AND consent_timestamp IS NULL"
    expected: 0
    severity: "error"

sla:
  freshness:
    max_age_hours: 24
  data_retention:
    max_age_days: 2555  # 7 years (GDPR)

privacy:
  classification: "PII"
  gdpr_applicable: true
  data_subject_rights: ["access", "rectification", "erasure"]
```

Reference contracts in `configs/transformations.yml`:
```yaml
silver:
  - name: gdpr_customers
    quality_checks:
      - contract:gdpr_customers  # References transformations/contracts/gdpr_customers.yml
```

## 🔧 Contract Generator

Use `build.py` to automatically generate data contracts from your source schemas:

```bash
# Generate a contract from a DuckDB-accessible source
python build.py --source "path/to/data.parquet" --output transformations/contracts/my_dataset.yml

# Generate from a database table
python build.py --connection "azure_sql_connection_string" --table "dbo.transactions" --output transformations/contracts/transactions.yml
```

The generator will:
- Introspect the source schema
- Generate column definitions with appropriate types
- Add basic quality rules (not_null, uniqueness for primary keys)
- Create a ready-to-use contract YAML file

## 🔧 Operational Components

- **Driver** (`pipeline/driver.py`): Orchestrates pipeline execution, builds task map, computes execution order
- **Executor** (`pipeline/executor.py`): Runs stages sequentially with progress tracking
- **Monitor** (`pipeline/monitoring.py`): Structured logging with structlog, metrics persistence
- **Queue** (`pipeline/queue.py`): Azure Storage Queue operations for stage chaining
- **Azure Functions**: Timer-triggered scheduler and queue-triggered executor

## 🛠️ Extending the System

### Quick Start with Scaffolding Tool

Use the scaffolding tool to quickly create new transformations:

```bash
# Create a SQL transformation with data contract
python tools/scaffold_transformation.py \
  --name my_transformation \
  --type sql \
  --stage silver \
  --contract

# Create just a data contract
python tools/scaffold_transformation.py \
  --name my_dataset \
  --contract-only \
  --stage silver
```

The tool automatically creates:
- ✅ Transformation file (SQL) with template code
- ✅ Data contract YAML (optional)
- ✅ Configuration snippet for `transformations.yml`

### Add New Sources

1. Create a connector class in `src/comboi/connectors/`
2. Reference it in `configs/initial.yml` under `sources`

### Add Transformations

**Using Contract-Driven SQL:**
1. Create a data contract in `transformations/contracts/` defining the desired output schema
2. Generate SQL transformation from contract: `python build.py --generate-sql --contract <contract.yml> --output <output.sql>`
3. Optionally customize the generated SQL for complex business logic
4. Add to `configs/transformations.yml` with `type: sql`
5. See `transformations/sql/README.md` for detailed guidance

### Add Data Contracts

1. Create a YAML file in `transformations/contracts/` defining schema, quality rules, and SLAs
2. Use `build.py` to generate contracts from source schemas automatically
3. Reference using `contract:contract_name` in `quality_checks` within `configs/transformations.yml`
4. See `transformations/contracts/README.md` for detailed contract format

### Add Quality Checks

All quality checks are defined in data contracts. See `transformations/contracts/README.md` for available quality rule types:
- `uniqueness` - Validates unique column values
- `not_null` - Ensures no NULL values
- `volume` - Validates minimum row counts
- `custom_sql` - Custom SQL validation queries

## 🐛 Troubleshooting

### Missing DuckDB Extensions
- Ensure DuckDB 0.10+ is installed
- Extensions (`odbc`, `postgres_scanner`) auto-install but require network access

### Transformation Errors

**SQL transformations:**
- Verify SQL files exist in `transformations/sql/` directory
- Check SQL syntax using: `duckdb -c ".read transformations/sql/your_file.sql"`
- Ensure transformation names in config match SQL file names (without `.sql`)
- Verify input aliases match those defined in the configuration

### Data Contract Validation Failures
- Verify contract YAML files exist in `transformations/contracts/` directory
- Check contract name matches file name (without `.yml`)
- Ensure `contracts_path` is configured in `configs/initial.yml`
- Review validation error messages in logs

### Quality Check Failures
- Check contract YAML syntax and validation rules
- Validate contract using: `python build.py --validate-contract --contract <contract.yml>`
- Review error messages in structured logs
- Test quality rules in DuckDB directly to debug failures

### ADLS Authentication Issues
- Provide valid credential in configuration
- Or export Azure identity context: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`

### Key Vault Access Issues
- Ensure identity has `get` and `list` secret permissions
- Verify secret names exist in Key Vault
- Check Key Vault URL in configuration

### Azure Functions Issues
- Validate app settings: `COMBOI_QUEUE_NAME`, `COMBOI_QUEUE_CONNECTION`, `COMBOI_TIMER_SCHEDULE`
- Ensure `transformations/` and `contracts/` directories are included in deployment
- Check Function App logs for detailed error messages

### Logging Issues
- Verify `log_path` directory is writable
- Check that logs are being written (structured JSON format)
- Review Function App logs in Azure Portal

## 📚 Additional Resources

- **Data Contracts**: See [transformations/contracts/README.md](transformations/contracts/README.md) for contract documentation
- **SQL Transformations**: See [transformations/sql/README.md](transformations/sql/README.md) for SQL transformation guidelines
- **DuckDB**: [https://duckdb.org/](https://duckdb.org/)
- **DuckDB SQL Reference**: [https://duckdb.org/docs/sql/introduction](https://duckdb.org/docs/sql/introduction)
- **Splink**: [https://github.com/moj-analytical-services/splink](https://github.com/moj-analytical-services/splink)

## 🗺️ Roadmap

- Multicloud support
- Industry-specific building blocks
- Streaming broker integration
- Change Data Capture (CDC)
- Delta Lake and Apache Iceberg support

---

**Made in Berlin** | Contributions welcome!
