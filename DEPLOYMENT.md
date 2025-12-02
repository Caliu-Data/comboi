# Comboi Deployment Guide

This guide provides step-by-step instructions for deploying Comboi to Azure.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [Configuration](#configuration)
4. [Infrastructure Deployment](#infrastructure-deployment)
5. [Application Deployment](#application-deployment)
6. [Post-Deployment Verification](#post-deployment-verification)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Tools

- **Python 3.9 or higher**
  ```bash
  python --version  # Should be >= 3.9
  ```

- **Azure CLI** - [Install guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
  ```bash
  az --version
  az login  # Authenticate to Azure
  ```

- **Terraform >= 1.5** - [Install guide](https://developer.hashicorp.com/terraform/install)
  ```bash
  terraform --version
  ```

- **Azure Functions Core Tools v4** - [Install guide](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local)
  ```bash
  func --version
  ```

### Azure Permissions

Ensure your Azure account has:
- Contributor access to the subscription
- User Access Administrator (for managed identity setup)
- Key Vault Administrator (for secret management)

---

## Local Development Setup

### 1. Clone and Install

```bash
# Clone the repository
git clone <repository-url>
cd serverless-duckdb

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On Linux/Mac:
source venv/bin/activate

# Install package with development dependencies
pip install -e ".[dev]"
```

### 2. Verify Installation

```bash
# Test the CLI
comboi --help

# Run code quality checks
black --check src/
ruff check src/
```

---

## Configuration

### 1. Create Configuration Files

```bash
# Copy templates
cp configs/initial.yml configs/production.yml
cp configs/transformations.yml configs/production-transformations.yml
```

### 2. Configure Sources (`configs/production.yml`)

Edit `configs/production.yml`:

```yaml
environment: production
key_vault_url: "https://your-keyvault.vault.azure.net/"

sources:
  - name: sales_azure
    type: azure_sql
    dsn: "Driver={ODBC Driver 17 for SQL Server};Server=your-server.database.windows.net;Database=your-db"
    password_secret: azure-sql-password  # Secret name in Key Vault
    tables:
      - name: orders
        query: "SELECT * FROM sales.orders"
        incremental_column: "updated_at"
      - name: customers
        query: "SELECT * FROM sales.customers"
        incremental_column: "updated_at"

stages:
  bronze:
    data_lake:
      account_name: "yourstorageaccount"
      container: "bronze"
      credential_secret: "adls-storage-key"
    local_path: "data/bronze"
    remote_path_template: "{stage}/{source}/{table}.parquet"

  silver:
    data_lake:
      account_name: "yourstorageaccount"
      container: "silver"
      credential_secret: "adls-storage-key"
    local_path: "data/silver"
    remote_path_template: "{stage}/{source}/{table}.parquet"
    transformations_path: "transformations"
    contracts_path: "contracts"
    dbt_project_path: "dbt_project"  # Enable dbt transformations

  gold:
    data_lake:
      account_name: "yourstorageaccount"
      container: "gold"
      credential_secret: "adls-storage-key"
    local_path: "data/gold"
    remote_path_template: "{stage}/{source}/{table}.parquet"
    transformations_path: "transformations"
    dbt_project_path: "dbt_project"  # Enable dbt transformations

monitoring:
  log_path: "logs/comboi.log"
  metrics_path: "logs/metrics.json"
```

### 3. Configure Transformations (`configs/production-transformations.yml`)

Choose between Bruin (Python) or dbt (SQL) transformations:

```yaml
silver:
  # Option 1: Use Bruin (Python) transformation
  - name: orders_clean
    type: bruin
    inputs:
      - alias: bronze_orders
        stage: bronze
        source_path: "sales_azure/orders.parquet"
    quality_checks:
      - contract:orders_clean
    splink:
      link_type: "dedupe_only"
      unique_id_column_name: "order_id"

  # Option 2: Use dbt (SQL) transformation
  - name: customers_clean
    type: dbt
    model: customers_clean  # SQL file in dbt_project/models/silver/
    inputs:
      - alias: bronze_customers
        stage: bronze
        source_path: "sales_azure/customers.parquet"

gold:
  # Bruin transformation for complex analytics
  - name: customer_metrics
    type: bruin
    inputs:
      - alias: customers_clean
        stage: silver
        source_path: "refined/customers_clean.parquet"
      - alias: orders_clean
        stage: silver
        source_path: "refined/orders_clean.parquet"

  # dbt transformation for simple aggregations
  - name: daily_sales
    type: dbt
    model: daily_sales
    inputs:
      - alias: orders_clean
        stage: silver
        source_path: "refined/orders_clean.parquet"
```

### 4. Test Locally

```bash
# Test bronze extraction
comboi run bronze --config configs/production.yml

# Test full pipeline
comboi run all --config configs/production.yml

# View execution plan
comboi plan --config configs/production.yml
```

---

## Infrastructure Deployment

### 1. Configure Terraform Variables

Create `terraform/terraform.tfvars`:

```hcl
prefix = "myproject"
environment = "prod"
location = "eastus"

# Optional: Custom tags
tags = {
  Project     = "Comboi"
  Environment = "Production"
  ManagedBy   = "Terraform"
}
```

### 2. Deploy Infrastructure

```bash
cd terraform

# Initialize Terraform
terraform init

# Review plan
terraform plan -out=tfplan

# Apply infrastructure
terraform apply tfplan

# Save outputs
terraform output -json > outputs.json
```

This creates:
- Resource Group
- Storage Account for Functions
- Data Lake Storage (Bronze, Silver, Gold containers)
- Key Vault with managed identity access
- Azure Storage Queue
- Function App (Linux Consumption)

### 3. Retrieve Infrastructure Details

```bash
# Get Function App name
terraform output function_app_name

# Get Key Vault URL
terraform output key_vault_url

# Get Storage Account name
terraform output storage_account_name
```

---

## Application Deployment

### 1. Configure Secrets in Key Vault

```bash
# Set variables
KEY_VAULT_NAME="<from terraform output>"

# Add database passwords
az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name azure-sql-password \
  --value "YourDatabasePassword"

az keyvault secret set \
  --vault-name $KEY_VAULT_NAME \
  --name postgres-password \
  --value "YourPostgresPassword"

# Storage key is automatically created by Terraform
# Queue connection string is automatically created by Terraform

# Verify secrets
az keyvault secret list --vault-name $KEY_VAULT_NAME --output table
```

### 2. Prepare Deployment Package

```bash
# Return to project root
cd ..

# Embed comboi package into Azure Functions
python tools/embed_comboi.py

# Copy configuration and models
cp -r transformations azure_functions/
cp -r dbt_project azure_functions/
cp -r contracts azure_functions/
cp -r configs azure_functions/

# Verify requirements.txt exists
cat azure_functions/requirements.txt
```

### 3. Deploy to Azure Functions

```bash
cd azure_functions

# Get function app name from Terraform
FUNCTION_APP_NAME="<from terraform output>"

# Deploy
func azure functionapp publish $FUNCTION_APP_NAME

# Wait for deployment to complete (2-5 minutes)
```

### 4. Configure Function App Settings

```bash
# Set application settings
az functionapp config appsettings set \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --settings \
    "COMBOI_CONFIG_PATH=configs/production.yml" \
    "COMBOI_TRANSFORMATIONS_CONFIG=configs/production-transformations.yml" \
    "COMBOI_TIMER_SCHEDULE=0 */15 * * * *"  # Run every 15 minutes

# Enable managed identity (already done by Terraform)
# Grant Key Vault access (already done by Terraform)
```

---

## Post-Deployment Verification

### 1. Verify Function Deployment

```bash
# List functions
func azure functionapp list-functions $FUNCTION_APP_NAME

# Expected output:
# - driver (Timer trigger)
# - executor (Queue trigger)
```

### 2. Check Function Logs

```bash
# View logs in real-time
func azure functionapp logstream $FUNCTION_APP_NAME

# Or use Azure Portal:
# Functions → Monitor → Logs
```

### 3. Test Execution

#### Option A: Trigger via Azure Portal
1. Navigate to Function App in Azure Portal
2. Select `driver` function
3. Click "Code + Test" → "Test/Run"
4. Click "Run"

#### Option B: Trigger via Azure CLI
```bash
# Manually trigger the driver function
az functionapp function invoke \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --function-name driver
```

### 4. Verify Pipeline Execution

Check for successful execution:

```bash
# Check queue messages
az storage queue list \
  --account-name <storage-account-name> \
  --auth-mode login

# View Application Insights (if configured)
az monitor app-insights query \
  --app <app-insights-name> \
  --analytics-query "traces | where message contains 'comboi' | order by timestamp desc | take 50"
```

### 5. Verify Data Lake Output

```bash
# List Bronze outputs
az storage blob list \
  --account-name <storage-account-name> \
  --container-name bronze \
  --auth-mode login \
  --output table

# List Silver outputs
az storage blob list \
  --account-name <storage-account-name> \
  --container-name silver \
  --auth-mode login \
  --output table

# List Gold outputs
az storage blob list \
  --account-name <storage-account-name> \
  --container-name gold \
  --auth-mode login \
  --output table
```

---

## Troubleshooting

### Function Not Triggering

**Issue**: Timer trigger doesn't fire

**Solutions**:
```bash
# Check function status
az functionapp show \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --query state

# Restart function app
az functionapp restart \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name>

# Verify timer schedule format
# CRON format: {second} {minute} {hour} {day} {month} {day-of-week}
# Example: "0 */15 * * * *" = every 15 minutes
```

### Key Vault Access Denied

**Issue**: Function can't read secrets

**Solutions**:
```bash
# Verify managed identity is enabled
az functionapp identity show \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name>

# Grant Key Vault access
PRINCIPAL_ID=$(az functionapp identity show \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --query principalId -o tsv)

az keyvault set-policy \
  --name $KEY_VAULT_NAME \
  --object-id $PRINCIPAL_ID \
  --secret-permissions get list
```

### DuckDB Extension Errors

**Issue**: `INSTALL odbc` or `INSTALL postgres_scanner` fails

**Solutions**:
- Ensure Function App has internet access for extension download
- Check if DuckDB version is compatible (>=0.10.0)
- Verify `requirements.txt` includes correct DuckDB version

```bash
# Check installed packages
func azure functionapp invoke \
  --name $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --function-name driver \
  --method GET
```

### Transformation Errors

**Issue**: Bruin or dbt transformations fail

**Solutions**:

**For Bruin:**
```bash
# Verify transformation files exist
ls azure_functions/transformations/

# Check function logs for Python errors
func azure functionapp logstream $FUNCTION_APP_NAME --filter trace
```

**For dbt:**
```bash
# Verify dbt project structure
ls azure_functions/dbt_project/models/

# Test dbt locally first
cd dbt_project
dbt debug
dbt run --select <model-name>
```

### Data Contract Validation Failures

**Issue**: Quality checks fail

**Solutions**:
```bash
# Review contract YAML syntax
cat contracts/<contract-name>.yml

# Test contract locally
comboi run silver --config configs/production.yml

# Check logs for specific validation errors
func azure functionapp logstream $FUNCTION_APP_NAME --filter error
```

### Storage Access Issues

**Issue**: Can't write to Data Lake

**Solutions**:
```bash
# Verify storage account key in Key Vault
az keyvault secret show \
  --vault-name $KEY_VAULT_NAME \
  --name adls-storage-key

# Test storage access
az storage blob list \
  --account-name <storage-account-name> \
  --container-name bronze \
  --auth-mode key \
  --account-key "<from-keyvault>"

# Grant Function managed identity access (alternative to keys)
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<rg-name>/providers/Microsoft.Storage/storageAccounts/<storage-account-name>"
```

---

## Maintenance and Updates

### Update Application Code

```bash
# Make code changes
git pull origin main

# Test locally
comboi run all --config configs/production.yml

# Redeploy
python tools/embed_comboi.py
cd azure_functions
func azure functionapp publish $FUNCTION_APP_NAME
```

### Update Dependencies

```bash
# Update pyproject.toml
pip install -e ".[dev]"

# Update azure_functions/requirements.txt
# Redeploy
```

### Monitor Performance

```bash
# View metrics
az monitor metrics list \
  --resource $FUNCTION_APP_NAME \
  --resource-group <resource-group-name> \
  --resource-type "Microsoft.Web/sites" \
  --metric FunctionExecutionCount,FunctionExecutionUnits

# Set up alerts
az monitor metrics alert create \
  --name "comboi-failures" \
  --resource-group <resource-group-name> \
  --scopes $FUNCTION_APP_NAME \
  --condition "count FunctionExecutionCount < 1" \
  --window-size 1h
```

---

## Security Best Practices

1. **Never commit secrets** to version control
   - Use `.gitignore` for `local.settings.json`
   - Store all secrets in Key Vault

2. **Use managed identities** instead of connection strings where possible
   - Azure Storage
   - Key Vault
   - Azure SQL (with Azure AD authentication)

3. **Restrict network access**
   - Configure Function App VNET integration
   - Enable private endpoints for Storage/Key Vault
   - Use Azure Firewall or NSGs

4. **Enable logging and monitoring**
   - Application Insights
   - Log Analytics workspace
   - Storage account access logs

5. **Regular updates**
   - Keep Python dependencies up to date
   - Monitor for DuckDB security advisories
   - Update Azure Functions runtime

---

## Additional Resources

- [Comboi README](README.md) - Architecture and feature overview
- [dbt Project Guide](dbt_project/README.md) - dbt-specific documentation
- [Data Contracts Guide](contracts/README.md) - Contract validation documentation
- [Terraform Documentation](terraform/README.md) - Infrastructure details
- [Azure Functions Python Guide](https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python)

---

## Support

For issues and questions:
- Check [Troubleshooting](#troubleshooting) section
- Review logs in Azure Portal
- Open an issue in the repository
- Consult Azure Functions documentation

