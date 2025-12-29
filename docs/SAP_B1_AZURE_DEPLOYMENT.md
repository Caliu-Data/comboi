# SAP Business One Azure Functions Deployment Guide

## Overview

This guide covers deploying the SAP Business One GDPR-compliant data pipeline to Azure Functions for automated, timer-triggered execution.

## Architecture

```
Azure Functions Timer Trigger (Daily 2 AM UTC)
    ↓
Driver Function (Timer-Triggered)
    ↓
Azure Storage Queue (comboi-sap-tasks)
    ↓
Executor Function (Queue-Triggered)
    ├─ Bronze Stage: Extract from ADLS raw → Bronze with GDPR
    ├─ Silver Stage: Transform Bronze → Silver with contract validation
    └─ Gold Stage: Calculate KPIs → Gold metrics
```

## Prerequisites

### 1. Azure Resources

- **Azure subscription** with appropriate permissions
- **Resource Group**: For all SAP B1 pipeline resources
- **Azure Function App**:
  - Runtime: Python 3.11
  - Plan: Consumption (serverless) or Premium
  - OS: Linux
- **Storage Account** (ADLS Gen2):
  - Hierarchical namespace enabled
  - Containers: `bronze`, `silver`, `gold`, `raw`
- **Storage Queue**:
  - Queue name: `comboi-sap-tasks`
- **Azure Key Vault**:
  - Secrets management
  - Managed Identity access from Function App

### 2. SAP B1 Data Export

- SAP B1 data must be **pre-extracted** to ADLS in Parquet format
- Recommended structure:
  ```
  /raw/sap_b1/
  ├── ocrd/dt=2024-12-29/*.parquet
  ├── oprj/dt=2024-12-29/*.parquet
  ├── oslp/*.parquet
  ├── oinv/dt=2024-12-29/*.parquet
  ├── inv1/dt=2024-12-29/*.parquet
  └── pch1/dt=2024-12-29/*.parquet
  ```

### 3. Local Development Tools

- Python 3.9+
- Azure CLI (`az login`)
- Azure Functions Core Tools (`func` command)
- uv (fast Python package installer)

---

## Step 1: Provision Azure Infrastructure

### Option A: Using Terraform (Recommended)

```bash
cd terraform
terraform init
terraform apply \
  -var="prefix=sapb1" \
  -var="environment=prod" \
  -var="location=westeurope"
```

This provisions:
- Resource group: `sapb1-comboi-prod`
- Function App: `sapb1-comboi-prod`
- Storage account with ADLS Gen2
- Containers: bronze, silver, gold, raw
- Storage Queue: `comboi-sap-tasks`
- Key Vault: `sapb1-kv-prod`

### Option B: Using Azure Portal

1. **Create Resource Group**:
   - Name: `sapb1-comboi-prod`
   - Location: West Europe

2. **Create Storage Account**:
   - Name: `sapb1storage`
   - Performance: Standard
   - Replication: LRS
   - **Enable hierarchical namespace** (ADLS Gen2)
   - Create containers: `bronze`, `silver`, `gold`, `raw`

3. **Create Storage Queue**:
   - In the same storage account
   - Queue name: `comboi-sap-tasks`

4. **Create Key Vault**:
   - Name: `sapb1-kv-prod`
   - Enable RBAC

5. **Create Function App**:
   - Name: `sapb1-comboi-prod`
   - Runtime: Python 3.11
   - Region: Same as storage account
   - Storage: Use the ADLS storage account
   - Plan: Consumption (Y1)
   - Enable System-Assigned Managed Identity

---

## Step 2: Configure Secrets in Key Vault

### Add Required Secrets

```bash
# Queue connection string
az keyvault secret set \
  --vault-name sapb1-kv-prod \
  --name queue-connection-string \
  --value "<storage-account-connection-string>"

# ADLS storage key
az keyvault secret set \
  --vault-name sapb1-kv-prod \
  --name adls-storage-key \
  --value "<storage-account-key>"
```

### Grant Function App Access to Key Vault

```bash
# Get Function App managed identity
FUNCTION_PRINCIPAL_ID=$(az functionapp identity show \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod \
  --query principalId -o tsv)

# Grant Key Vault secrets access
az keyvault set-policy \
  --name sapb1-kv-prod \
  --object-id $FUNCTION_PRINCIPAL_ID \
  --secret-permissions get list
```

---

## Step 3: Configure Function App Settings

### Set Application Settings

```bash
az functionapp config appsettings set \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod \
  --settings \
    "COMBOI_CONFIG=configs/sap_b1_example.yml" \
    "COMBOI_TIMER_SCHEDULE=0 0 2 * * *" \
    "COMBOI_START_STAGE=all" \
    "COMBOI_QUEUE_NAME=comboi-sap-tasks" \
    "DATA_LAKE_ACCOUNT_NAME=sapb1storage" \
    "COMBOI_QUEUE_CONNECTION=@Microsoft.KeyVault(SecretUri=https://sapb1-kv-prod.vault.azure.net/secrets/queue-connection-string/)"
```

### Environment Variables Reference

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `COMBOI_CONFIG` | Path to configuration file | `configs/sap_b1_example.yml` |
| `COMBOI_TIMER_SCHEDULE` | CRON schedule for timer trigger | `0 0 2 * * *` (Daily 2 AM UTC) |
| `COMBOI_START_STAGE` | Starting stage | `all` (bronze, silver, gold) |
| `COMBOI_QUEUE_NAME` | Azure Storage Queue name | `comboi-sap-tasks` |
| `DATA_LAKE_ACCOUNT_NAME` | ADLS storage account name | `sapb1storage` |
| `COMBOI_QUEUE_CONNECTION` | Queue connection string | Key Vault reference |

---

## Step 4: Prepare Deployment Package

### 1. Install Dependencies

```bash
cd comboi
uv pip install -e .
```

### 2. Embed Comboi Package in Azure Functions

```bash
# Copy comboi package to Azure Functions shared_packages
python tools/embed_comboi.py
```

### 3. Copy Configuration and Transformations

```bash
# Copy configurations
cp -r configs azure_functions/

# Copy transformations (SQL and contracts)
cp -r transformations azure_functions/
```

### 4. Verify Deployment Structure

```
azure_functions/
├── driver/
│   ├── __init__.py
│   └── function.json
├── executor/
│   ├── __init__.py
│   └── function.json
├── shared_packages/
│   └── comboi/
├── configs/
│   ├── sap_b1_example.yml
│   └── transformations.yml
├── transformations/
│   ├── sql/
│   └── contracts/
├── host.json
└── requirements.txt
```

---

## Step 5: Deploy to Azure

### Deploy Function Code

```bash
cd azure_functions

# Deploy to Azure
func azure functionapp publish sapb1-comboi-prod --python
```

### Verify Deployment

```bash
# Check functions are deployed
az functionapp function list \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod \
  --output table

# Expected output:
# Name      Trigger
# --------  --------
# driver    Timer
# executor  Queue
```

---

## Step 6: Enable Timer Trigger

### Update Timer Trigger Schedule

Edit `azure_functions/driver/function.json`:

```json
{
  "scriptFile": "__init__.py",
  "bindings": [
    {
      "name": "mytimer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "%COMBOI_TIMER_SCHEDULE%"
    }
  ]
}
```

The schedule uses CRON format:
- `0 0 2 * * *`: Daily at 2 AM UTC
- `0 0 */6 * * *`: Every 6 hours
- `0 */5 * * * *`: Every 5 minutes (testing)

### Test Timer Trigger Manually

```bash
# Trigger driver function manually
az functionapp function keys list \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod \
  --function-name driver

# Use the key to trigger via HTTP POST
curl -X POST "https://sapb1-comboi-prod.azurewebsites.net/admin/functions/driver" \
  -H "Content-Type: application/json" \
  -H "x-functions-key: <master-key>" \
  -d '{}'
```

---

## Step 7: Monitor Execution

### View Logs in Application Insights

```bash
# Query logs
az monitor app-insights query \
  --app sapb1-comboi-prod \
  --analytics-query "traces | where timestamp > ago(1h) | order by timestamp desc" \
  --output table
```

### Check Storage Queue

```bash
# View queue messages
az storage queue list \
  --account-name sapb1storage \
  --output table

# Peek at messages (non-destructive)
az storage message peek \
  --queue-name comboi-sap-tasks \
  --account-name sapb1storage \
  --num-messages 5
```

### Verify Output Data

```bash
# Check Bronze layer
az storage fs file list \
  --file-system bronze \
  --path bronze/sap_b1_services/ \
  --account-name sapb1storage \
  --output table

# Check Silver layer
az storage fs file list \
  --file-system silver \
  --path silver/refined/ \
  --account-name sapb1storage \
  --output table

# Check Gold layer
az storage fs file list \
  --file-system gold \
  --path gold/metrics/ \
  --account-name sapb1storage \
  --output table
```

---

## Troubleshooting

### Issue: Function Fails to Start

**Symptoms**: Function App shows "Running" but functions don't execute

**Solution**:
```bash
# Restart Function App
az functionapp restart \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod

# Check logs
az functionapp log tail \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod
```

### Issue: Key Vault Access Denied

**Symptoms**: Error message contains "Access denied" or "KeyVault"

**Solution**:
```bash
# Verify managed identity is enabled
az functionapp identity show \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod

# Re-grant Key Vault access (see Step 2)
```

### Issue: Queue Messages Not Processing

**Symptoms**: Messages appear in queue but executor doesn't run

**Solution**:
```bash
# Check queue trigger binding
az functionapp config show \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod

# Verify COMBOI_QUEUE_CONNECTION is set correctly
```

### Issue: GDPR Validation Fails

**Symptoms**: Contract validation errors about PII columns or hash formats

**Solution**:
```bash
# Test locally first
comboi run bronze --config configs/sap_b1_example.yml

# Verify Bronze output
duckdb -c "SELECT * FROM read_parquet('data/bronze/sap_b1_services/OCRD.parquet') LIMIT 5"

# Check for PII columns (should return 0)
duckdb -c "SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM read_parquet('data/bronze/sap_b1_services/OCRD.parquet')) WHERE column_name IN ('Phone1', 'Email')"
```

### Issue: Transformation SQL Errors

**Symptoms**: Silver or Gold stage fails with SQL syntax errors

**Solution**:
```bash
# Test SQL locally
duckdb -c ".read transformations/sql/sap_ocrd_silver.sql"

# Check for missing input tables
duckdb -c "SELECT * FROM bronze_ocrd" # Should fail if Bronze not run yet

# Run stages sequentially for debugging
comboi run bronze --config configs/sap_b1_example.yml
comboi run silver --config configs/sap_b1_example.yml
comboi run gold --config configs/sap_b1_example.yml
```

---

## Local Testing

### Test Entire Pipeline Locally

```bash
# Set environment variables
export DATA_LAKE_ACCOUNT_NAME=sapb1storage
export COMBOI_CONFIG=configs/sap_b1_example.yml

# Run all stages
comboi run all --config $COMBOI_CONFIG

# Or run stage by stage
comboi run bronze --config $COMBOI_CONFIG
comboi run silver --config $COMBOI_CONFIG
comboi run gold --config $COMBOI_CONFIG
```

### Test Azure Functions Locally

```bash
cd azure_functions

# Start Functions runtime
func start

# Trigger driver manually (in another terminal)
curl -X POST "http://localhost:7071/admin/functions/driver"

# Monitor logs in first terminal
```

---

## Performance Optimization

### Adjust Function Timeout

For large datasets, increase timeout in `host.json`:

```json
{
  "version": "2.0",
  "functionTimeout": "00:10:00",
  "extensions": {
    "queues": {
      "maxDequeueCount": 5,
      "visibilityTimeout": "00:10:00"
    }
  }
}
```

### Scale Out with Premium Plan

For faster processing, upgrade to Premium plan:

```bash
az functionapp plan create \
  --name sapb1-premium-plan \
  --resource-group sapb1-comboi-prod \
  --sku EP1 \
  --is-linux true

az functionapp update \
  --name sapb1-comboi-prod \
  --resource-group sapb1-comboi-prod \
  --plan sapb1-premium-plan
```

---

## Cost Estimation

### Consumption Plan

- **Function execution**: ~500 executions/month = ~$0.20
- **ADLS storage**: ~1 GB = ~$0.02/month
- **Storage queue**: Negligible (~$0.01/month)
- **Key Vault**: $0.03/secret/month × 2 = ~$0.06/month

**Total**: ~$0.30/month (minimal)

### Premium Plan (EP1)

- **Function App**: ~$150/month
- **Storage**: Same as above
- **Total**: ~$150/month

---

## Security Best Practices

1. **Managed Identity**: Always use managed identity for Key Vault access
2. **Network Security**: Consider VNet integration for production
3. **RBAC**: Grant minimal required permissions
4. **Secrets Rotation**: Rotate storage keys quarterly
5. **Monitoring**: Enable Application Insights for all functions

---

## Next Steps

1. ✅ Deploy infrastructure
2. ✅ Configure secrets
3. ✅ Deploy function code
4. ✅ Test pipeline execution
5. ✅ Monitor first run
6. 📊 Set up dashboards (Power BI, Grafana)
7. 🔔 Configure alerts (Azure Monitor)
8. 📈 Optimize performance based on metrics

---

## References

- [Azure Functions Python Developer Guide](https://learn.microsoft.com/azure/azure-functions/functions-reference-python)
- [Azure Data Lake Storage Gen2](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Azure Key Vault Best Practices](https://learn.microsoft.com/azure/key-vault/general/best-practices)
- [Comboi Documentation](../README.md)
- [SAP B1 Connector Documentation](../SAP_B1_CONNECTOR.md)
