# Data Contracts

This directory contains data contract definitions for datasets in the pipeline. Data contracts provide formal specifications for:

- **Schema**: Column names, types, nullability, and constraints
- **Quality Rules**: Business rules and data quality checks
- **SLAs**: Freshness and completeness requirements
- **Evolution Policy**: Rules for how contracts can change over time
- **Schema Registry**: Version tracking and compatibility enforcement

## Contract Structure

Each contract is a YAML file with the following structure:

```yaml
version: "1.0.0"
dataset: "dataset_name"
stage: "silver"  # or "bronze", "gold"
owner: "team-name"
description: "Description of the dataset"

schema:
  columns:
    - name: column_name
      type: VARCHAR  # or INTEGER, DECIMAL, DATE, etc.
      nullable: false
      description: "Column description"
      constraints:
        - unique: true
        - not_null: true
        - min_value: 0
        - max_value: 100
        - allowed_values: ["value1", "value2"]
        - pattern: "^regex_pattern$"

quality_rules:
  - name: "rule_name"
    type: "uniqueness"  # or "not_null", "volume", "custom_sql"
    column: "column_name"  # required for uniqueness/not_null
    severity: "error"  # or "warning"
    query: "SELECT ..."  # for custom_sql
    expected: 0  # expected result for custom_sql
    min_rows: 1  # for volume checks

sla:
  freshness:
    max_age_hours: 24
    schedule: "daily"
  completeness:
    min_row_count: 1
    expected_growth_rate: 0.05

evolution:
  backward_compatible: true
  breaking_changes_allowed: false
  deprecation_notice_days: 30
  schema_registry:
    enabled: true
    registry_path: "transformations/contracts/.registry"
  migration_strategy: "strict"  # strict, lenient, or auto
  changelog:
    - version: "1.0.0"
      date: "2024-12-05"
      changes:
        - "Initial schema definition"
```

## Schema Enforcement

Comboi enforces schema contracts at multiple levels:

### 1. Type Enforcement

All columns must match their declared types:

```yaml
schema:
  columns:
    - name: amount
      type: DECIMAL  # Must be numeric
      nullable: false  # Cannot be NULL
```

**Enforcement:**
- DuckDB automatically validates types during SQL execution
- Contract validation occurs before and after transformations
- Type mismatches fail the pipeline immediately

### 2. Constraint Enforcement

Constraints are enforced through SQL generation and validation:

```yaml
constraints:
  - unique: true  # Generates QUALIFY ROW_NUMBER()
  - not_null: true  # Generates WHERE column IS NOT NULL
  - min_value: 0  # Generates WHERE column >= 0
  - pattern: "^[A-Z]{2}$"  # Validates format
```

**Generated SQL includes:**
- WHERE clauses for not_null and value ranges
- QUALIFY clauses for uniqueness
- Custom validation for patterns and allowed values

### 3. Quality Rule Enforcement

Quality rules run after transformation:

```yaml
quality_rules:
  - name: "no_duplicates"
    type: "uniqueness"
    column: "id"
    severity: "error"  # Fails pipeline
```

**Validation process:**
1. Transformation completes
2. Quality rules execute via contract validator
3. Errors halt pipeline, warnings log only

## Schema Evolution

### Evolution Strategies

Comboi supports three evolution strategies:

#### 1. Strict Mode (Recommended)
```yaml
evolution:
  migration_strategy: "strict"
```

- All changes require explicit version bump
- Breaking changes are blocked by default
- Requires migration scripts for schema changes

#### 2. Lenient Mode
```yaml
evolution:
  migration_strategy: "lenient"
```

- Allows backward-compatible changes automatically
- Breaking changes require explicit approval
- Logs warnings for schema drift

#### 3. Auto Mode
```yaml
evolution:
  migration_strategy: "auto"
```

- Automatically adapts to schema changes
- Adds missing columns with NULL values
- Removes deprecated columns silently
- **Use with caution in production**

### Version Management

Every schema change increments the version:

```yaml
version: "1.1.0"  # Major.Minor.Patch

evolution:
  changelog:
    - version: "1.1.0"
      date: "2024-12-10"
      changes:
        - "Added phone_hash column for contact info"
        - "Deprecated email field (use email_hash)"
    - version: "1.0.0"
      date: "2024-12-05"
      changes:
        - "Initial schema"
```

**Versioning rules:**
- **Patch (1.0.X)**: Documentation, quality rule updates
- **Minor (1.X.0)**: Backward-compatible changes (add optional columns)
- **Major (X.0.0)**: Breaking changes (remove columns, change types)

### Schema Registry

The schema registry tracks all versions:

```
transformations/contracts/.registry/
├── gdpr_customers_v1.0.0.json
├── gdpr_customers_v1.1.0.json
└── schema_compatibility_matrix.json
```

**Registry file format:**
```json
{
  "dataset": "gdpr_customers",
  "version": "1.1.0",
  "timestamp": "2024-12-10T10:00:00Z",
  "checksum": "sha256:def456...",
  "schema_fingerprint": {
    "columns": [...]
  },
  "compatible_with": ["1.0.0"],
  "incompatible_with": []
}
```

### Backward-Compatible Changes

**Allowed without breaking changes:**

```yaml
# Adding optional columns
- name: new_optional_field
  type: VARCHAR
  nullable: true  # Must be nullable

# Relaxing constraints
- name: amount
  type: DECIMAL
  nullable: true  # Was false, now true

# Adding allowed values
constraints:
  - allowed_values: ["a", "b", "c", "d"]  # Added "d"

# Loosening numeric ranges
constraints:
  - min_value: -100  # Was 0, now -100
```

### Breaking Changes

**Require major version bump:**

```yaml
# Removing columns
# Before: customer_id, email, phone
# After: customer_id, email  # phone removed

# Changing types
- name: amount
  type: INTEGER  # Was DECIMAL

# Making columns non-nullable
- name: optional_field
  type: VARCHAR
  nullable: false  # Was true

# Tightening constraints
constraints:
  - allowed_values: ["a", "b"]  # Was ["a", "b", "c"]
```

### Migration Scripts

Generate migration scripts with build.py:

```bash
# Compare two contract versions
python build.py --compare-schemas \
  --old transformations/contracts/.registry/gdpr_customers_v1.0.0.json \
  --new transformations/contracts/gdpr_customers.yml \
  --output migrations/gdpr_customers_v1.0.0_to_v1.1.0.sql
```

**Generated migration SQL:**
```sql
-- Migration: gdpr_customers v1.0.0 -> v1.1.0
-- Type: Backward-compatible
-- Date: 2024-12-10

-- Add new optional column
ALTER TABLE gdpr_customers ADD COLUMN phone_hash VARCHAR;

-- Backfill with NULL (already default)
-- No data migration needed for nullable columns

-- Update contract validation
-- New quality rules will apply to future data only
```

## Compatibility Checks

Use build.py to validate schema compatibility:

```bash
# Check if new schema is compatible with old
python build.py --validate-compatibility \
  --old-contract transformations/contracts/.registry/gdpr_customers_v1.0.0.json \
  --new-contract transformations/contracts/gdpr_customers.yml
```

**Output:**
```
✅ Schema Compatibility Check: gdpr_customers

Comparing versions:
  Old: v1.0.0 (2024-12-05)
  New: v1.1.0 (2024-12-10)

✅ Backward Compatible: Yes
   - Added 1 optional column (phone_hash)
   - No columns removed
   - No type changes

❌ Forward Compatible: No
   - Old consumers cannot read new phone_hash field
   - Migration recommended for downstream systems

Recommendation:
  ✅ Safe to deploy with minor version bump (1.1.0)
  ⚠️  Notify downstream consumers about new field
```

## Schema Enforcement in SQL

Generated SQL automatically enforces contract schemas:

```sql
-- Generated from contract: gdpr_customers v1.1.0
SELECT
    customer_id,  -- Enforced: NOT NULL, UNIQUE
    email_hash,   -- Enforced: SHA2 pseudonymization
    country_code, -- Enforced: 2-char uppercase
    phone_hash,   -- New in v1.1.0
    consent_marketing,
    consent_analytics,
    consent_timestamp,
    retention_category,
    created_at,
    updated_at
FROM bronze_customers
WHERE
    customer_id IS NOT NULL  -- Schema constraint
    AND country_code ~ '^[A-Z]{2}$'  -- Pattern constraint
-- Enforce uniqueness
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
```

## Best Practices

### 1. Always Version Your Changes
```yaml
# ❌ Bad: No version tracking
version: "1.0.0"
# Modified schema without version bump

# ✅ Good: Version bumped with changelog
version: "1.1.0"
evolution:
  changelog:
    - version: "1.1.0"
      changes: ["Added phone_hash field"]
```

### 2. Use Schema Registry
```yaml
evolution:
  schema_registry:
    enabled: true  # Always enable in production
```

### 3. Document Breaking Changes
```yaml
evolution:
  breaking_changes_allowed: false  # Default
  deprecation_notice_days: 30  # Warn users first
```

### 4. Test Migrations
```bash
# Generate migration script
python build.py --generate-migration \
  --contract transformations/contracts/customers.yml \
  --output migrations/customers_v1.1.0.sql

# Test migration on sample data
duckdb test.db < migrations/customers_v1.1.0.sql
```

### 5. Validate Before Deployment
```bash
# Run compatibility check
python build.py --validate-compatibility \
  --old-contract .registry/customers_v1.0.0.json \
  --new-contract contracts/customers.yml

# Validate contract syntax
python build.py --validate-contract \
  --contract contracts/customers.yml
```

## Evolution Patterns

### Pattern 1: Adding Optional Fields
```yaml
# v1.0.0 -> v1.1.0
schema:
  columns:
    - name: customer_id
      type: VARCHAR
      nullable: false
    - name: new_field  # ✅ Backward compatible
      type: VARCHAR
      nullable: true  # Must be nullable
```

### Pattern 2: Field Deprecation
```yaml
# v1.1.0: Mark as deprecated
schema:
  columns:
    - name: old_field
      type: VARCHAR
      nullable: true
      deprecated: true
      deprecation_date: "2024-12-10"
      replacement: "new_field"

# v2.0.0: Remove after deprecation period
# Remove old_field entirely
```

### Pattern 3: Type Evolution
```yaml
# v1.0.0: String field
- name: amount
  type: VARCHAR

# v1.1.0: Add typed version (parallel run)
- name: amount_str
  type: VARCHAR
- name: amount
  type: DECIMAL

# v2.0.0: Remove old version
- name: amount
  type: DECIMAL
```

### Pattern 4: Constraint Tightening
```yaml
# v1.0.0: Lenient validation
constraints:
  - min_value: 0

# v1.1.0: Add warning-level validation
quality_rules:
  - name: "strict_minimum"
    type: "custom_sql"
    query: "SELECT COUNT(*) FROM {dataset} WHERE amount < 10"
    expected: 0
    severity: "warning"  # Warn first

# v2.0.0: Enforce as constraint
constraints:
  - min_value: 10  # Now enforced
```

## Quality Rule Types

### Uniqueness
```yaml
- name: "no_duplicates"
  type: "uniqueness"
  column: "id"
  severity: "error"
```

### Not Null
```yaml
- name: "no_null_ids"
  type: "not_null"
  column: "id"
  severity: "error"
```

### Volume
```yaml
- name: "minimum_rows"
  type: "volume"
  min_rows: 1
  severity: "error"
```

### Custom SQL
```yaml
- name: "positive_totals"
  type: "custom_sql"
  query: "SELECT COUNT(*) FROM {dataset} WHERE total <= 0"
  expected: 0
  severity: "error"
```

## See Also

- [SQL Transformations](../sql/README.md)
- [Schema Evolution Guide](../../docs/schema_evolution.md)
- [DuckDB Type System](https://duckdb.org/docs/sql/data_types/overview)
