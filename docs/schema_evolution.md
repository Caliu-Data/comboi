# Schema Enforcement & Evolution Guide

This guide provides comprehensive information on how Comboi enforces schemas and manages evolution efficiently.

## Overview

Comboi implements a **contract-first** approach where:
1. **Contracts define the truth** - Schema is explicitly declared in YAML
2. **Enforcement is automatic** - SQL generation enforces constraints
3. **Evolution is tracked** - Schema registry maintains version history
4. **Changes are validated** - Compatibility checks prevent breaking changes

## Enforcement Mechanisms

### 1. **Compile-Time Enforcement** (Contract Validation)

**When:** Before SQL generation
**What:** Validates contract structure and rules
**How:** YAML validation + schema analysis

```bash
# Validate contract syntax and structure
python build.py --validate-contract \
  --contract transformations/contracts/customers.yml
```

**Checks:**
- ✅ YAML syntax correctness
- ✅ Required fields present (version, dataset, schema)
- ✅ Valid column types (VARCHAR, INTEGER, DECIMAL, etc.)
- ✅ Constraint logic validity
- ✅ Quality rule syntax
- ✅ Version format (semver)

**Example validation error:**
```
❌ Contract Validation Failed: customers.yml

Errors:
  - Line 15: Invalid type 'STRING' (use 'VARCHAR')
  - Line 23: Constraint 'min_value' requires numeric type (found VARCHAR)
  - Version '1.0' invalid (use semver: '1.0.0')
```

### 2. **Generation-Time Enforcement** (SQL Generation)

**When:** During `build.py --generate-sql`
**What:** Embeds constraints into SQL
**How:** Automatic SQL generation from contract

```bash
# Generate SQL with embedded enforcement
python build.py --generate-sql \
  --contract transformations/contracts/customers.yml \
  --output transformations/sql/customers.sql
```

**Generated enforcement:**

```sql
-- From contract constraint: not_null
WHERE customer_id IS NOT NULL

-- From contract constraint: unique
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1

-- From contract constraint: min_value
WHERE age >= 18

-- From contract constraint: pattern
WHERE country_code ~ '^[A-Z]{2}$'
```

### 3. **Runtime Enforcement** (DuckDB Execution)

**When:** During pipeline execution
**What:** DuckDB validates types and executes constraints
**How:** Native DuckDB type system + SQL constraints

**Type enforcement:**
```sql
-- DuckDB enforces type mismatch
SELECT CAST('abc' AS INTEGER)  -- ❌ Error: Cannot cast VARCHAR to INTEGER

-- Contract declares DECIMAL, input has VARCHAR
SHA2(email, 256) AS email_hash  -- ✅ VARCHAR from SHA2 matches contract
```

**Constraint enforcement:**
```sql
-- Unique constraint via QUALIFY
QUALIFY ROW_NUMBER() OVER (...) = 1  -- ❌ Fails if duplicates exist

-- Not-null constraint via WHERE
WHERE customer_id IS NOT NULL  -- ❌ Excludes rows with NULL
```

### 4. **Post-Transformation Enforcement** (Quality Checks)

**When:** After transformation completes
**What:** Validates output against contract quality rules
**How:** Contract validator executes quality checks

```yaml
quality_rules:
  - name: "no_duplicates"
    type: "uniqueness"
    column: "customer_id"
    severity: "error"  # Fails pipeline
```

**Execution:**
```
🔧 Stage: Silver
  ⚙ Transforming customers_clean...
    ✓ SQL execution completed (5,222 rows)
  ✅ Running quality checks (contract:customers_clean)...
    ✓ no_null_customer_ids: PASS (0 violations)
    ✓ no_duplicates: PASS (0 duplicates)
    ❌ minimum_rows: FAIL (expected >= 10000, got 5222)
Pipeline halted due to quality check failure.
```

## Evolution Strategies

### Strict Mode (Production)

**Use when:** Production systems requiring stability

```yaml
evolution:
  migration_strategy: "strict"
  breaking_changes_allowed: false
```

**Behavior:**
- ✅ All changes require explicit version bump
- ✅ Breaking changes blocked by default
- ✅ Manual migration scripts required
- ✅ Schema registry mandatory

**Workflow:**
1. Update contract (bump version)
2. Generate migration script
3. Test migration on staging data
4. Review compatibility report
5. Deploy with approval

### Lenient Mode (Development)

**Use when:** Active development with frequent changes

```yaml
evolution:
  migration_strategy: "lenient"
  breaking_changes_allowed: false
```

**Behavior:**
- ⚠️ Backward-compatible changes auto-allowed
- ❌ Breaking changes still blocked
- ⚠️ Schema drift warnings logged
- ✅ Faster iteration

**Workflow:**
1. Update contract
2. Add optional columns freely
3. Breaking changes require override
4. Compatibility checks on commit

### Auto Mode (Experimental)

**Use when:** Prototyping with unstable schemas

```yaml
evolution:
  migration_strategy: "auto"
  breaking_changes_allowed: true
```

**Behavior:**
- ⚠️ Automatically adapts to changes
- ⚠️ Adds NULL columns silently
- ⚠️ No validation of compatibility
- ❌ **Never use in production**

## Efficient Evolution Patterns

### Pattern 1: Column Addition (Zero Downtime)

**Goal:** Add a new optional field without breaking existing consumers

**Steps:**
1. **Add column to contract (v1.0.0 → v1.1.0)**
   ```yaml
   schema:
     columns:
       - name: existing_field
         type: VARCHAR
       - name: new_field  # New optional field
         type: VARCHAR
         nullable: true  # Critical: must be nullable
   ```

2. **Generate new SQL**
   ```bash
   python build.py --generate-sql \
     --contract transformations/contracts/customers.yml \
     --output transformations/sql/customers.sql
   ```

3. **Deploy** - No data migration needed
   - Existing data: `new_field` = NULL
   - New data: `new_field` populated

**Efficiency:** ✅ Zero data migration, instant deployment

### Pattern 2: Column Deprecation (Graceful)

**Goal:** Remove a field with advance notice

**Steps:**
1. **v1.0.0: Mark as deprecated**
   ```yaml
   - name: old_field
     type: VARCHAR
     deprecated: true
     deprecation_date: "2025-01-05"  # 30 days notice
     replacement: "new_field"
   ```

2. **v1.1.0: Still present, warnings logged**
   ```
   ⚠️  Field 'old_field' deprecated, use 'new_field' instead
   ```

3. **v2.0.0: Remove after deprecation period**
   ```yaml
   # old_field removed from schema
   ```

**Efficiency:** ✅ Consumers have time to adapt, no sudden breakage

### Pattern 3: Type Migration (Parallel Fields)

**Goal:** Change column type safely

**Steps:**
1. **v1.0.0: Original VARCHAR field**
   ```yaml
   - name: amount
     type: VARCHAR
   ```

2. **v1.1.0: Add typed version alongside**
   ```yaml
   - name: amount  # Keep original
     type: VARCHAR
     deprecated: true
   - name: amount_decimal  # New typed version
     type: DECIMAL
   ```

3. **v1.1.0 SQL: Populate both**
   ```sql
   SELECT
       amount,  -- Original
       CAST(amount AS DECIMAL) AS amount_decimal  -- Typed
   FROM source
   ```

4. **v2.0.0: Remove old, rename new**
   ```yaml
   - name: amount  # Was amount_decimal
     type: DECIMAL
   ```

**Efficiency:** ✅ Consumers migrate at their pace, no data loss

### Pattern 4: Constraint Tightening (Gradual)

**Goal:** Add stricter validation without breaking existing data

**Steps:**
1. **v1.0.0: Lenient constraint**
   ```yaml
   constraints:
     - min_value: 0
   ```

2. **v1.1.0: Add warning-level quality rule**
   ```yaml
   quality_rules:
     - name: "strict_minimum"
       type: "custom_sql"
       query: "SELECT COUNT(*) FROM {dataset} WHERE amount < 10"
       severity: "warning"  # Log, don't fail
   ```

3. **Monitor warnings, fix data**
   ```
   ⚠️  Warning: 23 rows with amount < 10 (will be enforced in v2.0.0)
   ```

4. **v2.0.0: Enforce as hard constraint**
   ```yaml
   constraints:
     - min_value: 10  # Now enforced in SQL
   ```

**Efficiency:** ✅ Data cleaned gradually, no surprise failures

## Performance Optimization

### 1. Constraint Pushdown

**Bad:** Validate after loading
```sql
-- Load all data
SELECT * FROM large_table
-- Then filter
WHERE customer_id IS NOT NULL  -- After load
```

**Good:** Filter during load (contract-driven SQL)
```sql
-- Contract enforces at source
SELECT *
FROM large_table
WHERE customer_id IS NOT NULL  -- DuckDB pushes down to scan
```

**Efficiency:** ✅ 10-100x faster for large datasets

### 2. Incremental Validation

**Bad:** Re-validate entire dataset
```sql
-- Check all rows every time
SELECT COUNT(*) FROM dataset WHERE id IS NULL  -- Full scan
```

**Good:** Validate only new data
```sql
-- Only check new/changed rows
SELECT COUNT(*)
FROM dataset
WHERE updated_at > (SELECT MAX(last_check) FROM validation_log)
  AND id IS NULL
```

**Efficiency:** ✅ Constant time vs linear time

### 3. Schema Registry Caching

**Bad:** Parse contract every execution
```python
# Every run
contract = yaml.load(open('contract.yml'))  # Slow
validate_schema(contract)
```

**Good:** Cache parsed contracts
```python
# Once per deployment
registry = SchemaRegistry.load_from_disk()  # Pre-parsed
contract = registry.get('customers', version='1.1.0')  # O(1)
```

**Efficiency:** ✅ Sub-millisecond lookups

### 4. Lazy Quality Checks

**Bad:** Run all checks always
```yaml
quality_rules:
  - name: "expensive_check"
    type: "custom_sql"
    query: "SELECT COUNT(*) FROM dataset JOIN other_table ..."  # Slow
```

**Good:** Conditional checks
```yaml
quality_rules:
  - name: "expensive_check"
    type: "custom_sql"
    run_on: "full_load"  # Only on full reloads
    skip_if: "incremental"  # Skip for incremental
```

**Efficiency:** ✅ 90% faster for incremental loads

## Monitoring & Alerting

### Schema Drift Detection

**Automatic monitoring:**
```yaml
evolution:
  drift_detection:
    enabled: true
    alert_on: "schema_mismatch"
    check_frequency: "daily"
```

**Alert example:**
```
🚨 Schema Drift Detected: customers

Expected (contract v1.1.0):
  - customer_id: VARCHAR NOT NULL
  - email_hash: VARCHAR

Actual (data):
  - customer_id: VARCHAR NOT NULL ✅
  - email_hash: VARCHAR ✅
  - unexpected_field: INTEGER ❌  # Not in contract!

Action Required:
  1. Update contract to v1.2.0 with new field, OR
  2. Remove unexpected field from transformation
```

### Version Compatibility Matrix

**Track consumer compatibility:**
```json
{
  "dataset": "customers",
  "current_version": "1.1.0",
  "consumers": [
    {
      "name": "analytics-dashboard",
      "compatible_versions": ["1.0.0", "1.1.0"],
      "requires_migration": false
    },
    {
      "name": "ml-model",
      "compatible_versions": ["1.0.0"],
      "requires_migration": true,  // Needs v1.1.0 update
      "migration_deadline": "2025-01-15"
    }
  ]
}
```

## Best Practices Summary

1. **Always use strict mode in production**
   ```yaml
   evolution:
     migration_strategy: "strict"
   ```

2. **Version every change**
   ```yaml
   version: "1.1.0"  # Bump for any schema change
   changelog:
     - version: "1.1.0"
       changes: ["Added phone_hash field"]
   ```

3. **Enable schema registry**
   ```yaml
   evolution:
     schema_registry:
       enabled: true
   ```

4. **Validate before deploy**
   ```bash
   python build.py --validate-compatibility \
     --old-contract .registry/v1.0.0.json \
     --new-contract contract.yml
   ```

5. **Test migrations on staging**
   ```bash
   # Generate migration
   python build.py --generate-migration \
     --contract contract.yml \
     --output migrations/v1.1.0.sql

   # Test on staging
   duckdb staging.db < migrations/v1.1.0.sql
   ```

6. **Monitor schema drift**
   ```yaml
   evolution:
     drift_detection:
       enabled: true
   ```

## Troubleshooting

### Schema Mismatch Error

```
❌ Schema Mismatch: Column 'email' not found

Contract expects: email (VARCHAR)
Data has: email_hash (VARCHAR)
```

**Solution:** Update contract or fix SQL
```yaml
# Option 1: Update contract
- name: email_hash  # Match actual data
  type: VARCHAR

# Option 2: Fix SQL
SELECT email AS email_hash  # Map correctly
```

### Breaking Change Blocked

```
❌ Breaking Change Detected

Cannot remove column 'phone' (present in v1.0.0)
Set breaking_changes_allowed: true to override
```

**Solution:** Follow deprecation pattern
```yaml
# v1.1.0: Deprecate first
- name: phone
  deprecated: true
  deprecation_notice_days: 30

# v2.0.0: Remove after notice period
# Remove phone column
```

### Performance Degradation

```
⚠️  Quality check 'expensive_validation' took 5 minutes
```

**Solution:** Optimize or defer check
```yaml
quality_rules:
  - name: "expensive_validation"
    run_on: "full_load"  # Skip incremental
    sampling_rate: 0.1  # Check 10% of data
```

## See Also

- [Contract Documentation](../transformations/contracts/README.md)
- [SQL Transformation Guidelines](../transformations/sql/README.md)
- [DuckDB Performance Tuning](https://duckdb.org/docs/guides/performance/overview)
