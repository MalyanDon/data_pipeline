# 🔄 Data Processing Workflow Guide

## 📊 Data Type Classification

### 1. 📂 **MASTER DATA** (Reference Data - Updated Periodically)
**Purpose:** Reference data that changes infrequently  
**Processing:** Date-based table storage (historical snapshots)  
**Frequency:** Weekly/Monthly uploads

| Collection Pattern | Target Table Pattern | Update Frequency |
|-------------------|---------------------|------------------|
| `client_info_data_YYYY_MM_DD_*` | `clients_YYYY_MM_DD` | Monthly |
| `broker_master_data_YYYY_MM_DD_*` | `brokers_YYYY_MM_DD` | Quarterly |
| `strategy_master_data_YYYY_MM_DD_*` | `strategies_YYYY_MM_DD` | As needed |
| `distributor_master_data_YYYY_MM_DD_*` | `distributors_YYYY_MM_DD` | Quarterly |

### 2. 💸 **TRANSACTION DATA** (Daily Operations)
**Purpose:** Daily business transactions  
**Processing:** Date-based table storage (daily snapshots)  
**Frequency:** Daily uploads

| Collection Pattern | Target Table Pattern | Update Frequency |
|-------------------|---------------------|------------------|
| `contract_notes_data_YYYY_MM_DD_*` | `contract_notes_YYYY_MM_DD` | Daily |
| `cash_capital_flow_data_YYYY_MM_DD_*` | `cash_capital_flow_YYYY_MM_DD` | Daily |
| `stock_capital_flow_data_YYYY_MM_DD_*` | `stock_capital_flow_YYYY_MM_DD` | Daily |
| `mf_allocation_data_YYYY_MM_DD_*` | `mf_allocations_YYYY_MM_DD` | Daily |

### 3. 🏦 **CUSTODY DATA** (Daily Holdings - Creates Unified Master)
**Purpose:** Daily holdings snapshots from custody systems  
**Processing:** Daily table partitioning  
**Frequency:** Daily uploads

| Collection Pattern | Target Table | Primary Key | Update Frequency |
|-------------------|--------------|-------------|------------------|
| `axis_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |
| `hdfc_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |
| `kotak_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |
| `deutsche_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |
| `orbis_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |
| `trustpms_custody_data_*` | `unified_custody_master_YYYY_MM_DD` | Composite | Daily |

## 🎯 When is Unified Custody Master Created?

The `unified_custody_master_YYYY_MM_DD` tables are **ONLY** created when **actual custody files** are processed:

```javascript
// Custody file examples that CREATE unified master:
hdfc_custody_20241215.xlsx        → unified_custody_master_2024_12_15
axis_eod_custody_20241215.csv     → unified_custody_master_2024_12_15  
kotak_custody_20241215.xlsx       → unified_custody_master_2024_12_15
deutsche_164_ec0000720_20241215.xlsx → unified_custody_master_2024_12_15
```

### ✅ Your Current Test Data (Proper Date-Based Processing):
```
✅ client_info_data_2025_06_28_14_56_24          → clients_2025_06_28 table
✅ broker_master_data_2025_06_28_14_56_37        → brokers_2025_06_28 table  
✅ strategy_master_data_2025_06_28_14_56_37      → strategies_2025_06_28 table
✅ distributor_master_data_2025_06_28_14_56_37   → distributors_2025_06_28 table
✅ contract_notes_data_2025_06_28_14_56_37       → contract_notes_2025_06_28 table
✅ cash_capital_flow_data_2025_06_28_14_56_37    → cash_capital_flow_2025_06_28 table
✅ stock_capital_flow_data_2025_06_28_14_56_37   → stock_capital_flow_2025_06_28 table
✅ mf_allocation_data_2025_06_28_14_56_37        → mf_allocations_2025_06_28 table

⚠️ NO CUSTODY FILES = NO unified_custody_master_* tables created (but that's expected!)
```

## 📈 Processing Logic Changes

### 🔧 **BEFORE** (Wrong - All data treated the same):
```sql
-- Everything used simple INSERT with conflict ignore
INSERT INTO table (columns) VALUES (values) ON CONFLICT DO NOTHING;
```

### ✅ **NOW** (Correct - Date-based table storage):

#### Master Data (Date-based Storage):
```sql
-- Create date-based table and insert records
CREATE TABLE brokers_2025_06_28 (LIKE brokers INCLUDING ALL);
INSERT INTO brokers_2025_06_28 (broker_code, broker_name, contact_info)
VALUES ('BRK001', 'Updated Broker Name', 'new@email.com')
ON CONFLICT DO NOTHING;
```

#### Transaction Data (Date-based Storage):
```sql  
-- Create date-based table and insert records
CREATE TABLE contract_notes_2025_06_28 (LIKE contract_notes INCLUDING ALL);
INSERT INTO contract_notes_2025_06_28 (ecn_number, client_code, amount)
VALUES ('ECN123', 'CLIENT001', 50000)
ON CONFLICT DO NOTHING;
```

#### Custody Data (DAILY TABLES):
```sql
-- Create daily snapshot tables
CREATE TABLE unified_custody_master_2025_06_28 (
  LIKE unified_custody_master INCLUDING ALL
);
```

## 🚀 Recommended Testing Workflow

### Step 1: Test Master Data First
```bash
# Upload master data files (should now work with UPSERT logic)
# - Broker Master OMS template.xlsx
# - Client_Info.csv  
# - Strategy Master.xlsx
# - Distributor Master.xlsx
```

### Step 2: Test Transaction Data
```bash
# Upload transaction files (should append new records)
# - Contract Note format.xlsx
# - Cash Capital Flow for Hdfc Custodian.xlsx
# - Stock Capital Flow for Hdfc Custodian.xlsx  
# - MF Buy Allocation file format.xlsx
```

### Step 3: Test Custody Data (Creates Unified Master)
```bash
# Upload actual custody files to create unified_custody_master_*
# - hdfc_custody_20241215.xlsx
# - axis_eod_custody_20241215.csv
# - kotak_custody_20241215.xlsx
```

## 🔍 Success Metrics

| Data Type | Expected Behavior | Success Indicator |
|-----------|------------------|-------------------|
| **Master Data** | UPSERT existing records | Records updated, not duplicated |
| **Transaction Data** | APPEND new records | New records added, duplicates ignored |
| **Custody Data** | Create daily tables | `unified_custody_master_YYYY_MM_DD` created |

## 🚨 Current Issues Fixed

1. **✅ Master Data Processing:** Now uses UPSERT logic instead of simple INSERT
2. **✅ Transaction Data Processing:** Continues to use APPEND logic  
3. **✅ Field Mappings:** Fixed for stock_capital_flow and mf_allocations
4. **⚠️ Custody Data:** No custody files in test data = no unified master creation

## 🎯 Next Steps

1. **Fix remaining field mappings** for contract_notes and cash_capital_flow
2. **Test master data processing** with new UPSERT logic
3. **Add actual custody files** to test unified_custody_master creation
4. **Verify success rates** improve dramatically with proper data type handling 