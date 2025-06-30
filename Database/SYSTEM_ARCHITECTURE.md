# Data Pipeline Architecture: Tier 1 vs Tier 2

## 🎯 **System Status: PARTIALLY WORKING** ✅

### **Current Working Components**
- ✅ **File Upload**: MongoDB raw dump with timestamped collections
- ✅ **Smart Detection**: Intelligent file type detection
- ✅ **Table Routing**: Correct routing to target PostgreSQL tables
- ⚠️ **Field Validation**: Needs refinement for better success rates

---

## 🏗️ **Two-Tier Architecture Overview**

### **TIER 1: Raw Data Storage (MongoDB)**
**Purpose**: Store unprocessed, raw files exactly as uploaded
**Technology**: MongoDB Atlas
**Data Format**: JSON documents with original file structure

```
📁 TIER 1 - MongoDB Collections (Raw Data)
├── broker_master_data_2025_06_28_13_14_33
├── cash_capital_flow_data_2025_06_28_14_15_22
├── contract_notes_data_2025_06_28_14_16_45
├── client_info_data_2025_06_28_14_17_12
├── strategy_master_data_2025_06_28_14_18_30
├── distributor_master_data_2025_06_28_14_19_15
├── mf_allocation_data_2025_06_28_14_20_00
├── stock_capital_flow_data_2025_06_28_14_21_30
├── axis_custody_data_2025_06_28_14_22_45
├── hdfc_custody_data_2025_06_28_14_23_15
├── kotak_custody_data_2025_06_28_14_24_00
└── deutsche_custody_data_2025_06_28_14_25_30
```

**Key Features**:
- **Timestamped Collections**: `{type}_{YYYY_MM_DD_HH_MM_SS}`
- **Version Control**: Multiple versions of same file type
- **Raw Preservation**: Original data structure maintained
- **Fast Upload**: No processing during upload

---

### **TIER 2: Processed Data Storage (PostgreSQL)**
**Purpose**: Clean, normalized, validated business data
**Technology**: PostgreSQL
**Data Format**: Structured relational tables

## 🏗️ **Tier 2 Database Schema (PostgreSQL)**

### **📊 Simplified Table Structure (12 Total Tables)**

### **🔧 Master Data Tables (5 tables)**
```sql
1. brokers             -- Broker/custodian definitions
2. clients             -- Client master data  
3. distributors        -- Distributor master data
4. strategies          -- Investment strategy definitions
5. securities          -- Security/instrument master
```

### **💰 Transaction Tables (4 tables)**
```sql
6. contract_notes      -- All contract note transactions
7. cash_capital_flow   -- All cash movement transactions
8. stock_capital_flow  -- All stock movement transactions  
9. mf_allocations      -- All mutual fund allocations
```

### **🏦 Custody Holdings (Daily Tables Only)**
```sql
10. unified_custody_master_YYYY_MM_DD -- Daily snapshot tables (multiple)
```
**Note:** We use ONLY daily tables. No redundant main table.

### **📝 System Tables (2 tables)**
```sql
11. raw_uploads        -- Upload audit trail
12. processed_files    -- Processing history
```

---

## ✅ **Why This Structure is Better**

### **Custody Data - Daily Tables Only**
- ❌ ~~`unified_custody_master`~~ (Removed - redundant)
- ❌ ~~`custody_holdings`~~ (Removed - redundant) 
- ✅ `unified_custody_master_YYYY_MM_DD` (Keep - efficient daily snapshots)

**Benefits:**
- Natural partitioning by date
- Faster queries (smaller tables)
- Easy historical analysis
- No data mixing between dates

### **Transaction Data - Single Purpose Tables**
- ❌ ~~`enso_contract_notes`~~ → ✅ `contract_notes` (Unified for all sources)
- ❌ ~~`enso_cash_capital_flow`~~ → ✅ `cash_capital_flow` (Unified)
- ❌ ~~`enso_stock_capital_flow`~~ → ✅ `stock_capital_flow` (Unified)
- ❌ ~~`enso_mf_allocations`~~ → ✅ `mf_allocations` (Unified)
- ❌ ~~`capital_flows`~~ (Removed - redundant with cash/stock flows)
- ❌ ~~`trades`~~ (Removed - same as contract_notes)

**Benefits:**
- One table per data type
- No source-specific prefixes needed
- Simpler queries and joins
- Less confusion

---

## 🔄 **Simplified Data Flow**

### **Step 1: Upload (Tier 1)**
```
User Files → MongoDB Collections (Raw)
├── broker_master_data_2025_06_28_13_14_33
├── contract_notes_data_2025_06_28_13_14_33
├── cash_capital_flow_data_2025_06_28_13_14_33
└── custody_data_2025_06_28_13_14_33
```

### **Step 2: Processing (Tier 1 → Tier 2)**
```
MongoDB Collections → PostgreSQL Tables

Smart Routing:
├── broker_master_data → brokers
├── contract_notes_data → contract_notes  
├── cash_capital_flow_data → cash_capital_flow
├── stock_capital_flow_data → stock_capital_flow
├── mf_allocation_data → mf_allocations
└── custody_data → unified_custody_master_YYYY_MM_DD
```

### **Step 3: Business Use (Tier 2)**
```
PostgreSQL Tables → Reports & Analytics
├── Daily custody positions from date-specific tables
├── Transaction analysis from unified transaction tables
├── Client portfolios across all brokers
└── Risk & compliance reporting
```

---

## 🎯 **Processing Intelligence**

### **Smart File Type Detection**
The system automatically detects file types from collection names:

```javascript
// Master Data Detection
broker_master_data → Broker Master Processor
client_info_data → Client Master Processor
distributor_master_data → Distributor Master Processor

// Transaction Data Detection  
contract_notes_data → Contract Notes Processor
cash_capital_flow_data → Cash Flow Processor
stock_capital_flow_data → Stock Flow Processor

// Custody Data Detection
axis_custody_data → Custody Processor (Axis)
hdfc_custody_data → Custody Processor (HDFC)
```

### **Field Mapping Strategy**
Each processor handles different field structures:

```
Master Data: broker_code, broker_name, contact_person...
Transaction Data: transaction_id, client_code, amount...
Custody Data: client_reference, instrument_isin, total_position...
```

---

## 📈 **Current Performance**

### **✅ What's Working**
1. **Upload Pipeline**: 100% functional
2. **Collection Naming**: Timestamped versions working
3. **File Detection**: Smart routing operational  
4. **Table Creation**: PostgreSQL tables ready
5. **Multi-threading**: Parallel processing active

### **⚠️ What Needs Optimization**
1. **Field Validation**: Refinement needed for higher success rates
2. **Error Handling**: Better validation messages
3. **Data Quality**: Some records failing validation

### **🎯 Next Steps**
1. Refine field validation rules
2. Add more flexible field mapping
3. Improve error messages
4. Add data quality reports

---

## 🔍 **System Verification**

**Test File Upload**: ✅ Working
```
test_broker_master.csv → broker_master_data_2025_06_28_13_14_33
✅ 3 records stored in MongoDB
✅ Correct collection naming
✅ Smart type detection: 'broker_master' → 'brokers' table
```

**Processing Test**: ✅ Routing Correctly
```
🎯 Worker 1: Detected type 'stock_flow' → target table 'enso_stock_capital_flow'
✅ Smart file processor working
✅ Correct table routing
⚠️ Field validation needs refinement
```

---

**Architecture Status**: ✅ **OPERATIONAL WITH OPTIMIZATIONS NEEDED**
**Last Updated**: 2025-01-15 14:30:00 

# Database Management System Architecture

## System Overview
This system provides a comprehensive database management solution with unified dashboard capabilities, multi-database support, and intelligent file processing.

## Orbis Corrections Implementation
**CRITICAL: Orbis custody files require specific data corrections due to system limitations:**

### 🔧 **Applied Corrections:**
1. **`client_name`**: Always set to `"N/A"` (Orbis doesn't provide proper client names)
2. **`instrument_name`**: Always set to `NULL` (Orbis doesn't have standard instrument names)  
3. **`instrument_code`**: Always set to `NULL` (Orbis doesn't provide standard instrument codes)

### 📍 **Implementation Locations:**
- **Primary**: `custody-normalization/extractors/fieldMapper.js` (lines 24-44)
- **Secondary**: `custody-normalization/config/custody-mappings.js` (`normalizeOrbisRecord` function)
- **Applied in**: Multi-threaded ETL, Smart File Processor, Legacy processors

### ✅ **Validation:**
- System validates that Orbis records have exactly these values
- Processing fails if Orbis corrections are not properly applied
- All processing pipelines consistently apply these corrections

**Note**: Orbis-specific fields (BSE Code, Asset Class, Description) are preserved separately but not mapped to standard fields.

## Core Components 