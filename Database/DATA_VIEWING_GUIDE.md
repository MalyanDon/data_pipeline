# 📊 Data Viewing Hierarchy Guide

## 🎯 **How to View Your Data at Different Levels**

### **Level 1: Dashboard UI (Easiest)** 🌐
**Access**: Open browser → `http://localhost:3006`

#### **What You Can See:**
```
📤 Upload Status
├── Files uploaded successfully
├── Collection names created
├── Record counts
└── Upload timestamps

📋 Collection Overview  
├── MongoDB collections list
├── Record counts per collection
├── Data type detection
└── Processing status

⚡ Processing Progress
├── Real-time worker status
├── Success/error rates
├── Processing speed
└── Completion percentage

📊 Success Metrics
├── Overall success rate
├── Valid vs error records
├── Processing time
└── Data quality metrics
```

---

### **Level 2: Database Direct Access** 🔧

#### **MongoDB Atlas (Raw Data)** 🍃
**Purpose**: View uploaded files as-is
**Access**: MongoDB Atlas Dashboard

```
📁 Collections Structure:
financial_data_2025/
├── broker_master_data_2025_06_28_13_14_33
│   ├── Original Excel/CSV data
│   ├── Upload metadata
│   └── 29 documents
├── cash_capital_flow_data_2025_06_28_14_15_22
│   ├── Transaction records
│   └── 2 documents
├── contract_notes_data_2025_06_28_14_16_45
│   ├── Contract confirmations
│   └── 6 documents
└── client_info_data_2025_06_28_14_17_12
    ├── Client master data
    └── 119 documents
```

**How to View:**
1. Login to MongoDB Atlas
2. Browse Collections
3. Click collection name
4. View documents in JSON format

#### **PostgreSQL (Clean Data)** 🐘
**Purpose**: View processed, normalized business data
**Access**: Database client (pgAdmin, DBeaver, psql)

```sql
-- Connection Details:
Host: localhost
Port: 5432
Database: custody_system
User: postgres
Password: postgres
```

**Table Hierarchy:**
```
🏢 Master Data Tables (5)
├── SELECT * FROM brokers;           -- Broker information
├── SELECT * FROM clients;           -- Client master data
├── SELECT * FROM distributors;      -- Distributor details
├── SELECT * FROM strategies;        -- Investment strategies
└── SELECT * FROM securities;        -- Instrument master

💰 ENSO Transaction Tables (4)
├── SELECT * FROM enso_contract_notes;      -- Trade confirmations
├── SELECT * FROM enso_cash_capital_flow;   -- Cash movements
├── SELECT * FROM enso_stock_capital_flow;  -- Stock transfers
└── SELECT * FROM enso_mf_allocations;      -- MF transactions

🏦 Custody Holdings Tables (2)
├── SELECT * FROM unified_custody_master;           -- Main holdings
└── SELECT * FROM unified_custody_master_2025_06_28; -- Daily snapshot

📈 Operational Tables (4)
├── SELECT * FROM custody_holdings;  -- Position tracking
├── SELECT * FROM capital_flows;     -- Fund movements
├── SELECT * FROM trades;            -- Execution records
└── SELECT * FROM mf_allocations;    -- MF tracking

📝 System Table (1)
└── SELECT * FROM raw_uploads;       -- Upload audit trail
```

---

### **Level 3: API Access (Programmatic)** 🚀

#### **REST API Endpoints:**
```bash
# Upload Status
curl http://localhost:3006/api/collections

# Latest Collections by Type
curl http://localhost:3006/api/latest-collections

# PostgreSQL Data
curl http://localhost:3006/api/postgresql-data

# Upload Files
curl -X POST -F "files=@myfile.csv" http://localhost:3006/api/upload
```

#### **Response Examples:**
```json
{
  "success": true,
  "latestCollections": {
    "broker_master_data": {
      "collectionName": "broker_master_data_2025_06_28_13_14_33",
      "timestamp": "2025_06_28_13_14_33",
      "database": "financial_data_2025"
    }
  }
}
```

---

### **Level 4: System Files** 📁

#### **Local File Structure:**
```
Database/
├── temp_uploads/           -- Temporary file storage
├── temp_processing/        -- Processing workspace
├── logs/                   -- System logs
│   ├── upload.log
│   ├── processing.log
│   └── error.log
├── TEST_RESULTS.md         -- Test documentation
├── SYSTEM_ARCHITECTURE.md  -- System overview
└── DATA_VIEWING_GUIDE.md   -- This guide
```

---

## 🔍 **Data Flow Visualization**

### **Tier 1 → Tier 2 Flow:**
```
📁 Your Files
    ↓
🍃 MongoDB (Raw)
    ├── broker_master_data_2025_06_28_13_14_33
    ├── cash_capital_flow_data_2025_06_28_14_15_22
    └── contract_notes_data_2025_06_28_14_16_45
    ↓ (Smart Processing)
🐘 PostgreSQL (Clean)
    ├── brokers table (3 records)
    ├── enso_cash_capital_flow table (2 records)
    └── enso_contract_notes table (6 records)
    ↓
📊 Business Applications
    ├── Reports & Analytics
    ├── Risk Management
    └── Client Portal
```

---

## 🎯 **Recommended Viewing Approach**

### **For Daily Use:**
1. **Start with Dashboard**: `http://localhost:3006`
2. **Monitor Progress**: Real-time processing status
3. **Check Results**: PostgreSQL table data

### **For Data Analysis:**
1. **Connect to PostgreSQL**: Use SQL queries
2. **Join Tables**: Cross-reference data
3. **Export Reports**: Business intelligence tools

### **For Troubleshooting:**
1. **Check MongoDB**: Raw data integrity
2. **Review Logs**: Error analysis
3. **API Testing**: Programmatic access

### **For Auditing:**
1. **Version History**: Timestamped collections
2. **Upload Trail**: raw_uploads table
3. **Processing Logs**: System audit trail

---

## 📱 **Quick Access Commands**

```bash
# View PostgreSQL Tables
psql -d "host=localhost port=5432 dbname=custody_system user=postgres password=postgres" -c "\dt"

# Count Records in All Tables
psql -d "host=localhost port=5432 dbname=custody_system user=postgres password=postgres" -c "
SELECT schemaname,tablename,n_tup_ins as record_count 
FROM pg_stat_user_tables 
ORDER BY n_tup_ins DESC;"

# Check Dashboard Status
curl -s http://localhost:3006/api/status

# View Latest Collections
curl -s http://localhost:3006/api/latest-collections | jq
```

---

**Your data is accessible at 4 different levels - choose the one that fits your needs!** 🎯 