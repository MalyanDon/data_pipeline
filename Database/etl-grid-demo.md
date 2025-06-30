# 🎯 ETL Visual Grid Mapping System - Demo Guide

## 🚀 **System Overview**

Your **ETL Visual Grid Mapping System** is now running at **http://localhost:3001**

This system provides a powerful visual interface for mapping different source columns from each custody file to unified column names, enabling flexible data consolidation across multiple file formats.

## 📊 **Current Custody Files Detected**

Based on your MongoDB data, the system will show:

1. **HDFC File** - 53 records with custody data
2. **AXIS File** - 100+ records with portfolio holdings  
3. **BROKER_MASTER File** - 29 records with broker information
4. **CONTRACT_NOTE File** - Transaction details

## 🎯 **Key Features Demonstrated**

### **1. Visual Grid Layout**
```
┌─────────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│ Unified Column Name │     HDFC        │     AXIS        │  BROKER_MASTER  │ CONTRACT_NOTE   │
├─────────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ [client_name      ] │ [Client Name   ▼]│ [ClientName    ▼]│ [Name          ▼]│ [NULL         ▼]│
│ [account_balance  ] │ [NULL          ▼]│ [NetBalance    ▼]│ [NULL          ▼]│ [Net Amount   ▼]│
│ [security_name    ] │ [Logical Hold..▼]│ [SecurityName  ▼]│ [NULL          ▼]│ [NULL         ▼]│
└─────────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

### **2. Column Mapping Options**
- **📄 Direct Column Mapping**: Select actual columns from each file
- **⚪ NULL Handling**: When a file doesn't have the needed column
- **✏️ Custom Values**: Set default values (e.g., "UNKNOWN", "0.00")

### **3. Data Type Selection**
- **📝 Text (VARCHAR)**: Names, descriptions, codes
- **🔢 Number (DECIMAL)**: Balances, amounts, quantities  
- **📅 Date**: Transaction dates, timestamps
- **🔢 Integer**: IDs, counts
- **✅ Boolean**: True/false flags

## 🛠️ **Step-by-Step Demo Workflow**

### **Step 1: Access the System**
```bash
# Open your browser and navigate to:
http://localhost:3001
```

### **Step 2: Create Your First Unified Column**
1. Click **"➕ Add New Unified Column"**
2. Enter name: `client_name`
3. Select type: **📝 Text**

### **Step 3: Map Source Columns**
For the `client_name` unified column:
- **HDFC dropdown**: Select `Client Name`
- **AXIS dropdown**: Select `ClientName` 
- **BROKER_MASTER dropdown**: Select `Name`
- **CONTRACT_NOTE dropdown**: Select `⚪ NULL` (no client name in this file)

### **Step 4: Add More Columns**
Create additional mappings:

**Account Balance Mapping:**
- Unified Column: `account_balance` (🔢 Number)
- HDFC: `⚪ NULL`
- AXIS: `NetBalance`
- BROKER_MASTER: `⚪ NULL`
- CONTRACT_NOTE: `Net Amount`

**Security Information:**
- Unified Column: `security_name` (📝 Text)
- HDFC: `Logical Holding Saleable Report`
- AXIS: `SecurityName`
- BROKER_MASTER: `✏️ Custom Value` → "N/A"
- CONTRACT_NOTE: `✏️ Custom Value` → "TRANSACTION"

### **Step 5: Save Template**
1. Enter template name: `"Daily Custody Mapping v1"`
2. Click **💾 Save Template**

### **Step 6: Execute ETL**
1. Click **🚀 Execute ETL Pipeline**
2. System creates unified table: `unified_custody_2025_01_20`
3. Processes all mapped data according to your configuration

## 📈 **Example Mapping Results**

### **Before (Separate Files):**

**HDFC Data:**
```json
{
  "Client Name": "ENSO CAPITAL PRIVATE LIMITED",
  "Logical Holding Saleable Report": "BCPL140"
}
```

**AXIS Data:**
```json
{
  "ClientName": "HARISH KRISHNAN WARRIER- INR",
  "SecurityName": "Hindustan Aeronautics Limited - INR",
  "NetBalance": 36
}
```

### **After (Unified Table):**
```sql
unified_custody_2025_01_20:
┌─────────────────────────────┬─────────────────────────────┬─────────────────┬─────────────┐
│ client_name                 │ security_name               │ account_balance │ source_file │
├─────────────────────────────┼─────────────────────────────┼─────────────────┼─────────────┤
│ ENSO CAPITAL PRIVATE LTD    │ BCPL140                     │ NULL            │ hdfc_eod... │
│ HARISH KRISHNAN WARRIER-INR │ Hindustan Aeronautics...    │ 36              │ axis_eod... │
└─────────────────────────────┴─────────────────────────────┴─────────────────┴─────────────┘
```

## 🎯 **Advanced Features**

### **1. Real-Time Validation**
- ✅ Green tags: Successfully mapped columns
- ⚪ Orange tags: NULL values  
- 💬 Blue tags: Custom default values
- ❌ Red tags: Unmapped (need attention)

### **2. Progress Tracking**
- **Total Columns**: Number of unified columns defined
- **Complete Mappings**: Fully configured column mappings
- **Progress Bar**: Visual completion percentage

### **3. Template Management**
- Save multiple mapping configurations
- Reuse templates for daily processing
- Export mapping configurations

## 🔧 **API Integration**

### **Get File Schemas**
```bash
curl "http://localhost:3001/api/custody/file-schemas"
```

### **Save Mapping Template**
```bash
curl -X POST "http://localhost:3001/api/mappings/save-grid" \
  -H "Content-Type: application/json" \
  -d '{
    "templateName": "Daily Mapping v1",
    "columnMappings": [...]
  }'
```

### **Execute ETL Pipeline**
```bash
curl -X POST "http://localhost:3001/api/pipeline/execute-grid" \
  -H "Content-Type: application/json" \
  -d '{
    "mappingConfig": [...],
    "targetTable": "unified_custody_2025_01_20"
  }'
```

## 📊 **Real Business Use Cases**

### **Use Case 1: Client Identification**
Map different client name formats across custody systems:
- HDFC: `"Client Name"`
- AXIS: `"ClientName"`  
- Deutsche Bank: `"Customer_Name"`
- KOTAK: `"CLIENT_NAME"`

### **Use Case 2: Balance Consolidation**
Combine various balance types:
- Portfolio values, market values, net balances
- Handle missing data with defaults
- Standardize decimal formatting

### **Use Case 3: Security Master Data**
Unify security identification:
- ISIN codes, security names, symbols
- Map exchange-specific identifiers
- Default values for missing references

## 🚀 **Production Benefits**

1. **⚡ Fast Setup**: Visual interface eliminates coding
2. **🔄 Reusable**: Save templates for daily processing
3. **🛡️ Robust**: Handle missing columns gracefully
4. **📈 Scalable**: Add new custody systems easily
5. **👁️ Transparent**: Visual mapping shows data flow
6. **🔍 Auditable**: Source file tracking in output

## 📝 **Next Steps**

1. **Test the Interface**: Navigate to http://localhost:3001
2. **Create Sample Mappings**: Try the demo workflow above
3. **Execute ETL**: Process your actual custody data
4. **Verify Results**: Check PostgreSQL unified tables
5. **Scale Up**: Add more custody systems as needed

Your ETL Visual Grid Mapping System is now ready for production use! 🎉 

## 🚀 **System Overview**

Your **ETL Visual Grid Mapping System** is now running at **http://localhost:3001**

This system provides a powerful visual interface for mapping different source columns from each custody file to unified column names, enabling flexible data consolidation across multiple file formats.

## 📊 **Current Custody Files Detected**

Based on your MongoDB data, the system will show:

1. **HDFC File** - 53 records with custody data
2. **AXIS File** - 100+ records with portfolio holdings  
3. **BROKER_MASTER File** - 29 records with broker information
4. **CONTRACT_NOTE File** - Transaction details

## 🎯 **Key Features Demonstrated**

### **1. Visual Grid Layout**
```
┌─────────────────────┬─────────────────┬─────────────────┬─────────────────┬─────────────────┐
│ Unified Column Name │     HDFC        │     AXIS        │  BROKER_MASTER  │ CONTRACT_NOTE   │
├─────────────────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ [client_name      ] │ [Client Name   ▼]│ [ClientName    ▼]│ [Name          ▼]│ [NULL         ▼]│
│ [account_balance  ] │ [NULL          ▼]│ [NetBalance    ▼]│ [NULL          ▼]│ [Net Amount   ▼]│
│ [security_name    ] │ [Logical Hold..▼]│ [SecurityName  ▼]│ [NULL          ▼]│ [NULL         ▼]│
└─────────────────────┴─────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

### **2. Column Mapping Options**
- **📄 Direct Column Mapping**: Select actual columns from each file
- **⚪ NULL Handling**: When a file doesn't have the needed column
- **✏️ Custom Values**: Set default values (e.g., "UNKNOWN", "0.00")

### **3. Data Type Selection**
- **📝 Text (VARCHAR)**: Names, descriptions, codes
- **🔢 Number (DECIMAL)**: Balances, amounts, quantities  
- **📅 Date**: Transaction dates, timestamps
- **🔢 Integer**: IDs, counts
- **✅ Boolean**: True/false flags

## 🛠️ **Step-by-Step Demo Workflow**

### **Step 1: Access the System**
```bash
# Open your browser and navigate to:
http://localhost:3001
```

### **Step 2: Create Your First Unified Column**
1. Click **"➕ Add New Unified Column"**
2. Enter name: `client_name`
3. Select type: **📝 Text**

### **Step 3: Map Source Columns**
For the `client_name` unified column:
- **HDFC dropdown**: Select `Client Name`
- **AXIS dropdown**: Select `ClientName` 
- **BROKER_MASTER dropdown**: Select `Name`
- **CONTRACT_NOTE dropdown**: Select `⚪ NULL` (no client name in this file)

### **Step 4: Add More Columns**
Create additional mappings:

**Account Balance Mapping:**
- Unified Column: `account_balance` (🔢 Number)
- HDFC: `⚪ NULL`
- AXIS: `NetBalance`
- BROKER_MASTER: `⚪ NULL`
- CONTRACT_NOTE: `Net Amount`

**Security Information:**
- Unified Column: `security_name` (📝 Text)
- HDFC: `Logical Holding Saleable Report`
- AXIS: `SecurityName`
- BROKER_MASTER: `✏️ Custom Value` → "N/A"
- CONTRACT_NOTE: `✏️ Custom Value` → "TRANSACTION"

### **Step 5: Save Template**
1. Enter template name: `"Daily Custody Mapping v1"`
2. Click **💾 Save Template**

### **Step 6: Execute ETL**
1. Click **🚀 Execute ETL Pipeline**
2. System creates unified table: `unified_custody_2025_01_20`
3. Processes all mapped data according to your configuration

## 📈 **Example Mapping Results**

### **Before (Separate Files):**

**HDFC Data:**
```json
{
  "Client Name": "ENSO CAPITAL PRIVATE LIMITED",
  "Logical Holding Saleable Report": "BCPL140"
}
```

**AXIS Data:**
```json
{
  "ClientName": "HARISH KRISHNAN WARRIER- INR",
  "SecurityName": "Hindustan Aeronautics Limited - INR",
  "NetBalance": 36
}
```

### **After (Unified Table):**
```sql
unified_custody_2025_01_20:
┌─────────────────────────────┬─────────────────────────────┬─────────────────┬─────────────┐
│ client_name                 │ security_name               │ account_balance │ source_file │
├─────────────────────────────┼─────────────────────────────┼─────────────────┼─────────────┤
│ ENSO CAPITAL PRIVATE LTD    │ BCPL140                     │ NULL            │ hdfc_eod... │
│ HARISH KRISHNAN WARRIER-INR │ Hindustan Aeronautics...    │ 36              │ axis_eod... │
└─────────────────────────────┴─────────────────────────────┴─────────────────┴─────────────┘
```

## 🎯 **Advanced Features**

### **1. Real-Time Validation**
- ✅ Green tags: Successfully mapped columns
- ⚪ Orange tags: NULL values  
- 💬 Blue tags: Custom default values
- ❌ Red tags: Unmapped (need attention)

### **2. Progress Tracking**
- **Total Columns**: Number of unified columns defined
- **Complete Mappings**: Fully configured column mappings
- **Progress Bar**: Visual completion percentage

### **3. Template Management**
- Save multiple mapping configurations
- Reuse templates for daily processing
- Export mapping configurations

## 🔧 **API Integration**

### **Get File Schemas**
```bash
curl "http://localhost:3001/api/custody/file-schemas"
```

### **Save Mapping Template**
```bash
curl -X POST "http://localhost:3001/api/mappings/save-grid" \
  -H "Content-Type: application/json" \
  -d '{
    "templateName": "Daily Mapping v1",
    "columnMappings": [...]
  }'
```

### **Execute ETL Pipeline**
```bash
curl -X POST "http://localhost:3001/api/pipeline/execute-grid" \
  -H "Content-Type: application/json" \
  -d '{
    "mappingConfig": [...],
    "targetTable": "unified_custody_2025_01_20"
  }'
```

## 📊 **Real Business Use Cases**

### **Use Case 1: Client Identification**
Map different client name formats across custody systems:
- HDFC: `"Client Name"`
- AXIS: `"ClientName"`  
- Deutsche Bank: `"Customer_Name"`
- KOTAK: `"CLIENT_NAME"`

### **Use Case 2: Balance Consolidation**
Combine various balance types:
- Portfolio values, market values, net balances
- Handle missing data with defaults
- Standardize decimal formatting

### **Use Case 3: Security Master Data**
Unify security identification:
- ISIN codes, security names, symbols
- Map exchange-specific identifiers
- Default values for missing references

## 🚀 **Production Benefits**

1. **⚡ Fast Setup**: Visual interface eliminates coding
2. **🔄 Reusable**: Save templates for daily processing
3. **🛡️ Robust**: Handle missing columns gracefully
4. **📈 Scalable**: Add new custody systems easily
5. **👁️ Transparent**: Visual mapping shows data flow
6. **🔍 Auditable**: Source file tracking in output

## 📝 **Next Steps**

1. **Test the Interface**: Navigate to http://localhost:3001
2. **Create Sample Mappings**: Try the demo workflow above
3. **Execute ETL**: Process your actual custody data
4. **Verify Results**: Check PostgreSQL unified tables
5. **Scale Up**: Add more custody systems as needed

Your ETL Visual Grid Mapping System is now ready for production use! 🎉 