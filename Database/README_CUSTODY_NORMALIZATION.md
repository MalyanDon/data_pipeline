# 🏦 Custody File Normalization System

A comprehensive system for normalizing and standardizing custody files from different financial institutions into a unified PostgreSQL database.

## 📋 Overview

This system extracts 5 key fields from various custody file formats and normalizes them into a standardized PostgreSQL table:

- **client_reference** - Standardized client reference code
- **client_name** - Normalized client name  
- **instrument_isin** - ISIN code of financial instruments
- **instrument_name** - Name of financial instruments
- **instrument_code** - Additional instrument codes (BSE, NSE, etc.)

## 🏗️ System Architecture

```
📁 custody-normalization/
├── 🔧 config/
│   ├── custody-mappings.js      # File type detection & field mappings
│   └── normalization-schema.js  # PostgreSQL schema & validation
├── 📖 extractors/
│   ├── custodyFileReader.js     # Read Excel/CSV files with different formats
│   ├── fieldMapper.js           # Map varying column names to standard fields
│   └── dataNormalizer.js        # Clean & validate data
├── 💾 loaders/
│   └── postgresLoader.js        # Batch upsert to PostgreSQL
└── 🔄 pipeline/
    └── custodyNormalizationPipeline.js  # Main orchestration

📡 api/custody/
└── normalizationControl.js      # REST API endpoints

⚡ scripts/
├── process-custody-files.js     # Command line processing
└── test-normalization.js        # Testing & validation
```

## 🗂️ Supported File Types

| System | File Pattern | Format | Headers Row | Example |
|--------|-------------|--------|-------------|---------|
| **Axis** | `axis.*eod.*custody` | .xlsx | 1 | `axis_eod_custody_2025-06-25.xlsx` |
| **Deutsche** | `DL_.*EC\d+` | .xlsx | 8 | `DL_164_EC0000720_25_06_2025.xlsx` |
| **Trust PMS** | `End_Client_Holding.*TRUSTPMS` | .xls | 1 | `End_Client_Holding_TRUSTPMS_2025.xls` |
| **HDFC** | `hdfc.*eod.*custody` | .csv | 15 | `hdfc_eod_custody_2025-06-25.csv` |
| **Kotak** | `kotak.*eod.*custody` | .xlsx | 1 | `kotak_eod_custody_2025-06-25.xlsx` |
| **Orbis** | `orbisCustody\|orbis.*custody` | .xlsx | 1 | `orbisCustody25_06_2025.xlsx` |

## 🗺️ Field Mappings

| Standard Field | Axis | Deutsche | Trust PMS | HDFC | Kotak | Orbis |
|---------------|------|----------|-----------|------|-------|-------|
| **client_reference** | UCC | Client Code | Client Code | Client Code | Cln Code | OFIN Code |
| **client_name** | ClientName | Master Name | Client Name | Client Name | Cln Name | Description |
| **instrument_isin** | ISIN | ISIN | Instrument ISIN | ISIN Code | Instr ISIN | ISIN |
| **instrument_name** | SecurityName | Instrument Name | Instrument Name | Instrument Name | Instr Name | Instrument Name |
| **instrument_code** | Security Code | Instrument Code | Instrument Code | Instrument Code | Instr Code | BSE Code |

## 🚀 Quick Start

### 1️⃣ Initialize Database
```bash
# Initialize PostgreSQL schema
npm run init-custody-db
# OR
node scripts/process-custody-files.js --init-db
```

### 2️⃣ Start API Server
```bash
# Start the API server on port 3003
npm run custody-api

# For development with auto-restart
npm run custody-api-dev
```

### 3️⃣ Test System
```bash
# Run comprehensive tests
npm run test-custody
# OR
node scripts/test-normalization.js
```

## 📄 Processing Files

### Command Line
```bash
# Process all files in a directory
node scripts/process-custody-files.js --directory ./custody_files

# Process single file
node scripts/process-custody-files.js --file axis_custody.xlsx

# Preview file without processing
node scripts/process-custody-files.js --preview --file custody.xlsx

# Process with custom date
node scripts/process-custody-files.js --file custody.xlsx --date 2025-06-25

# Get database statistics
npm run custody-stats
```

### REST API

#### Initialize Database
```bash
curl -X POST http://localhost:3003/api/custody/init-database
```

#### Upload & Process Files
```bash
curl -X POST http://localhost:3003/api/custody/upload-and-process \
  -F "files=@axis_custody.xlsx" \
  -F "recordDate=2025-06-25"
```

#### Query Unified Data
```bash
# Get all data
curl "http://localhost:3003/api/custody/unified-data?limit=10"

# Filter by source system
curl "http://localhost:3003/api/custody/unified-data?source_system=AXIS"

# Search by client name
curl "http://localhost:3003/api/custody/unified-data?client_name_search=CLIENT"

# Get client's instruments
curl "http://localhost:3003/api/custody/client/CLIENT123"

# Get instrument holders
curl "http://localhost:3003/api/custody/instrument/US1234567890"
```

## 📊 Database Schema

### Unified Custody Master Table
```sql
CREATE TABLE unified_custody_master (
  id SERIAL PRIMARY KEY,
  client_reference VARCHAR(50) NOT NULL,
  client_name VARCHAR(200) NOT NULL,
  instrument_isin VARCHAR(20) NOT NULL,
  instrument_name VARCHAR(300) NOT NULL,
  instrument_code VARCHAR(100),
  source_system VARCHAR(20) NOT NULL,
  file_name VARCHAR(255) NOT NULL,
  record_date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  
  UNIQUE (client_reference, instrument_isin, source_system, record_date)
);
```

### Indexes
- `idx_client_instrument` - Fast client-instrument lookups
- `idx_instrument_isin` - Fast ISIN searches
- `idx_client_reference` - Fast client searches
- `idx_source_date` - Fast source system & date queries

## 🔧 Data Normalization Rules

### Client Reference
- Convert to uppercase
- Remove special characters (keep only A-Z, 0-9, _, -)
- Trim whitespace

### Client Name
- Convert to uppercase
- Replace multiple spaces with single space
- Trim whitespace

### ISIN Validation
- Must be 12 characters: 2 letters + 10 alphanumeric
- Convert to uppercase
- Remove spaces and special characters

### Instrument Name
- Capitalize first letter of each word
- Replace multiple spaces with single space
- Trim whitespace

### Instrument Code
- Convert to uppercase
- Remove spaces
- Optional field

## 🔗 API Endpoints

### Core Processing
- `POST /api/custody/init-database` - Initialize schema
- `POST /api/custody/upload-and-process` - Upload & process files
- `POST /api/custody/process-directory` - Process directory of files
- `POST /api/custody/process-file/:filename` - Process single file
- `GET /api/custody/preview/:filename` - Preview file without processing

### Data Queries
- `GET /api/custody/unified-data` - Query with filters
- `GET /api/custody/client/:clientRef` - Get client's instruments
- `GET /api/custody/instrument/:isin` - Get instrument holders
- `GET /api/custody/stats` - Database statistics

### Configuration
- `GET /api/custody/mappings/:custodyType` - Get field mappings
- `GET /api/custody/health` - Health check

## 🧪 Testing

### Automated Tests
```bash
npm run test-custody
```

Tests include:
- ✅ Database connectivity
- ✅ Field mappings for all custody types
- ✅ File type detection
- ✅ Sample file processing
- ✅ Data validation rules

### Manual Testing
```bash
# Preview files
node scripts/process-custody-files.js --preview --file sample.xlsx

# Process without loading to database
node scripts/process-custody-files.js --file sample.xlsx --skip-loading
```

## 📈 Monitoring & Statistics

### Get Statistics
```bash
# Command line
npm run custody-stats

# API
curl "http://localhost:3003/api/custody/stats"
```

Returns:
- Total records by source system
- Date ranges
- Latest data by source
- Record counts

## 🔍 Data Quality Features

### Validation
- Required field validation
- ISIN format validation
- Date validation (no future dates)
- Source system validation

### Data Quality Reports
- Field completeness percentages
- Data quality scores
- Recommendations for improvement

### Error Handling
- Detailed error logging
- Graceful handling of malformed files
- Rollback capability for batch operations

## 🚨 Error Handling

### File Processing Errors
- Invalid file formats
- Missing required fields
- Malformed data
- Date extraction failures

### Database Errors
- Connection failures
- Constraint violations
- Batch operation failures

### API Errors
- File upload errors
- Validation failures
- Authentication issues

## 🔧 Configuration

### Environment Variables
```bash
# PostgreSQL Connection (in config.js)
POSTGRES_CONNECTION_STRING="postgresql://user:password@host:port/database"

# API Server Port
PORT=3003
```

### Custom Field Mappings
Edit `custody-normalization/config/custody-mappings.js` to:
- Add new custody systems
- Modify field mappings
- Update file detection patterns
- Configure processing rules

## 📝 Usage Examples

### Process Custody Files from MongoDB
```javascript
// Extract custody files from existing MongoDB and process
const pipeline = new CustodyNormalizationPipeline();

// From your existing MongoDB files
const files = ['axis_eod_custody_2025-06-25.xlsx', 'hdfc_custody.csv'];

for (const file of files) {
  const result = await pipeline.processFile(`./temp_uploads/${file}`, {
    recordDate: '2025-06-25'
  });
  console.log(`${file}: ${result.success ? 'Success' : 'Failed'}`);
}
```

### Query Unified Data
```javascript
const pipeline = new CustodyNormalizationPipeline();

// Get all Axis data
const axisData = await pipeline.queryData(
  { source_system: 'AXIS' },
  { limit: 100, sortField: 'record_date', sortOrder: 'desc' }
);

// Get client's holdings
const clientHoldings = await pipeline.postgresLoader.getInstrumentsByClient('CLIENT123');

// Get who holds an instrument
const instrumentHolders = await pipeline.postgresLoader.getClientsByInstrument('US1234567890');
```

## 🔄 Integration with Existing System

This custody normalization system integrates with your existing MongoDB dashboard:

1. **MongoDB** → Stores raw custody files (as currently working)
2. **Normalization Pipeline** → Extracts, normalizes, and standardizes data
3. **PostgreSQL** → Stores clean, queryable, unified custody data
4. **APIs** → Access both raw and normalized data

### Workflow
```
MongoDB Files → Custody Pipeline → PostgreSQL → APIs → Reports
     ↓                ↓               ↓         ↓        ↓
Raw Storage    Normalization    Clean Data   Queries  Analytics
```

## 📋 Next Steps

1. **Initialize database**: `npm run init-custody-db`
2. **Test with sample files**: `npm run test-custody`
3. **Start API server**: `npm run custody-api`
4. **Process your existing files**: Upload via dashboard or API
5. **Query unified data**: Use APIs to access normalized data

## 🛠️ Troubleshooting

### Common Issues

**PostgreSQL Connection Failed**
```bash
# Check connection string in config.js
# Ensure PostgreSQL is running
# Verify network access
```

**File Detection Failed**
```bash
# Check filename patterns in custody-mappings.js
# Ensure file follows expected naming convention
# Use --preview to see detected fields
```

**Field Mapping Issues**
```bash
# Use preview mode to see available fields
# Check field mappings in custody-mappings.js
# Review validation errors in logs
```

**Low Data Quality Score**
```bash
# Review field completeness in test results
# Check for missing required fields
# Verify ISIN format validity
```

---

## 🎯 System Benefits

✅ **Unified Data Model** - Single table for all custody data  
✅ **Automatic File Detection** - No manual file type specification  
✅ **Smart Field Mapping** - Handles varying column names  
✅ **Data Validation** - Ensures data quality and consistency  
✅ **Batch Processing** - Handles large files efficiently  
✅ **REST APIs** - Easy integration with other systems  
✅ **Command Line Tools** - Flexible processing options  
✅ **Comprehensive Testing** - Validates all components  
✅ **Error Handling** - Graceful handling of issues  
✅ **Performance Optimized** - Indexed queries and batch operations

This system provides a robust foundation for custody data management, ensuring data quality, consistency, and easy access across your financial systems! 🚀 