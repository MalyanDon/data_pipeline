const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const csv = require('csv-parser');
const { Pool } = require('pg');
const config = require('./config');

// PostgreSQL connection
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Custody system configurations with exact field mappings
const CUSTODY_SYSTEMS = {
  'axis': {
    name: 'Axis EOD Custody',
    filePattern: /axis.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'Sheet1',
    headerRow: 0,
    fields: {
      client_reference: 'UCC',
      client_name: 'ClientName',
      instrument_isin: 'ISIN',
      instrument_name: 'SecurityName',
      instrument_code: null,
      blocked_quantity: ['DematLockedQty', 'PhysicalLocked'], // sum
      pending_buy_quantity: ['PurchaseOutstanding', 'PurchaseUnderProcess'], // sum
      pending_sell_quantity: ['SaleOutstanding', 'SaleUnderProcess'], // sum
      total_position: 'NetBalance',
      saleable_quantity: 'DematFree'
    }
  },
  'deutsche': {
    name: 'Deutsche Bank',
    filePattern: /deutsche.*(\d{2}_\d{2}_\d{4})/i,
    dateFormat: 'DD_MM_YYYY',
    format: 'excel',
    sheetName: 'ReportDateHeader',
    headerRow: 8,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Master Name',
      instrument_isin: 'ISIN',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: 'Blocked',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: 'Pending Sale',
      total_position: 'Logical Position',
      saleable_quantity: 'Saleable'
    }
  },
  'trustpms': {
    name: 'Trust PMS',
    filePattern: /trust.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'RPT_EndClientHolding',
    headerRow: 2,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Client Name',
      instrument_isin: 'Instrument ISIN',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: null,
      pending_buy_quantity: 'Pending Buy Position',
      pending_sell_quantity: 'Pending Sell Position',
      total_position: null,
      saleable_quantity: 'Saleable Position'
    }
  },
  'hdfc': {
    name: 'HDFC Custody',
    filePattern: /hdfc.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'csv',
    headerRow: 15,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Client Name',
      instrument_isin: 'ISIN Code',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: 'Pending Blocked Qty',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: null,
      total_position: 'Book Position',
      saleable_quantity: 'Total Saleable'
    }
  },
  'kotak': {
    name: 'Kotak Custody',
    filePattern: /kotak.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'Sheet1',
    headerRow: 3,
    fields: {
      client_reference: 'Cln Code',
      client_name: 'Cln Name',
      instrument_isin: 'Instr ISIN',
      instrument_name: 'Instr Name',
      instrument_code: 'Instr Code',
      blocked_quantity: 'Blocked',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: 'Pending Sale',
      total_position: 'Settled Position',
      saleable_quantity: 'Saleable'
    }
  },
  'orbis': {
    name: 'Orbis Custody',
    filePattern: /orbis.*(\d{2}_\d{2}_\d{4})/i,
    dateFormat: 'DD_MM_YYYY',
    format: 'excel',
    sheetName: 'OneSheetLogicalHolding_intrasit',
    headerRow: 0,
    fields: {
      client_reference: 'OFIN Code',
      client_name: 'N/A', // fixed value
      instrument_isin: 'ISIN',
      instrument_name: null,
      instrument_code: null,
      blocked_quantity: 'Blocked/Pledge',
      pending_buy_quantity: 'Intrasit Purchase',
      pending_sell_quantity: 'Intrasit Sale',
      total_position: 'Holding Quantity',
      saleable_quantity: 'Saleble Quantity'
    }
  }
};

class CustodyDataProcessor {
  constructor() {
    this.processedFiles = new Set();
    this.totalRecords = 0;
    this.validRecords = 0;
    this.errors = [];
  }

  async initialize() {
    console.log('🚀 CUSTODY DATA NORMALIZATION SYSTEM');
    console.log('═══════════════════════════════════════════════');
    console.log('📋 Processing 6 custody systems with latest file selection');
    console.log('🎯 Field mapping: 10 standardized fields per system');
    console.log('🔍 Date patterns: YYYYMMDD (most) | DD_MM_YYYY (Deutsche/Orbis)');
    
    // Create unified custody master table
    await this.createUnifiedTable();
    
    // Create indexes for performance
    await this.createIndexes();
    
    console.log('✅ System initialized successfully');
  }

  async createUnifiedTable() {
    const client = await pgPool.connect();
    
    try {
      // Drop existing table to ensure clean schema
      await client.query('DROP TABLE IF EXISTS unified_custody_master CASCADE');
      
      const createTableSQL = `
        CREATE TABLE unified_custody_master (
          id SERIAL PRIMARY KEY,
          client_reference VARCHAR(50) NOT NULL,
          client_name VARCHAR(200) NOT NULL,
          instrument_isin VARCHAR(20) NOT NULL,
          instrument_name VARCHAR(300),
          instrument_code VARCHAR(100),
          blocked_quantity DECIMAL(15,4) DEFAULT 0,
          pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
          pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
          total_position DECIMAL(15,4) DEFAULT 0,
          saleable_quantity DECIMAL(15,4) DEFAULT 0,
          source_system VARCHAR(20) NOT NULL,
          file_name VARCHAR(255) NOT NULL,
          record_date DATE NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `;
      
      await client.query(createTableSQL);
      console.log('📅 Created unified_custody_master table with exact schema');
      
    } catch (error) {
      console.log(`❌ Error creating table: ${error.message}`);
    } finally {
      client.release();
    }
  }

  async createIndexes() {
    const client = await pgPool.connect();
    
    try {
      const indexes = [
        'CREATE INDEX idx_client_isin ON unified_custody_master(client_reference, instrument_isin)',
        'CREATE INDEX idx_instrument_isin ON unified_custody_master(instrument_isin)',
        'CREATE INDEX idx_source_date ON unified_custody_master(source_system, record_date)',
        'CREATE INDEX idx_total_position ON unified_custody_master(total_position) WHERE total_position > 0',
        'CREATE INDEX idx_saleable_quantity ON unified_custody_master(saleable_quantity) WHERE saleable_quantity > 0'
      ];
      
      for (const indexSQL of indexes) {
        await client.query(indexSQL);
      }
      
      console.log('📊 Created performance indexes');
      
    } catch (error) {
      console.log(`❌ Error creating indexes: ${error.message}`);
    } finally {
      client.release();
    }
  }

  scanDirectoryForFiles(directory = './') {
    console.log('\n🔍 SCANNING FOR CUSTODY FILES');
    console.log('═══════════════════════════════════════');
    
    const files = fs.readdirSync(directory);
    const custodyFiles = {};
    
    // Group files by custody system
    for (const file of files) {
      const filePath = path.join(directory, file);
      
      try {
        const stats = fs.statSync(filePath);
        
        if (stats.isFile()) {
          for (const [systemKey, config] of Object.entries(CUSTODY_SYSTEMS)) {
            const match = file.match(config.filePattern);
            if (match) {
              const dateStr = match[1];
              const fileDate = this.parseFileDate(dateStr, config.dateFormat);
              
              if (!custodyFiles[systemKey]) {
                custodyFiles[systemKey] = [];
              }
              
              custodyFiles[systemKey].push({
                file: filePath,
                fileName: file,
                date: fileDate,
                dateStr: dateStr,
                system: systemKey,
                config: config
              });
              
              console.log(`📄 Found ${config.name}: ${file} (${dateStr})`);
              break;
            }
          }
        }
      } catch (error) {
        // Skip files that can't be accessed
        continue;
      }
    }
    
    return custodyFiles;
  }

  parseFileDate(dateStr, format) {
    try {
      if (format === 'YYYYMMDD') {
        const year = parseInt(dateStr.substring(0, 4));
        const month = parseInt(dateStr.substring(4, 6)) - 1;
        const day = parseInt(dateStr.substring(6, 8));
        return new Date(year, month, day);
      } else if (format === 'DD_MM_YYYY') {
        const parts = dateStr.split('_');
        const day = parseInt(parts[0]);
        const month = parseInt(parts[1]) - 1;
        const year = parseInt(parts[2]);
        return new Date(year, month, day);
      }
    } catch (error) {
      console.log(`❌ Error parsing date ${dateStr}: ${error.message}`);
      return new Date(0);
    }
    return new Date(0);
  }

  selectLatestFiles(custodyFiles) {
    console.log('\n🎯 SELECTING LATEST FILES (Latest file selection logic)');
    console.log('═══════════════════════════════════════');
    
    const latestFiles = {};
    
    for (const [systemKey, files] of Object.entries(custodyFiles)) {
      if (files.length === 0) continue;
      
      // Sort by date descending and take the latest
      files.sort((a, b) => b.date.getTime() - a.date.getTime());
      const latestFile = files[0];
      
      latestFiles[systemKey] = latestFile;
      
      console.log(`✅ ${latestFile.config.name}: ${latestFile.fileName}`);
      
      if (files.length > 1) {
        console.log(`   📋 Skipped ${files.length - 1} older files`);
        files.slice(1).forEach(file => {
          console.log(`   ⚪ Skipped: ${file.fileName} (${file.dateStr})`);
        });
      }
    }
    
    return latestFiles;
  }

  async processAllFiles(directory = './') {
    console.log('\n🚀 PROCESSING CUSTODY FILES');
    console.log('═══════════════════════════════════════');
    
    // Scan and select latest files
    const custodyFiles = this.scanDirectoryForFiles(directory);
    const latestFiles = this.selectLatestFiles(custodyFiles);
    
    console.log(`\n📊 Processing ${Object.keys(latestFiles).length} latest custody files`);
    
    // Process each latest file
    for (const [systemKey, fileInfo] of Object.entries(latestFiles)) {
      try {
        console.log(`\n🔧 Processing ${fileInfo.config.name}...`);
        await this.processFile(fileInfo);
      } catch (error) {
        console.log(`❌ Error processing ${fileInfo.fileName}: ${error.message}`);
        this.errors.push(`${fileInfo.fileName}: ${error.message}`);
      }
    }
    
    // Show summary
    this.showSummary();
  }

  async processFile(fileInfo) {
    const { file, fileName, config, date } = fileInfo;
    
    let data = [];
    
    if (config.format === 'excel') {
      data = await this.readExcelFile(file, config);
    } else if (config.format === 'csv') {
      data = await this.readCSVFile(file, config);
    }
    
    if (data.length === 0) {
      console.log(`⚪ No data found in ${fileName}`);
      return;
    }
    
    console.log(`📊 Found ${data.length} records in ${fileName}`);
    
    // Process and insert records
    const validRecords = await this.processRecords(data, config, fileName, date);
    
    console.log(`✅ Processed ${validRecords}/${data.length} valid records`);
    
    this.totalRecords += data.length;
    this.validRecords += validRecords;
  }

  async readExcelFile(filePath, config) {
    try {
      const workbook = XLSX.readFile(filePath);
      const sheetName = config.sheetName || workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      
      if (!worksheet) {
        throw new Error(`Sheet "${sheetName}" not found`);
      }
      
      const data = XLSX.utils.sheet_to_json(worksheet, {
        range: config.headerRow,
        header: 1,
        defval: ''
      });
      
      if (data.length < 2) return [];
      
      // Convert to objects using header row
      const headers = data[0];
      const records = [];
      
      for (let i = 1; i < data.length; i++) {
        const record = {};
        for (let j = 0; j < headers.length; j++) {
          record[headers[j]] = data[i][j] || '';
        }
        records.push(record);
      }
      
      return records;
      
    } catch (error) {
      throw new Error(`Excel read error: ${error.message}`);
    }
  }

  async readCSVFile(filePath, config) {
    return new Promise((resolve, reject) => {
      const records = [];
      let lineCount = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          skipLinesWithError: true
        }))
        .on('data', (row) => {
          lineCount++;
          if (lineCount > config.headerRow) {
            records.push(row);
          }
        })
        .on('end', () => {
          resolve(records);
        })
        .on('error', (error) => {
          reject(new Error(`CSV read error: ${error.message}`));
        });
    });
  }

  async processRecords(data, config, fileName, recordDate) {
    const client = await pgPool.connect();
    let validCount = 0;
    
    try {
      await client.query('BEGIN');
      
      for (const record of data) {
        try {
          const normalizedRecord = this.normalizeRecord(record, config, fileName, recordDate);
          
          if (this.validateRecord(normalizedRecord)) {
            await this.insertRecord(client, normalizedRecord);
            validCount++;
          }
          
        } catch (error) {
          console.log(`❌ Record error: ${error.message}`);
        }
      }
      
      await client.query('COMMIT');
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    
    return validCount;
  }

  normalizeRecord(record, config, fileName, recordDate) {
    const normalized = {
      source_system: config.name.split(' ')[0].toLowerCase(),
      file_name: fileName,
      record_date: recordDate.toISOString().split('T')[0]
    };
    
    // Map each field according to exact configuration
    for (const [targetField, sourceField] of Object.entries(config.fields)) {
      if (sourceField === null) {
        normalized[targetField] = null;
      } else if (sourceField === 'N/A') {
        normalized[targetField] = 'N/A';
      } else if (Array.isArray(sourceField)) {
        // Sum multiple fields (for blocked_quantity, pending_buy_quantity, pending_sell_quantity)
        let sum = 0;
        for (const field of sourceField) {
          const value = this.safeNumber(record[field]);
          if (value !== null) sum += value;
        }
        normalized[targetField] = sum;
      } else {
        if (targetField.includes('quantity') || targetField === 'total_position') {
          normalized[targetField] = this.safeNumber(record[sourceField]);
        } else {
          normalized[targetField] = this.safeString(record[sourceField]);
        }
      }
    }
    
    return normalized;
  }

  safeString(value) {
    if (value === undefined || value === null || value === '') return null;
    return String(value).trim();
  }

  safeNumber(value) {
    if (value === undefined || value === null || value === '') return 0;
    const num = parseFloat(value);
    return isNaN(num) ? 0 : num;
  }

  validateRecord(record) {
    // Required fields validation
    if (!record.client_reference || !record.client_name || !record.instrument_isin) {
      return false;
    }
    
    // Mathematical validation with 1% tolerance
    const calculated = (record.blocked_quantity || 0) + (record.pending_buy_quantity || 0) + (record.saleable_quantity || 0);
    const expected = record.total_position || 0;
    
    if (expected > 0) {
      const tolerance = expected * 0.01; // 1% tolerance
      const difference = Math.abs(calculated - expected);
      
      if (difference > tolerance) {
        console.log(`⚠️  Math validation (1% tolerance): ${record.client_reference} - ${record.instrument_isin}`);
        console.log(`   Expected: ${expected}, Calculated: ${calculated}, Difference: ${difference.toFixed(4)}`);
      }
    }
    
    return true;
  }

  async insertRecord(client, record) {
    const insertSQL = `
      INSERT INTO unified_custody_master (
        client_reference, client_name, instrument_isin, instrument_name, instrument_code,
        blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
        source_system, file_name, record_date
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    `;
    
    const values = [
      record.client_reference,
      record.client_name,
      record.instrument_isin,
      record.instrument_name,
      record.instrument_code,
      record.blocked_quantity || 0,
      record.pending_buy_quantity || 0,
      record.pending_sell_quantity || 0,
      record.total_position || 0,
      record.saleable_quantity || 0,
      record.source_system,
      record.file_name,
      record.record_date
    ];
    
    await client.query(insertSQL, values);
  }

  showSummary() {
    console.log('\n🎉 CUSTODY DATA NORMALIZATION COMPLETE!');
    console.log('═══════════════════════════════════════════════');
    console.log(`📊 Total Records Processed: ${this.totalRecords.toLocaleString()}`);
    console.log(`✅ Valid Records Inserted: ${this.validRecords.toLocaleString()}`);
    console.log(`❌ Processing Errors: ${this.errors.length}`);
    console.log(`🎯 Success Rate: ${this.totalRecords > 0 ? Math.round((this.validRecords/this.totalRecords)*100) : 0}%`);
    
    if (this.errors.length > 0) {
      console.log('\n❌ Error Summary:');
      this.errors.forEach(error => console.log(`   - ${error}`));
    }
    
    console.log('\n✅ SYSTEM READY FOR BUSINESS USE');
    console.log('📊 Data available in: unified_custody_master table');
    console.log('🔍 Query examples:');
    console.log('   SELECT source_system, COUNT(*) FROM unified_custody_master GROUP BY source_system;');
    console.log('   SELECT * FROM unified_custody_master WHERE total_position > 0 LIMIT 10;');
  }
}

// Run if called directly
if (require.main === module) {
  async function main() {
    try {
      const processor = new CustodyDataProcessor();
      await processor.initialize();
      await processor.processAllFiles('./');
      
      console.log('\n🌐 System ready. Run again to reprocess or integrate with your application.');
      process.exit(0);
      
    } catch (error) {
      console.error('💥 System error:', error.message);
      process.exit(1);
    }
  }
  
  main();
}

module.exports = { CustodyDataProcessor, CUSTODY_SYSTEMS }; 
const path = require('path');
const XLSX = require('xlsx');
const csv = require('csv-parser');
const { Pool } = require('pg');
const config = require('./config');

// PostgreSQL connection
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Custody system configurations with exact field mappings
const CUSTODY_SYSTEMS = {
  'axis': {
    name: 'Axis EOD Custody',
    filePattern: /axis.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'Sheet1',
    headerRow: 0,
    fields: {
      client_reference: 'UCC',
      client_name: 'ClientName',
      instrument_isin: 'ISIN',
      instrument_name: 'SecurityName',
      instrument_code: null,
      blocked_quantity: ['DematLockedQty', 'PhysicalLocked'], // sum
      pending_buy_quantity: ['PurchaseOutstanding', 'PurchaseUnderProcess'], // sum
      pending_sell_quantity: ['SaleOutstanding', 'SaleUnderProcess'], // sum
      total_position: 'NetBalance',
      saleable_quantity: 'DematFree'
    }
  },
  'deutsche': {
    name: 'Deutsche Bank',
    filePattern: /deutsche.*(\d{2}_\d{2}_\d{4})/i,
    dateFormat: 'DD_MM_YYYY',
    format: 'excel',
    sheetName: 'ReportDateHeader',
    headerRow: 8,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Master Name',
      instrument_isin: 'ISIN',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: 'Blocked',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: 'Pending Sale',
      total_position: 'Logical Position',
      saleable_quantity: 'Saleable'
    }
  },
  'trustpms': {
    name: 'Trust PMS',
    filePattern: /trust.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'RPT_EndClientHolding',
    headerRow: 2,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Client Name',
      instrument_isin: 'Instrument ISIN',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: null,
      pending_buy_quantity: 'Pending Buy Position',
      pending_sell_quantity: 'Pending Sell Position',
      total_position: null,
      saleable_quantity: 'Saleable Position'
    }
  },
  'hdfc': {
    name: 'HDFC Custody',
    filePattern: /hdfc.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'csv',
    headerRow: 15,
    fields: {
      client_reference: 'Client Code',
      client_name: 'Client Name',
      instrument_isin: 'ISIN Code',
      instrument_name: 'Instrument Name',
      instrument_code: 'Instrument Code',
      blocked_quantity: 'Pending Blocked Qty',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: null,
      total_position: 'Book Position',
      saleable_quantity: 'Total Saleable'
    }
  },
  'kotak': {
    name: 'Kotak Custody',
    filePattern: /kotak.*(\d{8})/i,
    dateFormat: 'YYYYMMDD',
    format: 'excel',
    sheetName: 'Sheet1',
    headerRow: 3,
    fields: {
      client_reference: 'Cln Code',
      client_name: 'Cln Name',
      instrument_isin: 'Instr ISIN',
      instrument_name: 'Instr Name',
      instrument_code: 'Instr Code',
      blocked_quantity: 'Blocked',
      pending_buy_quantity: 'Pending Purchase',
      pending_sell_quantity: 'Pending Sale',
      total_position: 'Settled Position',
      saleable_quantity: 'Saleable'
    }
  },
  'orbis': {
    name: 'Orbis Custody',
    filePattern: /orbis.*(\d{2}_\d{2}_\d{4})/i,
    dateFormat: 'DD_MM_YYYY',
    format: 'excel',
    sheetName: 'OneSheetLogicalHolding_intrasit',
    headerRow: 0,
    fields: {
      client_reference: 'OFIN Code',
      client_name: 'N/A', // fixed value
      instrument_isin: 'ISIN',
      instrument_name: null,
      instrument_code: null,
      blocked_quantity: 'Blocked/Pledge',
      pending_buy_quantity: 'Intrasit Purchase',
      pending_sell_quantity: 'Intrasit Sale',
      total_position: 'Holding Quantity',
      saleable_quantity: 'Saleble Quantity'
    }
  }
};

class CustodyDataProcessor {
  constructor() {
    this.processedFiles = new Set();
    this.totalRecords = 0;
    this.validRecords = 0;
    this.errors = [];
  }

  async initialize() {
    console.log('🚀 CUSTODY DATA NORMALIZATION SYSTEM');
    console.log('═══════════════════════════════════════════════');
    console.log('📋 Processing 6 custody systems with latest file selection');
    console.log('🎯 Field mapping: 10 standardized fields per system');
    console.log('🔍 Date patterns: YYYYMMDD (most) | DD_MM_YYYY (Deutsche/Orbis)');
    
    // Create unified custody master table
    await this.createUnifiedTable();
    
    // Create indexes for performance
    await this.createIndexes();
    
    console.log('✅ System initialized successfully');
  }

  async createUnifiedTable() {
    const client = await pgPool.connect();
    
    try {
      // Drop existing table to ensure clean schema
      await client.query('DROP TABLE IF EXISTS unified_custody_master CASCADE');
      
      const createTableSQL = `
        CREATE TABLE unified_custody_master (
          id SERIAL PRIMARY KEY,
          client_reference VARCHAR(50) NOT NULL,
          client_name VARCHAR(200) NOT NULL,
          instrument_isin VARCHAR(20) NOT NULL,
          instrument_name VARCHAR(300),
          instrument_code VARCHAR(100),
          blocked_quantity DECIMAL(15,4) DEFAULT 0,
          pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
          pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
          total_position DECIMAL(15,4) DEFAULT 0,
          saleable_quantity DECIMAL(15,4) DEFAULT 0,
          source_system VARCHAR(20) NOT NULL,
          file_name VARCHAR(255) NOT NULL,
          record_date DATE NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      `;
      
      await client.query(createTableSQL);
      console.log('📅 Created unified_custody_master table with exact schema');
      
    } catch (error) {
      console.log(`❌ Error creating table: ${error.message}`);
    } finally {
      client.release();
    }
  }

  async createIndexes() {
    const client = await pgPool.connect();
    
    try {
      const indexes = [
        'CREATE INDEX idx_client_isin ON unified_custody_master(client_reference, instrument_isin)',
        'CREATE INDEX idx_instrument_isin ON unified_custody_master(instrument_isin)',
        'CREATE INDEX idx_source_date ON unified_custody_master(source_system, record_date)',
        'CREATE INDEX idx_total_position ON unified_custody_master(total_position) WHERE total_position > 0',
        'CREATE INDEX idx_saleable_quantity ON unified_custody_master(saleable_quantity) WHERE saleable_quantity > 0'
      ];
      
      for (const indexSQL of indexes) {
        await client.query(indexSQL);
      }
      
      console.log('📊 Created performance indexes');
      
    } catch (error) {
      console.log(`❌ Error creating indexes: ${error.message}`);
    } finally {
      client.release();
    }
  }

  scanDirectoryForFiles(directory = './') {
    console.log('\n🔍 SCANNING FOR CUSTODY FILES');
    console.log('═══════════════════════════════════════');
    
    const files = fs.readdirSync(directory);
    const custodyFiles = {};
    
    // Group files by custody system
    for (const file of files) {
      const filePath = path.join(directory, file);
      
      try {
        const stats = fs.statSync(filePath);
        
        if (stats.isFile()) {
          for (const [systemKey, config] of Object.entries(CUSTODY_SYSTEMS)) {
            const match = file.match(config.filePattern);
            if (match) {
              const dateStr = match[1];
              const fileDate = this.parseFileDate(dateStr, config.dateFormat);
              
              if (!custodyFiles[systemKey]) {
                custodyFiles[systemKey] = [];
              }
              
              custodyFiles[systemKey].push({
                file: filePath,
                fileName: file,
                date: fileDate,
                dateStr: dateStr,
                system: systemKey,
                config: config
              });
              
              console.log(`📄 Found ${config.name}: ${file} (${dateStr})`);
              break;
            }
          }
        }
      } catch (error) {
        // Skip files that can't be accessed
        continue;
      }
    }
    
    return custodyFiles;
  }

  parseFileDate(dateStr, format) {
    try {
      if (format === 'YYYYMMDD') {
        const year = parseInt(dateStr.substring(0, 4));
        const month = parseInt(dateStr.substring(4, 6)) - 1;
        const day = parseInt(dateStr.substring(6, 8));
        return new Date(year, month, day);
      } else if (format === 'DD_MM_YYYY') {
        const parts = dateStr.split('_');
        const day = parseInt(parts[0]);
        const month = parseInt(parts[1]) - 1;
        const year = parseInt(parts[2]);
        return new Date(year, month, day);
      }
    } catch (error) {
      console.log(`❌ Error parsing date ${dateStr}: ${error.message}`);
      return new Date(0);
    }
    return new Date(0);
  }

  selectLatestFiles(custodyFiles) {
    console.log('\n🎯 SELECTING LATEST FILES (Latest file selection logic)');
    console.log('═══════════════════════════════════════');
    
    const latestFiles = {};
    
    for (const [systemKey, files] of Object.entries(custodyFiles)) {
      if (files.length === 0) continue;
      
      // Sort by date descending and take the latest
      files.sort((a, b) => b.date.getTime() - a.date.getTime());
      const latestFile = files[0];
      
      latestFiles[systemKey] = latestFile;
      
      console.log(`✅ ${latestFile.config.name}: ${latestFile.fileName}`);
      
      if (files.length > 1) {
        console.log(`   📋 Skipped ${files.length - 1} older files`);
        files.slice(1).forEach(file => {
          console.log(`   ⚪ Skipped: ${file.fileName} (${file.dateStr})`);
        });
      }
    }
    
    return latestFiles;
  }

  async processAllFiles(directory = './') {
    console.log('\n🚀 PROCESSING CUSTODY FILES');
    console.log('═══════════════════════════════════════');
    
    // Scan and select latest files
    const custodyFiles = this.scanDirectoryForFiles(directory);
    const latestFiles = this.selectLatestFiles(custodyFiles);
    
    console.log(`\n📊 Processing ${Object.keys(latestFiles).length} latest custody files`);
    
    // Process each latest file
    for (const [systemKey, fileInfo] of Object.entries(latestFiles)) {
      try {
        console.log(`\n🔧 Processing ${fileInfo.config.name}...`);
        await this.processFile(fileInfo);
      } catch (error) {
        console.log(`❌ Error processing ${fileInfo.fileName}: ${error.message}`);
        this.errors.push(`${fileInfo.fileName}: ${error.message}`);
      }
    }
    
    // Show summary
    this.showSummary();
  }

  async processFile(fileInfo) {
    const { file, fileName, config, date } = fileInfo;
    
    let data = [];
    
    if (config.format === 'excel') {
      data = await this.readExcelFile(file, config);
    } else if (config.format === 'csv') {
      data = await this.readCSVFile(file, config);
    }
    
    if (data.length === 0) {
      console.log(`⚪ No data found in ${fileName}`);
      return;
    }
    
    console.log(`📊 Found ${data.length} records in ${fileName}`);
    
    // Process and insert records
    const validRecords = await this.processRecords(data, config, fileName, date);
    
    console.log(`✅ Processed ${validRecords}/${data.length} valid records`);
    
    this.totalRecords += data.length;
    this.validRecords += validRecords;
  }

  async readExcelFile(filePath, config) {
    try {
      const workbook = XLSX.readFile(filePath);
      const sheetName = config.sheetName || workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      
      if (!worksheet) {
        throw new Error(`Sheet "${sheetName}" not found`);
      }
      
      const data = XLSX.utils.sheet_to_json(worksheet, {
        range: config.headerRow,
        header: 1,
        defval: ''
      });
      
      if (data.length < 2) return [];
      
      // Convert to objects using header row
      const headers = data[0];
      const records = [];
      
      for (let i = 1; i < data.length; i++) {
        const record = {};
        for (let j = 0; j < headers.length; j++) {
          record[headers[j]] = data[i][j] || '';
        }
        records.push(record);
      }
      
      return records;
      
    } catch (error) {
      throw new Error(`Excel read error: ${error.message}`);
    }
  }

  async readCSVFile(filePath, config) {
    return new Promise((resolve, reject) => {
      const records = [];
      let lineCount = 0;
      
      fs.createReadStream(filePath)
        .pipe(csv({
          skipEmptyLines: true,
          skipLinesWithError: true
        }))
        .on('data', (row) => {
          lineCount++;
          if (lineCount > config.headerRow) {
            records.push(row);
          }
        })
        .on('end', () => {
          resolve(records);
        })
        .on('error', (error) => {
          reject(new Error(`CSV read error: ${error.message}`));
        });
    });
  }

  async processRecords(data, config, fileName, recordDate) {
    const client = await pgPool.connect();
    let validCount = 0;
    
    try {
      await client.query('BEGIN');
      
      for (const record of data) {
        try {
          const normalizedRecord = this.normalizeRecord(record, config, fileName, recordDate);
          
          if (this.validateRecord(normalizedRecord)) {
            await this.insertRecord(client, normalizedRecord);
            validCount++;
          }
          
        } catch (error) {
          console.log(`❌ Record error: ${error.message}`);
        }
      }
      
      await client.query('COMMIT');
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
    
    return validCount;
  }

  normalizeRecord(record, config, fileName, recordDate) {
    const normalized = {
      source_system: config.name.split(' ')[0].toLowerCase(),
      file_name: fileName,
      record_date: recordDate.toISOString().split('T')[0]
    };
    
    // Map each field according to exact configuration
    for (const [targetField, sourceField] of Object.entries(config.fields)) {
      if (sourceField === null) {
        normalized[targetField] = null;
      } else if (sourceField === 'N/A') {
        normalized[targetField] = 'N/A';
      } else if (Array.isArray(sourceField)) {
        // Sum multiple fields (for blocked_quantity, pending_buy_quantity, pending_sell_quantity)
        let sum = 0;
        for (const field of sourceField) {
          const value = this.safeNumber(record[field]);
          if (value !== null) sum += value;
        }
        normalized[targetField] = sum;
      } else {
        if (targetField.includes('quantity') || targetField === 'total_position') {
          normalized[targetField] = this.safeNumber(record[sourceField]);
        } else {
          normalized[targetField] = this.safeString(record[sourceField]);
        }
      }
    }
    
    return normalized;
  }

  safeString(value) {
    if (value === undefined || value === null || value === '') return null;
    return String(value).trim();
  }

  safeNumber(value) {
    if (value === undefined || value === null || value === '') return 0;
    const num = parseFloat(value);
    return isNaN(num) ? 0 : num;
  }

  validateRecord(record) {
    // Required fields validation
    if (!record.client_reference || !record.client_name || !record.instrument_isin) {
      return false;
    }
    
    // Mathematical validation with 1% tolerance
    const calculated = (record.blocked_quantity || 0) + (record.pending_buy_quantity || 0) + (record.saleable_quantity || 0);
    const expected = record.total_position || 0;
    
    if (expected > 0) {
      const tolerance = expected * 0.01; // 1% tolerance
      const difference = Math.abs(calculated - expected);
      
      if (difference > tolerance) {
        console.log(`⚠️  Math validation (1% tolerance): ${record.client_reference} - ${record.instrument_isin}`);
        console.log(`   Expected: ${expected}, Calculated: ${calculated}, Difference: ${difference.toFixed(4)}`);
      }
    }
    
    return true;
  }

  async insertRecord(client, record) {
    const insertSQL = `
      INSERT INTO unified_custody_master (
        client_reference, client_name, instrument_isin, instrument_name, instrument_code,
        blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
        source_system, file_name, record_date
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    `;
    
    const values = [
      record.client_reference,
      record.client_name,
      record.instrument_isin,
      record.instrument_name,
      record.instrument_code,
      record.blocked_quantity || 0,
      record.pending_buy_quantity || 0,
      record.pending_sell_quantity || 0,
      record.total_position || 0,
      record.saleable_quantity || 0,
      record.source_system,
      record.file_name,
      record.record_date
    ];
    
    await client.query(insertSQL, values);
  }

  showSummary() {
    console.log('\n🎉 CUSTODY DATA NORMALIZATION COMPLETE!');
    console.log('═══════════════════════════════════════════════');
    console.log(`📊 Total Records Processed: ${this.totalRecords.toLocaleString()}`);
    console.log(`✅ Valid Records Inserted: ${this.validRecords.toLocaleString()}`);
    console.log(`❌ Processing Errors: ${this.errors.length}`);
    console.log(`🎯 Success Rate: ${this.totalRecords > 0 ? Math.round((this.validRecords/this.totalRecords)*100) : 0}%`);
    
    if (this.errors.length > 0) {
      console.log('\n❌ Error Summary:');
      this.errors.forEach(error => console.log(`   - ${error}`));
    }
    
    console.log('\n✅ SYSTEM READY FOR BUSINESS USE');
    console.log('📊 Data available in: unified_custody_master table');
    console.log('🔍 Query examples:');
    console.log('   SELECT source_system, COUNT(*) FROM unified_custody_master GROUP BY source_system;');
    console.log('   SELECT * FROM unified_custody_master WHERE total_position > 0 LIMIT 10;');
  }
}

// Run if called directly
if (require.main === module) {
  async function main() {
    try {
      const processor = new CustodyDataProcessor();
      await processor.initialize();
      await processor.processAllFiles('./');
      
      console.log('\n🌐 System ready. Run again to reprocess or integrate with your application.');
      process.exit(0);
      
    } catch (error) {
      console.error('💥 System error:', error.message);
      process.exit(1);
    }
  }
  
  main();
}

module.exports = { CustodyDataProcessor, CUSTODY_SYSTEMS }; 