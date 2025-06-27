#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const { Pool } = require('pg');
const express = require('express');
const multer = require('multer');
const moment = require('moment');

/**
 * LATEST FILE CUSTODY PROCESSING SYSTEM
 * Processes only the latest file from each custody system with unified field mapping
 */
class LatestCustodyProcessor {
  constructor(config) {
    this.config = config;
    this.pgPool = new Pool(config.postgresql);
    this.processingSummary = {
      filesProcessed: [],
      totalRecords: 0,
      validRecords: 0,
      errorRecords: 0,
      processingTime: 0,
      dataQualityMetrics: {}
    };
    
    // COMPLETE FIELD MAPPING CONFIGURATION for 6 custody systems
    this.custodyMappings = {
      axis: {
        pattern: /axis.*eod.*custody/i,
        fileExtensions: ['.xlsx', '.xls'],
        sheetName: 'Sheet1',
        headerRow: 0,
        sourceSystem: 'AXIS',
        datePattern: /(\d{4})(\d{2})(\d{2})/,
        fieldMappings: {
          client_reference: 'UCC',
          client_name: 'ClientName',
          instrument_isin: 'ISIN',
          instrument_name: 'SecurityName',
          instrument_code: null,
          // Axis requires summing multiple columns
          blocked_quantity: ['DematLockedQty', 'PhysicalLocked'],
          pending_buy_quantity: ['PurchaseOutstanding', 'PurchaseUnderProcess'],
          pending_sell_quantity: ['SaleOutstanding', 'SaleUnderProcess'],
          total_position: 'NetBalance',
          saleable_quantity: 'DematFree'
        }
      },
      
      deutsche: {
        pattern: /DL_.*EC\d+/i,
        fileExtensions: ['.xlsx', '.xls'],
        sheetName: 'ReportDateHeader',
        headerRow: 8,
        sourceSystem: 'DEUTSCHE',
        datePattern: /(\d{2})_(\d{2})_(\d{4})/, // DD_MM_YYYY format
        fieldMappings: {
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
      
      trustpms: {
        pattern: /End_Client_Holding.*TRUSTPMS/i,
        fileExtensions: ['.xls', '.xlsx'],
        sheetName: 'RPT_EndClientHolding',
        headerRow: 2,
        sourceSystem: 'TRUSTPMS',
        datePattern: /(\d{4})(\d{2})(\d{2})/,
        fieldMappings: {
          client_reference: 'Client Code',
          client_name: 'Client Name',
          instrument_isin: 'Instrument ISIN',
          instrument_name: 'Instrument Name',
          instrument_code: 'Instrument Code',
          blocked_quantity: null, // Trust PMS doesn't track blocked
          pending_buy_quantity: 'Pending Buy Position',
          pending_sell_quantity: 'Pending Sell Position',
          total_position: null, // Trust PMS only tracks saleable
          saleable_quantity: 'Saleable Position'
        }
      },
      
      hdfc: {
        pattern: /hdfc.*custody/i,
        fileExtensions: ['.csv'],
        headerRow: 15,
        sourceSystem: 'HDFC',
        datePattern: /(\d{4})_(\d{2})_(\d{2})/,
        fieldMappings: {
          client_reference: 'Client Code',
          client_name: 'Client Name',
          instrument_isin: 'ISIN Code',
          instrument_name: 'Instrument Name',
          instrument_code: 'Instrument Code',
          blocked_quantity: 'Pending Blocked Qty',
          pending_buy_quantity: 'Pending Purchase',
          pending_sell_quantity: null, // HDFC doesn't track pending sell
          total_position: 'Book Position',
          saleable_quantity: 'Total Saleable'
        }
      },
      
      kotak: {
        pattern: /kotak.*custody/i,
        fileExtensions: ['.xlsx', '.xls'],
        sheetName: 'Sheet1',
        headerRow: 3,
        sourceSystem: 'KOTAK',
        datePattern: /(\d{4})(\d{2})(\d{2})/,
        fieldMappings: {
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
      
      orbis: {
        pattern: /orbis.*holding/i,
        fileExtensions: ['.xlsx', '.xls'],
        sheetName: 'OneSheetLogicalHolding_intrasit',
        headerRow: 0,
        sourceSystem: 'ORBIS',
        datePattern: /(\d{2})_(\d{2})_(\d{4})/, // DD_MM_YYYY format
        fieldMappings: {
          client_reference: 'OFIN Code',
          client_name: 'N/A', // Fixed value for Orbis
          instrument_isin: 'ISIN',
          instrument_name: null, // Orbis doesn't have instrument names
          instrument_code: null,
          blocked_quantity: 'Blocked/Pledge',
          pending_buy_quantity: 'Intrasit Purchase',
          pending_sell_quantity: 'Intrasit Sale',
          total_position: 'Holding Quantity',
          saleable_quantity: 'Saleble Quantity'
        }
      }
    };
  }

  /**
   * LATEST FILE SELECTION LOGIC
   * Scans directory and selects most recent file from each custody system
   */
  async selectLatestFiles(directoryPath) {
    console.log('🔍 Scanning directory for latest custody files...');
    
    if (!fs.existsSync(directoryPath)) {
      throw new Error(`Directory not found: ${directoryPath}`);
    }

    const files = fs.readdirSync(directoryPath);
    const latestFiles = {};
    
    // Group files by custody system and find latest
    for (const file of files) {
      const filePath = path.join(directoryPath, file);
      const stats = fs.statSync(filePath);
      
      if (!stats.isFile()) continue;
      
      // Detect custody system from filename
      const custodyType = this.detectCustodySystem(file);
      if (!custodyType) continue;
      
      // Extract date from filename
      const fileDate = this.extractDateFromFilename(file, custodyType);
      if (!fileDate) {
        console.log(`⚠️ Could not extract date from ${file}, using file modification time`);
        continue;
      }
      
      // Track latest file for each custody system
      if (!latestFiles[custodyType] || fileDate > latestFiles[custodyType].date) {
        latestFiles[custodyType] = {
          file: file,
          path: filePath,
          date: fileDate,
          custodyType: custodyType,
          config: this.custodyMappings[custodyType]
        };
      }
    }
    
    console.log('📋 Latest files selected:');
    Object.values(latestFiles).forEach(fileInfo => {
      console.log(`   📄 ${fileInfo.custodyType}: ${fileInfo.file} (${fileInfo.date})`);
    });
    
    return Object.values(latestFiles);
  }

  /**
   * Detect custody system from filename patterns
   */
  detectCustodySystem(filename) {
    const lowerName = filename.toLowerCase();
    
    for (const [system, config] of Object.entries(this.custodyMappings)) {
      if (config.pattern.test(lowerName)) {
        return system;
      }
    }
    
    return null;
  }

  /**
   * Extract date from filename using system-specific patterns
   */
  extractDateFromFilename(filename, custodyType) {
    const config = this.custodyMappings[custodyType];
    const match = filename.match(config.datePattern);
    
    if (!match) return null;
    
    let year, month, day;
    
    if (custodyType === 'deutsche' || custodyType === 'orbis') {
      // DD_MM_YYYY format
      [, day, month, year] = match;
    } else {
      // YYYYMMDD or YYYY_MM_DD format
      [, year, month, day] = match;
    }
    
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
  }

  /**
   * FILE PROCESSING ENGINE
   * Handles CSV and Excel formats with different configurations
   */
  async processFile(fileInfo) {
    console.log(`\n🔄 Processing ${fileInfo.custodyType}: ${fileInfo.file}`);
    
    try {
      let rawData = [];
      const config = fileInfo.config;
      
      if (config.fileExtensions.includes('.csv')) {
        rawData = await this.readCSVFile(fileInfo.path, config);
      } else {
        rawData = await this.readExcelFile(fileInfo.path, config);
      }
      
      console.log(`📊 Read ${rawData.length} raw records`);
      
      // Transform data to unified format
      const transformedData = await this.transformData(rawData, config, fileInfo);
      
      console.log(`✅ Transformed ${transformedData.valid.length} valid records`);
      console.log(`❌ Rejected ${transformedData.errors.length} invalid records`);
      
      return transformedData;
      
    } catch (error) {
      console.error(`❌ Error processing ${fileInfo.file}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Read CSV file with specific header row
   */
  async readCSVFile(filePath, config) {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    if (lines.length <= config.headerRow) {
      throw new Error(`File has insufficient rows. Expected header at row ${config.headerRow}`);
    }
    
    const headers = lines[config.headerRow].split(',').map(h => h.trim().replace(/"/g, ''));
    const dataLines = lines.slice(config.headerRow + 1);
    
    return dataLines.map(line => {
      const values = line.split(',').map(v => v.trim().replace(/"/g, ''));
      const record = {};
      
      headers.forEach((header, index) => {
        record[header] = values[index] || '';
      });
      
      return record;
    });
  }

  /**
   * Read Excel file with specific sheet and header row
   */
  async readExcelFile(filePath, config) {
    const workbook = XLSX.readFile(filePath);
    
    let sheetName = config.sheetName || workbook.SheetNames[0];
    if (!workbook.Sheets[sheetName]) {
      console.log(`⚠️ Sheet '${sheetName}' not found, using first sheet: ${workbook.SheetNames[0]}`);
      sheetName = workbook.SheetNames[0];
    }
    
    const worksheet = workbook.Sheets[sheetName];
    const jsonData = XLSX.utils.sheet_to_json(worksheet, {
      header: 1,
      defval: '',
      blankrows: false
    });
    
    if (jsonData.length <= config.headerRow) {
      throw new Error(`Sheet has insufficient rows. Expected header at row ${config.headerRow}`);
    }
    
    const headers = jsonData[config.headerRow];
    const dataRows = jsonData.slice(config.headerRow + 1);
    
    return dataRows.map(row => {
      const record = {};
      headers.forEach((header, index) => {
        record[header] = row[index] || '';
      });
      return record;
    });
  }

  /**
   * DATA TRANSFORMATION
   * Transform raw data to unified format with validation
   */
  async transformData(rawData, config, fileInfo) {
    const validRecords = [];
    const errorRecords = [];
    const mappings = config.fieldMappings;
    
    for (const [index, rawRecord] of rawData.entries()) {
      try {
        const unifiedRecord = {
          client_reference: this.extractField(rawRecord, mappings.client_reference, config.sourceSystem),
          client_name: this.extractField(rawRecord, mappings.client_name, config.sourceSystem),
          instrument_isin: this.extractField(rawRecord, mappings.instrument_isin, config.sourceSystem),
          instrument_name: this.extractField(rawRecord, mappings.instrument_name, config.sourceSystem),
          instrument_code: this.extractField(rawRecord, mappings.instrument_code, config.sourceSystem),
          blocked_quantity: this.extractNumericField(rawRecord, mappings.blocked_quantity, config.sourceSystem),
          pending_buy_quantity: this.extractNumericField(rawRecord, mappings.pending_buy_quantity, config.sourceSystem),
          pending_sell_quantity: this.extractNumericField(rawRecord, mappings.pending_sell_quantity, config.sourceSystem),
          total_position: this.extractNumericField(rawRecord, mappings.total_position, config.sourceSystem),
          saleable_quantity: this.extractNumericField(rawRecord, mappings.saleable_quantity, config.sourceSystem),
          source_system: config.sourceSystem,
          file_name: fileInfo.file,
          record_date: fileInfo.date
        };
        
        // Validate record
        const validation = this.validateRecord(unifiedRecord);
        if (validation.isValid) {
          validRecords.push(unifiedRecord);
        } else {
          errorRecords.push({
            rowIndex: index + 1,
            errors: validation.errors,
            rawRecord: rawRecord
          });
        }
        
      } catch (error) {
        errorRecords.push({
          rowIndex: index + 1,
          errors: [error.message],
          rawRecord: rawRecord
        });
      }
    }
    
    return {
      valid: validRecords,
      errors: errorRecords
    };
  }

  /**
   * Extract field value with system-specific handling
   */
  extractField(record, fieldMapping, sourceSystem) {
    if (fieldMapping === null) {
      return null;
    }
    
    if (fieldMapping === 'N/A') {
      return 'N/A'; // Fixed value for Orbis client_name
    }
    
    if (Array.isArray(fieldMapping)) {
      // Handle multiple field summing (Axis case)
      return fieldMapping.reduce((sum, field) => {
        const value = parseFloat(record[field]) || 0;
        return sum + value;
      }, 0);
    }
    
    return record[fieldMapping] || '';
  }

  /**
   * Extract numeric field with proper conversion
   */
  extractNumericField(record, fieldMapping, sourceSystem) {
    if (fieldMapping === null) {
      return null;
    }
    
    if (Array.isArray(fieldMapping)) {
      // Sum multiple fields (Axis case)
      return fieldMapping.reduce((sum, field) => {
        const value = parseFloat(record[field]) || 0;
        return sum + value;
      }, 0);
    }
    
    const value = record[fieldMapping];
    if (value === '' || value === undefined || value === null) {
      return 0;
    }
    
    return parseFloat(value) || 0;
  }

  /**
   * VALIDATION LOGIC
   * Validate record data and mathematical relationships
   */
  validateRecord(record) {
    const errors = [];
    
    // Required field validation
    if (!record.client_reference || record.client_reference.trim() === '') {
      errors.push('client_reference is required');
    }
    
    if (!record.client_name || record.client_name.trim() === '') {
      errors.push('client_name is required');
    }
    
    if (!record.instrument_isin || record.instrument_isin.trim() === '') {
      errors.push('instrument_isin is required');
    }
    
    // ISIN validation (basic format check)
    if (record.instrument_isin && !/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(record.instrument_isin)) {
      errors.push('instrument_isin format is invalid');
    }
    
    // Mathematical relationship validation
    if (record.total_position !== null && record.total_position > 0) {
      const expectedSaleable = record.total_position - (record.blocked_quantity || 0);
      const actualSaleable = record.saleable_quantity || 0;
      const tolerance = Math.max(record.total_position * 0.01, 0.01); // 1% tolerance
      
      if (Math.abs(expectedSaleable - actualSaleable) > tolerance) {
        errors.push(`Saleable quantity validation failed: expected ~${expectedSaleable.toFixed(2)}, got ${actualSaleable.toFixed(2)}`);
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors: errors
    };
  }

  /**
   * UNIFIED TABLE SCHEMA CREATION
   */
  async initializeDatabase() {
    console.log('🔧 Initializing unified_custody_master table...');
    
    const client = await this.pgPool.connect();
    
    try {
      // Create table with exact schema specified
      await client.query(`
        CREATE TABLE IF NOT EXISTS unified_custody_master (
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
      `);
      
      // Create performance indexes
      const indexes = [
        'CREATE INDEX IF NOT EXISTS idx_unified_client_instrument ON unified_custody_master(client_reference, instrument_isin)',
        'CREATE INDEX IF NOT EXISTS idx_unified_instrument ON unified_custody_master(instrument_isin)',
        'CREATE INDEX IF NOT EXISTS idx_unified_source_date ON unified_custody_master(source_system, record_date)',
        'CREATE INDEX IF NOT EXISTS idx_unified_total_position ON unified_custody_master(total_position) WHERE total_position > 0',
        'CREATE INDEX IF NOT EXISTS idx_unified_saleable ON unified_custody_master(saleable_quantity) WHERE saleable_quantity > 0'
      ];
      
      for (const indexQuery of indexes) {
        await client.query(indexQuery);
      }
      
      // Create updated_at trigger
      await client.query(`
        CREATE OR REPLACE FUNCTION update_unified_custody_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
          NEW.updated_at = CURRENT_TIMESTAMP;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS unified_custody_updated_at_trigger ON unified_custody_master;
        CREATE TRIGGER unified_custody_updated_at_trigger
          BEFORE UPDATE ON unified_custody_master
          FOR EACH ROW
          EXECUTE FUNCTION update_unified_custody_updated_at();
      `);
      
      console.log('✅ Database schema initialized successfully');
      
    } finally {
      client.release();
    }
  }

  /**
   * Load data to PostgreSQL with upsert logic
   */
  async loadDataToDatabase(validRecords, fileInfo) {
    console.log(`💾 Loading ${validRecords.length} records to database...`);
    
    const client = await this.pgPool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Clear existing records for this source system and date
      const deleteResult = await client.query(`
        DELETE FROM unified_custody_master 
        WHERE source_system = $1 AND record_date = $2
      `, [fileInfo.config.sourceSystem, fileInfo.date]);
      
      console.log(`🗑️ Removed ${deleteResult.rowCount} existing records for ${fileInfo.config.sourceSystem}`);
      
      // Insert new records
      let insertedCount = 0;
      
      for (const record of validRecords) {
        await client.query(`
          INSERT INTO unified_custody_master (
            client_reference, client_name, instrument_isin, instrument_name, instrument_code,
            blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
            source_system, file_name, record_date
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        `, [
          record.client_reference, record.client_name, record.instrument_isin, 
          record.instrument_name, record.instrument_code,
          record.blocked_quantity, record.pending_buy_quantity, record.pending_sell_quantity,
          record.total_position, record.saleable_quantity,
          record.source_system, record.file_name, record.record_date
        ]);
        
        insertedCount++;
      }
      
      await client.query('COMMIT');
      
      console.log(`✅ Successfully inserted ${insertedCount} records`);
      
      return insertedCount;
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * MAIN PROCESSING PIPELINE
   * Complete end-to-end processing workflow
   */
  async processLatestFiles(directoryPath) {
    const startTime = Date.now();
    
    try {
      console.log('🚀 Starting Latest File Custody Processing Pipeline');
      console.log('═══════════════════════════════════════════════════════════');
      
      // Initialize database
      await this.initializeDatabase();
      
      // Select latest files
      const latestFiles = await this.selectLatestFiles(directoryPath);
      
      if (latestFiles.length === 0) {
        throw new Error('No valid custody files found in directory');
      }
      
      // Reset processing summary
      this.processingSummary = {
        filesProcessed: [],
        totalRecords: 0,
        validRecords: 0,
        errorRecords: 0,
        processingTime: 0,
        dataQualityMetrics: {}
      };
      
      // Process each latest file
      for (const fileInfo of latestFiles) {
        try {
          const result = await this.processFile(fileInfo);
          
          // Load valid records to database
          const insertedCount = await this.loadDataToDatabase(result.valid, fileInfo);
          
          // Update summary
          this.processingSummary.filesProcessed.push({
            custodySystem: fileInfo.config.sourceSystem,
            fileName: fileInfo.file,
            date: fileInfo.date,
            totalRecords: result.valid.length + result.errors.length,
            validRecords: result.valid.length,
            errorRecords: result.errors.length,
            insertedRecords: insertedCount
          });
          
          this.processingSummary.totalRecords += result.valid.length + result.errors.length;
          this.processingSummary.validRecords += result.valid.length;
          this.processingSummary.errorRecords += result.errors.length;
          
        } catch (fileError) {
          console.error(`❌ Failed to process ${fileInfo.file}: ${fileError.message}`);
          
          this.processingSummary.filesProcessed.push({
            custodySystem: fileInfo.config.sourceSystem,
            fileName: fileInfo.file,
            error: fileError.message
          });
        }
      }
      
      this.processingSummary.processingTime = (Date.now() - startTime) / 1000;
      
      // Generate data quality report
      await this.generateDataQualityReport();
      
      // Print summary
      this.printProcessingSummary();
      
      return this.processingSummary;
      
    } catch (error) {
      console.error('💥 Processing pipeline failed:', error.message);
      throw error;
    }
  }

  /**
   * Generate data quality report with validation metrics
   */
  async generateDataQualityReport() {
    console.log('\n📊 Generating data quality report...');
    
    const client = await this.pgPool.connect();
    
    try {
      // Get overall statistics
      const overallStats = await client.query(`
        SELECT 
          source_system,
          COUNT(*) as total_records,
          COUNT(*) FILTER (WHERE total_position > 0) as records_with_position,
          AVG(total_position) as avg_total_position,
          AVG(saleable_quantity) as avg_saleable_quantity,
          COUNT(*) FILTER (WHERE 
            total_position IS NOT NULL AND total_position > 0 AND
            ABS((total_position - COALESCE(blocked_quantity, 0)) - COALESCE(saleable_quantity, 0)) <= (total_position * 0.01)
          ) as formula_compliant_records
        FROM unified_custody_master
        WHERE record_date = CURRENT_DATE
        GROUP BY source_system
      `);
      
      this.processingSummary.dataQualityMetrics = overallStats.rows.reduce((acc, row) => {
        const complianceRate = row.records_with_position > 0 ? 
          (parseFloat(row.formula_compliant_records) / parseFloat(row.records_with_position) * 100).toFixed(2) : 0;
        
        acc[row.source_system] = {
          totalRecords: parseInt(row.total_records),
          recordsWithPosition: parseInt(row.records_with_position),
          avgTotalPosition: parseFloat(row.avg_total_position || 0).toFixed(2),
          avgSaleableQuantity: parseFloat(row.avg_saleable_quantity || 0).toFixed(2),
          formulaComplianceRate: `${complianceRate}%`
        };
        
        return acc;
      }, {});
      
    } finally {
      client.release();
    }
  }

  /**
   * Print comprehensive processing summary
   */
  printProcessingSummary() {
    console.log('\n🎉 PROCESSING SUMMARY');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`⏱️  Total Processing Time: ${this.processingSummary.processingTime.toFixed(2)}s`);
    console.log(`📊 Overall Records: ${this.processingSummary.totalRecords.toLocaleString()}`);
    console.log(`✅ Valid Records: ${this.processingSummary.validRecords.toLocaleString()}`);
    console.log(`❌ Error Records: ${this.processingSummary.errorRecords.toLocaleString()}`);
    
    if (this.processingSummary.totalRecords > 0) {
      const successRate = (this.processingSummary.validRecords / this.processingSummary.totalRecords * 100).toFixed(2);
      console.log(`🎯 Success Rate: ${successRate}%`);
    }
    
    console.log('\n📁 Files Processed:');
    this.processingSummary.filesProcessed.forEach(file => {
      if (file.error) {
        console.log(`   ❌ ${file.custodySystem}: ${file.fileName} - ERROR: ${file.error}`);
      } else {
        console.log(`   ✅ ${file.custodySystem}: ${file.fileName} (${file.date.toDateString()})`);
        console.log(`      📊 ${file.validRecords}/${file.totalRecords} valid records inserted`);
      }
    });
    
    console.log('\n📈 Data Quality Metrics:');
    Object.entries(this.processingSummary.dataQualityMetrics).forEach(([system, metrics]) => {
      console.log(`   🏦 ${system}:`);
      console.log(`      📋 Total Records: ${metrics.totalRecords.toLocaleString()}`);
      console.log(`      💰 Avg Total Position: ${metrics.avgTotalPosition}`);
      console.log(`      📊 Formula Compliance: ${metrics.formulaComplianceRate}`);
    });
  }

  /**
   * Close database connections
   */
  async close() {
    await this.pgPool.end();
  }
}

module.exports = { LatestCustodyProcessor };

// CLI execution
if (require.main === module) {
  const config = require('./config');
  
  (async () => {
    const processor = new LatestCustodyProcessor(config);
    
    try {
      const directoryPath = process.argv[2] || './temp_uploads';
      await processor.processLatestFiles(directoryPath);
    } catch (error) {
      console.error('💥 Processing failed:', error.message);
    } finally {
      await processor.close();
    }
  })();
} 