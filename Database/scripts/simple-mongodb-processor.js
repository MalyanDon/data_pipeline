#!/usr/bin/env node

const mongoose = require('mongoose');
const config = require('../config');
const FieldMapper = require('../custody-normalization/extractors/fieldMapper');
const DataNormalizer = require('../custody-normalization/extractors/dataNormalizer');
const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
const { detectCustodyType, getCustodyConfig, normalizeOrbisRecord, validateCustodyRecord } = require('../custody-normalization/config/custody-mappings');

class SimpleCustodyProcessor {
  constructor() {
    this.mongoClient = null;
    this.pgPool = new Pool(config.postgresql);
    this.schema = new NormalizationSchema();
    this.loader = new PostgresLoader();
  }

  async connect() {
    try {
      console.log('🔌 Connecting to databases...');
      this.mongoClient = new MongoClient(config.mongodb.uri);
      await this.mongoClient.connect();
      console.log('✅ Connected to MongoDB');
      
      // Initialize PostgreSQL schema for daily tables
      await this.schema.initializeDatabase();
      console.log('✅ PostgreSQL ready for daily tables');
      
    } catch (error) {
      console.error('❌ Database connection failed:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      if (this.mongoClient) {
        await this.mongoClient.close();
        console.log('📴 MongoDB disconnected');
      }
      
      await this.pgPool.end();
      await this.schema.close();
      await this.loader.close();
      console.log('📴 PostgreSQL disconnected');
      
    } catch (error) {
      console.error('❌ Disconnect error:', error);
    }
  }

  detectCustodyTypeFromCollection(collectionName) {
    // First try to detect from collection name using existing patterns
    const detectedType = detectCustodyType(collectionName);
    
    if (detectedType !== 'unknown') {
      console.log(`🔍 Collection '${collectionName}' detected as: ${detectedType.toUpperCase()}`);
      return detectedType.toUpperCase();
    }
    
    console.log(`⚠️  Collection '${collectionName}' - unknown custody type, defaulting to ORBIS`);
    return 'ORBIS';
  }

  extractDateFromCollection(collectionName) {
    // Try to extract date from collection name (format: filetype_MM_DD)
    const match = collectionName.match(/_(\d{2})_(\d{2})$/);
    if (match) {
      const [, month, day] = match;
      return `2025-${month}-${day}`;
    }
    
    // Default to current date if no date found
    return new Date().toISOString().split('T')[0];
  }

  normalizeRecord(record, custodyType, recordDate) {
    try {
      const config = getCustodyConfig(custodyType.toLowerCase());
      if (!config) {
        throw new Error(`No configuration found for custody type: ${custodyType}`);
      }

      // Special handling for Orbis
      if (custodyType === 'ORBIS') {
        const { normalized, orbisSpecific } = normalizeOrbisRecord(record, config);
        
        // Validate required fields for Orbis
        if (!normalized.client_reference || !normalized.instrument_isin) {
          throw new Error(`Missing required fields for Orbis record`);
        }

        // Validate ISIN format
        if (!/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(normalized.instrument_isin)) {
          throw new Error(`Invalid ISIN format: ${normalized.instrument_isin}`);
        }

        return {
          ...normalized,
          source_system: custodyType,
          record_date: recordDate,
          // Store Orbis-specific data for reference
          _orbis_metadata: orbisSpecific
        };
      }

      // Standard processing for non-Orbis systems
      const mapping = config.fieldMappings;
      
      // Extract basic fields
      const client_reference = this.extractField(record, mapping.client_reference);
      const client_name = this.extractField(record, mapping.client_name);
      const instrument_isin = this.extractField(record, mapping.instrument_isin);
      const instrument_name = this.extractField(record, mapping.instrument_name);
      const instrument_code = this.extractField(record, mapping.instrument_code);

      // Extract financial fields with validation
      const blocked_quantity = this.normalizeFinancialAmount(
        this.extractFinancialField(record, mapping.blocked_quantity, custodyType)
      );
      const pending_buy_quantity = this.normalizeFinancialAmount(
        this.extractFinancialField(record, mapping.pending_buy_quantity, custodyType)
      );
      const pending_sell_quantity = this.normalizeFinancialAmount(
        this.extractFinancialField(record, mapping.pending_sell_quantity, custodyType)
      );

      // Validate required fields
      if (!client_reference || !client_name || !instrument_isin || !instrument_name) {
        throw new Error(`Missing required fields in record`);
      }

      // Validate ISIN format
      if (!/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(instrument_isin)) {
        throw new Error(`Invalid ISIN format: ${instrument_isin}`);
      }

      const normalizedRecord = {
        client_reference: client_reference.trim(),
        client_name: client_name.trim(),
        instrument_isin: instrument_isin.trim(),
        instrument_name: instrument_name.trim(),
        instrument_code: instrument_code?.trim() || null,
        blocked_quantity: blocked_quantity,
        pending_buy_quantity: pending_buy_quantity,
        pending_sell_quantity: pending_sell_quantity,
        source_system: custodyType,
        record_date: recordDate
      };

      // Final validation
      const validation = validateCustodyRecord(normalizedRecord, custodyType);
      if (!validation.isValid) {
        throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
      }

      return normalizedRecord;

    } catch (error) {
      throw new Error(`Record normalization failed: ${error.message}`);
    }
  }

  extractField(record, fieldMappings) {
    if (!fieldMappings || fieldMappings.length === 0) {
      return null;
    }

    for (const fieldName of fieldMappings) {
      if (record[fieldName] !== undefined && record[fieldName] !== null && record[fieldName] !== '') {
        return String(record[fieldName]);
      }
    }
    
    return null;
  }

  extractFinancialField(record, fieldMappings, custodyType) {
    if (!fieldMappings || fieldMappings.length === 0) {
      return 0;
    }

    // Special handling for Axis (summing multiple fields)
    if (custodyType === 'AXIS' && fieldMappings.includes('DematLockedQty')) {
      const dematLocked = parseFloat(record.DematLockedQty || 0);
      const physicalLocked = parseFloat(record.PhysicalLocked || 0);
      return dematLocked + physicalLocked;
    }

    // Standard single field extraction
    for (const fieldName of fieldMappings) {
      if (record[fieldName] !== undefined && record[fieldName] !== null && record[fieldName] !== '') {
        const value = parseFloat(record[fieldName]);
        return isNaN(value) ? 0 : value;
      }
    }
    
    return 0;
  }

  normalizeFinancialAmount(amount) {
    const numAmount = parseFloat(amount);
    if (isNaN(numAmount) || numAmount < 0) {
      return 0;
    }
    return Math.round(numAmount * 10000) / 10000; // Round to 4 decimal places
  }

  async processCollections() {
    try {
      console.log('🚀 Starting custody data processing with daily tables...');
      
      const databases = ['financial_data_2024', 'financial_data_2025'];
      let totalProcessed = 0;
      let totalValid = 0;
      let totalErrors = 0;
      const errorsByDate = {};
      const orbisStats = { processed: 0, valid: 0, errors: 0 };

      for (const dbName of databases) {
        console.log(`\n📊 Processing database: ${dbName}`);
        
        const db = this.mongoClient.db(dbName);
        const collections = await db.listCollections().toArray();
        
        console.log(`📋 Found ${collections.length} collections in ${dbName}`);

        for (const collectionInfo of collections) {
          const collectionName = collectionInfo.name;
          const custodyType = this.detectCustodyTypeFromCollection(collectionName);
          const recordDate = this.extractDateFromCollection(collectionName);
          
          console.log(`\n📅 Processing ${collectionName} → ${recordDate} (${custodyType})`);
          
          try {
            // Ensure daily table exists for this date
            const tableExists = await this.schema.dailyTableExists(recordDate);
            if (!tableExists) {
              console.log(`🏗️  Creating daily table for ${recordDate}`);
              await this.schema.createDailyTable(recordDate);
            }

            const collection = db.collection(collectionName);
            const totalRecords = await collection.countDocuments();
            
            console.log(`   📊 Total records: ${totalRecords}`);
            
            if (totalRecords === 0) {
              console.log(`   ⚠️  Empty collection, skipping`);
              continue;
            }

            // Process in small batches
            const batchSize = 250;
            let processedInCollection = 0;
            let validInCollection = 0;
            let errorsInCollection = 0;

            const cursor = collection.find({});
            
            while (await cursor.hasNext()) {
              const batch = [];
              
              // Collect batch
              for (let i = 0; i < batchSize && await cursor.hasNext(); i++) {
                const doc = await cursor.next();
                batch.push(doc);
              }

              if (batch.length === 0) break;

              // Process batch
              const normalizedRecords = [];
              
              for (const record of batch) {
                try {
                  const normalized = this.normalizeRecord(record, custodyType, recordDate);
                  
                  // Remove Orbis metadata before storing in PostgreSQL
                  if (normalized._orbis_metadata) {
                    delete normalized._orbis_metadata;
                  }
                  
                  normalizedRecords.push(normalized);
                  validInCollection++;
                  
                  if (custodyType === 'ORBIS') {
                    orbisStats.valid++;
                  }
                } catch (error) {
                  errorsInCollection++;
                  if (custodyType === 'ORBIS') {
                    orbisStats.errors++;
                  }
                  
                  if (!errorsByDate[recordDate]) errorsByDate[recordDate] = [];
                  errorsByDate[recordDate].push({
                    collection: collectionName,
                    custodyType: custodyType,
                    error: error.message,
                    record: record._id
                  });
                }
              }

              // Load to PostgreSQL daily table
              if (normalizedRecords.length > 0) {
                const tableName = this.schema.getTableName(recordDate);
                const result = await this.loader._loadBatchToTable(
                  normalizedRecords, 
                  tableName, 
                  custodyType, 
                  collectionName
                );
                
                console.log(`   ✅ Batch: ${result.inserted} inserted, ${result.updated} updated`);
              }

              processedInCollection += batch.length;
              if (custodyType === 'ORBIS') {
                orbisStats.processed += batch.length;
              }
              
              console.log(`   📊 Progress: ${processedInCollection}/${totalRecords} (${validInCollection} valid, ${errorsInCollection} errors)`);

              // Small delay to reduce load
              await new Promise(resolve => setTimeout(resolve, 50));
            }

            await cursor.close();
            totalProcessed += processedInCollection;
            totalValid += validInCollection;
            totalErrors += errorsInCollection;

            console.log(`   🎉 Collection complete: ${validInCollection}/${processedInCollection} records normalized`);

          } catch (error) {
            console.error(`   ❌ Collection processing failed:`, error.message);
            totalErrors++;
          }
        }
      }

      // Final summary
      console.log(`\n🎯 PROCESSING COMPLETE`);
      console.log(`📊 Total processed: ${totalProcessed}`);
      console.log(`✅ Total valid: ${totalValid}`);
      console.log(`❌ Total errors: ${totalErrors}`);
      console.log(`📈 Success rate: ${((totalValid / totalProcessed) * 100).toFixed(2)}%`);

      // Orbis-specific stats
      if (orbisStats.processed > 0) {
        console.log(`\n🌟 ORBIS-SPECIFIC STATISTICS`);
        console.log(`📊 Orbis processed: ${orbisStats.processed}`);
        console.log(`✅ Orbis valid: ${orbisStats.valid}`);
        console.log(`❌ Orbis errors: ${orbisStats.errors}`);
        console.log(`📈 Orbis success rate: ${((orbisStats.valid / orbisStats.processed) * 100).toFixed(2)}%`);
        console.log(`📝 Note: Orbis records have client_name="N/A" and instrument_name=NULL by design`);
      }

      // Show daily table summary
      const overallStats = await this.loader.getOverallStats();
      console.log(`\n📋 Daily Tables Summary:`);
      console.log(`🗓️  Total daily tables: ${overallStats.totalTables}`);
      console.log(`📊 Total records in PostgreSQL: ${overallStats.totalRecords}`);
      console.log(`📅 Date range: ${overallStats.dateRange?.from} to ${overallStats.dateRange?.to}`);
      console.log(`🏢 Source systems: ${overallStats.sourceSystems?.join(', ')}`);

      if (Object.keys(errorsByDate).length > 0) {
        console.log(`\n⚠️  Errors by date and custody type:`);
        for (const [date, errors] of Object.entries(errorsByDate)) {
          const errorCounts = {};
          errors.forEach(error => {
            const key = error.custodyType || 'UNKNOWN';
            errorCounts[key] = (errorCounts[key] || 0) + 1;
          });
          
          console.log(`   ${date}:`);
          Object.entries(errorCounts).forEach(([type, count]) => {
            console.log(`     ${type}: ${count} errors`);
          });
        }
      }

      return {
        success: true,
        totalProcessed,
        totalValid,
        totalErrors,
        dailyTables: overallStats.totalTables,
        dateRange: overallStats.dateRange,
        orbisStats
      };

    } catch (error) {
      console.error('❌ Processing failed:', error);
      throw error;
    }
  }

  async showDailyTableStats() {
    try {
      console.log('\n📊 DAILY TABLE STATISTICS');
      console.log('=' .repeat(50));
      
      const tables = await this.schema.getAllDailyTables();
      
      for (const tableName of tables) {
        const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
        if (match) {
          const [, year, month, day] = match;
          const date = `${year}-${month}-${day}`;
          const stats = await this.loader.getDailyStats(date);
          
          console.log(`\n📅 ${date} (${tableName})`);
          console.log(`   📊 Total records: ${stats.totalRecords}`);
          console.log(`   👥 Unique clients: ${stats.uniqueClients}`);
          console.log(`   📈 Unique instruments: ${stats.uniqueInstruments}`);
          console.log(`   🏢 Source systems: ${stats.sourceSystems?.join(', ')}`);
          console.log(`   🔒 Records with blocked: ${stats.recordsWithBlocked}`);
          console.log(`   ⏳ Records with pending buy: ${stats.recordsWithPendingBuy}`);
          console.log(`   ⏳ Records with pending sell: ${stats.recordsWithPendingSell}`);
          
          // Show sample of client names to verify Orbis handling
          if (stats.totalRecords > 0) {
            const sampleRecords = await this.loader.queryByDate(date, { limit: 3 });
            if (sampleRecords.records.length > 0) {
              console.log(`   📝 Sample data:`);
              sampleRecords.records.forEach((record, index) => {
                console.log(`     ${index + 1}. Client: ${record.client_name} | Instrument: ${record.instrument_name || 'NULL'} | Source: ${record.source_system}`);
              });
            }
          }
        }
      }
      
    } catch (error) {
      console.error('❌ Failed to show stats:', error);
    }
  }
}

// CLI execution
async function main() {
  const processor = new SimpleCustodyProcessor();
  
  try {
    await processor.connect();
    
    // First migrate existing data to daily tables if needed
    console.log('🔄 Checking for existing data migration...');
    await processor.schema.migrateToDateBasedTables();
    
    // Process all collections
    const result = await processor.processCollections();
    
    // Show detailed statistics
    await processor.showDailyTableStats();
    
    console.log('\n🎉 SUCCESS: All custody data organized into daily tables!');
    console.log('\n💡 Data Quality Notes:');
    console.log('   📝 Orbis records: client_name="N/A", instrument_name=NULL (by design)');
    console.log('   📝 Other systems: Full client and instrument data available');
    console.log('   📝 Financial data: All systems have blocked/pending quantities');
    
  } catch (error) {
    console.error('💥 FATAL ERROR:', error.message);
    process.exit(1);
  } finally {
    await processor.disconnect();
  }
}

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = { SimpleCustodyProcessor }; 