#!/usr/bin/env node

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('../config');
const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');
const { detectCustodyType, getCustodyConfig } = require('../custody-normalization/config/custody-mappings');

// Worker thread code
if (!isMainThread) {
  const { collectionInfo, dbName, mongoUri } = workerData;
  
  class WorkerProcessor {
    constructor() {
      this.mongoClient = null;
      this.schema = new NormalizationSchema();
      this.loader = new PostgresLoader();
    }

    async connect() {
      this.mongoClient = new MongoClient(mongoUri);
      await this.mongoClient.connect();
    }

    async disconnect() {
      if (this.mongoClient) await this.mongoClient.close();
      await this.schema.close();
      await this.loader.close();
    }

    detectCustodyTypeFromCollection(collectionName) {
      const detectedType = detectCustodyType(collectionName);
      return detectedType !== 'unknown' ? detectedType.toUpperCase() : 'ORBIS';
    }

    extractDateFromCollection(collectionName) {
      const match = collectionName.match(/_(\d{2})_(\d{2})$/);
      if (match) {
        const [, month, day] = match;
        return `2025-${month}-${day}`;
      }
      return new Date().toISOString().split('T')[0];
    }

    extractField(record, fieldMappings) {
      if (!fieldMappings || fieldMappings.length === 0) return null;
      
      for (const fieldName of fieldMappings) {
        if (record[fieldName] !== undefined && record[fieldName] !== null && record[fieldName] !== '') {
          return String(record[fieldName]);
        }
      }
      return null;
    }

    extractFinancialField(record, fieldMappings, custodyType) {
      if (!fieldMappings || fieldMappings.length === 0) return 0;

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
      if (isNaN(numAmount) || numAmount < 0) return 0;
      return Math.round(numAmount * 10000) / 10000;
    }

    normalizeRecord(record, custodyType, recordDate) {
      try {
        const config = getCustodyConfig(custodyType.toLowerCase());
        if (!config) {
          throw new Error(`No configuration found for custody type: ${custodyType}`);
        }

        // Special handling for Orbis
        if (custodyType === 'ORBIS') {
          const { normalizeOrbisRecord } = require('../custody-normalization/config/custody-mappings');
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
            record_date: recordDate
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

        // Extract financial fields
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
        const { validateCustodyRecord } = require('../custody-normalization/config/custody-mappings');
        const validation = validateCustodyRecord(normalizedRecord, custodyType);
        if (!validation.isValid) {
          throw new Error(`Validation failed: ${validation.errors.join(', ')}`);
        }

        return normalizedRecord;

      } catch (error) {
        throw new Error(`Record normalization failed: ${error.message}`);
      }
    }

    async processCollection() {
      try {
        await this.connect();
        
        const collectionName = collectionInfo.name;
        const custodyType = this.detectCustodyTypeFromCollection(collectionName);
        const recordDate = this.extractDateFromCollection(collectionName);
        
        // Send progress update
        parentPort.postMessage({
          type: 'progress',
          data: { 
            collection: collectionName, 
            status: 'starting', 
            custodyType, 
            recordDate,
            isOrbis: custodyType === 'ORBIS'
          }
        });

        // Ensure daily table exists
        const tableExists = await this.schema.dailyTableExists(recordDate);
        if (!tableExists) {
          await this.schema.createDailyTable(recordDate);
        }

        const db = this.mongoClient.db(dbName);
        const collection = db.collection(collectionName);
        const totalRecords = await collection.countDocuments();

        if (totalRecords === 0) {
          parentPort.postMessage({
            type: 'result',
            data: {
              collection: collectionName,
              success: true,
              totalProcessed: 0,
              validRecords: 0,
              errors: 0,
              message: 'Empty collection'
            }
          });
          return;
        }

        // Process in batches
        const batchSize = 500; // Larger batches for worker threads
        let processedInCollection = 0;
        let validInCollection = 0;
        let errorsInCollection = 0;
        const errors = [];

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
              normalizedRecords.push(normalized);
              validInCollection++;
            } catch (error) {
              errorsInCollection++;
              errors.push({
                record: record._id,
                error: error.message,
                custodyType: custodyType
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

            // Send progress update
            parentPort.postMessage({
              type: 'progress',
              data: {
                collection: collectionName,
                status: 'processing',
                processed: processedInCollection + batch.length,
                total: totalRecords,
                valid: validInCollection,
                errors: errorsInCollection,
                inserted: result.inserted,
                updated: result.updated,
                custodyType: custodyType
              }
            });
          }

          processedInCollection += batch.length;
        }

        await cursor.close();

        // Send final result
        parentPort.postMessage({
          type: 'result',
          data: {
            collection: collectionName,
            success: true,
            totalProcessed: processedInCollection,
            validRecords: validInCollection,
            errors: errorsInCollection,
            errorDetails: errors.slice(0, 10), // First 10 errors only
            custodyType,
            recordDate,
            dataQualityNotes: custodyType === 'ORBIS' ? 'Orbis: client_name="N/A", instrument_name=NULL (by design)' : null
          }
        });

      } catch (error) {
        parentPort.postMessage({
          type: 'result',
          data: {
            collection: collectionInfo.name,
            success: false,
            error: error.message
          }
        });
      } finally {
        await this.disconnect();
      }
    }
  }

  // Start worker processing
  const worker = new WorkerProcessor();
  worker.processCollection().catch(error => {
    parentPort.postMessage({
      type: 'error',
      data: { error: error.message }
    });
  });
}

// Main thread code
if (isMainThread) {
  class ParallelCustodyProcessor {
    constructor() {
      this.numCPUs = os.cpus().length;
      this.maxWorkers = Math.min(this.numCPUs, 6); // Conservative limit
      this.activeWorkers = new Set();
      this.schema = new NormalizationSchema();
      this.loader = new PostgresLoader();
      this.results = [];
      this.totalCollections = 0;
      this.completedCollections = 0;
      this.startTime = Date.now();
    }

    async initialize() {
      console.log(`🚀 Parallel Custody Processor`);
      console.log(`💻 CPU cores detected: ${this.numCPUs}`);
      console.log(`👥 Worker threads: ${this.maxWorkers}`);
      console.log(`==============================\n`);

      // Initialize PostgreSQL schema
      await this.schema.initializeDatabase();
      console.log('✅ PostgreSQL schema ready\n');

      // Migrate existing data if needed
      console.log('🔄 Checking for existing data migration...');
      await this.schema.migrateToDateBasedTables();
    }

    async getAllCollections() {
      const mongoClient = new MongoClient(config.mongodb.uri);
      await mongoClient.connect();
      
      const databases = ['financial_data_2024', 'financial_data_2025'];
      const allCollections = [];

      for (const dbName of databases) {
        try {
          const db = mongoClient.db(dbName);
          const collections = await db.listCollections().toArray();
          
          console.log(`📋 Database ${dbName}: ${collections.length} collections`);
          
          for (const collection of collections) {
            allCollections.push({
              dbName,
              collection
            });
          }
        } catch (error) {
          console.warn(`⚠️  Could not access database ${dbName}:`, error.message);
        }
      }

      await mongoClient.close();
      return allCollections;
    }

    async processWorker(dbName, collectionInfo) {
      return new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
          workerData: {
            collectionInfo,
            dbName,
            mongoUri: config.mongodb.uri
          }
        });

        this.activeWorkers.add(worker);

        worker.on('message', (message) => {
          if (message.type === 'progress') {
            this.handleProgress(message.data);
          } else if (message.type === 'result') {
            this.handleResult(message.data);
            resolve(message.data);
          } else if (message.type === 'error') {
            reject(new Error(message.data.error));
          }
        });

        worker.on('error', reject);
        
        worker.on('exit', (code) => {
          this.activeWorkers.delete(worker);
          if (code !== 0) {
            reject(new Error(`Worker stopped with exit code ${code}`));
          }
        });
      });
    }

    handleProgress(data) {
      if (data.status === 'starting') {
        const orbisNote = data.isOrbis ? ' [Orbis: N/A client names]' : '';
        console.log(`🔄 [Worker] ${data.collection} → ${data.recordDate} (${data.custodyType})${orbisNote}`);
      } else if (data.status === 'processing') {
        const progress = Math.round((data.processed / data.total) * 100);
        const custodyNote = data.custodyType === 'ORBIS' ? ' 🌟' : '';
        console.log(`   📊 ${progress}% | Valid: ${data.valid} | Errors: ${data.errors} | DB: +${data.inserted}↑ +${data.updated}↻${custodyNote}`);
      }
    }

    handleResult(data) {
      this.results.push(data);
      this.completedCollections++;
      
      if (data.success) {
        const rate = data.totalProcessed > 0 ? ((data.validRecords / data.totalProcessed) * 100).toFixed(1) : '0';
        const qualityNote = data.dataQualityNotes ? ` (${data.dataQualityNotes})` : '';
        console.log(`✅ [${data.collection}] ${data.validRecords}/${data.totalProcessed} records (${rate}% success)${qualityNote}`);
      } else {
        console.log(`❌ [${data.collection}] Failed: ${data.error}`);
      }
      
      console.log(`📈 Overall Progress: ${this.completedCollections}/${this.totalCollections} collections\n`);
    }

    async processCollections() {
      try {
        console.log('🔍 Discovering collections across databases...');
        const allCollections = await this.getAllCollections();
        this.totalCollections = allCollections.length;
        
        console.log(`\n🎯 Total collections to process: ${this.totalCollections}`);
        console.log(`⚡ Starting parallel processing with ${this.maxWorkers} workers...\n`);

        if (this.totalCollections === 0) {
          console.log('📝 No collections found to process');
          return;
        }

        // Process collections in parallel with controlled concurrency
        const results = [];
        const executing = [];

        for (const { dbName, collection } of allCollections) {
          const promise = this.processWorker(dbName, collection)
            .catch(error => {
              console.error(`❌ Worker error for ${collection.name}:`, error.message);
              return { collection: collection.name, success: false, error: error.message };
            });

          results.push(promise);

          if (results.length >= this.maxWorkers) {
            executing.push(results.shift());
          }

          // If we have max workers running, wait for one to complete
          if (executing.length >= this.maxWorkers) {
            await Promise.race(executing);
            executing.splice(executing.findIndex(p => p.finished), 1);
          }
        }

        // Wait for remaining workers
        await Promise.all([...executing, ...results]);

        // Final summary
        this.printSummary();

      } catch (error) {
        console.error('❌ Processing failed:', error.message);
        throw error;
      }
    }

    printSummary() {
      const processingTime = ((Date.now() - this.startTime) / 1000).toFixed(1);
      
      console.log('\n🎯 PARALLEL PROCESSING SUMMARY');
      console.log('==============================');

      const successfulResults = this.results.filter(r => r.success);
      const failedResults = this.results.filter(r => !r.success);
      
      const totalProcessed = successfulResults.reduce((sum, r) => sum + (r.totalProcessed || 0), 0);
      const totalValid = successfulResults.reduce((sum, r) => sum + (r.validRecords || 0), 0);
      const totalErrors = successfulResults.reduce((sum, r) => sum + (r.errors || 0), 0);

      console.log(`⏱️  Processing time: ${processingTime}s`);
      console.log(`👥 Workers used: ${this.maxWorkers}`);
      console.log(`📊 Collections processed: ${successfulResults.length}/${this.totalCollections}`);
      console.log(`✅ Successful: ${successfulResults.length}`);
      console.log(`❌ Failed: ${failedResults.length}`);
      console.log(`📈 Total records: ${totalProcessed.toLocaleString()}`);
      console.log(`✅ Valid records: ${totalValid.toLocaleString()}`);
      console.log(`❌ Error records: ${totalErrors.toLocaleString()}`);
      
      if (totalProcessed > 0) {
        const throughput = Math.round(totalProcessed / parseFloat(processingTime));
        console.log(`📊 Success rate: ${((totalValid / totalProcessed) * 100).toFixed(2)}%`);
        console.log(`⚡ Throughput: ${throughput.toLocaleString()} records/second`);
      }

      // Group by date
      const byDate = {};
      successfulResults.forEach(result => {
        if (result.recordDate && result.validRecords > 0) {
          if (!byDate[result.recordDate]) byDate[result.recordDate] = 0;
          byDate[result.recordDate] += result.validRecords;
        }
      });

      if (Object.keys(byDate).length > 0) {
        console.log('\n📅 Records by date:');
        Object.entries(byDate).forEach(([date, count]) => {
          console.log(`   ${date}: ${count.toLocaleString()} records`);
        });
      }

      if (failedResults.length > 0) {
        console.log('\n❌ Failed collections:');
        failedResults.forEach(result => {
          console.log(`   ${result.collection}: ${result.error}`);
        });
      }
    }

    async showFinalStats() {
      try {
        console.log('\n📊 FINAL DATABASE STATISTICS');
        console.log('============================');
        
        const overallStats = await this.loader.getOverallStats();
        console.log(`🗓️  Daily tables: ${overallStats.totalTables}`);
        console.log(`📊 Total records: ${overallStats.totalRecords.toLocaleString()}`);
        console.log(`👥 Unique clients: ${overallStats.uniqueClients.toLocaleString()}`);
        console.log(`🏢 Unique instruments: ${overallStats.uniqueInstruments.toLocaleString()}`);
        console.log(`🏛️  Source systems: ${overallStats.sourceSystems?.join(', ')}`);
        
        if (overallStats.dateRange) {
          console.log(`📅 Date range: ${overallStats.dateRange.from} → ${overallStats.dateRange.to}`);
        }
        
        console.log(`🔒 Blocked holdings: ${overallStats.recordsWithBlocked}`);
        console.log(`⏳ Pending buy: ${overallStats.recordsWithPendingBuy}`);
        console.log(`⏳ Pending sell: ${overallStats.recordsWithPendingSell}`);

      } catch (error) {
        console.error('❌ Failed to get final stats:', error.message);
      }
    }

    async cleanup() {
      // Terminate any remaining workers
      for (const worker of this.activeWorkers) {
        await worker.terminate();
      }
      
      await this.schema.close();
      await this.loader.close();
    }
  }

  // Main execution
  async function main() {
    const processor = new ParallelCustodyProcessor();
    
    try {
      await processor.initialize();
      await processor.processCollections();
      await processor.showFinalStats();
      
      console.log('\n🎉 SUCCESS: Parallel processing completed!');
      console.log('\n💡 Next steps:');
      console.log('   📊 View stats: node scripts/migrate-to-daily-tables.js');
      console.log('   🌐 Start API: node custody-api-server.js');
      console.log('   🔍 Query: curl http://localhost:3003/api/custody/overall-stats');
      
    } catch (error) {
      console.error('💥 FATAL ERROR:', error.message);
      process.exit(1);
    } finally {
      await processor.cleanup();
    }
  }

  // Run if called directly
  if (require.main === module) {
    main();
  }

  module.exports = ParallelCustodyProcessor;
} 