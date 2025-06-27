#!/usr/bin/env node

const mongoose = require('mongoose');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const os = require('os');
const config = require('../config');

// Worker thread code
if (!isMainThread) {
  const FieldMapper = require('../custody-normalization/extractors/fieldMapper');
  const DataNormalizer = require('../custody-normalization/extractors/dataNormalizer');
  const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');

  const processBatchWorker = async () => {
    const { batch, mapping, workerId } = workerData;
    
    try {
      const fieldMapper = new FieldMapper();
      const dataNormalizer = new DataNormalizer();
      const postgresLoader = new PostgresLoader();

      // Metadata for mapping
      const metadata = {
        sourceSystem: mapping.custodyType.toUpperCase(),
        fileName: mapping.fileName,
        recordDate: '2025-06-25'
      };

      // Map fields
      const mappedRecords = [];
      for (const record of batch) {
        const { mappedRecord } = fieldMapper.mapRecord(record, mapping.custodyType, metadata);
        mappedRecords.push(mappedRecord);
      }

      // Normalize data
      const normalizationResult = dataNormalizer.normalizeRecords(mappedRecords);
      
      if (normalizationResult.normalizedRecords.length === 0) {
        parentPort.postMessage({
          success: true,
          workerId,
          loadedRecords: 0,
          validRecords: 0,
          invalidRecords: batch.length
        });
        await postgresLoader.close();
        return;
      }

      // Load to PostgreSQL
      const loadResult = await postgresLoader.loadRecords(normalizationResult.normalizedRecords);
      
      const loadedRecords = loadResult.stats ? 
        (loadResult.stats.insertedRecords + loadResult.stats.updatedRecords) : 0;
      
      await postgresLoader.close();
      
      parentPort.postMessage({
        success: true,
        workerId,
        loadedRecords,
        validRecords: normalizationResult.normalizedRecords.length,
        invalidRecords: normalizationResult.invalidCount,
        originalBatchSize: batch.length
      });

    } catch (error) {
      parentPort.postMessage({
        success: false,
        workerId,
        error: error.message,
        loadedRecords: 0
      });
    }
  };

  processBatchWorker();
  return;
}

// Main thread code
class ParallelMongoDBProcessor {
  constructor() {
    this.numWorkers = Math.min(os.cpus().length, 8); // Use up to 8 workers
    this.activeWorkers = new Set();
    this.processedFiles = 0;
    this.totalRecords = 0;
    this.totalValid = 0;
    this.totalInvalid = 0;
    this.errors = [];
    this.startTime = Date.now();
  }

  async processAllCustodyData() {
    console.log('🚀 High-Performance Parallel MongoDB → PostgreSQL Processing');
    console.log(`⚡ Using ${this.numWorkers} worker threads for maximum speed\n`);

    try {
      // Connect to MongoDB
      await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
      console.log('✅ Connected to MongoDB Atlas (financial_data_2025)\n');

      // Define custody collection mappings
      const custodyMappings = [
        { collection: 'axis_06_25', custodyType: 'axis', fileName: 'axis_eod_custody_2025-06-25.xlsx' },
        { collection: 'kotak_06_25', custodyType: 'kotak', fileName: 'kotak_eod_custody_2025-06-25.xlsx' },
        { collection: 'orbis_06_25', custodyType: 'orbis', fileName: 'orbisCustody25_06_2025.xlsx' },
        { collection: 'end_client_holding_trustpms_20250625004802_270225530_new_06_25', custodyType: 'trustpms', fileName: 'End_Client_Holding_TRUSTPMS_2025.xls' }
      ];

      // Process all collections in parallel
      const collectionPromises = custodyMappings.map(mapping => 
        this.processCollectionParallel(mapping)
      );

      await Promise.all(collectionPromises);

      // Print summary
      const totalTime = (Date.now() - this.startTime) / 1000;
      console.log('\n📊 High-Performance Processing Summary');
      console.log('======================================');
      console.log(`Collections processed: ${this.processedFiles}/${custodyMappings.length}`);
      console.log(`Total records processed: ${this.totalRecords.toLocaleString()}`);
      console.log(`Valid records loaded: ${this.totalValid.toLocaleString()}`);
      console.log(`Invalid records skipped: ${this.totalInvalid.toLocaleString()}`);
      console.log(`Processing time: ${totalTime.toFixed(2)}s`);
      console.log(`Throughput: ${Math.round(this.totalRecords / totalTime).toLocaleString()} records/second`);
      
      if (this.errors.length > 0) {
        console.log(`\n❌ Errors (${this.errors.length}):`);
        this.errors.forEach(error => {
          console.log(`   - ${error.collection}: ${error.error}`);
        });
      }

      if (this.processedFiles > 0) {
        console.log('\n🎉 High-speed processing completed!');
        console.log('\n📈 View statistics:');
        console.log('   npm run custody-stats');
        console.log('\n🔍 Query unified data:');
        console.log('   curl "http://localhost:3003/api/custody/unified-data?limit=5"');
      }

    } catch (error) {
      console.error('❌ Processing failed:', error.message);
    } finally {
      await mongoose.disconnect();
    }
  }

  async processCollectionParallel(mapping) {
    try {
      console.log(`🔄 Processing: ${mapping.collection} (${mapping.custodyType})`);
      
      const db = mongoose.connection.db;
      const collection = db.collection(mapping.collection);
      
      // Get total count for progress
      const totalCount = await collection.countDocuments();
      console.log(`   📊 Total records: ${totalCount.toLocaleString()}`);
      
      // Get all documents and prepare batches
      const batchSize = 2000; // Larger batches for parallel processing
      const allDocs = await collection.find({}).toArray();
      
      // Clean documents
      const cleanedDocs = allDocs.map(doc => {
        delete doc._id;
        delete doc.__v;
        delete doc.month;
        delete doc.date;
        delete doc.fullDate;
        delete doc.fileName;
        delete doc.fileType;
        delete doc.uploadedAt;
        return doc;
      });

      // Create batches
      const batches = [];
      for (let i = 0; i < cleanedDocs.length; i += batchSize) {
        batches.push(cleanedDocs.slice(i, i + batchSize));
      }

      console.log(`   📦 Processing ${batches.length} batches with ${this.numWorkers} workers`);
      
      // Process batches in parallel with worker threads
      let collectionTotal = 0;
      let collectionValid = 0;
      let collectionInvalid = 0;
      
      // Process batches in chunks to avoid overwhelming the system
      const workerChunkSize = this.numWorkers;
      
      for (let i = 0; i < batches.length; i += workerChunkSize) {
        const batchChunk = batches.slice(i, i + workerChunkSize);
        
        const workerPromises = batchChunk.map((batch, index) => 
          this.processWithWorker(batch, mapping, i + index + 1)
        );
        
        const results = await Promise.all(workerPromises);
        
        // Aggregate results
        for (const result of results) {
          if (result.success) {
            collectionTotal += result.loadedRecords;
            collectionValid += result.validRecords;
            collectionInvalid += result.invalidRecords;
            console.log(`      ✅ Batch ${result.batchNumber}: ${result.loadedRecords}/${result.originalBatchSize} loaded`);
          } else {
            console.log(`      ❌ Batch ${result.batchNumber}: ${result.error}`);
          }
        }
        
        // Progress update
        const processedBatches = Math.min(i + workerChunkSize, batches.length);
        const progress = Math.round((processedBatches / batches.length) * 100);
        console.log(`   📈 Progress: ${progress}% (${processedBatches}/${batches.length} batches)`);
      }
      
      console.log(`   ✅ Collection completed: ${collectionTotal.toLocaleString()} records loaded`);
      console.log(`   📊 Valid: ${collectionValid.toLocaleString()}, Invalid: ${collectionInvalid.toLocaleString()}`);
      
      this.processedFiles++;
      this.totalRecords += cleanedDocs.length;
      this.totalValid += collectionValid;
      this.totalInvalid += collectionInvalid;

    } catch (error) {
      console.error(`❌ Error processing ${mapping.collection}:`, error.message);
      this.errors.push({
        collection: mapping.collection,
        error: error.message
      });
    }
  }

  async processWithWorker(batch, mapping, batchNumber) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(__filename, {
        workerData: { 
          batch, 
          mapping, 
          workerId: `worker-${Date.now()}-${Math.random().toString(36).substr(2, 9)}` 
        }
      });

      const timeout = setTimeout(() => {
        worker.terminate();
        resolve({
          success: false,
          batchNumber,
          error: 'Worker timeout',
          loadedRecords: 0
        });
      }, 60000); // 60 second timeout per batch

      worker.on('message', (result) => {
        clearTimeout(timeout);
        resolve({
          ...result,
          batchNumber
        });
      });

      worker.on('error', (error) => {
        clearTimeout(timeout);
        resolve({
          success: false,
          batchNumber,
          error: error.message,
          loadedRecords: 0
        });
      });

      worker.on('exit', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          resolve({
            success: false,
            batchNumber,
            error: `Worker exited with code ${code}`,
            loadedRecords: 0
          });
        }
      });
    });
  }
}

// Main execution
async function main() {
  const processor = new ParallelMongoDBProcessor();
  await processor.processAllCustodyData();
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = ParallelMongoDBProcessor; 