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

      // Load to PostgreSQL in smaller chunks to avoid timeout
      const chunkSize = 500;
      let totalLoaded = 0;
      
      for (let i = 0; i < normalizationResult.normalizedRecords.length; i += chunkSize) {
        const chunk = normalizationResult.normalizedRecords.slice(i, i + chunkSize);
        const loadResult = await postgresLoader.loadRecords(chunk);
        
        const chunkLoaded = loadResult.stats ? 
          (loadResult.stats.insertedRecords + loadResult.stats.updatedRecords) : 0;
        totalLoaded += chunkLoaded;
      }
      
      await postgresLoader.close();
      
      parentPort.postMessage({
        success: true,
        workerId,
        loadedRecords: totalLoaded,
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
class FastMongoDBProcessor {
  constructor() {
    this.numWorkers = Math.min(os.cpus().length, 4); // Reduced workers for stability
    this.processedFiles = 0;
    this.totalRecords = 0;
    this.totalValid = 0;
    this.totalInvalid = 0;
    this.errors = [];
    this.startTime = Date.now();
  }

  async processAllCustodyData() {
    console.log('⚡ Fast MongoDB → PostgreSQL Processing');
    console.log(`🚀 Using ${this.numWorkers} worker threads with optimized batching\n`);

    try {
      // Connect to MongoDB
      await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
      console.log('✅ Connected to MongoDB Atlas (financial_data_2025)\n');

      // Define custody collection mappings
      const custodyMappings = [
        { collection: 'axis_06_25', custodyType: 'axis', fileName: 'axis_eod_custody_2025-06-25.xlsx' },
        { collection: 'kotak_06_25', custodyType: 'kotak', fileName: 'kotak_eod_custody_2025-06-25.xlsx' },
        { collection: 'orbis_06_25', custodyType: 'orbis', fileName: 'orbisCustody25_06_2025.xlsx' }
        // Note: Skipping TrustPMS as it's already processed successfully
      ];

      // Process collections sequentially to avoid overwhelming
      for (const mapping of custodyMappings) {
        await this.processCollectionFast(mapping);
      }

      // Print summary
      const totalTime = (Date.now() - this.startTime) / 1000;
      console.log('\n📊 Fast Processing Summary');
      console.log('===========================');
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
        console.log('\n🎉 Fast processing completed!');
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

  async processCollectionFast(mapping) {
    try {
      console.log(`🔄 Processing: ${mapping.collection} (${mapping.custodyType})`);
      
      const db = mongoose.connection.db;
      const collection = db.collection(mapping.collection);
      
      // Get total count for progress
      const totalCount = await collection.countDocuments();
      console.log(`   📊 Total records: ${totalCount.toLocaleString()}`);
      
      // Use smaller batch sizes for better performance
      const batchSize = 1000;
      let collectionTotal = 0;
      let collectionValid = 0;
      let collectionInvalid = 0;
      let processedCount = 0;

      // Process in cursor-based streaming to avoid memory issues
      const cursor = collection.find({});
      
      while (await cursor.hasNext()) {
        const batch = [];
        
        // Collect batch
        for (let i = 0; i < batchSize && await cursor.hasNext(); i++) {
          const doc = await cursor.next();
          // Clean document
          delete doc._id;
          delete doc.__v;
          delete doc.month;
          delete doc.date;
          delete doc.fullDate;
          delete doc.fileName;
          delete doc.fileType;
          delete doc.uploadedAt;
          batch.push(doc);
        }
        
        if (batch.length === 0) break;
        
        processedCount += batch.length;
        const progress = Math.round((processedCount / totalCount) * 100);
        
        console.log(`   📦 Processing batch: ${batch.length} records (${progress}% complete)`);
        
        // Process batch with worker
        const result = await this.processWithWorker(batch, mapping, Math.ceil(processedCount / batchSize));
        
        if (result.success) {
          collectionTotal += result.loadedRecords;
          collectionValid += result.validRecords;
          collectionInvalid += result.invalidRecords;
          console.log(`      ✅ Loaded: ${result.loadedRecords}/${result.originalBatchSize} records`);
        } else {
          console.log(`      ❌ Batch failed: ${result.error}`);
        }
      }
      
      console.log(`   ✅ Collection completed: ${collectionTotal.toLocaleString()} records loaded`);
      console.log(`   📊 Valid: ${collectionValid.toLocaleString()}, Invalid: ${collectionInvalid.toLocaleString()}`);
      
      this.processedFiles++;
      this.totalRecords += totalCount;
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
          error: 'Worker timeout (120s)',
          loadedRecords: 0
        });
      }, 120000); // 2 minute timeout per batch

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
  const processor = new FastMongoDBProcessor();
  await processor.processAllCustodyData();
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = FastMongoDBProcessor; 