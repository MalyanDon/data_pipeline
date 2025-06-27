#!/usr/bin/env node

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const path = require('path');
const os = require('os');
const EventEmitter = require('events');
const mongoose = require('mongoose');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

class MultiThreadedETLProcessor extends EventEmitter {
  constructor(options = {}) {
    super();
    this.maxWorkers = options.maxWorkers || Math.min(6, os.cpus().length - 2);
    this.isProcessing = false;
    this.activeWorkers = new Set();
    this.completedWorkers = 0;
    this.stats = {
      totalCollections: 0,
      totalRecords: 0,
      processedRecords: 0,
      validRecords: 0,
      errorRecords: 0,
      startTime: null,
      endTime: null
    };
  }

  /**
   * Discover all collections in MongoDB databases
   */
  async discoverCollections() {
    console.log('🔍 Discovering MongoDB collections...');
    
    try {
      const databases = ['financial_data_2024', 'financial_data_2025'];
      const allCollections = [];

      // Use MongoClient like the working implementation
      const mongoClient = new MongoClient(config.mongodb.uri);
      await mongoClient.connect();
      console.log('✅ Connected to MongoDB for discovery');

      for (const dbName of databases) {
        try {
          const db = mongoClient.db(dbName);
          const collections = await db.listCollections().toArray();
          
          console.log(`📋 Found ${collections.length} collections in ${dbName}`);
          
          for (const collectionInfo of collections) {
            const collectionName = collectionInfo.name;
            
            // Skip system collections
            if (collectionName.startsWith('system.')) continue;
            
            // Get record count
            const collection = db.collection(collectionName);
            const recordCount = await collection.countDocuments();
            
            if (recordCount > 0) {
              // Parse collection name for enhanced metadata
              const custodyType = this.detectCustodyType(collectionName);
              const nameParts = collectionName.split('_');
              let dataDate = null;
              let sourceType = collectionName;
              
              if (nameParts.length >= 4) {
                // Format: sourceType_YYYY_MM_DD
                sourceType = nameParts.slice(0, -3).join('_');
                dataDate = `${nameParts[nameParts.length-3]}-${nameParts[nameParts.length-2]}-${nameParts[nameParts.length-1]}`;
              }
              
              allCollections.push({
                database: dbName,
                collectionName,
                recordCount,
                custodyType,
                sourceType,
                dataDate,
                displayName: dataDate ? `${custodyType} (${dataDate})` : custodyType
              });
            }
          }
          
        } catch (dbError) {
          console.log(`⚠️  Database ${dbName} not accessible:`, dbError.message);
        }
      }

      await mongoClient.close();
      console.log(`✅ Found ${allCollections.length} collections with data`);
      return allCollections;
      
    } catch (error) {
      console.error('❌ Collection discovery failed:', error.message);
      throw error;
    }
  }

  /**
   * Detect custody type from collection name (supports both old and new date-based naming)
   */
  detectCustodyType(collectionName) {
    const name = collectionName.toLowerCase();
    
    // Handle date-based naming: sourceType_YYYY_MM_DD
    const dateParts = name.split('_');
    if (dateParts.length >= 4) {
      // Extract source type (everything before the date)
      const sourceType = dateParts.slice(0, -3).join('_');
      if (sourceType.includes('axis')) return 'AXIS';
      if (sourceType.includes('kotak')) return 'KOTAK';
      if (sourceType.includes('orbis')) return 'ORBIS';
      if (sourceType.includes('deutsche') || sourceType.includes('164_ec0000720')) return 'DEUTSCHE';
      if (sourceType.includes('trustpms')) return 'TRUSTPMS';
      if (sourceType.includes('hdfc')) return 'HDFC';
      if (sourceType.includes('icici')) return 'ICICI';
      if (sourceType.includes('edelweiss')) return 'EDELWEISS';
      if (sourceType.includes('zerodha')) return 'ZERODHA';
      if (sourceType.includes('nuvama')) return 'NUVAMA';
    }
    
    // Fallback to legacy naming (just source type)
    if (name.includes('axis')) return 'AXIS';
    if (name.includes('kotak')) return 'KOTAK';
    if (name.includes('orbis')) return 'ORBIS';
    if (name.includes('deutsche') || name.includes('164_ec0000720')) return 'DEUTSCHE';
    if (name.includes('trustpms')) return 'TRUSTPMS';
    if (name.includes('hdfc')) return 'HDFC';
    if (name.includes('icici')) return 'ICICI';
    if (name.includes('edelweiss')) return 'EDELWEISS';
    if (name.includes('zerodha')) return 'ZERODHA';
    if (name.includes('nuvama')) return 'NUVAMA';
    
    return 'UNKNOWN';
  }

  /**
   * Process all collections using worker threads
   */
  async processAllCollections() {
    if (this.isProcessing) {
      throw new Error('Processing already in progress');
    }

    try {
      this.isProcessing = true;
      this.stats.startTime = Date.now();
      
      console.log('🚀 Starting multi-threaded ETL processing...');
      
      // Discover all collections
      const collections = await this.discoverCollections();
      
      if (collections.length === 0) {
        throw new Error('No collections found to process');
      }

      this.stats.totalCollections = collections.length;
      this.stats.totalRecords = collections.reduce((sum, col) => sum + col.recordCount, 0);

      this.emit('start', {
        totalCollections: this.stats.totalCollections,
        totalRecords: this.stats.totalRecords,
        maxWorkers: this.maxWorkers
      });

      // Process collections in batches using worker threads
      const workers = [];
      const workerPromises = [];
      
      for (let i = 0; i < Math.min(this.maxWorkers, collections.length); i++) {
        const collectionBatch = collections.slice(i, i + 1);
        
        if (collectionBatch.length > 0) {
          const workerPromise = this.createWorker(i + 1, collectionBatch[0]);
          workers.push(workerPromise.worker);
          workerPromises.push(workerPromise.promise);
        }
      }

      // Handle remaining collections
      let collectionIndex = workers.length;
      const processNext = () => {
        if (collectionIndex < collections.length) {
          const collection = collections[collectionIndex++];
          const workerId = workers.length + 1;
          const workerPromise = this.createWorker(workerId, collection);
          workers.push(workerPromise.worker);
          workerPromises.push(workerPromise.promise);
          
          workerPromise.promise.then(() => {
            processNext();
          }).catch(() => {
            processNext();
          });
        }
      };

      // Wait for all workers to complete
      await Promise.allSettled(workerPromises);
      
      // Cleanup workers
      workers.forEach(worker => {
        if (!worker.isTerminated) {
          worker.terminate();
        }
      });

      this.stats.endTime = Date.now();
      const processingTime = (this.stats.endTime - this.stats.startTime) / 1000;
      const successRate = Math.round((this.stats.validRecords / this.stats.processedRecords) * 100);

      this.emit('finished', {
        totalProcessed: this.stats.processedRecords,
        validRecords: this.stats.validRecords,
        errorRecords: this.stats.errorRecords,
        processingTime: processingTime,
        successRate: successRate
      });

      console.log(`🎉 Multi-threaded processing complete!`);
      console.log(`📊 Processed: ${this.stats.processedRecords.toLocaleString()} records`);
      console.log(`✅ Valid: ${this.stats.validRecords.toLocaleString()} records`);
      console.log(`❌ Errors: ${this.stats.errorRecords.toLocaleString()} records`);
      console.log(`⏱️  Time: ${processingTime.toFixed(2)}s`);
      console.log(`🎯 Success Rate: ${successRate}%`);

    } catch (error) {
      this.emit('error', { error: error.message });
      throw error;
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Create a worker thread for processing a collection
   */
  createWorker(workerId, collection) {
    const worker = new Worker(__filename, {
      workerData: {
        workerId,
        collection,
        config: config
      }
    });

    this.activeWorkers.add(workerId);

    const promise = new Promise((resolve, reject) => {
      worker.on('message', (message) => {
        switch (message.type) {
          case 'progress':
            this.stats.processedRecords += message.data.batchSize;
            this.stats.validRecords += message.data.validCount;
            this.stats.errorRecords += message.data.errorCount;
            
            this.emit('progress', {
              workerId,
              collectionName: collection.collectionName,
              custodyType: collection.custodyType,
              collectionProgress: message.data.collectionProgress,
              overallProgress: {
                processedRecords: this.stats.processedRecords,
                totalRecords: this.stats.totalRecords,
                validRecords: this.stats.validRecords,
                errorRecords: this.stats.errorRecords,
                overallPercentage: Math.round((this.stats.processedRecords / this.stats.totalRecords) * 100)
              }
            });
            break;

          case 'complete':
            this.activeWorkers.delete(workerId);
            this.completedWorkers++;
            
            this.emit('complete', {
              workerId,
              collectionName: collection.collectionName,
              custodyType: collection.custodyType,
              result: message.data
            });
            
            resolve(message.data);
            break;

          case 'error':
            this.activeWorkers.delete(workerId);
            this.emit('error', {
              workerId,
              collectionName: collection.collectionName,
              error: message.error
            });
            reject(new Error(message.error));
            break;
        }
      });

      worker.on('error', (error) => {
        this.activeWorkers.delete(workerId);
        this.emit('error', {
          workerId,
          collectionName: collection.collectionName,
          error: error.message
        });
        reject(error);
      });

      worker.on('exit', (code) => {
        this.activeWorkers.delete(workerId);
        if (code !== 0) {
          reject(new Error(`Worker ${workerId} exited with code ${code}`));
        }
      });
    });

    return { worker, promise };
  }
}

// Worker thread code
if (!isMainThread) {
  const { workerId, collection, config } = workerData;
  
  processCollectionInWorker(workerId, collection, config)
    .then(result => {
      parentPort.postMessage({ type: 'complete', data: result });
    })
    .catch(error => {
      parentPort.postMessage({ type: 'error', error: error.message });
    });
}

async function processCollectionInWorker(workerId, collection, config) {
  const { MongoClient } = require('mongodb');
  const { Pool } = require('pg');
  const { CustodyNormalizationPipeline } = require('./custody-normalization/pipeline/custodyNormalizationPipeline');

  try {
    console.log(`🔧 Worker ${workerId}: Processing ${collection.collectionName} (${collection.custodyType})`);

    // Connect to MongoDB using MongoClient like the working implementation
    const mongoClient = new MongoClient(config.mongodb.uri);
    await mongoClient.connect();
    console.log(`✅ Worker ${workerId}: Connected to MongoDB`);
    
    const db = mongoClient.db(collection.database);
    const mongoCollection = db.collection(collection.collectionName);

    // Connect to PostgreSQL with pool
    const pgPool = new Pool({
      connectionString: config.postgresql.connectionString,
      max: 2,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
    });

    // Initialize pipeline
    const pipeline = new CustodyNormalizationPipeline(pgPool);

    // Process in batches
    const batchSize = 250;
    const totalRecords = collection.recordCount;
    let processed = 0;
    let validCount = 0;
    let errorCount = 0;

    const cursor = mongoCollection.find({}).batchSize(batchSize);

    while (await cursor.hasNext()) {
      const batch = [];
      
      // Collect batch
      for (let i = 0; i < batchSize && await cursor.hasNext(); i++) {
        batch.push(await cursor.next());
      }

      if (batch.length === 0) break;

      try {
        // Process batch through pipeline
        const results = await pipeline.processBatch(batch, collection.custodyType, collection.collectionName);
        
        validCount += results.validCount;
        errorCount += results.errorCount;
        processed += batch.length;

        // Send progress update
        parentPort.postMessage({
          type: 'progress',
          data: {
            batchSize: batch.length,
            validCount: results.validCount,
            errorCount: results.errorCount,
            collectionProgress: {
              processed,
              total: totalRecords,
              percentage: Math.round((processed / totalRecords) * 100)
            }
          }
        });

      } catch (batchError) {
        console.error(`❌ Worker ${workerId}: Batch processing error:`, batchError.message);
        errorCount += batch.length;
        processed += batch.length;
      }
    }

    // Cleanup
    await mongoClient.close();
    await pgPool.end();

    console.log(`✅ Worker ${workerId}: Completed ${collection.collectionName} - ${validCount}/${processed} valid`);

    return {
      processed,
      valid: validCount,
      errors: errorCount,
      collectionName: collection.collectionName,
      custodyType: collection.custodyType
    };

  } catch (error) {
    console.error(`❌ Worker ${workerId}: Failed to process ${collection.collectionName}:`, error.message);
    throw error;
  }
}

module.exports = { MultiThreadedETLProcessor };

// CLI execution
if (require.main === module && isMainThread) {
  const { MultiThreadedETLProcessor } = require(__filename);
  
  (async () => {
    const processor = new MultiThreadedETLProcessor();
    
    processor.on('start', (data) => {
      console.log(`🎯 Started: ${data.totalCollections} collections, ${data.totalRecords} records, ${data.maxWorkers} workers`);
    });

    processor.on('progress', (data) => {
      console.log(`📊 Worker ${data.workerId} | ${data.collectionName} (${data.custodyType}): ${data.collectionProgress.percentage}% | Overall: ${data.overallProgress.overallPercentage}%`);
    });

    processor.on('complete', (data) => {
      console.log(`✅ Worker ${data.workerId} completed: ${data.collectionName} (${data.custodyType}) - ${data.result.valid}/${data.result.processed} valid`);
    });

    processor.on('finished', (summary) => {
      console.log(`🎉 All processing complete! Success rate: ${summary.successRate}%`);
    });

    try {
      await processor.processAllCollections();
    } catch (error) {
      console.error('💥 Processing failed:', error.message);
    }
  })();
} 