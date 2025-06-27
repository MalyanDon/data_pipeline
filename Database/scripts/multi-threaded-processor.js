#!/usr/bin/env node

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const mongoose = require('mongoose');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('../config');
const os = require('os');
const EventEmitter = require('events');

// Worker thread code for processing individual collections
if (!isMainThread) {
  const FieldMapper = require('../custody-normalization/extractors/fieldMapper');
  const DataNormalizer = require('../custody-normalization/extractors/dataNormalizer');
  const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');
  const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
  const { detectCustodyType, getCustodyConfig, normalizeOrbisRecord, validateCustodyRecord } = require('../custody-normalization/config/custody-mappings');

  class WorkerProcessor {
    constructor() {
      this.mongoClient = null;
      this.pgPool = new Pool(config.postgresql);
      this.schema = new NormalizationSchema();
      this.loader = new PostgresLoader();
    }

    async connect() {
      this.mongoClient = new MongoClient(config.mongodb.uri);
      await this.mongoClient.connect();
      await this.schema.initializeDatabase();
    }

    async disconnect() {
      if (this.mongoClient) await this.mongoClient.close();
      await this.pgPool.end();
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

    normalizeRecord(record, custodyType, recordDate) {
      try {
        const config = getCustodyConfig(custodyType.toLowerCase());
        if (!config) {
          throw new Error(`No configuration found for custody type: ${custodyType}`);
        }

        if (custodyType === 'ORBIS') {
          const { normalized, orbisSpecific } = normalizeOrbisRecord(record, config);
          
          if (!normalized.client_reference || !normalized.instrument_isin) {
            throw new Error(`Missing required fields for Orbis record`);
          }

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
        
        const client_reference = this.extractField(record, mapping.client_reference);
        const client_name = this.extractField(record, mapping.client_name);
        const instrument_isin = this.extractField(record, mapping.instrument_isin);
        const instrument_name = this.extractField(record, mapping.instrument_name);
        const instrument_code = this.extractField(record, mapping.instrument_code);

        const blocked_quantity = this.normalizeFinancialAmount(
          this.extractFinancialField(record, mapping.blocked_quantity, custodyType)
        );
        const pending_buy_quantity = this.normalizeFinancialAmount(
          this.extractFinancialField(record, mapping.pending_buy_quantity, custodyType)
        );
        const pending_sell_quantity = this.normalizeFinancialAmount(
          this.extractFinancialField(record, mapping.pending_sell_quantity, custodyType)
        );

        if (!client_reference || !client_name || !instrument_isin || !instrument_name) {
          throw new Error(`Missing required fields in record`);
        }

        if (!/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(instrument_isin)) {
          throw new Error(`Invalid ISIN format: ${instrument_isin}`);
        }

        const normalizedRecord = {
          client_reference: client_reference.trim(),
          client_name: client_name.trim(),
          instrument_isin: instrument_isin.trim(),
          instrument_name: instrument_name.trim(),
          instrument_code: instrument_code?.trim() || null,
          blocked_quantity,
          pending_buy_quantity,
          pending_sell_quantity,
          source_system: custodyType,
          record_date: recordDate
        };

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
      
      if (custodyType === 'AXIS' && fieldMappings.includes('DematLockedQty')) {
        const dematLocked = parseFloat(record.DematLockedQty || 0);
        const physicalLocked = parseFloat(record.PhysicalLocked || 0);
        return dematLocked + physicalLocked;
      }

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

    async processCollection(collectionInfo) {
      const { dbName, collectionName, workerId } = collectionInfo;
      
      try {
        const db = this.mongoClient.db(dbName);
        const collection = db.collection(collectionName);
        const custodyType = this.detectCustodyTypeFromCollection(collectionName);
        const recordDate = this.extractDateFromCollection(collectionName);
        
        const totalRecords = await collection.countDocuments();
        
        if (totalRecords === 0) {
          parentPort.postMessage({
            type: 'complete',
            workerId,
            collectionName,
            result: { processed: 0, valid: 0, errors: 0 }
          });
          return;
        }

        // Ensure daily table exists
        const tableExists = await this.schema.dailyTableExists(recordDate);
        if (!tableExists) {
          await this.schema.createDailyTable(recordDate);
        }

        const batchSize = 500;
        let processed = 0;
        let valid = 0;
        let errors = 0;

        const cursor = collection.find({});
        
        while (await cursor.hasNext()) {
          const batch = [];
          
          for (let i = 0; i < batchSize && await cursor.hasNext(); i++) {
            batch.push(await cursor.next());
          }

          if (batch.length === 0) break;

          const normalizedRecords = [];
          
          for (const record of batch) {
            try {
              const normalized = this.normalizeRecord(record, custodyType, recordDate);
              normalizedRecords.push(normalized);
              valid++;
            } catch (error) {
              errors++;
            }
          }

          if (normalizedRecords.length > 0) {
            const tableName = this.schema.getTableName(recordDate);
            await this.loader._loadBatchToTable(
              normalizedRecords, 
              tableName, 
              custodyType, 
              collectionName
            );
          }

          processed += batch.length;
          
          // Send progress update
          parentPort.postMessage({
            type: 'progress',
            workerId,
            collectionName,
            progress: {
              processed,
              total: totalRecords,
              valid,
              errors,
              percentage: Math.round((processed / totalRecords) * 100)
            }
          });
        }

        await cursor.close();

        parentPort.postMessage({
          type: 'complete',
          workerId,
          collectionName,
          result: { processed, valid, errors }
        });

      } catch (error) {
        parentPort.postMessage({
          type: 'error',
          workerId,
          collectionName,
          error: error.message
        });
      }
    }
  }

  // Worker main execution
  (async () => {
    const processor = new WorkerProcessor();
    try {
      await processor.connect();
      await processor.processCollection(workerData);
    } catch (error) {
      parentPort.postMessage({
        type: 'error',
        workerId: workerData.workerId,
        collectionName: workerData.collectionName,
        error: error.message
      });
    } finally {
      await processor.disconnect();
    }
  })();

} else {
  // Main thread code
  class MultiThreadedETLProcessor extends EventEmitter {
    constructor() {
      super();
      this.maxWorkers = Math.min(os.cpus().length, 8); // Use up to 8 cores
      this.activeWorkers = new Map();
      this.results = new Map();
      this.overallProgress = {
        totalCollections: 0,
        completedCollections: 0,
        totalRecords: 0,
        processedRecords: 0,
        validRecords: 0,
        errorRecords: 0
      };
    }

    async discoverCollections() {
      const mongoClient = new MongoClient(config.mongodb.uri);
      await mongoClient.connect();
      
      const collections = [];
      const databases = ['financial_data_2024', 'financial_data_2025'];
      
      for (const dbName of databases) {
        try {
          const db = mongoClient.db(dbName);
          const collectionList = await db.listCollections().toArray();
          
          for (const collectionInfo of collectionList) {
            const collectionName = collectionInfo.name;
            const recordCount = await db.collection(collectionName).countDocuments();
            
            if (recordCount > 0) {
              collections.push({
                dbName,
                collectionName,
                recordCount
              });
              this.overallProgress.totalRecords += recordCount;
            }
          }
        } catch (error) {
          console.error(`Error accessing database ${dbName}:`, error.message);
        }
      }
      
      await mongoClient.close();
      this.overallProgress.totalCollections = collections.length;
      return collections;
    }

    createWorker(collectionInfo, workerId) {
      return new Promise((resolve, reject) => {
        const worker = new Worker(__filename, {
          workerData: { ...collectionInfo, workerId }
        });

        const timeout = setTimeout(() => {
          worker.terminate();
          reject(new Error(`Worker ${workerId} timed out`));
        }, 300000); // 5 minute timeout

        worker.on('message', (message) => {
          clearTimeout(timeout);
          
          if (message.type === 'progress') {
            this.handleProgress(message);
          } else if (message.type === 'complete') {
            this.handleComplete(message);
            resolve(message.result);
          } else if (message.type === 'error') {
            this.handleError(message);
            reject(new Error(message.error));
          }
        });

        worker.on('error', (error) => {
          clearTimeout(timeout);
          reject(error);
        });

        this.activeWorkers.set(workerId, worker);
      });
    }

    handleProgress(message) {
      const { workerId, collectionName, progress } = message;
      
      // Update overall progress
      this.overallProgress.processedRecords = Array.from(this.results.values())
        .reduce((sum, result) => sum + (result.processed || 0), 0) + progress.processed;
      
      this.overallProgress.validRecords = Array.from(this.results.values())
        .reduce((sum, result) => sum + (result.valid || 0), 0) + progress.valid;
      
      this.overallProgress.errorRecords = Array.from(this.results.values())
        .reduce((sum, result) => sum + (result.errors || 0), 0) + progress.errors;

      // Emit progress event for real-time updates
      this.emit('progress', {
        workerId,
        collectionName,
        collectionProgress: progress,
        overallProgress: {
          ...this.overallProgress,
          overallPercentage: Math.round((this.overallProgress.processedRecords / this.overallProgress.totalRecords) * 100)
        }
      });
    }

    handleComplete(message) {
      const { workerId, collectionName, result } = message;
      this.results.set(collectionName, result);
      this.overallProgress.completedCollections++;
      
      console.log(`✅ Worker ${workerId} completed: ${collectionName} (${result.valid}/${result.processed} valid)`);
      
      this.emit('complete', {
        workerId,
        collectionName,
        result,
        overallProgress: this.overallProgress
      });
    }

    handleError(message) {
      const { workerId, collectionName, error } = message;
      console.error(`❌ Worker ${workerId} failed: ${collectionName} - ${error}`);
      
      this.emit('error', {
        workerId,
        collectionName,
        error
      });
    }

    async processAllCollections() {
      console.log(`🚀 Starting multi-threaded ETL processing with ${this.maxWorkers} workers...`);
      
      try {
        const collections = await this.discoverCollections();
        
        if (collections.length === 0) {
          throw new Error('No collections found to process');
        }

        console.log(`📊 Processing ${collections.length} collections with ${this.overallProgress.totalRecords} total records`);
        
        this.emit('start', {
          totalCollections: collections.length,
          totalRecords: this.overallProgress.totalRecords,
          maxWorkers: this.maxWorkers
        });

        // Process collections in batches based on available workers
        const results = [];
        for (let i = 0; i < collections.length; i += this.maxWorkers) {
          const batch = collections.slice(i, i + this.maxWorkers);
          
          const batchPromises = batch.map((collection, index) => {
            const workerId = i + index + 1;
            return this.createWorker(collection, workerId);
          });

          const batchResults = await Promise.allSettled(batchPromises);
          results.push(...batchResults);
        }

        // Clean up workers
        this.activeWorkers.forEach(worker => worker.terminate());
        this.activeWorkers.clear();

        const summary = {
          totalCollections: collections.length,
          successfulCollections: results.filter(r => r.status === 'fulfilled').length,
          failedCollections: results.filter(r => r.status === 'rejected').length,
          totalProcessed: this.overallProgress.processedRecords,
          totalValid: this.overallProgress.validRecords,
          totalErrors: this.overallProgress.errorRecords,
          successRate: Math.round((this.overallProgress.validRecords / this.overallProgress.processedRecords) * 100)
        };

        this.emit('finished', summary);
        return summary;

      } catch (error) {
        this.emit('error', { error: error.message });
        throw error;
      }
    }
  }

  module.exports = { MultiThreadedETLProcessor };
}

// CLI execution for testing
if (require.main === module && isMainThread) {
  const { MultiThreadedETLProcessor } = require(__filename);
  
  (async () => {
    const processor = new MultiThreadedETLProcessor();
    
    processor.on('start', (data) => {
      console.log(`🎯 Started: ${data.totalCollections} collections, ${data.totalRecords} records, ${data.maxWorkers} workers`);
    });

    processor.on('progress', (data) => {
      console.log(`📊 ${data.collectionName}: ${data.collectionProgress.percentage}% | Overall: ${data.overallProgress.overallPercentage}%`);
    });

    processor.on('complete', (data) => {
      console.log(`✅ Completed: ${data.collectionName}`);
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