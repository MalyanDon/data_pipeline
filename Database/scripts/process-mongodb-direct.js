#!/usr/bin/env node

const mongoose = require('mongoose');
const config = require('../config');
const FieldMapper = require('../custody-normalization/extractors/fieldMapper');
const DataNormalizer = require('../custody-normalization/extractors/dataNormalizer');
const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');

class DirectMongoDBProcessor {
  constructor() {
    this.fieldMapper = new FieldMapper();
    this.dataNormalizer = new DataNormalizer();
    this.postgresLoader = new PostgresLoader();
    this.processedFiles = 0;
    this.totalRecords = 0;
    this.errors = [];
  }

  async processAllCustodyData() {
    console.log('🚀 Direct MongoDB → PostgreSQL Processing\n');

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

      for (const mapping of custodyMappings) {
        await this.processCollection(mapping);
      }

      // Print summary
      console.log('\n📊 Processing Summary');
      console.log('=====================');
      console.log(`Collections processed: ${this.processedFiles}/${custodyMappings.length}`);
      console.log(`Total records loaded: ${this.totalRecords.toLocaleString()}`);
      
      if (this.errors.length > 0) {
        console.log(`\n❌ Errors (${this.errors.length}):`);
        this.errors.forEach(error => {
          console.log(`   - ${error.collection}: ${error.error}`);
        });
      }

      if (this.processedFiles > 0) {
        console.log('\n🎉 Processing completed!');
        console.log('\n📈 View statistics:');
        console.log('   npm run custody-stats');
        console.log('\n🔍 Query unified data:');
        console.log('   curl "http://localhost:3003/api/custody/unified-data?limit=5"');
      }

    } catch (error) {
      console.error('❌ Processing failed:', error.message);
    } finally {
      await mongoose.disconnect();
      await this.postgresLoader.close();
    }
  }

  async processCollection(mapping) {
    try {
      console.log(`🔄 Processing: ${mapping.collection} (${mapping.custodyType})`);
      
      const db = mongoose.connection.db;
      const collection = db.collection(mapping.collection);
      
      // Get documents in batches to avoid memory issues
      const cursor = collection.find({});
      const batchSize = 1000;
      let batchNumber = 1;
      let collectionTotal = 0;
      
      while (await cursor.hasNext()) {
        const batch = [];
        
        // Collect batch
        for (let i = 0; i < batchSize && await cursor.hasNext(); i++) {
          const doc = await cursor.next();
          // Remove MongoDB metadata
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
        
        console.log(`   📦 Processing batch ${batchNumber} (${batch.length} records)`);
        
        // Process batch
        const batchResult = await this.processBatch(batch, mapping);
        collectionTotal += batchResult.loadedRecords;
        
        batchNumber++;
      }
      
      console.log(`   ✅ Collection completed: ${collectionTotal} records loaded`);
      this.processedFiles++;
      this.totalRecords += collectionTotal;

    } catch (error) {
      console.error(`❌ Error processing ${mapping.collection}:`, error.message);
      this.errors.push({
        collection: mapping.collection,
        error: error.message
      });
    }
  }

  async processBatch(batch, mapping) {
    try {
      // Metadata for mapping
      const metadata = {
        sourceSystem: mapping.custodyType.toUpperCase(),
        fileName: mapping.fileName,
        recordDate: '2025-06-25'
      };

      // Map fields
      const mappedRecords = [];
      for (const record of batch) {
        const { mappedRecord } = this.fieldMapper.mapRecord(record, mapping.custodyType, metadata);
        mappedRecords.push(mappedRecord);
      }

      // Normalize data
      const normalizationResult = this.dataNormalizer.normalizeRecords(mappedRecords);
      
      if (normalizationResult.normalizedRecords.length === 0) {
        console.log(`      ⚠️ No valid records in batch`);
        return { loadedRecords: 0 };
      }

      // Load to PostgreSQL
      const loadResult = await this.postgresLoader.loadRecords(normalizationResult.normalizedRecords);
      
      const loadedRecords = loadResult.stats ? 
        (loadResult.stats.insertedRecords + loadResult.stats.updatedRecords) : 0;
      
      console.log(`      ✅ Loaded: ${loadedRecords}/${batch.length} (${normalizationResult.invalidCount} validation errors)`);
      
      return { loadedRecords };

    } catch (error) {
      console.error(`      ❌ Batch error: ${error.message}`);
      return { loadedRecords: 0 };
    }
  }
}

// Main execution
async function main() {
  const processor = new DirectMongoDBProcessor();
  await processor.processAllCustodyData();
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = DirectMongoDBProcessor; 