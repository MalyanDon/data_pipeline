#!/usr/bin/env node

const mongoose = require('mongoose');
const fs = require('fs-extra');
const path = require('path');
const XLSX = require('xlsx');
const config = require('../config');
const CustodyNormalizationPipeline = require('../custody-normalization/pipeline/custodyNormalizationPipeline');

class MongoDBCustodyProcessor {
  constructor() {
    this.pipeline = new CustodyNormalizationPipeline();
    this.processedFiles = 0;
    this.totalRecords = 0;
    this.errors = [];
  }

  async connectToMongoDB() {
    try {
      // Connect to MongoDB
      await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
      console.log('✅ Connected to MongoDB Atlas (financial_data_2025)');
      return true;
    } catch (error) {
      console.error('❌ MongoDB connection failed:', error.message);
      return false;
    }
  }

  async getCustodyCollections() {
    try {
      const db = mongoose.connection.db;
      const collections = await db.listCollections().toArray();
      
      // Filter custody collections based on patterns
      const custodyCollections = collections.filter(col => {
        const name = col.name.toLowerCase();
        return (
          name.includes('axis') ||
          name.includes('hdfc') ||
          name.includes('kotak') ||
          name.includes('orbis') ||
          name.includes('deutsche') || name.includes('164_ec0000720') ||
          name.includes('trustpms') || name.includes('end_client_holding')
        );
      });

      console.log(`📊 Found ${custodyCollections.length} custody collections:`);
      custodyCollections.forEach(col => {
        console.log(`   - ${col.name}`);
      });

      return custodyCollections.map(col => col.name);
    } catch (error) {
      console.error('❌ Error getting collections:', error.message);
      return [];
    }
  }

  async processCollection(collectionName) {
    try {
      console.log(`\n🔄 Processing collection: ${collectionName}`);
      
      const db = mongoose.connection.db;
      const collection = db.collection(collectionName);
      
      // Get all data from collection
      const documents = await collection.find({}).toArray();
      
      if (documents.length === 0) {
        console.log('   ⚠️ No documents found');
        return { success: false, message: 'No documents found' };
      }

      console.log(`   📄 Found ${documents.length} documents`);

      // Get metadata from first document
      const firstDoc = documents[0];
      const fileName = firstDoc.fileName || `${collectionName}.xlsx`;
      const recordDate = firstDoc.fullDate || firstDoc.record_date || '2025-06-25';

      console.log(`   📅 Record date: ${recordDate}`);
      console.log(`   📁 File name: ${fileName}`);

      // Create Excel file from MongoDB data
      const tempFilePath = await this.createExcelFromMongoDB(documents, fileName);

      // Process through normalization pipeline
      const result = await this.pipeline.processFile(tempFilePath, {
        recordDate: recordDate
      });

      // Clean up temp file
      await fs.remove(tempFilePath);

      if (result.success) {
        console.log(`   ✅ Processed successfully: ${result.stats.loadedRecords} records loaded`);
        this.processedFiles++;
        this.totalRecords += result.stats.loadedRecords || 0;
      } else {
        console.log(`   ❌ Processing failed: ${result.error}`);
        this.errors.push({
          collection: collectionName,
          error: result.error
        });
      }

      return result;

    } catch (error) {
      console.error(`❌ Error processing ${collectionName}:`, error.message);
      this.errors.push({
        collection: collectionName,
        error: error.message
      });
      return { success: false, error: error.message };
    }
  }

  async createExcelFromMongoDB(documents, fileName) {
    try {
      // Remove MongoDB-specific fields and prepare data for Excel
      const cleanData = documents.map(doc => {
        const cleanDoc = { ...doc };
        delete cleanDoc._id;
        delete cleanDoc.__v;
        delete cleanDoc.month;
        delete cleanDoc.date;
        delete cleanDoc.fullDate;
        delete cleanDoc.fileName;
        delete cleanDoc.fileType;
        delete cleanDoc.uploadedAt;
        return cleanDoc;
      });

      // Create Excel workbook
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(cleanData);
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1');

      // Save to temp file
      const tempFilePath = path.join(__dirname, '..', 'temp_uploads', `temp_${Date.now()}_${fileName}`);
      XLSX.writeFile(wb, tempFilePath);

      return tempFilePath;

    } catch (error) {
      console.error('❌ Error creating Excel file:', error.message);
      throw error;
    }
  }

  async processAllCustodyData() {
    console.log('🏦 MongoDB Custody Data → PostgreSQL Normalization Pipeline\n');

    try {
      // Connect to MongoDB
      const mongoConnected = await this.connectToMongoDB();
      if (!mongoConnected) {
        return;
      }

      // Get custody collections
      const custodyCollections = await this.getCustodyCollections();
      
      if (custodyCollections.length === 0) {
        console.log('❌ No custody collections found');
        return;
      }

      // Process each collection
      for (const collectionName of custodyCollections) {
        await this.processCollection(collectionName);
      }

      // Print summary
      console.log('\n📊 Processing Summary');
      console.log('=====================');
      console.log(`Collections processed: ${this.processedFiles}/${custodyCollections.length}`);
      console.log(`Total records loaded: ${this.totalRecords.toLocaleString()}`);
      
      if (this.errors.length > 0) {
        console.log(`\n❌ Errors (${this.errors.length}):`);
        this.errors.forEach(error => {
          console.log(`   - ${error.collection}: ${error.error}`);
        });
      }

      if (this.processedFiles === custodyCollections.length) {
        console.log('\n🎉 All custody collections processed successfully!');
        console.log('\n📈 View statistics:');
        console.log('   npm run custody-stats');
        console.log('\n🔍 Query unified data:');
        console.log('   curl "http://localhost:3003/api/custody/unified-data?limit=5"');
      }

    } catch (error) {
      console.error('❌ Processing failed:', error.message);
    } finally {
      await mongoose.disconnect();
      await this.pipeline.close();
    }
  }
}

// Main execution
async function main() {
  const processor = new MongoDBCustodyProcessor();
  await processor.processAllCustodyData();
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = MongoDBCustodyProcessor; 