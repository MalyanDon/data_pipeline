#!/usr/bin/env node

const mongoose = require('mongoose');
const config = require('../config');
const SimpleMongoDBProcessor = require('./simple-mongodb-processor');

async function reprocessWithFinancialFields() {
  console.log('🔄 Reprocessing Sample Data with Financial Fields\n');
  
  try {
    // Connect to MongoDB
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB\n');
    
    const db = mongoose.connection.db;
    
    // Get a small sample from each collection
    const collections = ['axis_06_25', 'orbis_06_25'];
    
    for (const collectionName of collections) {
      console.log(`📊 Processing sample from ${collectionName}...`);
      
      const collection = db.collection(collectionName);
      const count = await collection.countDocuments();
      
      console.log(`   Total documents in collection: ${count.toLocaleString()}`);
      
      // Take a small sample (100 records)
      const sampleSize = Math.min(100, count);
      const sampleDocs = await collection.find({}).limit(sampleSize).toArray();
      
      console.log(`   Processing ${sampleDocs.length} sample records...`);
      
      // Use our existing simple processor logic but with a smaller batch
      const processor = new SimpleMongoDBProcessor();
      
      // Process just this sample
      const custodyType = collectionName.includes('axis') ? 'axis' : 'orbis';
      const mapping = {
        collection: collectionName,
        custodyType: custodyType,
        fileName: `${custodyType}_sample_2025-06-25.xlsx`
      };
      
      // Process the sample batch directly
      const result = await processor.processBatchDirect(sampleDocs.map(doc => {
        // Clean document
        delete doc._id;
        delete doc.__v;
        delete doc.month;
        delete doc.date;
        delete doc.fullDate;
        delete doc.fileName;
        delete doc.fileType;
        delete doc.uploadedAt;
        return doc;
      }), mapping);
      
      if (result.success) {
        console.log(`   ✅ Successfully processed ${result.loadedRecords} records with financial data`);
        console.log(`   📊 Valid: ${result.validRecords}, Invalid: ${result.invalidRecords}`);
      } else {
        console.log(`   ❌ Processing failed: ${result.error}`);
      }
      
      console.log('');
    }
    
    console.log('🎉 Sample reprocessing completed!\n');
    
    // Test the results with some queries
    console.log('📊 Testing financial data results...');
    
    const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');
    const postgresLoader = new PostgresLoader();
    
    try {
      // Get some sample data to verify financial fields
      const sampleData = await postgresLoader.pool.query(`
        SELECT 
          client_reference, 
          instrument_name, 
          blocked_quantity, 
          pending_buy_quantity, 
          pending_sell_quantity,
          source_system
        FROM unified_custody_master 
        WHERE blocked_quantity > 0 OR pending_buy_quantity > 0 OR pending_sell_quantity > 0
        LIMIT 5
      `);
      
      console.log(`\n📈 Found ${sampleData.rows.length} records with financial data:`);
      sampleData.rows.forEach(row => {
        console.log(`   ${row.client_reference} (${row.source_system}): ${row.instrument_name}`);
        console.log(`      Blocked: ${row.blocked_quantity}, Pending Buy: ${row.pending_buy_quantity}, Pending Sell: ${row.pending_sell_quantity}`);
      });
      
      await postgresLoader.close();
      
    } catch (error) {
      console.error('❌ Query error:', error.message);
    }
    
  } catch (error) {
    console.error('❌ Processing error:', error.message);
  } finally {
    await mongoose.disconnect();
  }
}

// Run if called directly
if (require.main === module) {
  reprocessWithFinancialFields()
    .then(() => process.exit(0))
    .catch(error => {
      console.error('❌ Fatal error:', error.message);
      process.exit(1);
    });
}

 