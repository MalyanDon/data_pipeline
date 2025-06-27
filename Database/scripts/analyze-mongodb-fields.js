#!/usr/bin/env node

const mongoose = require('mongoose');
const config = require('../config');

async function analyzeMongoDBFields() {
  try {
    console.log('🔍 Analyzing MongoDB Custody Fields\n');
    
    // Connect to MongoDB
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB Atlas (financial_data_2025)\n');
    
    const db = mongoose.connection.db;
    const collections = await db.listCollections().toArray();
    
    // Filter custody collections
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
    
    console.log(`📊 Analyzing ${custodyCollections.length} custody collections...\n`);
    
    for (const collectionInfo of custodyCollections) {
      const collectionName = collectionInfo.name;
      console.log(`📁 Collection: ${collectionName}`);
      console.log('=' .repeat(50));
      
      const collection = db.collection(collectionName);
      
      // Get first few documents to analyze structure
      const samples = await collection.find({}).limit(3).toArray();
      
      if (samples.length === 0) {
        console.log('   ⚠️ No documents found\n');
        continue;
      }
      
      console.log(`   📄 Sample size: ${samples.length} documents`);
      
      // Get all unique field names
      const allFields = new Set();
      samples.forEach(doc => {
        Object.keys(doc).forEach(key => allFields.add(key));
      });
      
      console.log(`   🔑 Available fields (${allFields.size}):`);
      Array.from(allFields).sort().forEach(field => {
        // Skip MongoDB internal fields
        if (!['_id', '__v', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt'].includes(field)) {
          // Show sample values
          const sampleValues = samples.map(doc => doc[field]).filter(val => val !== undefined && val !== null && val !== '');
          const uniqueValues = [...new Set(sampleValues)].slice(0, 3);
          console.log(`      ${field}: ${uniqueValues.join(', ')}${uniqueValues.length < sampleValues.length ? '...' : ''}`);
        }
      });
      
      console.log(`\n   📋 First document structure:`);
      const firstDoc = samples[0];
      Object.entries(firstDoc).forEach(([key, value]) => {
        if (!['_id', '__v', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt'].includes(key)) {
          const displayValue = typeof value === 'string' && value.length > 50 
            ? value.substring(0, 50) + '...' 
            : value;
          console.log(`      ${key}: ${displayValue}`);
        }
      });
      
      console.log('\n' + '-'.repeat(80) + '\n');
    }
    
    console.log('💡 Field Mapping Recommendations:');
    console.log('==================================');
    console.log('Based on the analysis above, update the field mappings in:');
    console.log('custody-normalization/config/custody-mappings.js');
    console.log('\nLook for fields that might represent:');
    console.log('• client_reference: UCC, Client Code, Account Number, etc.');
    console.log('• client_name: Client Name, Account Name, Holder Name, etc.');
    console.log('• instrument_isin: ISIN, Security ISIN, Instrument ISIN, etc.');
    console.log('• instrument_name: Security Name, Instrument Name, Script Name, etc.');
    console.log('• instrument_code: Security Code, Script Code, Symbol, etc.');
    
  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await mongoose.disconnect();
  }
}

// Run the analysis
analyzeMongoDBFields(); 