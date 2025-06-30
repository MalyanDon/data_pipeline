const { MongoClient } = require('mongodb');
const config = require('./config');

async function checkActualData() {
  const client = new MongoClient(config.mongodb.uri);
  await client.connect();
  console.log('✅ Connected to MongoDB\n');
  
  const db = client.db('financial_data_2025');
  const collections = await db.listCollections().toArray();
  
  console.log('🔍 ACTUAL COLUMN NAMES IN UPLOADED FILES:\n');
  
  for (const collInfo of collections) {
    const collName = collInfo.name;
    const collection = db.collection(collName);
    const sampleRecord = await collection.findOne({});
    
    if (sampleRecord) {
      console.log(`📄 ${collName}:`);
      console.log('   🔸 Actual columns found:');
      Object.keys(sampleRecord).forEach(key => {
        if (!key.startsWith('_')) {
          const value = sampleRecord[key];
          const preview = typeof value === 'string' ? value.substring(0, 30) : value;
          console.log(`      • "${key}": ${preview}`);
        }
      });
      console.log('');
    }
  }
  
  await client.close();
}

checkActualData().catch(console.error); 
const config = require('./config');

async function checkActualData() {
  const client = new MongoClient(config.mongodb.uri);
  await client.connect();
  console.log('✅ Connected to MongoDB\n');
  
  const db = client.db('financial_data_2025');
  const collections = await db.listCollections().toArray();
  
  console.log('🔍 ACTUAL COLUMN NAMES IN UPLOADED FILES:\n');
  
  for (const collInfo of collections) {
    const collName = collInfo.name;
    const collection = db.collection(collName);
    const sampleRecord = await collection.findOne({});
    
    if (sampleRecord) {
      console.log(`📄 ${collName}:`);
      console.log('   🔸 Actual columns found:');
      Object.keys(sampleRecord).forEach(key => {
        if (!key.startsWith('_')) {
          const value = sampleRecord[key];
          const preview = typeof value === 'string' ? value.substring(0, 30) : value;
          console.log(`      • "${key}": ${preview}`);
        }
      });
      console.log('');
    }
  }
  
  await client.close();
}

checkActualData().catch(console.error); 