const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

async function testProcessor() {
  console.log('🧪 Testing working processor...');
  
  const pgPool = new Pool(config.postgresql);
  const mongoClient = new MongoClient(config.mongodb.uri);
  
  try {
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB');
    
    const db = mongoClient.db('financial_data_2025');
    
    // Test broker processing
    console.log('\n📊 Testing broker data...');
    const brokerCollection = db.collection('broker_master_data_2025_06_28_13_14_33');
    const brokerSample = await brokerCollection.findOne();
    
    if (brokerSample) {
      console.log('Sample broker data:', Object.keys(brokerSample));
      
      // Map broker record
      const mapped = {
        broker_code: brokerSample['Broker Code'] || 'BR001',
        broker_name: brokerSample['Broker Name'] || 'Unknown',
        broker_type: brokerSample['Broker Type'] || null,
        registration_number: brokerSample['Registration Number'] || null,
        contact_info: null
      };
      
      console.log('Mapped record:', mapped);
      
      // Test insert
      const client = await pgPool.connect();
      try {
        await client.query(`
          INSERT INTO brokers (broker_code, broker_name, broker_type, registration_number, contact_info)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (broker_name) DO NOTHING
        `, [mapped.broker_code, mapped.broker_name, mapped.broker_type, mapped.registration_number, mapped.contact_info]);
        
        console.log('✅ Successfully inserted broker record');
      } finally {
        client.release();
      }
    }
    
    // Check all collections
    console.log('\n📋 All collections:');
    const collections = await db.listCollections().toArray();
    collections.forEach(col => {
      console.log(`   - ${col.name}`);
    });
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await mongoClient.close();
    await pgPool.end();
  }
}

testProcessor(); 
const { MongoClient } = require('mongodb');
const config = require('./config');

async function testProcessor() {
  console.log('🧪 Testing working processor...');
  
  const pgPool = new Pool(config.postgresql);
  const mongoClient = new MongoClient(config.mongodb.uri);
  
  try {
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB');
    
    const db = mongoClient.db('financial_data_2025');
    
    // Test broker processing
    console.log('\n📊 Testing broker data...');
    const brokerCollection = db.collection('broker_master_data_2025_06_28_13_14_33');
    const brokerSample = await brokerCollection.findOne();
    
    if (brokerSample) {
      console.log('Sample broker data:', Object.keys(brokerSample));
      
      // Map broker record
      const mapped = {
        broker_code: brokerSample['Broker Code'] || 'BR001',
        broker_name: brokerSample['Broker Name'] || 'Unknown',
        broker_type: brokerSample['Broker Type'] || null,
        registration_number: brokerSample['Registration Number'] || null,
        contact_info: null
      };
      
      console.log('Mapped record:', mapped);
      
      // Test insert
      const client = await pgPool.connect();
      try {
        await client.query(`
          INSERT INTO brokers (broker_code, broker_name, broker_type, registration_number, contact_info)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (broker_name) DO NOTHING
        `, [mapped.broker_code, mapped.broker_name, mapped.broker_type, mapped.registration_number, mapped.contact_info]);
        
        console.log('✅ Successfully inserted broker record');
      } finally {
        client.release();
      }
    }
    
    // Check all collections
    console.log('\n📋 All collections:');
    const collections = await db.listCollections().toArray();
    collections.forEach(col => {
      console.log(`   - ${col.name}`);
    });
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await mongoClient.close();
    await pgPool.end();
  }
}

testProcessor(); 