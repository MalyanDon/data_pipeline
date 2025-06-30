const { MongoClient } = require('mongodb');
const config = require('./config');

async function checkUploadSuccess() {
    console.log('🔍 Checking upload success...');
    
    try {
        const mongoUri = config.mongodb.uri + config.mongodb.database;
        const client = new MongoClient(mongoUri);
        await client.connect();
        
        const db = client.db('financial_data_2025');
        
        // Check file tracker
        console.log('📋 Checking file tracker...');
        const tracker = await db.collection('file_versions_tracker').find({}).toArray();
        console.log('Found trackers:', tracker.length);
        tracker.forEach(t => {
            console.log(`  - ${t._id}: ${t.recordCount} records in ${t.latestCollection}`);
        });
        
        // Check custody files collection
        console.log('\n📊 Checking custody files collection...');
        const custodyData = await db.collection('custody_files.2025-06-25.hdfc').find({}).toArray();
        console.log(`Found ${custodyData.length} records in custody_files.2025-06-25.hdfc`);
        
        if (custodyData.length > 0) {
            console.log('✅ Sample record:');
            console.log(custodyData[0]);
        }
        
        await client.close();
        console.log('\n🎉 Upload verification complete!');
        
    } catch (error) {
        console.error('❌ Error checking upload:', error.message);
    }
}

checkUploadSuccess(); 
const config = require('./config');

async function checkUploadSuccess() {
    console.log('🔍 Checking upload success...');
    
    try {
        const mongoUri = config.mongodb.uri + config.mongodb.database;
        const client = new MongoClient(mongoUri);
        await client.connect();
        
        const db = client.db('financial_data_2025');
        
        // Check file tracker
        console.log('📋 Checking file tracker...');
        const tracker = await db.collection('file_versions_tracker').find({}).toArray();
        console.log('Found trackers:', tracker.length);
        tracker.forEach(t => {
            console.log(`  - ${t._id}: ${t.recordCount} records in ${t.latestCollection}`);
        });
        
        // Check custody files collection
        console.log('\n📊 Checking custody files collection...');
        const custodyData = await db.collection('custody_files.2025-06-25.hdfc').find({}).toArray();
        console.log(`Found ${custodyData.length} records in custody_files.2025-06-25.hdfc`);
        
        if (custodyData.length > 0) {
            console.log('✅ Sample record:');
            console.log(custodyData[0]);
        }
        
        await client.close();
        console.log('\n🎉 Upload verification complete!');
        
    } catch (error) {
        console.error('❌ Error checking upload:', error.message);
    }
}

checkUploadSuccess(); 