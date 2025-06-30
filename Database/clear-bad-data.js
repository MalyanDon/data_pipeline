const { MongoClient } = require('mongodb');
const config = require('./config');

async function clearBadData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Clear all existing data for fresh start
        console.log('🗑️ Clearing all custody data for fresh upload...');
        
        // Drop all custody collections
        const collections = await db.listCollections().toArray();
        const custodyCollections = collections.filter(col => col.name.includes('custody_files'));
        
        for (const collection of custodyCollections) {
            await db.collection(collection.name).drop();
            console.log(`❌ Dropped collection: ${collection.name}`);
        }
        
        // Clear all trackers
        await db.collection('file_versions_tracker').deleteMany({ category: 'custody_files' });
        console.log('❌ Cleared all custody trackers');
        
        console.log('✅ All custody data cleared. Ready for fresh uploads.');
        console.log('📋 Now please upload:');
        console.log('   1. HDFC custody file');
        console.log('   2. AXIS custody file'); 
        console.log('   3. KOTAK custody file');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

clearBadData(); 
const config = require('./config');

async function clearBadData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Clear all existing data for fresh start
        console.log('🗑️ Clearing all custody data for fresh upload...');
        
        // Drop all custody collections
        const collections = await db.listCollections().toArray();
        const custodyCollections = collections.filter(col => col.name.includes('custody_files'));
        
        for (const collection of custodyCollections) {
            await db.collection(collection.name).drop();
            console.log(`❌ Dropped collection: ${collection.name}`);
        }
        
        // Clear all trackers
        await db.collection('file_versions_tracker').deleteMany({ category: 'custody_files' });
        console.log('❌ Cleared all custody trackers');
        
        console.log('✅ All custody data cleared. Ready for fresh uploads.');
        console.log('📋 Now please upload:');
        console.log('   1. HDFC custody file');
        console.log('   2. AXIS custody file'); 
        console.log('   3. KOTAK custody file');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

clearBadData(); 