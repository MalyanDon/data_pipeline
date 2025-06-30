const { MongoClient } = require('mongodb');
const config = require('./config');

async function fixHDFCData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Clear corrupted HDFC data
        console.log('🗑️ Clearing corrupted HDFC data...');
        
        // Delete HDFC collection
        const collections = await db.listCollections().toArray();
        const hdfcCollections = collections.filter(col => col.name.includes('hdfc'));
        
        for (const collection of hdfcCollections) {
            await db.collection(collection.name).drop();
            console.log(`❌ Dropped collection: ${collection.name}`);
        }
        
        // Clear HDFC tracker
        await db.collection('file_versions_tracker').deleteOne({ _id: 'custody_files.hdfc' });
        console.log('❌ Cleared HDFC tracker');
        
        console.log('✅ HDFC data cleared. Please re-upload the HDFC file.');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

fixHDFCData(); 
const config = require('./config');

async function fixHDFCData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Clear corrupted HDFC data
        console.log('🗑️ Clearing corrupted HDFC data...');
        
        // Delete HDFC collection
        const collections = await db.listCollections().toArray();
        const hdfcCollections = collections.filter(col => col.name.includes('hdfc'));
        
        for (const collection of hdfcCollections) {
            await db.collection(collection.name).drop();
            console.log(`❌ Dropped collection: ${collection.name}`);
        }
        
        // Clear HDFC tracker
        await db.collection('file_versions_tracker').deleteOne({ _id: 'custody_files.hdfc' });
        console.log('❌ Cleared HDFC tracker');
        
        console.log('✅ HDFC data cleared. Please re-upload the HDFC file.');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

fixHDFCData(); 