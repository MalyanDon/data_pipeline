const { MongoClient } = require('mongodb');
const config = require('./config');

const mongoUri = config.mongodb.uri + config.mongodb.database;

async function dropAllCollections() {
    const client = new MongoClient(mongoUri);
    
    try {
        console.log('🔄 Connecting to MongoDB...');
        await client.connect();
        
        const db = client.db('financial_data_2025');
        
        // Get all collection names
        const collections = await db.listCollections().toArray();
        
        if (collections.length === 0) {
            console.log('ℹ️ No collections found in the database.');
            return;
        }

        console.log(`📋 Found ${collections.length} collections:`);
        for (const collection of collections) {
            console.log(`   - ${collection.name}`);
        }

        // Drop each collection
        console.log('\n🗑️ Removing collections completely...');
        for (const collection of collections) {
            await db.collection(collection.name).drop();
            console.log(`✅ Removed collection: ${collection.name}`);
        }

        console.log('\n✨ All collections have been completely removed!');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
        console.log('📡 MongoDB connection closed.');
    }
}

// Run the drop function
dropAllCollections(); 