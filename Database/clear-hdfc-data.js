const { MongoClient } = require('mongodb');
const config = require('./config');

async function clearHDFCData() {
    console.log('🧹 Clearing corrupted HDFC data...');
    
    const client = new MongoClient(config.mongodb.uri);
    
    try {
        await client.connect();
        const db = client.db(config.mongodb.database);
        
        // Find and clear HDFC collections
        const collections = await db.listCollections().toArray();
        
        for (const collection of collections) {
            const name = collection.name;
            if (name.includes('hdfc') || name.includes('HDFC')) {
                const count = await db.collection(name).countDocuments();
                await db.collection(name).drop();
                console.log(`✅ Dropped HDFC collection: ${name} (${count} documents)`);
            }
        }
        
        // Clear HDFC from file tracker
        await db.collection('file_versions_tracker').deleteOne({ '_id': 'custody_files.hdfc' });
        console.log('✅ Cleared HDFC from file tracker');
        
        console.log('🎉 HDFC data cleared! Ready for fresh upload.');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

clearHDFCData().catch(console.error); 
const config = require('./config');

async function clearHDFCData() {
    console.log('🧹 Clearing corrupted HDFC data...');
    
    const client = new MongoClient(config.mongodb.uri);
    
    try {
        await client.connect();
        const db = client.db(config.mongodb.database);
        
        // Find and clear HDFC collections
        const collections = await db.listCollections().toArray();
        
        for (const collection of collections) {
            const name = collection.name;
            if (name.includes('hdfc') || name.includes('HDFC')) {
                const count = await db.collection(name).countDocuments();
                await db.collection(name).drop();
                console.log(`✅ Dropped HDFC collection: ${name} (${count} documents)`);
            }
        }
        
        // Clear HDFC from file tracker
        await db.collection('file_versions_tracker').deleteOne({ '_id': 'custody_files.hdfc' });
        console.log('✅ Cleared HDFC from file tracker');
        
        console.log('🎉 HDFC data cleared! Ready for fresh upload.');
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

clearHDFCData().catch(console.error); 