const { MongoClient } = require('mongodb');
const config = require('./config');

async function inspectHDFCData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Find HDFC collection
        const collections = await db.listCollections().toArray();
        const hdfcCollections = collections.filter(col => col.name.includes('hdfc'));
        
        console.log('📂 HDFC Collections found:', hdfcCollections.map(c => c.name));
        
        for (const collection of hdfcCollections) {
            const colName = collection.name;
            console.log(`\n📊 Inspecting collection: ${colName}`);
            
            const sampleRecord = await db.collection(colName).findOne({});
            console.log('📋 Sample record structure:');
            console.log('Keys:', Object.keys(sampleRecord || {}));
            
            if (sampleRecord) {
                const cleanRecord = {...sampleRecord};
                delete cleanRecord._id;
                delete cleanRecord.recordIndex;
                delete cleanRecord.fileName;
                delete cleanRecord.fileSize;
                delete cleanRecord.category;
                delete cleanRecord.subcategory;
                delete cleanRecord.uploadedAt;
                delete cleanRecord.processingDate;
                
                console.log('🔍 Clean data columns:', Object.keys(cleanRecord));
                console.log('📄 First record data:');
                console.log(JSON.stringify(cleanRecord, null, 2));
            }
            
            const count = await db.collection(colName).countDocuments();
            console.log(`📊 Total records: ${count}`);
        }
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

inspectHDFCData(); 
const config = require('./config');

async function inspectHDFCData() {
    const mongoUri = config.mongodb.uri + config.mongodb.database;
    const client = new MongoClient(mongoUri);
    
    try {
        await client.connect();
        console.log('🔌 Connected to MongoDB');
        
        const db = client.db('financial_data_2025');
        
        // Find HDFC collection
        const collections = await db.listCollections().toArray();
        const hdfcCollections = collections.filter(col => col.name.includes('hdfc'));
        
        console.log('📂 HDFC Collections found:', hdfcCollections.map(c => c.name));
        
        for (const collection of hdfcCollections) {
            const colName = collection.name;
            console.log(`\n📊 Inspecting collection: ${colName}`);
            
            const sampleRecord = await db.collection(colName).findOne({});
            console.log('📋 Sample record structure:');
            console.log('Keys:', Object.keys(sampleRecord || {}));
            
            if (sampleRecord) {
                const cleanRecord = {...sampleRecord};
                delete cleanRecord._id;
                delete cleanRecord.recordIndex;
                delete cleanRecord.fileName;
                delete cleanRecord.fileSize;
                delete cleanRecord.category;
                delete cleanRecord.subcategory;
                delete cleanRecord.uploadedAt;
                delete cleanRecord.processingDate;
                
                console.log('🔍 Clean data columns:', Object.keys(cleanRecord));
                console.log('📄 First record data:');
                console.log(JSON.stringify(cleanRecord, null, 2));
            }
            
            const count = await db.collection(colName).countDocuments();
            console.log(`📊 Total records: ${count}`);
        }
        
    } catch (error) {
        console.error('❌ Error:', error);
    } finally {
        await client.close();
    }
}

inspectHDFCData(); 