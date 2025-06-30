const { MongoClient } = require('mongodb');
const config = require('./config');

async function reorganizeMongoData() {
    const client = new MongoClient(config.mongodb.uri + config.mongodb.database);
    await client.connect();
    const db = client.db('financial_data_2025');
    
    console.log('🏗️ Starting MongoDB data reorganization...');
    
    // Define the proper structure
    const dataStructure = {
        custody_files: ['hdfc', 'axis'],  // Only actual custody holdings
        broker_master_files: ['broker_master'],
        contract_notes: ['contract_note'],
        other_business_data: ['strategy_master', 'general_data', 'kotak', 'mf_allocations', 
                             'distributor_master', 'stock_capital_flow', 'cash_capital_flow', 'orbis']
    };
    
    // Process each category
    for (const [category, fileTypes] of Object.entries(dataStructure)) {
        console.log(`\n📁 Processing ${category}...`);
        
        for (const fileType of fileTypes) {
            console.log(`  📄 Moving ${fileType} data...`);
            
            // Get current collection data
            const currentCollection = `${fileType}_06_30`;
            try {
                const docs = await db.collection(currentCollection).find({}).toArray();
                
                if (docs.length > 0) {
                    // Extract date information
                    const sampleDoc = docs[0];
                    const date = sampleDoc.fullDate || '2025-06-30';
                    
                    // Create new collection structure: category.date.fileType
                    const newCollectionName = `${category}.${date}.${fileType}`;
                    
                    console.log(`    📋 ${currentCollection} (${docs.length} records) → ${newCollectionName}`);
                    
                    // Insert into new structure
                    await db.collection(newCollectionName).insertMany(docs);
                    
                    // Verify the migration
                    const newCount = await db.collection(newCollectionName).countDocuments();
                    console.log(`    ✅ Verified: ${newCount} records migrated`);
                    
                    // Optional: Remove old collection (commented for safety)
                    // await db.collection(currentCollection).drop();
                    // console.log(`    🗑️ Removed old collection: ${currentCollection}`);
                } else {
                    console.log(`    ⚠️ No data found in ${currentCollection}`);
                }
            } catch (error) {
                console.log(`    ❌ Error processing ${currentCollection}:`, error.message);
            }
        }
    }
    
    console.log('\n📊 New collection structure:');
    const collections = await db.listCollections().toArray();
    const newCollections = collections.filter(col => 
        col.name.includes('custody_files') || 
        col.name.includes('broker_master_files') || 
        col.name.includes('contract_notes') || 
        col.name.includes('other_business_data')
    );
    
    newCollections.forEach(col => {
        console.log(`  📋 ${col.name}`);
    });
    
    await client.close();
    console.log('\n🎉 Data reorganization complete!');
    console.log('\n💡 Now you can access data like:');
    console.log('   • db.collection("custody_files.2025-06-30.hdfc")');
    console.log('   • db.collection("custody_files.2025-06-30.axis")');
    console.log('   • db.collection("broker_master_files.2025-06-30.broker_master")');
}

// Run the reorganization
reorganizeMongoData().catch(console.error); 
const config = require('./config');

async function reorganizeMongoData() {
    const client = new MongoClient(config.mongodb.uri + config.mongodb.database);
    await client.connect();
    const db = client.db('financial_data_2025');
    
    console.log('🏗️ Starting MongoDB data reorganization...');
    
    // Define the proper structure
    const dataStructure = {
        custody_files: ['hdfc', 'axis'],  // Only actual custody holdings
        broker_master_files: ['broker_master'],
        contract_notes: ['contract_note'],
        other_business_data: ['strategy_master', 'general_data', 'kotak', 'mf_allocations', 
                             'distributor_master', 'stock_capital_flow', 'cash_capital_flow', 'orbis']
    };
    
    // Process each category
    for (const [category, fileTypes] of Object.entries(dataStructure)) {
        console.log(`\n📁 Processing ${category}...`);
        
        for (const fileType of fileTypes) {
            console.log(`  📄 Moving ${fileType} data...`);
            
            // Get current collection data
            const currentCollection = `${fileType}_06_30`;
            try {
                const docs = await db.collection(currentCollection).find({}).toArray();
                
                if (docs.length > 0) {
                    // Extract date information
                    const sampleDoc = docs[0];
                    const date = sampleDoc.fullDate || '2025-06-30';
                    
                    // Create new collection structure: category.date.fileType
                    const newCollectionName = `${category}.${date}.${fileType}`;
                    
                    console.log(`    📋 ${currentCollection} (${docs.length} records) → ${newCollectionName}`);
                    
                    // Insert into new structure
                    await db.collection(newCollectionName).insertMany(docs);
                    
                    // Verify the migration
                    const newCount = await db.collection(newCollectionName).countDocuments();
                    console.log(`    ✅ Verified: ${newCount} records migrated`);
                    
                    // Optional: Remove old collection (commented for safety)
                    // await db.collection(currentCollection).drop();
                    // console.log(`    🗑️ Removed old collection: ${currentCollection}`);
                } else {
                    console.log(`    ⚠️ No data found in ${currentCollection}`);
                }
            } catch (error) {
                console.log(`    ❌ Error processing ${currentCollection}:`, error.message);
            }
        }
    }
    
    console.log('\n📊 New collection structure:');
    const collections = await db.listCollections().toArray();
    const newCollections = collections.filter(col => 
        col.name.includes('custody_files') || 
        col.name.includes('broker_master_files') || 
        col.name.includes('contract_notes') || 
        col.name.includes('other_business_data')
    );
    
    newCollections.forEach(col => {
        console.log(`  📋 ${col.name}`);
    });
    
    await client.close();
    console.log('\n🎉 Data reorganization complete!');
    console.log('\n💡 Now you can access data like:');
    console.log('   • db.collection("custody_files.2025-06-30.hdfc")');
    console.log('   • db.collection("custody_files.2025-06-30.axis")');
    console.log('   • db.collection("broker_master_files.2025-06-30.broker_master")');
}

// Run the reorganization
reorganizeMongoData().catch(console.error); 