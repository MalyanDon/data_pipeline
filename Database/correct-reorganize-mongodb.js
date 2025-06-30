const { MongoClient } = require('mongodb');
const config = require('./config');

async function correctReorganizeMongoData() {
    const client = new MongoClient(config.mongodb.uri + config.mongodb.database);
    await client.connect();
    const db = client.db('financial_data_2025');
    
    console.log('🏗️ Corrected MongoDB data reorganization...');
    console.log('📋 All custody holdings: HDFC, AXIS, KOTAK, ORBIS');
    
    // Corrected structure - ALL custody holdings from different custodians
    const dataStructure = {
        custody_files: ['hdfc', 'axis', 'kotak', 'orbis'],  // All custody holdings
        broker_master_files: ['broker_master'],
        contract_notes: ['contract_note'],
        other_business_data: ['strategy_master', 'general_data', 'mf_allocations', 
                             'distributor_master', 'stock_capital_flow', 'cash_capital_flow']
    };
    
    // First, let's see what collections we have
    const collections = await db.listCollections().toArray();
    const oldCollections = collections.filter(col => col.name.includes('_06_30'));
    
    console.log('\n📊 Current collections:');
    for (const col of oldCollections) {
        const count = await db.collection(col.name).countDocuments();
        console.log(`  📋 ${col.name}: ${count} records`);
    }
    
    // Remove the incorrectly organized collections first
    const incorrectCollections = collections.filter(col => 
        col.name.includes('custody_files.') || 
        col.name.includes('broker_master_files.') || 
        col.name.includes('contract_notes.') || 
        col.name.includes('other_business_data.')
    );
    
    console.log('\n🗑️ Cleaning up incorrectly organized collections...');
    for (const col of incorrectCollections) {
        await db.collection(col.name).drop();
        console.log(`  ❌ Dropped: ${col.name}`);
    }
    
    // Now reorganize correctly
    console.log('\n🏗️ Reorganizing with correct structure...');
    
    for (const [category, fileTypes] of Object.entries(dataStructure)) {
        console.log(`\n📁 Processing ${category}...`);
        
        for (const fileType of fileTypes) {
            console.log(`  📄 Moving ${fileType} data...`);
            
            const currentCollection = `${fileType}_06_30`;
            try {
                const docs = await db.collection(currentCollection).find({}).toArray();
                
                if (docs.length > 0) {
                    const sampleDoc = docs[0];
                    const date = sampleDoc.fullDate || '2025-06-30';
                    
                    // Create new collection structure
                    const newCollectionName = `${category}.${date}.${fileType}`;
                    
                    console.log(`    📋 ${currentCollection} (${docs.length} records) → ${newCollectionName}`);
                    
                    // Sample file info
                    if (sampleDoc.fileName) {
                        console.log(`    📄 File: ${sampleDoc.fileName}`);
                    }
                    
                    await db.collection(newCollectionName).insertMany(docs);
                    
                    const newCount = await db.collection(newCollectionName).countDocuments();
                    console.log(`    ✅ Verified: ${newCount} records migrated`);
                } else {
                    console.log(`    ⚠️ No data found in ${currentCollection}`);
                }
            } catch (error) {
                console.log(`    ❌ Error processing ${currentCollection}:`, error.message);
            }
        }
    }
    
    console.log('\n📊 Final collection structure:');
    const finalCollections = await db.listCollections().toArray();
    const newCollections = finalCollections.filter(col => 
        col.name.includes('custody_files.') || 
        col.name.includes('broker_master_files.') || 
        col.name.includes('contract_notes.') || 
        col.name.includes('other_business_data.')
    );
    
    // Group by category
    const categorizedCollections = {
        'Custody Holdings': [],
        'Broker Master': [],
        'Contract Notes': [],
        'Other Business Data': []
    };
    
    newCollections.forEach(col => {
        if (col.name.includes('custody_files.')) {
            categorizedCollections['Custody Holdings'].push(col.name);
        } else if (col.name.includes('broker_master_files.')) {
            categorizedCollections['Broker Master'].push(col.name);
        } else if (col.name.includes('contract_notes.')) {
            categorizedCollections['Contract Notes'].push(col.name);
        } else if (col.name.includes('other_business_data.')) {
            categorizedCollections['Other Business Data'].push(col.name);
        }
    });
    
    for (const [category, collections] of Object.entries(categorizedCollections)) {
        if (collections.length > 0) {
            console.log(`\n🏦 ${category}:`);
            collections.forEach(col => console.log(`  📋 ${col}`));
        }
    }
    
    await client.close();
    console.log('\n🎉 Corrected data reorganization complete!');
    console.log('\n💡 Now you can access ALL custody holdings:');
    console.log('   • db.collection("custody_files.2025-06-30.hdfc")');
    console.log('   • db.collection("custody_files.2025-06-30.axis")');
    console.log('   • db.collection("custody_files.2025-06-30.kotak")');
    console.log('   • db.collection("custody_files.2025-06-30.orbis")');
}

correctReorganizeMongoData().catch(console.error); 
const config = require('./config');

async function correctReorganizeMongoData() {
    const client = new MongoClient(config.mongodb.uri + config.mongodb.database);
    await client.connect();
    const db = client.db('financial_data_2025');
    
    console.log('🏗️ Corrected MongoDB data reorganization...');
    console.log('📋 All custody holdings: HDFC, AXIS, KOTAK, ORBIS');
    
    // Corrected structure - ALL custody holdings from different custodians
    const dataStructure = {
        custody_files: ['hdfc', 'axis', 'kotak', 'orbis'],  // All custody holdings
        broker_master_files: ['broker_master'],
        contract_notes: ['contract_note'],
        other_business_data: ['strategy_master', 'general_data', 'mf_allocations', 
                             'distributor_master', 'stock_capital_flow', 'cash_capital_flow']
    };
    
    // First, let's see what collections we have
    const collections = await db.listCollections().toArray();
    const oldCollections = collections.filter(col => col.name.includes('_06_30'));
    
    console.log('\n📊 Current collections:');
    for (const col of oldCollections) {
        const count = await db.collection(col.name).countDocuments();
        console.log(`  📋 ${col.name}: ${count} records`);
    }
    
    // Remove the incorrectly organized collections first
    const incorrectCollections = collections.filter(col => 
        col.name.includes('custody_files.') || 
        col.name.includes('broker_master_files.') || 
        col.name.includes('contract_notes.') || 
        col.name.includes('other_business_data.')
    );
    
    console.log('\n🗑️ Cleaning up incorrectly organized collections...');
    for (const col of incorrectCollections) {
        await db.collection(col.name).drop();
        console.log(`  ❌ Dropped: ${col.name}`);
    }
    
    // Now reorganize correctly
    console.log('\n🏗️ Reorganizing with correct structure...');
    
    for (const [category, fileTypes] of Object.entries(dataStructure)) {
        console.log(`\n📁 Processing ${category}...`);
        
        for (const fileType of fileTypes) {
            console.log(`  📄 Moving ${fileType} data...`);
            
            const currentCollection = `${fileType}_06_30`;
            try {
                const docs = await db.collection(currentCollection).find({}).toArray();
                
                if (docs.length > 0) {
                    const sampleDoc = docs[0];
                    const date = sampleDoc.fullDate || '2025-06-30';
                    
                    // Create new collection structure
                    const newCollectionName = `${category}.${date}.${fileType}`;
                    
                    console.log(`    📋 ${currentCollection} (${docs.length} records) → ${newCollectionName}`);
                    
                    // Sample file info
                    if (sampleDoc.fileName) {
                        console.log(`    📄 File: ${sampleDoc.fileName}`);
                    }
                    
                    await db.collection(newCollectionName).insertMany(docs);
                    
                    const newCount = await db.collection(newCollectionName).countDocuments();
                    console.log(`    ✅ Verified: ${newCount} records migrated`);
                } else {
                    console.log(`    ⚠️ No data found in ${currentCollection}`);
                }
            } catch (error) {
                console.log(`    ❌ Error processing ${currentCollection}:`, error.message);
            }
        }
    }
    
    console.log('\n📊 Final collection structure:');
    const finalCollections = await db.listCollections().toArray();
    const newCollections = finalCollections.filter(col => 
        col.name.includes('custody_files.') || 
        col.name.includes('broker_master_files.') || 
        col.name.includes('contract_notes.') || 
        col.name.includes('other_business_data.')
    );
    
    // Group by category
    const categorizedCollections = {
        'Custody Holdings': [],
        'Broker Master': [],
        'Contract Notes': [],
        'Other Business Data': []
    };
    
    newCollections.forEach(col => {
        if (col.name.includes('custody_files.')) {
            categorizedCollections['Custody Holdings'].push(col.name);
        } else if (col.name.includes('broker_master_files.')) {
            categorizedCollections['Broker Master'].push(col.name);
        } else if (col.name.includes('contract_notes.')) {
            categorizedCollections['Contract Notes'].push(col.name);
        } else if (col.name.includes('other_business_data.')) {
            categorizedCollections['Other Business Data'].push(col.name);
        }
    });
    
    for (const [category, collections] of Object.entries(categorizedCollections)) {
        if (collections.length > 0) {
            console.log(`\n🏦 ${category}:`);
            collections.forEach(col => console.log(`  📋 ${col}`));
        }
    }
    
    await client.close();
    console.log('\n🎉 Corrected data reorganization complete!');
    console.log('\n💡 Now you can access ALL custody holdings:');
    console.log('   • db.collection("custody_files.2025-06-30.hdfc")');
    console.log('   • db.collection("custody_files.2025-06-30.axis")');
    console.log('   • db.collection("custody_files.2025-06-30.kotak")');
    console.log('   • db.collection("custody_files.2025-06-30.orbis")');
}

correctReorganizeMongoData().catch(console.error); 