const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

async function clearAllDatabases() {
    console.log('🧹 Starting complete database cleanup...');
    
    // Clear MongoDB
    try {
        console.log('🗃️ Clearing MongoDB...');
        const mongoUri = config.mongodb.uri + config.mongodb.database;
        const client = new MongoClient(mongoUri);
        await client.connect();
        
        const db = client.db('financial_data_2025');
        
        // Get all collections
        const collections = await db.listCollections().toArray();
        console.log(`📊 Found ${collections.length} collections in MongoDB`);
        
        // Drop all collections
        for (const collection of collections) {
            await db.collection(collection.name).drop();
            console.log(`✅ Dropped collection: ${collection.name}`);
        }
        
        await client.close();
        console.log('✅ MongoDB cleared successfully');
        
    } catch (error) {
        console.log('⚠️ MongoDB clear error (may be empty):', error.message);
    }
    
    // Clear PostgreSQL
    try {
        console.log('🐘 Clearing PostgreSQL...');
        const pgPool = new Pool(config.postgresql);
        
        // Get all tables
        const tablesResult = await pgPool.query(`
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
        `);
        
        console.log(`📊 Found ${tablesResult.rows.length} tables in PostgreSQL`);
        
        // Drop all tables
        for (const row of tablesResult.rows) {
            await pgPool.query(`DROP TABLE IF EXISTS "${row.tablename}" CASCADE`);
            console.log(`✅ Dropped table: ${row.tablename}`);
        }
        
        await pgPool.end();
        console.log('✅ PostgreSQL cleared successfully');
        
    } catch (error) {
        console.log('⚠️ PostgreSQL clear error (may be empty):', error.message);
    }
    
    console.log('🎉 All databases cleared successfully!');
    console.log('🚀 Ready for fresh testing!');
}

clearAllDatabases().catch(console.error); 
const { Pool } = require('pg');
const config = require('./config');

async function clearAllDatabases() {
    console.log('🧹 Starting complete database cleanup...');
    
    // Clear MongoDB
    try {
        console.log('🗃️ Clearing MongoDB...');
        const mongoUri = config.mongodb.uri + config.mongodb.database;
        const client = new MongoClient(mongoUri);
        await client.connect();
        
        const db = client.db('financial_data_2025');
        
        // Get all collections
        const collections = await db.listCollections().toArray();
        console.log(`📊 Found ${collections.length} collections in MongoDB`);
        
        // Drop all collections
        for (const collection of collections) {
            await db.collection(collection.name).drop();
            console.log(`✅ Dropped collection: ${collection.name}`);
        }
        
        await client.close();
        console.log('✅ MongoDB cleared successfully');
        
    } catch (error) {
        console.log('⚠️ MongoDB clear error (may be empty):', error.message);
    }
    
    // Clear PostgreSQL
    try {
        console.log('🐘 Clearing PostgreSQL...');
        const pgPool = new Pool(config.postgresql);
        
        // Get all tables
        const tablesResult = await pgPool.query(`
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
        `);
        
        console.log(`📊 Found ${tablesResult.rows.length} tables in PostgreSQL`);
        
        // Drop all tables
        for (const row of tablesResult.rows) {
            await pgPool.query(`DROP TABLE IF EXISTS "${row.tablename}" CASCADE`);
            console.log(`✅ Dropped table: ${row.tablename}`);
        }
        
        await pgPool.end();
        console.log('✅ PostgreSQL cleared successfully');
        
    } catch (error) {
        console.log('⚠️ PostgreSQL clear error (may be empty):', error.message);
    }
    
    console.log('🎉 All databases cleared successfully!');
    console.log('🚀 Ready for fresh testing!');
}

clearAllDatabases().catch(console.error); 