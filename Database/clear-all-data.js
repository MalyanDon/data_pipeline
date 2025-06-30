#!/usr/bin/env node

const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

async function clearAllData() {
    console.log('🧹 Starting complete database cleanup...');
    
    // Clear MongoDB
    console.log('\n📊 Clearing MongoDB...');
    const mongoClient = new MongoClient(config.mongodb.uri);
    
    try {
        await mongoClient.connect();
        const db = mongoClient.db(config.mongodb.database);
        
        // Get all collections
        const collections = await db.listCollections().toArray();
        console.log(`Found ${collections.length} MongoDB collections`);
        
        for (const collection of collections) {
            const name = collection.name;
            if (name !== 'system.indexes') {
                const count = await db.collection(name).countDocuments();
                await db.collection(name).drop();
                console.log(`✅ Dropped collection: ${name} (${count} documents)`);
            }
        }
        
        console.log('✅ MongoDB cleared completely');
        
    } catch (error) {
        console.error('❌ MongoDB error:', error);
    } finally {
        await mongoClient.close();
    }
    
    // Clear PostgreSQL
    console.log('\n🐘 Clearing PostgreSQL...');
    const pgPool = new Pool({
        host: config.postgresql.host,
        port: config.postgresql.port,
        user: config.postgresql.user,
        password: config.postgresql.password,
        database: config.postgresql.database
    });
    
    try {
        // Get all tables
        const tablesResult = await pgPool.query(`
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
        `);
        
        console.log(`Found ${tablesResult.rows.length} PostgreSQL tables`);
        
        for (const row of tablesResult.rows) {
            const tableName = row.tablename;
            try {
                const countResult = await pgPool.query(`SELECT COUNT(*) FROM "${tableName}"`);
                const count = countResult.rows[0].count;
                
                await pgPool.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
                console.log(`✅ Dropped table: ${tableName} (${count} records)`);
            } catch (error) {
                console.log(`⚠️ Could not drop table ${tableName}: ${error.message}`);
            }
        }
        
        console.log('✅ PostgreSQL cleared completely');
        
    } catch (error) {
        console.error('❌ PostgreSQL error:', error);
    } finally {
        await pgPool.end();
    }
    
    console.log('\n🎉 Complete database cleanup finished!');
    console.log('📤 Ready for fresh uploads with smart header detection');
}

clearAllData().catch(console.error);

module.exports = { clearAllData }; 

const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

async function clearAllData() {
    console.log('🧹 Starting complete database cleanup...');
    
    // Clear MongoDB
    console.log('\n📊 Clearing MongoDB...');
    const mongoClient = new MongoClient(config.mongodb.uri);
    
    try {
        await mongoClient.connect();
        const db = mongoClient.db(config.mongodb.database);
        
        // Get all collections
        const collections = await db.listCollections().toArray();
        console.log(`Found ${collections.length} MongoDB collections`);
        
        for (const collection of collections) {
            const name = collection.name;
            if (name !== 'system.indexes') {
                const count = await db.collection(name).countDocuments();
                await db.collection(name).drop();
                console.log(`✅ Dropped collection: ${name} (${count} documents)`);
            }
        }
        
        console.log('✅ MongoDB cleared completely');
        
    } catch (error) {
        console.error('❌ MongoDB error:', error);
    } finally {
        await mongoClient.close();
    }
    
    // Clear PostgreSQL
    console.log('\n🐘 Clearing PostgreSQL...');
    const pgPool = new Pool({
        host: config.postgresql.host,
        port: config.postgresql.port,
        user: config.postgresql.user,
        password: config.postgresql.password,
        database: config.postgresql.database
    });
    
    try {
        // Get all tables
        const tablesResult = await pgPool.query(`
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' 
            AND tablename NOT LIKE 'pg_%'
        `);
        
        console.log(`Found ${tablesResult.rows.length} PostgreSQL tables`);
        
        for (const row of tablesResult.rows) {
            const tableName = row.tablename;
            try {
                const countResult = await pgPool.query(`SELECT COUNT(*) FROM "${tableName}"`);
                const count = countResult.rows[0].count;
                
                await pgPool.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
                console.log(`✅ Dropped table: ${tableName} (${count} records)`);
            } catch (error) {
                console.log(`⚠️ Could not drop table ${tableName}: ${error.message}`);
            }
        }
        
        console.log('✅ PostgreSQL cleared completely');
        
    } catch (error) {
        console.error('❌ PostgreSQL error:', error);
    } finally {
        await pgPool.end();
    }
    
    console.log('\n🎉 Complete database cleanup finished!');
    console.log('📤 Ready for fresh uploads with smart header detection');
}

clearAllData().catch(console.error);

module.exports = { clearAllData }; 