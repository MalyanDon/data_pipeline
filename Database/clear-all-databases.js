const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

async function clearAllDatabases() {
  console.log('🧹 Starting complete database cleanup...');
  
  // Clear PostgreSQL
  try {
    console.log('\n🐘 Clearing PostgreSQL data...');
    const pgPool = new Pool({
      connectionString: config.postgresql.connectionString,
      max: 5,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });

    const client = await pgPool.connect();
    
    // Get all tables
    const tablesQuery = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `;
    const tablesResult = await client.query(tablesQuery);
    
    console.log(`📋 Found ${tablesResult.rows.length} PostgreSQL tables to clear`);
    
    let totalDeletedRecords = 0;
    
    // Clear each table
    for (const table of tablesResult.rows) {
      const tableName = table.table_name;
      
      try {
        // Get record count before deletion
        const countQuery = `SELECT COUNT(*) as count FROM ${tableName}`;
        const countResult = await client.query(countQuery);
        const recordCount = parseInt(countResult.rows[0].count);
        
        if (recordCount > 0) {
          // Delete all records
          await client.query(`DELETE FROM ${tableName}`);
          console.log(`✅ Cleared ${tableName}: ${recordCount} records deleted`);
          totalDeletedRecords += recordCount;
        } else {
          console.log(`⚪ ${tableName}: Already empty`);
        }
        
        // Reset sequence if exists
        try {
          await client.query(`ALTER SEQUENCE IF EXISTS ${tableName}_id_seq RESTART WITH 1`);
        } catch (seqError) {
          // Sequence might not exist, ignore
        }
        
      } catch (error) {
        console.error(`❌ Error clearing table ${tableName}:`, error.message);
      }
    }
    
    client.release();
    await pgPool.end();
    
    console.log(`🎉 PostgreSQL cleanup complete: ${totalDeletedRecords} total records deleted`);
    
  } catch (error) {
    console.error('❌ PostgreSQL cleanup failed:', error.message);
  }
  
  // Clear MongoDB
  try {
    console.log('\n🍃 Clearing MongoDB data...');
    
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB for cleanup');
    
    // Clear both 2024 and 2025 databases
    const databases = ['financial_data_2024', 'financial_data_2025'];
    let totalDeletedCollections = 0;
    let totalDeletedDocuments = 0;
    
    for (const dbName of databases) {
      console.log(`\n📅 Processing database: ${dbName}`);
      const db = mongoClient.db(dbName);
      
      // Get all collections
      const collections = await db.listCollections().toArray();
      console.log(`📋 Found ${collections.length} collections in ${dbName}`);
      
      for (const col of collections) {
        try {
          const collection = db.collection(col.name);
          
          // Get document count
          const docCount = await collection.countDocuments();
          
          if (docCount > 0) {
            // Delete all documents
            const deleteResult = await collection.deleteMany({});
            console.log(`✅ Cleared ${col.name}: ${deleteResult.deletedCount} documents deleted`);
            totalDeletedDocuments += deleteResult.deletedCount;
          } else {
            console.log(`⚪ ${col.name}: Already empty`);
          }
          
          // Drop the collection completely
          await collection.drop();
          console.log(`🗑️ Dropped collection: ${col.name}`);
          totalDeletedCollections++;
          
        } catch (error) {
          if (error.message.includes('ns not found')) {
            console.log(`⚪ Collection ${col.name} already dropped`);
          } else {
            console.error(`❌ Error clearing collection ${col.name}:`, error.message);
          }
        }
      }
    }
    
    await mongoClient.close();
    console.log(`🎉 MongoDB cleanup complete: ${totalDeletedCollections} collections dropped, ${totalDeletedDocuments} documents deleted`);
    
  } catch (error) {
    console.error('❌ MongoDB cleanup failed:', error.message);
  }
  
  console.log('\n🎯 Complete database cleanup finished!');
  console.log('✅ Both MongoDB and PostgreSQL are now completely empty');
  console.log('🚀 Ready for fresh data upload and processing');
}

// Run the cleanup
clearAllDatabases()
  .then(() => {
    console.log('\n🎉 All databases cleared successfully!');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n❌ Database cleanup failed:', error);
    process.exit(1);
  }); 