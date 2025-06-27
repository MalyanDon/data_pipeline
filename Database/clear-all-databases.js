const { MongoClient } = require('mongodb');
const { Client } = require('pg');
const config = require('./config');

async function clearMongoDB() {
  console.log('🗑️  Clearing MongoDB data...');
  
  const client = new MongoClient(config.mongodb.uri);
  
  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');
    
    const admin = client.db().admin();
    const databases = await admin.listDatabases();
    
    let clearedDBs = 0;
    
    for (const db of databases.databases) {
      // Clear financial data databases and main database
      if (db.name.startsWith('financial_data') || db.name === 'financial_data') {
        console.log(`📂 Clearing database: ${db.name}`);
        
        const database = client.db(db.name);
        const collections = await database.listCollections().toArray();
        
        console.log(`   📋 Found ${collections.length} collections`);
        
        for (const collection of collections) {
          const collectionName = collection.name;
          console.log(`   🗑️  Dropping collection: ${collectionName}`);
          await database.collection(collectionName).drop();
        }
        
        console.log(`   ✅ Database ${db.name} cleared`);
        clearedDBs++;
      }
    }
    
    console.log(`🎉 MongoDB cleanup complete! Cleared ${clearedDBs} databases`);
    
  } catch (error) {
    console.error('❌ MongoDB cleanup error:', error.message);
    throw error;
  } finally {
    await client.close();
  }
}

async function clearPostgreSQL() {
  console.log('🗑️  Clearing PostgreSQL data...');
  
  const client = new Client({
    connectionString: config.postgresql.connectionString,
  });
  
  try {
    await client.connect();
    console.log('✅ Connected to PostgreSQL');
    
    // Get all tables in the public schema
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name;
    `);
    
    console.log(`📋 Found ${tablesResult.rows.length} tables to clear`);
    
    // Drop all tables with CASCADE to handle foreign key constraints
    for (const row of tablesResult.rows) {
      const tableName = row.table_name;
      console.log(`   🗑️  Dropping table: ${tableName}`);
      await client.query(`DROP TABLE IF EXISTS ${tableName} CASCADE;`);
    }
    
    // Get all views in the public schema
    const viewsResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'VIEW'
      ORDER BY table_name;
    `);
    
    console.log(`👁️  Found ${viewsResult.rows.length} views to clear`);
    
    // Drop all views
    for (const row of viewsResult.rows) {
      const viewName = row.table_name;
      console.log(`   🗑️  Dropping view: ${viewName}`);
      await client.query(`DROP VIEW IF EXISTS ${viewName} CASCADE;`);
    }
    
    // Get all sequences (for SERIAL columns)
    const sequencesResult = await client.query(`
      SELECT sequence_name 
      FROM information_schema.sequences 
      WHERE sequence_schema = 'public'
      ORDER BY sequence_name;
    `);
    
    console.log(`🔢 Found ${sequencesResult.rows.length} sequences to clear`);
    
    // Drop all sequences
    for (const row of sequencesResult.rows) {
      const sequenceName = row.sequence_name;
      console.log(`   🗑️  Dropping sequence: ${sequenceName}`);
      await client.query(`DROP SEQUENCE IF EXISTS ${sequenceName} CASCADE;`);
    }
    
    console.log('🎉 PostgreSQL cleanup complete!');
    
  } catch (error) {
    console.error('❌ PostgreSQL cleanup error:', error.message);
    throw error;
  } finally {
    await client.end();
  }
}

async function clearAllDatabases() {
  console.log('🚀 Starting complete database cleanup...\n');
  
  try {
    // Clear MongoDB first
    await clearMongoDB();
    console.log();
    
    // Clear PostgreSQL
    await clearPostgreSQL();
    console.log();
    
    console.log('✨ All databases cleared successfully!');
    console.log('🔄 Ready for fresh start');
    console.log('\n📝 Next steps:');
    console.log('   1. Run: npm run setup-postgres    (to recreate PostgreSQL schema)');
    console.log('   2. Upload new files through dashboard');
    console.log('   3. Process data through ETL pipeline');
    
  } catch (error) {
    console.error('💥 Database cleanup failed:', error.message);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  clearAllDatabases();
}

module.exports = { clearAllDatabases, clearMongoDB, clearPostgreSQL }; 