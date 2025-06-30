const { Pool } = require('pg');
const config = require('./config');

// PostgreSQL connection
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

async function clearAllPostgreSQLData() {
  console.log('🧹 CLEARING ALL POSTGRESQL DATA');
  console.log('═══════════════════════════════════════');
  
  const client = await pgPool.connect();
  
  try {
    // Get all tables in the public schema
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    
    console.log(`📋 Found ${tablesResult.rows.length} tables to clear:`);
    
    let totalRecordsDeleted = 0;
    
    for (const table of tablesResult.rows) {
      const tableName = table.table_name;
      
      try {
        // Count records before deletion
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
        const recordCount = parseInt(countResult.rows[0].count);
        
        if (recordCount > 0) {
          // Clear the table
          await client.query(`DELETE FROM ${tableName}`);
          
          // Reset auto-increment sequences if they exist
          const sequenceResult = await client.query(`
            SELECT column_name, column_default
            FROM information_schema.columns
            WHERE table_name = $1 
            AND column_default LIKE 'nextval%'
          `, [tableName]);
          
          for (const seqRow of sequenceResult.rows) {
            const sequenceName = seqRow.column_default.match(/nextval\('([^']+)'/);
            if (sequenceName) {
              await client.query(`ALTER SEQUENCE ${sequenceName[1]} RESTART WITH 1`);
            }
          }
          
          console.log(`✅ Cleared ${tableName}: ${recordCount} records deleted`);
          totalRecordsDeleted += recordCount;
        } else {
          console.log(`⚪ ${tableName}: already empty`);
        }
        
      } catch (error) {
        console.log(`❌ Error clearing ${tableName}: ${error.message}`);
      }
    }
    
    console.log('\n📊 CLEANUP SUMMARY:');
    console.log(`🗑️  Total records deleted: ${totalRecordsDeleted.toLocaleString()}`);
    console.log(`📋 Tables processed: ${tablesResult.rows.length}`);
    console.log('✅ PostgreSQL database is now completely empty and ready for fresh processing!');
    
  } catch (error) {
    console.error('💥 Error during cleanup:', error.message);
    throw error;
  } finally {
    client.release();
    await pgPool.end();
  }
}

// Run the cleanup
if (require.main === module) {
  clearAllPostgreSQLData()
    .then(() => {
      console.log('\n🎉 PostgreSQL cleanup completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Cleanup failed:', error.message);
      process.exit(1);
    });
}

module.exports = { clearAllPostgreSQLData }; 