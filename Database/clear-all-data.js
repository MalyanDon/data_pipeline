#!/usr/bin/env node

const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

class DataCleaner {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
    this.mongoClient = new MongoClient(config.mongodb.uri);
  }

  async clearAllData() {
    console.log('🧹 CLEARING ALL DATA - FRESH START\n');
    console.log('⚠️  This will remove ALL data from both databases!');
    console.log('🔄 Starting complete cleanup...\n');
    
    try {
      // Clear MongoDB first
      await this.clearMongoDB();
      
      // Clear PostgreSQL
      await this.clearPostgreSQL();
      
      // Verify clean state
      await this.verifyCleanState();
      
      console.log('\n✅ COMPLETE! All data cleared successfully!');
      console.log('🚀 System is now ready for fresh data processing');
      
    } catch (error) {
      console.error('❌ Failed to clear data:', error.message);
      throw error;
    } finally {
      await this.mongoClient.close();
      await this.pgPool.end();
    }
  }

  async clearMongoDB() {
    console.log('🗑️  CLEARING MONGODB DATA...');
    
    try {
      await this.mongoClient.connect();
      
      // Clear financial_data_2025 database
      console.log('📊 Clearing financial_data_2025 database...');
      const db2025 = this.mongoClient.db('financial_data_2025');
      const collections2025 = await db2025.listCollections().toArray();
      
      if (collections2025.length > 0) {
        console.log(`   Found ${collections2025.length} collections to remove:`);
        
        for (const collection of collections2025) {
          const coll = db2025.collection(collection.name);
          const count = await coll.countDocuments();
          await coll.drop();
          console.log(`   ❌ Removed: ${collection.name} (${count} records)`);
        }
      } else {
        console.log('   📝 No collections found in financial_data_2025');
      }
      
      // Clear financial_data_2024 database (if exists)
      console.log('\n📊 Clearing financial_data_2024 database...');
      try {
        const db2024 = this.mongoClient.db('financial_data_2024');
        const collections2024 = await db2024.listCollections().toArray();
        
        if (collections2024.length > 0) {
          console.log(`   Found ${collections2024.length} collections to remove:`);
          
          for (const collection of collections2024) {
            const coll = db2024.collection(collection.name);
            const count = await coll.countDocuments();
            await coll.drop();
            console.log(`   ❌ Removed: ${collection.name} (${count} records)`);
          }
        } else {
          console.log('   📝 No collections found in financial_data_2024');
        }
      } catch (error) {
        console.log('   📝 financial_data_2024 database not found or empty');
      }
      
      console.log('✅ MongoDB cleanup completed');
      
    } catch (error) {
      console.log(`❌ MongoDB cleanup error: ${error.message}`);
    }
  }

  async clearPostgreSQL() {
    console.log('\n🗑️  CLEARING POSTGRESQL DATA...');
    
    const client = await this.pgPool.connect();
    
    try {
      // Get all tables
      const tablesResult = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
      `);
      
      if (tablesResult.rows.length === 0) {
        console.log('   📝 No tables found in PostgreSQL');
        return;
      }
      
      console.log(`📊 Found ${tablesResult.rows.length} tables to clear:`);
      
      let totalRecordsRemoved = 0;
      
      // Clear each table
      for (const row of tablesResult.rows) {
        const tableName = row.table_name;
        
        try {
          // Get record count before deletion
          const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
          const recordCount = parseInt(countResult.rows[0].count);
          
          if (recordCount > 0) {
            // Truncate table (faster than DELETE)
            await client.query(`TRUNCATE TABLE ${tableName} RESTART IDENTITY CASCADE`);
            console.log(`   ❌ Cleared: ${tableName} (${recordCount} records)`);
            totalRecordsRemoved += recordCount;
          } else {
            console.log(`   📝 Empty: ${tableName} (0 records)`);
          }
          
        } catch (error) {
          console.log(`   ⚠️  Error clearing ${tableName}: ${error.message}`);
        }
      }
      
      // Reset sequences for all tables
      console.log('\n🔄 Resetting ID sequences...');
      const sequenceResult = await client.query(`
        SELECT sequence_name 
        FROM information_schema.sequences 
        WHERE sequence_schema = 'public'
      `);
      
      for (const seq of sequenceResult.rows) {
        try {
          await client.query(`ALTER SEQUENCE ${seq.sequence_name} RESTART WITH 1`);
          console.log(`   ✅ Reset sequence: ${seq.sequence_name}`);
        } catch (error) {
          // Skip if sequence doesn't exist
        }
      }
      
      console.log(`\n✅ PostgreSQL cleanup completed (${totalRecordsRemoved} total records removed)`);
      
    } catch (error) {
      console.log(`❌ PostgreSQL cleanup error: ${error.message}`);
    } finally {
      client.release();
    }
  }

  async verifyCleanState() {
    console.log('\n📊 VERIFYING CLEAN STATE...');
    
    try {
      // Verify MongoDB
      await this.mongoClient.connect();
      
      const db2025 = this.mongoClient.db('financial_data_2025');
      const collections2025 = await db2025.listCollections().toArray();
      
      console.log('🔍 MongoDB Status:');
      if (collections2025.length === 0) {
        console.log('   ✅ financial_data_2025: EMPTY (ready for new data)');
      } else {
        console.log(`   ⚠️  financial_data_2025: ${collections2025.length} collections remaining`);
      }
      
      // Verify PostgreSQL
      const client = await this.pgPool.connect();
      
      try {
        const tablesResult = await client.query(`
          SELECT 
            table_name,
            (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns
          FROM information_schema.tables t
          WHERE table_schema = 'public' 
          AND table_type = 'BASE TABLE'
          ORDER BY table_name
        `);
        
        console.log('\n🔍 PostgreSQL Status:');
        if (tablesResult.rows.length === 0) {
          console.log('   ✅ No tables found');
        } else {
          console.log(`   📊 Tables ready for new data (${tablesResult.rows.length} tables):`);
          
          let totalRecords = 0;
          for (const table of tablesResult.rows) {
            try {
              const countResult = await client.query(`SELECT COUNT(*) as count FROM ${table.table_name}`);
              const count = parseInt(countResult.rows[0].count);
              totalRecords += count;
              
              const status = count === 0 ? '✅ EMPTY' : `⚠️  ${count} records`;
              console.log(`      ${table.table_name.padEnd(35)} ${status}`);
            } catch (error) {
              console.log(`      ${table.table_name.padEnd(35)} ❌ ERROR`);
            }
          }
          
          if (totalRecords === 0) {
            console.log('\n   ✅ All tables are empty and ready for new data!');
          } else {
            console.log(`\n   ⚠️  Warning: ${totalRecords} records still found in tables`);
          }
        }
        
      } finally {
        client.release();
      }
      
    } catch (error) {
      console.log(`❌ Verification error: ${error.message}`);
    }
  }

  async showNextSteps() {
    console.log('\n' + '='.repeat(60));
    console.log('🚀 SYSTEM READY FOR FRESH START!');
    console.log('='.repeat(60));
    console.log('📋 Next Steps:');
    console.log('1. Upload your files through the dashboard (http://localhost:3006)');
    console.log('2. Files will be stored in MongoDB as raw data');
    console.log('3. Click "Process Data" to transform to PostgreSQL');
    console.log('4. Ready for business analytics and reporting!');
    console.log('');
    console.log('✅ Both databases are now completely clean');
    console.log('🎯 Ready to process fresh data with 0% conflicts');
  }
}

// Run the cleaner
if (require.main === module) {
  const cleaner = new DataCleaner();
  cleaner.clearAllData()
    .then(() => cleaner.showNextSteps())
    .catch(console.error);
}

module.exports = { DataCleaner }; 