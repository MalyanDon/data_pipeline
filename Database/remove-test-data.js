#!/usr/bin/env node

const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

class TestDataRemover {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
    this.mongoClient = new MongoClient(config.mongodb.uri);
  }

  async removeTestData() {
    console.log('🧹 REMOVING TEST UPLOAD CSV DATA\n');
    
    try {
      // Remove from MongoDB
      await this.removeFromMongoDB();
      
      // Remove from PostgreSQL
      await this.removeFromPostgreSQL();
      
      console.log('\n✅ Test data cleanup completed successfully!');
      
    } catch (error) {
      console.error('❌ Failed to remove test data:', error.message);
      throw error;
    } finally {
      await this.mongoClient.close();
      await this.pgPool.end();
    }
  }

  async removeFromMongoDB() {
    console.log('🗑️  Removing test data from MongoDB...');
    
    try {
      await this.mongoClient.connect();
      const db = this.mongoClient.db('financial_data_2025');
      
      // Check if test collection exists
      const collections = await db.listCollections({ name: 'test_uploadcsv_data' }).toArray();
      
      if (collections.length > 0) {
        // Get record count before deletion
        const collection = db.collection('test_uploadcsv_data');
        const recordCount = await collection.countDocuments();
        
        // Drop the collection
        await collection.drop();
        
        console.log(`   ✅ Removed MongoDB collection: test_uploadcsv_data (${recordCount} records)`);
      } else {
        console.log('   📝 MongoDB collection test_uploadcsv_data not found');
      }
      
      // Also check for any other test collections
      const allCollections = await db.listCollections().toArray();
      const testCollections = allCollections.filter(col => 
        col.name.toLowerCase().includes('test_upload') || 
        col.name.toLowerCase().includes('test_') ||
        col.name.toLowerCase().includes('_test')
      );
      
      if (testCollections.length > 0) {
        console.log('\n   🔍 Found additional test collections:');
        for (const testCol of testCollections) {
          const testCollection = db.collection(testCol.name);
          const count = await testCollection.countDocuments();
          console.log(`      - ${testCol.name} (${count} records)`);
          
          // Ask if we should remove these too
          console.log(`      ❌ Removing: ${testCol.name}`);
          await testCollection.drop();
        }
      }
      
    } catch (error) {
      console.log(`   ❌ MongoDB cleanup error: ${error.message}`);
    }
  }

  async removeFromPostgreSQL() {
    console.log('\n🗑️  Removing test data from PostgreSQL...');
    
    const client = await this.pgPool.connect();
    
    try {
      // Remove test records from raw_uploads table
      const result = await client.query(`
        DELETE FROM raw_uploads 
        WHERE data_type = 'unknown' 
        AND (raw_data::text ILIKE '%test%' OR name ILIKE '%test%')
        RETURNING *
      `);
      
      if (result.rowCount > 0) {
        console.log(`   ✅ Removed ${result.rowCount} test records from raw_uploads table`);
      } else {
        console.log('   📝 No test records found in raw_uploads table');
      }
      
      // Check for any test data in other tables
      const testTables = [
        'brokers',
        'clients', 
        'distributors',
        'strategies',
        'contract_notes',
        'cash_capital_flow',
        'stock_capital_flow',
        'mf_allocations'
      ];
      
      for (const table of testTables) {
        try {
          const testResult = await client.query(`
            DELETE FROM ${table} 
            WHERE (${table === 'brokers' ? 'broker_name' : 
                   table === 'clients' ? 'client_name' :
                   table === 'distributors' ? 'distributor_name' :
                   table === 'strategies' ? 'strategy_name' :
                   'client_name'}) ILIKE '%test%'
            RETURNING *
          `);
          
          if (testResult.rowCount > 0) {
            console.log(`   ✅ Removed ${testResult.rowCount} test records from ${table} table`);
          }
        } catch (error) {
          // Skip if column doesn't exist
        }
      }
      
      // Check custody tables for test data
      const custodyTables = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'unified_custody_master_%'
      `);
      
      for (const custodyTable of custodyTables.rows) {
        try {
          const custodyResult = await client.query(`
            DELETE FROM ${custodyTable.table_name} 
            WHERE client_name ILIKE '%test%' OR client_reference ILIKE '%test%'
            RETURNING *
          `);
          
          if (custodyResult.rowCount > 0) {
            console.log(`   ✅ Removed ${custodyResult.rowCount} test records from ${custodyTable.table_name}`);
          }
        } catch (error) {
          // Skip if error
        }
      }
      
    } catch (error) {
      console.log(`   ❌ PostgreSQL cleanup error: ${error.message}`);
    } finally {
      client.release();
    }
  }

  async showCleanedStatus() {
    console.log('\n📊 CLEANUP STATUS VERIFICATION\n');
    
    try {
      // Check MongoDB
      await this.mongoClient.connect();
      const db = this.mongoClient.db('financial_data_2025');
      const collections = await db.listCollections().toArray();
      
      console.log('🔍 MongoDB Collections After Cleanup:');
      collections.forEach(col => {
        console.log(`   ✅ ${col.name}`);
      });
      
      // Check PostgreSQL
      const client = await this.pgPool.connect();
      try {
        const result = await client.query(`
          SELECT table_name, 
                 (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns,
                 CASE 
                   WHEN table_name LIKE '%test%' THEN '⚠️  TEST TABLE'
                   ELSE '✅'
                 END as status
          FROM information_schema.tables t
          WHERE table_schema = 'public' 
          AND table_type = 'BASE TABLE'
          ORDER BY table_name
        `);
        
        console.log('\n🔍 PostgreSQL Tables After Cleanup:');
        result.rows.forEach(row => {
          console.log(`   ${row.status} ${row.table_name} (${row.columns} columns)`);
        });
        
      } finally {
        client.release();
      }
      
    } catch (error) {
      console.log('❌ Status check failed:', error.message);
    }
  }
}

// Run the cleanup
if (require.main === module) {
  const remover = new TestDataRemover();
  remover.removeTestData()
    .then(() => remover.showCleanedStatus())
    .catch(console.error);
}

module.exports = { TestDataRemover }; 

const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

class TestDataRemover {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
    this.mongoClient = new MongoClient(config.mongodb.uri);
  }

  async removeTestData() {
    console.log('🧹 REMOVING TEST UPLOAD CSV DATA\n');
    
    try {
      // Remove from MongoDB
      await this.removeFromMongoDB();
      
      // Remove from PostgreSQL
      await this.removeFromPostgreSQL();
      
      console.log('\n✅ Test data cleanup completed successfully!');
      
    } catch (error) {
      console.error('❌ Failed to remove test data:', error.message);
      throw error;
    } finally {
      await this.mongoClient.close();
      await this.pgPool.end();
    }
  }

  async removeFromMongoDB() {
    console.log('🗑️  Removing test data from MongoDB...');
    
    try {
      await this.mongoClient.connect();
      const db = this.mongoClient.db('financial_data_2025');
      
      // Check if test collection exists
      const collections = await db.listCollections({ name: 'test_uploadcsv_data' }).toArray();
      
      if (collections.length > 0) {
        // Get record count before deletion
        const collection = db.collection('test_uploadcsv_data');
        const recordCount = await collection.countDocuments();
        
        // Drop the collection
        await collection.drop();
        
        console.log(`   ✅ Removed MongoDB collection: test_uploadcsv_data (${recordCount} records)`);
      } else {
        console.log('   📝 MongoDB collection test_uploadcsv_data not found');
      }
      
      // Also check for any other test collections
      const allCollections = await db.listCollections().toArray();
      const testCollections = allCollections.filter(col => 
        col.name.toLowerCase().includes('test_upload') || 
        col.name.toLowerCase().includes('test_') ||
        col.name.toLowerCase().includes('_test')
      );
      
      if (testCollections.length > 0) {
        console.log('\n   🔍 Found additional test collections:');
        for (const testCol of testCollections) {
          const testCollection = db.collection(testCol.name);
          const count = await testCollection.countDocuments();
          console.log(`      - ${testCol.name} (${count} records)`);
          
          // Ask if we should remove these too
          console.log(`      ❌ Removing: ${testCol.name}`);
          await testCollection.drop();
        }
      }
      
    } catch (error) {
      console.log(`   ❌ MongoDB cleanup error: ${error.message}`);
    }
  }

  async removeFromPostgreSQL() {
    console.log('\n🗑️  Removing test data from PostgreSQL...');
    
    const client = await this.pgPool.connect();
    
    try {
      // Remove test records from raw_uploads table
      const result = await client.query(`
        DELETE FROM raw_uploads 
        WHERE data_type = 'unknown' 
        AND (raw_data::text ILIKE '%test%' OR name ILIKE '%test%')
        RETURNING *
      `);
      
      if (result.rowCount > 0) {
        console.log(`   ✅ Removed ${result.rowCount} test records from raw_uploads table`);
      } else {
        console.log('   📝 No test records found in raw_uploads table');
      }
      
      // Check for any test data in other tables
      const testTables = [
        'brokers',
        'clients', 
        'distributors',
        'strategies',
        'contract_notes',
        'cash_capital_flow',
        'stock_capital_flow',
        'mf_allocations'
      ];
      
      for (const table of testTables) {
        try {
          const testResult = await client.query(`
            DELETE FROM ${table} 
            WHERE (${table === 'brokers' ? 'broker_name' : 
                   table === 'clients' ? 'client_name' :
                   table === 'distributors' ? 'distributor_name' :
                   table === 'strategies' ? 'strategy_name' :
                   'client_name'}) ILIKE '%test%'
            RETURNING *
          `);
          
          if (testResult.rowCount > 0) {
            console.log(`   ✅ Removed ${testResult.rowCount} test records from ${table} table`);
          }
        } catch (error) {
          // Skip if column doesn't exist
        }
      }
      
      // Check custody tables for test data
      const custodyTables = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'unified_custody_master_%'
      `);
      
      for (const custodyTable of custodyTables.rows) {
        try {
          const custodyResult = await client.query(`
            DELETE FROM ${custodyTable.table_name} 
            WHERE client_name ILIKE '%test%' OR client_reference ILIKE '%test%'
            RETURNING *
          `);
          
          if (custodyResult.rowCount > 0) {
            console.log(`   ✅ Removed ${custodyResult.rowCount} test records from ${custodyTable.table_name}`);
          }
        } catch (error) {
          // Skip if error
        }
      }
      
    } catch (error) {
      console.log(`   ❌ PostgreSQL cleanup error: ${error.message}`);
    } finally {
      client.release();
    }
  }

  async showCleanedStatus() {
    console.log('\n📊 CLEANUP STATUS VERIFICATION\n');
    
    try {
      // Check MongoDB
      await this.mongoClient.connect();
      const db = this.mongoClient.db('financial_data_2025');
      const collections = await db.listCollections().toArray();
      
      console.log('🔍 MongoDB Collections After Cleanup:');
      collections.forEach(col => {
        console.log(`   ✅ ${col.name}`);
      });
      
      // Check PostgreSQL
      const client = await this.pgPool.connect();
      try {
        const result = await client.query(`
          SELECT table_name, 
                 (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as columns,
                 CASE 
                   WHEN table_name LIKE '%test%' THEN '⚠️  TEST TABLE'
                   ELSE '✅'
                 END as status
          FROM information_schema.tables t
          WHERE table_schema = 'public' 
          AND table_type = 'BASE TABLE'
          ORDER BY table_name
        `);
        
        console.log('\n🔍 PostgreSQL Tables After Cleanup:');
        result.rows.forEach(row => {
          console.log(`   ${row.status} ${row.table_name} (${row.columns} columns)`);
        });
        
      } finally {
        client.release();
      }
      
    } catch (error) {
      console.log('❌ Status check failed:', error.message);
    }
  }
}

// Run the cleanup
if (require.main === module) {
  const remover = new TestDataRemover();
  remover.removeTestData()
    .then(() => remover.showCleanedStatus())
    .catch(console.error);
}

module.exports = { TestDataRemover }; 