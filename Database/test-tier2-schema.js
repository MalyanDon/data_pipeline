#!/usr/bin/env node

const { Pool } = require('pg');

async function testSchemaCreation() {
  const config = require('./config');
  const pool = new Pool(config.postgresql);
  const client = await pool.connect();
  
  try {
    console.log('🧪 Testing Tier 2 Transaction Tables Creation');
    
    // Test table creation
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_contract_notes (
        contract_id SERIAL PRIMARY KEY,
        ecn_number VARCHAR(50) UNIQUE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // Check if table exists
    const result = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name IN ('test_contract_notes', 'unified_custody_master')
    `);
    
    console.log('📊 Tables found:');
    result.rows.forEach(row => {
      console.log(`   ✅ ${row.table_name}`);
    });
    
    // Clean up test table
    await client.query('DROP TABLE IF EXISTS test_contract_notes');
    
    console.log('✅ Schema test completed successfully');
    
  } catch (error) {
    console.error('❌ Schema test failed:', error.message);
  } finally {
    client.release();
    await pool.end();
  }
}

testSchemaCreation(); 