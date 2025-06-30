#!/usr/bin/env node

const { Pool } = require('pg');

async function checkTables() {
  const config = require('./config');
  const pool = new Pool(config.postgresql);
  const client = await pool.connect();
  
  try {
    console.log('🔍 Checking Transaction Tables Structure');
    
    // List all tables
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      ORDER BY table_name
    `);
    
    console.log('\n📊 All Tables:');
    tablesResult.rows.forEach(row => {
      console.log(`   ✅ ${row.table_name}`);
    });
    
    // Check specific transaction tables
    const transactionTables = ['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'];
    
    for (const tableName of transactionTables) {
      try {
        const columnsResult = await client.query(`
          SELECT column_name, data_type, is_nullable
          FROM information_schema.columns 
          WHERE table_name = $1
          ORDER BY ordinal_position
        `, [tableName]);
        
        if (columnsResult.rows.length > 0) {
          console.log(`\n🔹 ${tableName.toUpperCase()} structure:`);
          columnsResult.rows.forEach(row => {
            console.log(`   ${row.column_name}: ${row.data_type} (${row.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'})`);
          });
        } else {
          console.log(`\n❌ ${tableName} - Table does not exist`);
        }
      } catch (error) {
        console.log(`\n❌ ${tableName} - Error: ${error.message}`);
      }
    }
    
  } catch (error) {
    console.error('❌ Check failed:', error.message);
  } finally {
    client.release();
    await pool.end();
  }
}

checkTables(); 