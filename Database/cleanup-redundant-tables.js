#!/usr/bin/env node

const { Pool } = require('pg');
const config = require('./config');

class DatabaseSimplification {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
  }

  async simplifyDatabase() {
    console.log('🧹 Starting Database Simplification...\n');
    
    const client = await this.pgPool.connect();
    
    try {
      // 1. Remove redundant tables
      await this.removeRedundantTables(client);
      
      // 2. Rename ENSO tables to simplified names
      await this.renameENSOTables(client);
      
      // 3. Create simplified transaction tables if they don't exist
      await this.createSimplifiedTables(client);
      
      // 4. Show final simplified structure
      await this.showFinalStructure(client);
      
      console.log('\n✅ Database simplification completed successfully!');
      
    } catch (error) {
      console.error('❌ Database simplification failed:', error.message);
      throw error;
    } finally {
      client.release();
      await this.pgPool.end();
    }
  }

  async removeRedundantTables(client) {
    console.log('🗑️  Removing redundant tables...');
    
    const redundantTables = [
      'unified_custody_master',    // Keep only daily tables
      'custody_holdings',          // Redundant with daily custody tables
      'capital_flows',            // Redundant with cash/stock flow tables
      'trades'                    // Same as contract_notes
    ];
    
    for (const table of redundantTables) {
      try {
        await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
        console.log(`   ❌ Removed redundant table: ${table}`);
      } catch (error) {
        console.log(`   ⚠️  Could not remove ${table}: ${error.message}`);
      }
    }
  }

  async renameENSOTables(client) {
    console.log('\n📝 Renaming ENSO tables to simplified names...');
    
    const renameMappings = [
      { from: 'enso_contract_notes', to: 'contract_notes' },
      { from: 'enso_cash_capital_flow', to: 'cash_capital_flow' },
      { from: 'enso_stock_capital_flow', to: 'stock_capital_flow' },
      { from: 'enso_mf_allocations', to: 'mf_allocations' }
    ];
    
    for (const mapping of renameMappings) {
      try {
        // Check if source table exists
        const sourceExists = await client.query(`
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
          )
        `, [mapping.from]);
        
        // Check if target table exists
        const targetExists = await client.query(`
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
          )
        `, [mapping.to]);
        
        if (sourceExists.rows[0].exists && !targetExists.rows[0].exists) {
          await client.query(`ALTER TABLE ${mapping.from} RENAME TO ${mapping.to}`);
          console.log(`   ✅ Renamed: ${mapping.from} → ${mapping.to}`);
        } else if (targetExists.rows[0].exists) {
          console.log(`   📝 Table ${mapping.to} already exists (keeping existing)`);
        } else {
          console.log(`   ⚠️  Source table ${mapping.from} not found`);
        }
      } catch (error) {
        console.log(`   ❌ Failed to rename ${mapping.from}: ${error.message}`);
      }
    }
  }

  async createSimplifiedTables(client) {
    console.log('\n🏗️  Creating simplified transaction tables...');
    
    // Contract Notes Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS contract_notes (
        contract_id SERIAL PRIMARY KEY,
        ecn_number VARCHAR(50) UNIQUE NOT NULL,
        trade_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        scrip_code VARCHAR(50),
        scrip_name VARCHAR(200),
        isin VARCHAR(20),
        buy_sell VARCHAR(10) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        rate DECIMAL(10,4) NOT NULL,
        brokerage DECIMAL(10,2),
        net_rate DECIMAL(10,4),
        closing_value DECIMAL(15,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ contract_notes table ready');

    // Cash Capital Flow Table  
    await client.query(`
      CREATE TABLE IF NOT EXISTS cash_capital_flow (
        flow_id SERIAL PRIMARY KEY,
        flow_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        dr_cr VARCHAR(10) NOT NULL,
        amount DECIMAL(15,2) NOT NULL,
        narration TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ cash_capital_flow table ready');

    // Stock Capital Flow Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS stock_capital_flow (
        flow_id SERIAL PRIMARY KEY,
        flow_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        scrip_code VARCHAR(50),
        scrip_name VARCHAR(200),
        isin VARCHAR(20),
        in_out VARCHAR(10) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        rate DECIMAL(10,4),
        value DECIMAL(15,2),
        narration TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ stock_capital_flow table ready');

    // MF Allocations Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS mf_allocations (
        allocation_id SERIAL PRIMARY KEY,
        allocation_date DATE NOT NULL,
        client_name VARCHAR(200) NOT NULL,
        pan VARCHAR(20),
        scheme_name VARCHAR(500),
        isin VARCHAR(20),
        purchase_amount DECIMAL(15,2) NOT NULL,
        folio_number VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ mf_allocations table ready');
  }

  async showFinalStructure(client) {
    console.log('\n📊 Final Simplified Database Structure:');
    
    const result = await client.query(`
      SELECT 
        table_name,
        (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY 
        CASE 
          WHEN table_name IN ('brokers', 'clients', 'distributors', 'strategies', 'securities') THEN 1
          WHEN table_name IN ('contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations') THEN 2
          WHEN table_name LIKE 'unified_custody_master_%' THEN 3
          ELSE 4
        END,
        table_name
    `);
    
    let currentCategory = '';
    result.rows.forEach(row => {
      const table = row.table_name;
      const cols = row.column_count;
      
      // Categorize tables
      let category = '';
      if (['brokers', 'clients', 'distributors', 'strategies', 'securities'].includes(table)) {
        category = '🔧 Master Data Tables';
      } else if (['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'].includes(table)) {
        category = '💰 Transaction Tables';
      } else if (table.startsWith('unified_custody_master_')) {
        category = '🏦 Daily Custody Tables';
      } else {
        category = '📝 System Tables';
      }
      
      if (category !== currentCategory) {
        console.log(`\n${category}:`);
        currentCategory = category;
      }
      
      console.log(`   ✅ ${table.padEnd(35)} (${cols} columns)`);
    });
    
    console.log('\n💡 Benefits of Simplified Structure:');
    console.log('   • No redundant tables');
    console.log('   • Clear single-purpose tables');
    console.log('   • Daily custody partitioning');
    console.log('   • Unified transaction tables (no ENSO prefixes)');
  }
}

// Run the simplification
if (require.main === module) {
  const simplifier = new DatabaseSimplification();
  simplifier.simplifyDatabase().catch(console.error);
}

module.exports = { DatabaseSimplification }; 

const { Pool } = require('pg');
const config = require('./config');

class DatabaseSimplification {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
  }

  async simplifyDatabase() {
    console.log('🧹 Starting Database Simplification...\n');
    
    const client = await this.pgPool.connect();
    
    try {
      // 1. Remove redundant tables
      await this.removeRedundantTables(client);
      
      // 2. Rename ENSO tables to simplified names
      await this.renameENSOTables(client);
      
      // 3. Create simplified transaction tables if they don't exist
      await this.createSimplifiedTables(client);
      
      // 4. Show final simplified structure
      await this.showFinalStructure(client);
      
      console.log('\n✅ Database simplification completed successfully!');
      
    } catch (error) {
      console.error('❌ Database simplification failed:', error.message);
      throw error;
    } finally {
      client.release();
      await this.pgPool.end();
    }
  }

  async removeRedundantTables(client) {
    console.log('🗑️  Removing redundant tables...');
    
    const redundantTables = [
      'unified_custody_master',    // Keep only daily tables
      'custody_holdings',          // Redundant with daily custody tables
      'capital_flows',            // Redundant with cash/stock flow tables
      'trades'                    // Same as contract_notes
    ];
    
    for (const table of redundantTables) {
      try {
        await client.query(`DROP TABLE IF EXISTS ${table} CASCADE`);
        console.log(`   ❌ Removed redundant table: ${table}`);
      } catch (error) {
        console.log(`   ⚠️  Could not remove ${table}: ${error.message}`);
      }
    }
  }

  async renameENSOTables(client) {
    console.log('\n📝 Renaming ENSO tables to simplified names...');
    
    const renameMappings = [
      { from: 'enso_contract_notes', to: 'contract_notes' },
      { from: 'enso_cash_capital_flow', to: 'cash_capital_flow' },
      { from: 'enso_stock_capital_flow', to: 'stock_capital_flow' },
      { from: 'enso_mf_allocations', to: 'mf_allocations' }
    ];
    
    for (const mapping of renameMappings) {
      try {
        // Check if source table exists
        const sourceExists = await client.query(`
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
          )
        `, [mapping.from]);
        
        // Check if target table exists
        const targetExists = await client.query(`
          SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = $1
          )
        `, [mapping.to]);
        
        if (sourceExists.rows[0].exists && !targetExists.rows[0].exists) {
          await client.query(`ALTER TABLE ${mapping.from} RENAME TO ${mapping.to}`);
          console.log(`   ✅ Renamed: ${mapping.from} → ${mapping.to}`);
        } else if (targetExists.rows[0].exists) {
          console.log(`   📝 Table ${mapping.to} already exists (keeping existing)`);
        } else {
          console.log(`   ⚠️  Source table ${mapping.from} not found`);
        }
      } catch (error) {
        console.log(`   ❌ Failed to rename ${mapping.from}: ${error.message}`);
      }
    }
  }

  async createSimplifiedTables(client) {
    console.log('\n🏗️  Creating simplified transaction tables...');
    
    // Contract Notes Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS contract_notes (
        contract_id SERIAL PRIMARY KEY,
        ecn_number VARCHAR(50) UNIQUE NOT NULL,
        trade_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        scrip_code VARCHAR(50),
        scrip_name VARCHAR(200),
        isin VARCHAR(20),
        buy_sell VARCHAR(10) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        rate DECIMAL(10,4) NOT NULL,
        brokerage DECIMAL(10,2),
        net_rate DECIMAL(10,4),
        closing_value DECIMAL(15,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ contract_notes table ready');

    // Cash Capital Flow Table  
    await client.query(`
      CREATE TABLE IF NOT EXISTS cash_capital_flow (
        flow_id SERIAL PRIMARY KEY,
        flow_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        dr_cr VARCHAR(10) NOT NULL,
        amount DECIMAL(15,2) NOT NULL,
        narration TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ cash_capital_flow table ready');

    // Stock Capital Flow Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS stock_capital_flow (
        flow_id SERIAL PRIMARY KEY,
        flow_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        client_name VARCHAR(200),
        scrip_code VARCHAR(50),
        scrip_name VARCHAR(200),
        isin VARCHAR(20),
        in_out VARCHAR(10) NOT NULL,
        quantity DECIMAL(15,4) NOT NULL,
        rate DECIMAL(10,4),
        value DECIMAL(15,2),
        narration TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ stock_capital_flow table ready');

    // MF Allocations Table
    await client.query(`
      CREATE TABLE IF NOT EXISTS mf_allocations (
        allocation_id SERIAL PRIMARY KEY,
        allocation_date DATE NOT NULL,
        client_name VARCHAR(200) NOT NULL,
        pan VARCHAR(20),
        scheme_name VARCHAR(500),
        isin VARCHAR(20),
        purchase_amount DECIMAL(15,2) NOT NULL,
        folio_number VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    console.log('   ✅ mf_allocations table ready');
  }

  async showFinalStructure(client) {
    console.log('\n📊 Final Simplified Database Structure:');
    
    const result = await client.query(`
      SELECT 
        table_name,
        (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY 
        CASE 
          WHEN table_name IN ('brokers', 'clients', 'distributors', 'strategies', 'securities') THEN 1
          WHEN table_name IN ('contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations') THEN 2
          WHEN table_name LIKE 'unified_custody_master_%' THEN 3
          ELSE 4
        END,
        table_name
    `);
    
    let currentCategory = '';
    result.rows.forEach(row => {
      const table = row.table_name;
      const cols = row.column_count;
      
      // Categorize tables
      let category = '';
      if (['brokers', 'clients', 'distributors', 'strategies', 'securities'].includes(table)) {
        category = '🔧 Master Data Tables';
      } else if (['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'].includes(table)) {
        category = '💰 Transaction Tables';
      } else if (table.startsWith('unified_custody_master_')) {
        category = '🏦 Daily Custody Tables';
      } else {
        category = '📝 System Tables';
      }
      
      if (category !== currentCategory) {
        console.log(`\n${category}:`);
        currentCategory = category;
      }
      
      console.log(`   ✅ ${table.padEnd(35)} (${cols} columns)`);
    });
    
    console.log('\n💡 Benefits of Simplified Structure:');
    console.log('   • No redundant tables');
    console.log('   • Clear single-purpose tables');
    console.log('   • Daily custody partitioning');
    console.log('   • Unified transaction tables (no ENSO prefixes)');
  }
}

// Run the simplification
if (require.main === module) {
  const simplifier = new DatabaseSimplification();
  simplifier.simplifyDatabase().catch(console.error);
}

module.exports = { DatabaseSimplification }; 