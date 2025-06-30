#!/usr/bin/env node

const { Pool } = require('pg');

/**
 * TIER 2 TRANSACTION TABLES SCHEMA - FIXED VERSION
 * Creates 4 transaction tables for ENSO financial data
 */
class Tier2TransactionSchema {
  constructor(config) {
    this.pool = new Pool(config.postgresql);
  }

  async initializeAllTables() {
    console.log('🚀 Creating Tier 2 Transaction Tables for ENSO Financial Data');
    console.log('═══════════════════════════════════════════════════════════');
    
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Create all 4 transaction tables
      await this.createContractNotesTable(client);
      await this.createCashCapitalFlowTable(client);
      await this.createStockCapitalFlowTable(client);
      await this.createMFAllocationsTable(client);
      
      // Create basic indexes
      await this.createBasicIndexes(client);
      
      await client.query('COMMIT');
      
      console.log('\n✅ All Tier 2 transaction tables created successfully!');
      console.log('\n📊 Tables Created:');
      console.log('   1. contract_notes - Contract note transactions');
      console.log('   2. cash_capital_flow - Cash movement transactions'); 
      console.log('   3. stock_capital_flow - Stock movement transactions');
      console.log('   4. mf_allocations - Mutual fund allocations');
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * 1. CONTRACT NOTES TABLE
   */
  async createContractNotesTable(client) {
    console.log('📋 Creating contract_notes table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS contract_notes (
        contract_id SERIAL PRIMARY KEY,
        ecn_number VARCHAR(50) UNIQUE NOT NULL,
        ecn_status VARCHAR(50),
        ecn_date DATE NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        broker_name VARCHAR(200),
        instrument_isin VARCHAR(20),
        instrument_name VARCHAR(300),
        transaction_type VARCHAR(10) NOT NULL,
        delivery_type VARCHAR(50),
        exchange VARCHAR(10),
        settlement_date DATE,
        market_type VARCHAR(20),
        settlement_number VARCHAR(50),
        quantity DECIMAL(15,4) NOT NULL DEFAULT 0,
        net_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
        net_rate DECIMAL(15,4) DEFAULT 0,
        brokerage_amount DECIMAL(15,2) DEFAULT 0,
        brokerage_rate DECIMAL(8,4) DEFAULT 0,
        service_tax DECIMAL(15,2) DEFAULT 0,
        stamp_duty DECIMAL(15,2) DEFAULT 0,
        stt_amount DECIMAL(15,2) DEFAULT 0,
        sebi_registration VARCHAR(50),
        scheme_name VARCHAR(300),
        custodian_name VARCHAR(200),
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log('   ✅ contract_notes table created');
  }

  /**
   * 2. CASH CAPITAL FLOW TABLE
   */
  async createCashCapitalFlowTable(client) {
    console.log('💰 Creating cash_capital_flow table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS cash_capital_flow (
        cash_flow_id SERIAL PRIMARY KEY,
        broker_code VARCHAR(50) NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        transaction_type VARCHAR(10) NOT NULL,
        acquisition_date DATE NOT NULL,
        settlement_date DATE,
        amount DECIMAL(15,2) NOT NULL DEFAULT 0,
        price DECIMAL(15,4) DEFAULT 0,
        brokerage DECIMAL(15,2) DEFAULT 0,
        service_tax DECIMAL(15,2) DEFAULT 0,
        settlement_date_flag VARCHAR(20),
        market_rate DECIMAL(15,4) DEFAULT 0,
        cash_symbol VARCHAR(20),
        stt_amount DECIMAL(15,2) DEFAULT 0,
        accrued_interest DECIMAL(15,2) DEFAULT 0,
        block_ref VARCHAR(100),
        transaction_ref VARCHAR(100),
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log('   ✅ cash_capital_flow table created');
  }

  /**
   * 3. STOCK CAPITAL FLOW TABLE
   */
  async createStockCapitalFlowTable(client) {
    console.log('📈 Creating stock_capital_flow table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS stock_capital_flow (
        stock_flow_id SERIAL PRIMARY KEY,
        broker_code VARCHAR(50) NOT NULL,
        client_code VARCHAR(50) NOT NULL,
        instrument_isin VARCHAR(20),
        exchange VARCHAR(10),
        transaction_type VARCHAR(10) NOT NULL,
        acquisition_date DATE NOT NULL,
        security_in_date DATE,
        quantity DECIMAL(15,4) NOT NULL DEFAULT 0,
        original_price DECIMAL(15,4) DEFAULT 0,
        brokerage DECIMAL(15,2) DEFAULT 0,
        service_tax DECIMAL(15,2) DEFAULT 0,
        settlement_date_flag VARCHAR(20),
        market_rate DECIMAL(15,4) DEFAULT 0,
        cash_symbol VARCHAR(20),
        stt_amount DECIMAL(15,2) DEFAULT 0,
        accrued_interest DECIMAL(15,2) DEFAULT 0,
        block_ref VARCHAR(100),
        transaction_ref VARCHAR(100),
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log('   ✅ stock_capital_flow table created');
  }

  /**
   * 4. MF ALLOCATIONS TABLE
   */
  async createMFAllocationsTable(client) {
    console.log('🏦 Creating mf_allocations table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS mf_allocations (
        allocation_id SERIAL PRIMARY KEY,
        allocation_date DATE NOT NULL,
        client_name VARCHAR(200) NOT NULL,
        custody_code VARCHAR(50),
        pan VARCHAR(20),
        debit_account_number VARCHAR(50),
        folio_number VARCHAR(50),
        amc_name VARCHAR(200),
        scheme_name VARCHAR(500),
        instrument_isin VARCHAR(20),
        purchase_amount DECIMAL(15,2) NOT NULL DEFAULT 0,
        beneficiary_account_name VARCHAR(200),
        beneficiary_account_number VARCHAR(50),
        beneficiary_bank_name VARCHAR(200),
        ifsc_code VARCHAR(20),
        euin VARCHAR(50),
        arn_code VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    console.log('   ✅ mf_allocations table created');
  }

  /**
   * CREATE BASIC PERFORMANCE INDEXES
   */
  async createBasicIndexes(client) {
    console.log('🔍 Creating performance indexes...');

    const indexes = [
      // Contract Notes Indexes
      'CREATE INDEX IF NOT EXISTS idx_contract_client_code ON contract_notes(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_contract_ecn_date ON contract_notes(ecn_date)',
      'CREATE INDEX IF NOT EXISTS idx_contract_instrument_isin ON contract_notes(instrument_isin)',
      
      // Cash Capital Flow Indexes  
      'CREATE INDEX IF NOT EXISTS idx_cash_client_code ON cash_capital_flow(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_cash_acquisition_date ON cash_capital_flow(acquisition_date)',
      
      // Stock Capital Flow Indexes
      'CREATE INDEX IF NOT EXISTS idx_stock_client_code ON stock_capital_flow(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_stock_instrument_isin ON stock_capital_flow(instrument_isin)',
      'CREATE INDEX IF NOT EXISTS idx_stock_acquisition_date ON stock_capital_flow(acquisition_date)',
      
      // MF Allocations Indexes
      'CREATE INDEX IF NOT EXISTS idx_mf_client_name ON mf_allocations(client_name)',
      'CREATE INDEX IF NOT EXISTS idx_mf_allocation_date ON mf_allocations(allocation_date)',
      'CREATE INDEX IF NOT EXISTS idx_mf_instrument_isin ON mf_allocations(instrument_isin)'
    ];

    for (const indexQuery of indexes) {
      await client.query(indexQuery);
    }

    console.log('   ✅ All performance indexes created');
  }

  /**
   * CHECK TABLE STATUS
   */
  async checkTableStatus() {
    const client = await this.pool.connect();
    
    try {
      const result = await client.query(`
        SELECT 
          table_name,
          (SELECT COUNT(*) FROM contract_notes) as contract_notes_count,
          (SELECT COUNT(*) FROM cash_capital_flow) as cash_flow_count,
          (SELECT COUNT(*) FROM stock_capital_flow) as stock_flow_count,
          (SELECT COUNT(*) FROM mf_allocations) as mf_allocations_count
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN ('contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations')
        LIMIT 1
      `);

      if (result.rows.length > 0) {
        const stats = result.rows[0];
        console.log('\n📊 Table Status:');
        console.log(`   contract_notes: ${stats.contract_notes_count} records`);
        console.log(`   cash_capital_flow: ${stats.cash_flow_count} records`);
        console.log(`   stock_capital_flow: ${stats.stock_flow_count} records`);
        console.log(`   mf_allocations: ${stats.mf_allocations_count} records`);
      }

      return result.rows[0];
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}

module.exports = { Tier2TransactionSchema };

// CLI execution
if (require.main === module) {
  const config = require('./config');
  
  (async () => {
    const schema = new Tier2TransactionSchema(config);
    
    try {
      await schema.initializeAllTables();
      await schema.checkTableStatus();
      
      console.log('\n🎉 Tier 2 Transaction Database Setup Complete!');
      console.log('📋 Ready for transaction data processing');
      
    } catch (error) {
      console.error('💥 Schema creation failed:', error.message);
    } finally {
      await schema.close();
    }
  })();
} 