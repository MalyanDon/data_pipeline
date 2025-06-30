#!/usr/bin/env node

const { Pool } = require('pg');

/**
 * ENSO TIER 2 TRANSACTION TABLES 
 * Creates 4 transaction tables with unique names for ENSO financial data
 */
class ENSOTransactionTables {
  constructor(config) {
    this.pool = new Pool(config.postgresql);
  }

  async createAllTables() {
    console.log('🚀 Creating ENSO Tier 2 Transaction Tables');
    console.log('═══════════════════════════════════════════════════════════');
    
    const client = await this.pool.connect();
    
    try {
      // Create tables one by one without transactions to avoid rollbacks
      await this.createENSOContractNotesTable(client);
      await this.createENSOCashFlowTable(client);
      await this.createENSOStockFlowTable(client);
      await this.createENSOMFAllocationsTable(client);
      
      // Create indexes
      await this.createAllIndexes(client);
      
      console.log('\n✅ All ENSO transaction tables created successfully!');
      
    } catch (error) {
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * 1. ENSO CONTRACT NOTES TABLE
   */
  async createENSOContractNotesTable(client) {
    console.log('📋 Creating enso_contract_notes table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS enso_contract_notes (
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

    console.log('   ✅ enso_contract_notes table created');
  }

  /**
   * 2. ENSO CASH CAPITAL FLOW TABLE
   */
  async createENSOCashFlowTable(client) {
    console.log('💰 Creating enso_cash_capital_flow table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS enso_cash_capital_flow (
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

    console.log('   ✅ enso_cash_capital_flow table created');
  }

  /**
   * 3. ENSO STOCK CAPITAL FLOW TABLE
   */
  async createENSOStockFlowTable(client) {
    console.log('📈 Creating enso_stock_capital_flow table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS enso_stock_capital_flow (
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

    console.log('   ✅ enso_stock_capital_flow table created');
  }

  /**
   * 4. ENSO MF ALLOCATIONS TABLE
   */
  async createENSOMFAllocationsTable(client) {
    console.log('🏦 Creating enso_mf_allocations table...');

    await client.query(`
      CREATE TABLE IF NOT EXISTS enso_mf_allocations (
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

    console.log('   ✅ enso_mf_allocations table created');
  }

  /**
   * CREATE PERFORMANCE INDEXES
   */
  async createAllIndexes(client) {
    console.log('🔍 Creating performance indexes...');

    const indexes = [
      // Contract Notes Indexes
      'CREATE INDEX IF NOT EXISTS idx_enso_contract_client_code ON enso_contract_notes(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_contract_ecn_date ON enso_contract_notes(ecn_date)',
      'CREATE INDEX IF NOT EXISTS idx_enso_contract_instrument_isin ON enso_contract_notes(instrument_isin)',
      'CREATE INDEX IF NOT EXISTS idx_enso_contract_broker_name ON enso_contract_notes(broker_name)',
      
      // Cash Capital Flow Indexes  
      'CREATE INDEX IF NOT EXISTS idx_enso_cash_client_code ON enso_cash_capital_flow(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_cash_broker_code ON enso_cash_capital_flow(broker_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_cash_acquisition_date ON enso_cash_capital_flow(acquisition_date)',
      'CREATE INDEX IF NOT EXISTS idx_enso_cash_transaction_type ON enso_cash_capital_flow(transaction_type)',
      
      // Stock Capital Flow Indexes
      'CREATE INDEX IF NOT EXISTS idx_enso_stock_client_code ON enso_stock_capital_flow(client_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_stock_broker_code ON enso_stock_capital_flow(broker_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_stock_instrument_isin ON enso_stock_capital_flow(instrument_isin)',
      'CREATE INDEX IF NOT EXISTS idx_enso_stock_acquisition_date ON enso_stock_capital_flow(acquisition_date)',
      'CREATE INDEX IF NOT EXISTS idx_enso_stock_transaction_type ON enso_stock_capital_flow(transaction_type)',
      
      // MF Allocations Indexes
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_client_name ON enso_mf_allocations(client_name)',
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_custody_code ON enso_mf_allocations(custody_code)',
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_allocation_date ON enso_mf_allocations(allocation_date)',
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_instrument_isin ON enso_mf_allocations(instrument_isin)',
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_folio_number ON enso_mf_allocations(folio_number)',
      'CREATE INDEX IF NOT EXISTS idx_enso_mf_pan ON enso_mf_allocations(pan)'
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
      // Get table counts
      const contractNotesCount = await client.query('SELECT COUNT(*) FROM enso_contract_notes');
      const cashFlowCount = await client.query('SELECT COUNT(*) FROM enso_cash_capital_flow');
      const stockFlowCount = await client.query('SELECT COUNT(*) FROM enso_stock_capital_flow');
      const mfAllocationsCount = await client.query('SELECT COUNT(*) FROM enso_mf_allocations');

      console.log('\n📊 ENSO Transaction Tables Status:');
      console.log(`   enso_contract_notes: ${contractNotesCount.rows[0].count} records`);
      console.log(`   enso_cash_capital_flow: ${cashFlowCount.rows[0].count} records`);
      console.log(`   enso_stock_capital_flow: ${stockFlowCount.rows[0].count} records`);
      console.log(`   enso_mf_allocations: ${mfAllocationsCount.rows[0].count} records`);

      return {
        contractNotes: parseInt(contractNotesCount.rows[0].count),
        cashFlow: parseInt(cashFlowCount.rows[0].count),
        stockFlow: parseInt(stockFlowCount.rows[0].count),
        mfAllocations: parseInt(mfAllocationsCount.rows[0].count)
      };
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}

module.exports = { ENSOTransactionTables };

// CLI execution
if (require.main === module) {
  const config = require('./config');
  
  (async () => {
    const ensoTables = new ENSOTransactionTables(config);
    
    try {
      await ensoTables.createAllTables();
      await ensoTables.checkTableStatus();
      
      console.log('\n🎉 ENSO Tier 2 Transaction Database Setup Complete!');
      console.log('\n📋 Created Tables:');
      console.log('   1. enso_contract_notes - Contract note transactions');
      console.log('   2. enso_cash_capital_flow - Cash movement transactions'); 
      console.log('   3. enso_stock_capital_flow - Stock movement transactions');
      console.log('   4. enso_mf_allocations - Mutual fund allocations');
      console.log('\n🔗 Ready to integrate with existing unified_custody_master table');
      
    } catch (error) {
      console.error('💥 ENSO table creation failed:', error.message);
    } finally {
      await ensoTables.close();
    }
  })();
} 