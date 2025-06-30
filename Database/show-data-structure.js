#!/usr/bin/env node

const { Pool } = require('pg');
const config = require('./config');

class DataStructureAnalyzer {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
  }

  async showDataStructure() {
    console.log('📊 SIMPLIFIED DATA STRUCTURE OVERVIEW\n');
    
    const client = await this.pgPool.connect();
    
    try {
      // Get all tables with record counts
      const tables = await this.getAllTables(client);
      
      // Group tables by category
      const categorizedTables = this.categorizeTables(tables);
      
      // Show structure by category
      await this.showMasterDataTables(client, categorizedTables.masterData);
      await this.showTransactionTables(client, categorizedTables.transactions);
      await this.showCustodyTables(client, categorizedTables.custody);
      await this.showSystemTables(client, categorizedTables.system);
      
      // Show sample data for key tables
      await this.showSampleData(client);
      
      // Show data flow summary
      this.showDataFlowSummary();
      
    } catch (error) {
      console.error('❌ Error analyzing data structure:', error.message);
    } finally {
      client.release();
      await this.pgPool.end();
    }
  }

  async getAllTables(client) {
    const result = await client.query(`
      SELECT 
        table_name,
        (SELECT COUNT(*) 
         FROM information_schema.columns 
         WHERE table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    
    // Get record counts
    const tablesWithCounts = [];
    for (const table of result.rows) {
      try {
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${table.table_name}`);
        tablesWithCounts.push({
          name: table.table_name,
          columns: table.column_count,
          records: parseInt(countResult.rows[0].count)
        });
      } catch (error) {
        tablesWithCounts.push({
          name: table.table_name,
          columns: table.column_count,
          records: 0
        });
      }
    }
    
    return tablesWithCounts;
  }

  categorizeTables(tables) {
    const categories = {
      masterData: [],
      transactions: [],
      custody: [],
      system: []
    };
    
    tables.forEach(table => {
      const name = table.name;
      
      if (['brokers', 'clients', 'distributors', 'strategies', 'securities'].includes(name)) {
        categories.masterData.push(table);
      } else if (['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'].includes(name)) {
        categories.transactions.push(table);
      } else if (name.startsWith('unified_custody_master_')) {
        categories.custody.push(table);
      } else {
        categories.system.push(table);
      }
    });
    
    return categories;
  }

  async showMasterDataTables(client, tables) {
    console.log('🔧 MASTER DATA TABLES (5 tables)\n');
    
    for (const table of tables) {
      console.log(`📋 ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Columns: ${table.columns}`);
      
      // Show column details
      const columns = await this.getColumnDetails(client, table.name);
      columns.forEach(col => {
        const key = col.constraint_type ? ` [${col.constraint_type}]` : '';
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}${key}`);
      });
      console.log('');
    }
  }

  async showTransactionTables(client, tables) {
    console.log('💰 TRANSACTION TABLES (4 tables)\n');
    
    for (const table of tables) {
      console.log(`📈 ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Columns: ${table.columns}`);
      
      // Show key columns only for transaction tables
      const keyColumns = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND column_name IN ('client_code', 'client_name', 'amount', 'quantity', 'date', 'trade_date', 'flow_date', 'allocation_date', 'isin', 'ecn_number')
        ORDER BY ordinal_position
      `, [table.name]);
      
      keyColumns.rows.forEach(col => {
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}`);
      });
      console.log('');
    }
  }

  async showCustodyTables(client, tables) {
    console.log('🏦 DAILY CUSTODY TABLES (Date-Partitioned)\n');
    
    if (tables.length === 0) {
      console.log('   📝 No daily custody tables found\n');
      return;
    }
    
    let totalCustodyRecords = 0;
    
    for (const table of tables) {
      const dateMatch = table.name.match(/unified_custody_master_(\d{4}_\d{2}_\d{2})/);
      const date = dateMatch ? dateMatch[1].replace(/_/g, '-') : 'Unknown';
      
      console.log(`📅 ${date}: ${table.records.toLocaleString()} holdings`);
      totalCustodyRecords += table.records;
    }
    
    console.log(`\n   Total Custody Records: ${totalCustodyRecords.toLocaleString()}`);
    
    // Show sample custody table structure
    if (tables.length > 0) {
      console.log('\n   📋 Custody Table Structure:');
      const sampleTable = tables[0].name;
      const columns = await this.getColumnDetails(client, sampleTable);
      columns.forEach(col => {
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}`);
      });
    }
    console.log('');
  }

  async showSystemTables(client, tables) {
    console.log('📝 SYSTEM TABLES\n');
    
    for (const table of tables) {
      console.log(`🗂️  ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Purpose: ${this.getTablePurpose(table.name)}`);
      console.log('');
    }
  }

  async getColumnDetails(client, tableName) {
    const result = await client.query(`
      SELECT 
        c.column_name,
        c.data_type,
        tc.constraint_type
      FROM information_schema.columns c
      LEFT JOIN information_schema.key_column_usage kcu 
        ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
      LEFT JOIN information_schema.table_constraints tc 
        ON kcu.constraint_name = tc.constraint_name
      WHERE c.table_name = $1
      ORDER BY c.ordinal_position
    `, [tableName]);
    
    return result.rows;
  }

  getTablePurpose(tableName) {
    const purposes = {
      'raw_uploads': 'Audit trail of file uploads',
      'processed_files': 'Processing history and status',
      'enso_mf_allocations': 'Legacy table (to be cleaned up)'
    };
    
    return purposes[tableName] || 'System support table';
  }

  async showSampleData(client) {
    console.log('📊 SAMPLE DATA OVERVIEW\n');
    
    // Show sample from each main category
    const sampleQueries = [
      {
        title: 'Master Data Sample (Brokers)',
        query: 'SELECT broker_name, broker_code FROM brokers LIMIT 3',
        table: 'brokers'
      },
      {
        title: 'Transaction Sample (Contract Notes)',
        query: 'SELECT ecn_number, client_code, quantity, rate FROM contract_notes LIMIT 3',
        table: 'contract_notes'
      }
    ];
    
    for (const sample of sampleQueries) {
      try {
        const result = await client.query(sample.query);
        if (result.rows.length > 0) {
          console.log(`📋 ${sample.title}:`);
          console.table(result.rows);
        }
      } catch (error) {
        console.log(`📋 ${sample.title}: No data available`);
      }
    }
  }

  showDataFlowSummary() {
    console.log('🔄 DATA FLOW SUMMARY\n');
    
    console.log('📤 TIER 1 (MongoDB) - Raw Data Storage:');
    console.log('   • broker_master_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • contract_notes_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • cash_capital_flow_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • client_info_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • custody_data_YYYY_MM_DD_HH_mm_ss');
    console.log('');
    
    console.log('📥 TIER 2 (PostgreSQL) - Structured Tables:');
    console.log('   Master Data → brokers, clients, distributors, strategies');
    console.log('   Transactions → contract_notes, cash_capital_flow, stock_capital_flow, mf_allocations');
    console.log('   Custody → unified_custody_master_YYYY_MM_DD (daily partitions)');
    console.log('   System → raw_uploads, processed_files');
    console.log('');
    
    console.log('✅ BENEFITS OF SIMPLIFIED STRUCTURE:');
    console.log('   • ✅ No redundant tables');
    console.log('   • ✅ Single-purpose tables');
    console.log('   • ✅ Natural date partitioning for custody data');
    console.log('   • ✅ Clean transaction tables (no ENSO prefixes)');
    console.log('   • ✅ Fast queries with proper indexing');
    console.log('   • ✅ Easy to understand and maintain');
  }
}

// Run the analyzer
if (require.main === module) {
  const analyzer = new DataStructureAnalyzer();
  analyzer.showDataStructure().catch(console.error);
}

module.exports = { DataStructureAnalyzer }; 

const { Pool } = require('pg');
const config = require('./config');

class DataStructureAnalyzer {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
  }

  async showDataStructure() {
    console.log('📊 SIMPLIFIED DATA STRUCTURE OVERVIEW\n');
    
    const client = await this.pgPool.connect();
    
    try {
      // Get all tables with record counts
      const tables = await this.getAllTables(client);
      
      // Group tables by category
      const categorizedTables = this.categorizeTables(tables);
      
      // Show structure by category
      await this.showMasterDataTables(client, categorizedTables.masterData);
      await this.showTransactionTables(client, categorizedTables.transactions);
      await this.showCustodyTables(client, categorizedTables.custody);
      await this.showSystemTables(client, categorizedTables.system);
      
      // Show sample data for key tables
      await this.showSampleData(client);
      
      // Show data flow summary
      this.showDataFlowSummary();
      
    } catch (error) {
      console.error('❌ Error analyzing data structure:', error.message);
    } finally {
      client.release();
      await this.pgPool.end();
    }
  }

  async getAllTables(client) {
    const result = await client.query(`
      SELECT 
        table_name,
        (SELECT COUNT(*) 
         FROM information_schema.columns 
         WHERE table_name = t.table_name) as column_count
      FROM information_schema.tables t
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name
    `);
    
    // Get record counts
    const tablesWithCounts = [];
    for (const table of result.rows) {
      try {
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${table.table_name}`);
        tablesWithCounts.push({
          name: table.table_name,
          columns: table.column_count,
          records: parseInt(countResult.rows[0].count)
        });
      } catch (error) {
        tablesWithCounts.push({
          name: table.table_name,
          columns: table.column_count,
          records: 0
        });
      }
    }
    
    return tablesWithCounts;
  }

  categorizeTables(tables) {
    const categories = {
      masterData: [],
      transactions: [],
      custody: [],
      system: []
    };
    
    tables.forEach(table => {
      const name = table.name;
      
      if (['brokers', 'clients', 'distributors', 'strategies', 'securities'].includes(name)) {
        categories.masterData.push(table);
      } else if (['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'].includes(name)) {
        categories.transactions.push(table);
      } else if (name.startsWith('unified_custody_master_')) {
        categories.custody.push(table);
      } else {
        categories.system.push(table);
      }
    });
    
    return categories;
  }

  async showMasterDataTables(client, tables) {
    console.log('🔧 MASTER DATA TABLES (5 tables)\n');
    
    for (const table of tables) {
      console.log(`📋 ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Columns: ${table.columns}`);
      
      // Show column details
      const columns = await this.getColumnDetails(client, table.name);
      columns.forEach(col => {
        const key = col.constraint_type ? ` [${col.constraint_type}]` : '';
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}${key}`);
      });
      console.log('');
    }
  }

  async showTransactionTables(client, tables) {
    console.log('💰 TRANSACTION TABLES (4 tables)\n');
    
    for (const table of tables) {
      console.log(`📈 ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Columns: ${table.columns}`);
      
      // Show key columns only for transaction tables
      const keyColumns = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND column_name IN ('client_code', 'client_name', 'amount', 'quantity', 'date', 'trade_date', 'flow_date', 'allocation_date', 'isin', 'ecn_number')
        ORDER BY ordinal_position
      `, [table.name]);
      
      keyColumns.rows.forEach(col => {
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}`);
      });
      console.log('');
    }
  }

  async showCustodyTables(client, tables) {
    console.log('🏦 DAILY CUSTODY TABLES (Date-Partitioned)\n');
    
    if (tables.length === 0) {
      console.log('   📝 No daily custody tables found\n');
      return;
    }
    
    let totalCustodyRecords = 0;
    
    for (const table of tables) {
      const dateMatch = table.name.match(/unified_custody_master_(\d{4}_\d{2}_\d{2})/);
      const date = dateMatch ? dateMatch[1].replace(/_/g, '-') : 'Unknown';
      
      console.log(`📅 ${date}: ${table.records.toLocaleString()} holdings`);
      totalCustodyRecords += table.records;
    }
    
    console.log(`\n   Total Custody Records: ${totalCustodyRecords.toLocaleString()}`);
    
    // Show sample custody table structure
    if (tables.length > 0) {
      console.log('\n   📋 Custody Table Structure:');
      const sampleTable = tables[0].name;
      const columns = await this.getColumnDetails(client, sampleTable);
      columns.forEach(col => {
        console.log(`   • ${col.column_name.padEnd(25)} ${col.data_type}`);
      });
    }
    console.log('');
  }

  async showSystemTables(client, tables) {
    console.log('📝 SYSTEM TABLES\n');
    
    for (const table of tables) {
      console.log(`🗂️  ${table.name.toUpperCase()}`);
      console.log(`   Records: ${table.records.toLocaleString()}`);
      console.log(`   Purpose: ${this.getTablePurpose(table.name)}`);
      console.log('');
    }
  }

  async getColumnDetails(client, tableName) {
    const result = await client.query(`
      SELECT 
        c.column_name,
        c.data_type,
        tc.constraint_type
      FROM information_schema.columns c
      LEFT JOIN information_schema.key_column_usage kcu 
        ON c.table_name = kcu.table_name AND c.column_name = kcu.column_name
      LEFT JOIN information_schema.table_constraints tc 
        ON kcu.constraint_name = tc.constraint_name
      WHERE c.table_name = $1
      ORDER BY c.ordinal_position
    `, [tableName]);
    
    return result.rows;
  }

  getTablePurpose(tableName) {
    const purposes = {
      'raw_uploads': 'Audit trail of file uploads',
      'processed_files': 'Processing history and status',
      'enso_mf_allocations': 'Legacy table (to be cleaned up)'
    };
    
    return purposes[tableName] || 'System support table';
  }

  async showSampleData(client) {
    console.log('📊 SAMPLE DATA OVERVIEW\n');
    
    // Show sample from each main category
    const sampleQueries = [
      {
        title: 'Master Data Sample (Brokers)',
        query: 'SELECT broker_name, broker_code FROM brokers LIMIT 3',
        table: 'brokers'
      },
      {
        title: 'Transaction Sample (Contract Notes)',
        query: 'SELECT ecn_number, client_code, quantity, rate FROM contract_notes LIMIT 3',
        table: 'contract_notes'
      }
    ];
    
    for (const sample of sampleQueries) {
      try {
        const result = await client.query(sample.query);
        if (result.rows.length > 0) {
          console.log(`📋 ${sample.title}:`);
          console.table(result.rows);
        }
      } catch (error) {
        console.log(`📋 ${sample.title}: No data available`);
      }
    }
  }

  showDataFlowSummary() {
    console.log('🔄 DATA FLOW SUMMARY\n');
    
    console.log('📤 TIER 1 (MongoDB) - Raw Data Storage:');
    console.log('   • broker_master_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • contract_notes_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • cash_capital_flow_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • client_info_data_YYYY_MM_DD_HH_mm_ss');
    console.log('   • custody_data_YYYY_MM_DD_HH_mm_ss');
    console.log('');
    
    console.log('📥 TIER 2 (PostgreSQL) - Structured Tables:');
    console.log('   Master Data → brokers, clients, distributors, strategies');
    console.log('   Transactions → contract_notes, cash_capital_flow, stock_capital_flow, mf_allocations');
    console.log('   Custody → unified_custody_master_YYYY_MM_DD (daily partitions)');
    console.log('   System → raw_uploads, processed_files');
    console.log('');
    
    console.log('✅ BENEFITS OF SIMPLIFIED STRUCTURE:');
    console.log('   • ✅ No redundant tables');
    console.log('   • ✅ Single-purpose tables');
    console.log('   • ✅ Natural date partitioning for custody data');
    console.log('   • ✅ Clean transaction tables (no ENSO prefixes)');
    console.log('   • ✅ Fast queries with proper indexing');
    console.log('   • ✅ Easy to understand and maintain');
  }
}

// Run the analyzer
if (require.main === module) {
  const analyzer = new DataStructureAnalyzer();
  analyzer.showDataStructure().catch(console.error);
}

module.exports = { DataStructureAnalyzer }; 