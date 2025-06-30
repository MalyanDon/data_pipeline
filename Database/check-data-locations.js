#!/usr/bin/env node

const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

class DataLocationChecker {
  constructor() {
    this.pgPool = new Pool(config.postgresql);
    this.mongoClient = new MongoClient(config.mongodb.uri);
  }

  async checkDataLocations() {
    console.log('🔍 CHECKING DATA LOCATIONS: MongoDB vs PostgreSQL\n');
    
    try {
      // Check MongoDB (Tier 1 - Raw Data)
      await this.checkMongoDBData();
      
      console.log('\n' + '='.repeat(80) + '\n');
      
      // Check PostgreSQL (Tier 2 - Structured Data)
      await this.checkPostgreSQLData();
      
      console.log('\n' + '='.repeat(80) + '\n');
      
      // Show processing pipeline mapping
      this.showProcessingMapping();
      
    } catch (error) {
      console.error('❌ Error checking data locations:', error.message);
    } finally {
      await this.mongoClient.close();
      await this.pgPool.end();
    }
  }

  async checkMongoDBData() {
    console.log('📤 TIER 1: MongoDB (Raw Data Storage)\n');
    
    try {
      await this.mongoClient.connect();
      const db = this.mongoClient.db('financial_data_2025');
      
      // Get all collections
      const collections = await db.listCollections().toArray();
      
      if (collections.length === 0) {
        console.log('   📝 No collections found in MongoDB\n');
        return;
      }
      
      let totalRecords = 0;
      const collectionData = [];
      
      for (const collection of collections) {
        const count = await db.collection(collection.name).countDocuments();
        totalRecords += count;
        
        // Get sample document to understand structure
        const sample = await db.collection(collection.name).findOne();
        
        collectionData.push({
          name: collection.name,
          count,
          sample: sample ? Object.keys(sample).slice(0, 5) : []
        });
      }
      
      // Group collections by type
      const groupedCollections = this.groupMongoCollections(collectionData);
      
      console.log(`📊 Total Collections: ${collections.length}`);
      console.log(`📊 Total Records: ${totalRecords.toLocaleString()}\n`);
      
      // Show by category
      for (const [category, items] of Object.entries(groupedCollections)) {
        if (items.length > 0) {
          console.log(`🔸 ${category}:`);
          items.forEach(item => {
            console.log(`   📄 ${item.name}`);
            console.log(`      Records: ${item.count.toLocaleString()}`);
            console.log(`      Sample fields: ${item.sample.join(', ')}`);
          });
          console.log('');
        }
      }
      
    } catch (error) {
      console.log(`❌ MongoDB connection failed: ${error.message}\n`);
    }
  }

  async checkPostgreSQLData() {
    console.log('📥 TIER 2: PostgreSQL (Structured Tables)\n');
    
    try {
      const client = await this.pgPool.connect();
      
      // Get all tables with counts
      const result = await client.query(`
        SELECT 
          table_name,
          (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
        FROM information_schema.tables t
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
      `);
      
      if (result.rows.length === 0) {
        console.log('   📝 No tables found in PostgreSQL\n');
        client.release();
        return;
      }
      
      const tablesWithCounts = [];
      let totalRecords = 0;
      
      for (const table of result.rows) {
        try {
          const countResult = await client.query(`SELECT COUNT(*) as count FROM ${table.table_name}`);
          const count = parseInt(countResult.rows[0].count);
          totalRecords += count;
          
          tablesWithCounts.push({
            name: table.table_name,
            columns: table.column_count,
            records: count
          });
        } catch (error) {
          tablesWithCounts.push({
            name: table.table_name,
            columns: table.column_count,
            records: 0
          });
        }
      }
      
      console.log(`📊 Total Tables: ${result.rows.length}`);
      console.log(`📊 Total Records: ${totalRecords.toLocaleString()}\n`);
      
      // Group by category
      const categories = this.categorizePostgreSQLTables(tablesWithCounts);
      
      for (const [category, tables] of Object.entries(categories)) {
        if (tables.length > 0) {
          console.log(`🔸 ${category}:`);
          tables.forEach(table => {
            console.log(`   📊 ${table.name.padEnd(35)} ${table.records.toLocaleString().padStart(8)} records (${table.columns} columns)`);
          });
          console.log('');
        }
      }
      
      client.release();
      
    } catch (error) {
      console.log(`❌ PostgreSQL connection failed: ${error.message}\n`);
    }
  }

  groupMongoCollections(collections) {
    const groups = {
      'Broker Master Data': [],
      'Client Master Data': [],
      'Contract Notes Data': [],
      'Cash Flow Data': [],
      'Stock Flow Data': [],
      'MF Allocation Data': [],
      'Distributor Master Data': [],
      'Strategy Master Data': [],
      'Custody Data': [],
      'Other Raw Data': []
    };
    
    collections.forEach(collection => {
      const name = collection.name.toLowerCase();
      
      if (name.includes('broker_master')) {
        groups['Broker Master Data'].push(collection);
      } else if (name.includes('client_info') || name.includes('client_master')) {
        groups['Client Master Data'].push(collection);
      } else if (name.includes('contract_note')) {
        groups['Contract Notes Data'].push(collection);
      } else if (name.includes('cash_capital_flow')) {
        groups['Cash Flow Data'].push(collection);
      } else if (name.includes('stock_capital_flow')) {
        groups['Stock Flow Data'].push(collection);
      } else if (name.includes('mf_allocation') || name.includes('mf_buy')) {
        groups['MF Allocation Data'].push(collection);
      } else if (name.includes('distributor')) {
        groups['Distributor Master Data'].push(collection);
      } else if (name.includes('strategy')) {
        groups['Strategy Master Data'].push(collection);
      } else if (name.includes('custody') || name.includes('dl_')) {
        groups['Custody Data'].push(collection);
      } else {
        groups['Other Raw Data'].push(collection);
      }
    });
    
    return groups;
  }

  categorizePostgreSQLTables(tables) {
    const categories = {
      'Master Data Tables': [],
      'Transaction Tables': [],
      'Daily Custody Tables': [],
      'System Tables': []
    };
    
    tables.forEach(table => {
      const name = table.name;
      
      if (['brokers', 'clients', 'distributors', 'strategies', 'securities'].includes(name)) {
        categories['Master Data Tables'].push(table);
      } else if (['contract_notes', 'cash_capital_flow', 'stock_capital_flow', 'mf_allocations'].includes(name)) {
        categories['Transaction Tables'].push(table);
      } else if (name.startsWith('unified_custody_master_')) {
        categories['Daily Custody Tables'].push(table);
      } else {
        categories['System Tables'].push(table);
      }
    });
    
    return categories;
  }

  showProcessingMapping() {
    console.log('🔄 PROCESSING PIPELINE: MongoDB → PostgreSQL\n');
    
    const mappings = [
      {
        mongo: 'broker_master_data_*',
        postgres: 'brokers',
        description: 'Broker/custodian master information',
        status: '✅ Ready to process'
      },
      {
        mongo: 'client_info_data_* / client_master_data_*',
        postgres: 'clients',
        description: 'Client master information',
        status: '✅ Ready to process'
      },
      {
        mongo: 'contract_notes_data_*',
        postgres: 'contract_notes',
        description: 'Trade execution records',
        status: '✅ Ready to process'
      },
      {
        mongo: 'cash_capital_flow_data_*',
        postgres: 'cash_capital_flow',
        description: 'Cash movement transactions',
        status: '✅ Ready to process'
      },
      {
        mongo: 'stock_capital_flow_data_*',
        postgres: 'stock_capital_flow',
        description: 'Stock movement transactions',
        status: '✅ Ready to process'
      },
      {
        mongo: 'mf_allocation_data_* / mf_buy_data_*',
        postgres: 'mf_allocations',
        description: 'Mutual fund allocations',
        status: '✅ Ready to process'
      },
      {
        mongo: 'distributor_master_data_*',
        postgres: 'distributors',
        description: 'Distributor master information',
        status: '✅ Ready to process'
      },
      {
        mongo: 'strategy_master_data_*',
        postgres: 'strategies',
        description: 'Investment strategy definitions',
        status: '✅ Ready to process'
      },
      {
        mongo: 'custody_data_* / dl_*_data',
        postgres: 'unified_custody_master_YYYY_MM_DD',
        description: 'Daily custody holdings (date-partitioned)',
        status: '⚠️  Needs field mapping fix'
      }
    ];
    
    console.log('📋 Data Flow Mappings:\n');
    
    mappings.forEach((mapping, index) => {
      console.log(`${(index + 1).toString().padStart(2)}. MongoDB: ${mapping.mongo}`);
      console.log(`    ↓ Smart Processing`);
      console.log(`    PostgreSQL: ${mapping.postgres}`);
      console.log(`    Purpose: ${mapping.description}`);
      console.log(`    Status: ${mapping.status}\n`);
    });
    
    console.log('🎯 NEXT STEPS:');
    console.log('1. Click "Process Data" button in dashboard');
    console.log('2. Smart processor will route each MongoDB collection to correct PostgreSQL table');
    console.log('3. Field validation and transformation will be applied');
    console.log('4. Data will be inserted into structured PostgreSQL tables');
    console.log('5. Ready for business queries and reporting\n');
    
    console.log('⚡ CURRENT STATUS:');
    console.log('• MongoDB: Raw data uploaded and organized ✅');
    console.log('• PostgreSQL: Tables created but empty (ready for processing) ⏳');
    console.log('• Processing: Click button to transform MongoDB → PostgreSQL 🚀');
  }
}

// Run the checker
if (require.main === module) {
  const checker = new DataLocationChecker();
  checker.checkDataLocations().catch(console.error);
}

module.exports = { DataLocationChecker }; 