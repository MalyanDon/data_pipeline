#!/usr/bin/env node

const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');

class DailyTableMigrator {
  constructor() {
    this.schema = new NormalizationSchema();
    this.loader = new PostgresLoader();
  }

  async migrate() {
    try {
      console.log('🚀 Daily Table Migration & Test');
      console.log('==============================\n');

      // Step 1: Initialize PostgreSQL schema
      console.log('1️⃣  Initializing PostgreSQL schema...');
      await this.schema.initializeDatabase();
      console.log('   ✅ Schema initialized\n');

      // Step 2: Migrate existing data to daily tables
      console.log('2️⃣  Migrating existing data to daily tables...');
      const migrationResult = await this.schema.migrateToDateBasedTables();
      
      if (migrationResult.migrated > 0) {
        console.log(`   🎉 Migration completed successfully!`);
        console.log(`   📊 Records migrated: ${migrationResult.migrated.toLocaleString()}`);
        console.log(`   🗓️  Daily tables created: ${migrationResult.dailyTables}`);
      } else {
        console.log('   📝 No existing data to migrate - starting fresh');
      }
      console.log();

      // Step 3: Show daily table structure
      console.log('3️⃣  Daily table structure:');
      const allTables = await this.schema.getAllDailyTables();
      
      if (allTables.length > 0) {
        console.log(`   📋 Found ${allTables.length} daily tables:`);
        for (const tableName of allTables) {
          const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
          if (match) {
            const [, year, month, day] = match;
            const date = `${year}-${month}-${day}`;
            console.log(`      📅 ${date} → ${tableName}`);
          }
        }
      } else {
        console.log('   📝 No daily tables found');
      }
      console.log();

      // Step 4: Show statistics for each daily table
      console.log('4️⃣  Daily table statistics:');
      if (allTables.length > 0) {
        for (const tableName of allTables) {
          const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
          if (match) {
            const [, year, month, day] = match;
            const date = `${year}-${month}-${day}`;
            const stats = await this.loader.getDailyStats(date);
            
            console.log(`\n   📊 ${date} Statistics:`);
            console.log(`      📈 Total records: ${stats.totalRecords.toLocaleString()}`);
            console.log(`      👥 Unique clients: ${stats.uniqueClients.toLocaleString()}`);
            console.log(`      🏢 Unique instruments: ${stats.uniqueInstruments.toLocaleString()}`);
            console.log(`      🏛️  Source systems: ${stats.sourceSystems?.join(', ')}`);
            console.log(`      🔒 Records with blocked qty: ${stats.recordsWithBlocked}`);
            console.log(`      ⏳ Records with pending buy: ${stats.recordsWithPendingBuy}`);
            console.log(`      ⏳ Records with pending sell: ${stats.recordsWithPendingSell}`);
          }
        }
      }
      console.log();

      // Step 5: Overall summary
      console.log('5️⃣  Overall summary:');
      const overallStats = await this.loader.getOverallStats();
      console.log(`   🗓️  Total daily tables: ${overallStats.totalTables}`);
      console.log(`   📊 Total records: ${overallStats.totalRecords.toLocaleString()}`);
      console.log(`   👥 Unique clients: ${overallStats.uniqueClients.toLocaleString()}`);
      console.log(`   🏢 Unique instruments: ${overallStats.uniqueInstruments.toLocaleString()}`);
      console.log(`   🏛️  Source systems: ${overallStats.sourceSystems?.join(', ')}`);
      
      if (overallStats.dateRange) {
        console.log(`   📅 Date range: ${overallStats.dateRange.from} → ${overallStats.dateRange.to}`);
      }
      
      console.log(`   🔒 Total blocked holdings: ${overallStats.recordsWithBlocked}`);
      console.log(`   ⏳ Total pending buy: ${overallStats.recordsWithPendingBuy}`);
      console.log(`   ⏳ Total pending sell: ${overallStats.recordsWithPendingSell}`);
      console.log();

      // Step 6: Test sample queries
      console.log('6️⃣  Testing sample queries:');
      
      if (overallStats.totalRecords > 0 && overallStats.dateRange) {
        const testDate = overallStats.dateRange.to; // Use the latest date
        console.log(`   🔍 Testing queries for date: ${testDate}`);
        
        // Test 1: Get first 5 records
        const sampleRecords = await this.loader.queryByDate(testDate, { limit: 5 });
        console.log(`   📋 Sample records: ${sampleRecords.records.length} found`);
        
        if (sampleRecords.records.length > 0) {
          const firstRecord = sampleRecords.records[0];
          console.log(`      👤 Sample client: ${firstRecord.client_reference} (${firstRecord.client_name})`);
          console.log(`      📊 Sample instrument: ${firstRecord.instrument_isin} (${firstRecord.instrument_name})`);
        }
        
        // Test 2: Get blocked holdings
        const blockedRecords = await this.loader.queryByDate(testDate, { hasBlocked: true, limit: 3 });
        console.log(`   🔒 Blocked holdings: ${blockedRecords.records.length} found`);
        
        // Test 3: Get pending transactions
        const pendingRecords = await this.loader.queryByDate(testDate, { hasPending: true, limit: 3 });
        console.log(`   ⏳ Pending transactions: ${pendingRecords.records.length} found`);
        
      } else {
        console.log('   📝 No data available for testing queries');
      }
      console.log();

      // Step 7: API endpoints information
      console.log('7️⃣  Available API endpoints:');
      console.log('   🌐 Base URL: http://localhost:3003/api/custody/');
      console.log('   📊 Daily stats: GET /daily-stats/:date (e.g., /daily-stats/2025-06-25)');
      console.log('   📈 Overall stats: GET /overall-stats');
      console.log('   🗓️  List tables: GET /daily-tables');
      console.log('   🔍 Query data: GET /query/:date?client=&isin=&source=');
      console.log('   💰 Financial data: GET /daily-financial/:date/:clientRef');
      console.log('   🔒 Blocked holdings: GET /daily-blocked/:date');
      console.log('   ⏳ Pending transactions: GET /daily-pending/:date');
      console.log('   📅 Date range: GET /date-range-summary?startDate=&endDate=');
      console.log('   ❤️  Health check: GET /health');
      console.log();

      console.log('🎉 MIGRATION & TEST COMPLETED SUCCESSFULLY!');
      console.log('\n💡 Next steps:');
      console.log('   1. Start API server: node custody-api-server.js');
      console.log('   2. Process new files: npm run process-custody-simple');
      console.log('   3. Query data via API endpoints above');
      
      return {
        success: true,
        dailyTables: overallStats.totalTables,
        totalRecords: overallStats.totalRecords,
        dateRange: overallStats.dateRange
      };

    } catch (error) {
      console.error('❌ Migration failed:', error.message);
      throw error;
    }
  }

  async cleanup() {
    try {
      await this.schema.close();
      await this.loader.close();
    } catch (error) {
      console.error('❌ Cleanup error:', error);
    }
  }
}

// CLI execution
async function main() {
  const migrator = new DailyTableMigrator();
  
  try {
    await migrator.migrate();
  } catch (error) {
    console.error('💥 FATAL ERROR:', error.message);
    process.exit(1);
  } finally {
    await migrator.cleanup();
  }
}

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = DailyTableMigrator; 