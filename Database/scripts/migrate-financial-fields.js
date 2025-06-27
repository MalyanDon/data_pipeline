#!/usr/bin/env node

const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
const { Pool } = require('pg');
const config = require('../config');

async function migrateFinancialFields() {
  console.log('🚀 Starting Financial Fields Migration...');
  console.log('📊 Adding total_position and saleable_quantity to existing tables');
  console.log('');

  const schema = new NormalizationSchema();
  
  try {
    // Step 1: Add new columns to existing tables
    console.log('🔧 Step 1: Adding new financial fields to existing tables...');
    const migrationResult = await schema.addNewFinancialFields();
    
    if (migrationResult.success) {
      console.log(`✅ Migration completed successfully!`);
      console.log(`📈 Tables updated: ${migrationResult.tablesUpdated}`);
    } else {
      console.log('❌ Migration failed');
      return;
    }
    
    console.log('');
    
    // Step 2: Validate financial relationships
    console.log('🔍 Step 2: Validating financial field relationships...');
    const validationResult = await schema.validateFinancialRelationships();
    
    if (validationResult.success) {
      console.log('✅ Validation completed successfully!');
      console.log('');
      console.log('📊 Validation Results:');
      console.log('═══════════════════════════════════════════════════════');
      
      validationResult.validationResults.forEach(result => {
        console.log(`📋 Table: ${result.table}`);
        console.log(`   📈 Total Records: ${result.totalRecords.toLocaleString()}`);
        console.log(`   🎯 Records with Position: ${result.recordsWithPosition.toLocaleString()}`);
        console.log(`   ✅ Formula Compliant: ${result.formulaCompliant.toLocaleString()}`);
        console.log(`   ❌ Formula Violations: ${result.formulaViolations.toLocaleString()}`);
        console.log(`   📊 Compliance Rate: ${result.complianceRate}`);
        console.log(`   📐 Avg Deviation: ${result.avgDeviationPercentage}%`);
        console.log('');
      });
      
      // Calculate overall statistics
      const overallStats = validationResult.validationResults.reduce((acc, result) => {
        acc.totalRecords += result.totalRecords;
        acc.recordsWithPosition += result.recordsWithPosition;
        acc.formulaCompliant += result.formulaCompliant;
        acc.formulaViolations += result.formulaViolations;
        return acc;
      }, { totalRecords: 0, recordsWithPosition: 0, formulaCompliant: 0, formulaViolations: 0 });
      
      const overallComplianceRate = overallStats.recordsWithPosition > 0 ? 
        (overallStats.formulaCompliant / overallStats.recordsWithPosition * 100).toFixed(2) : 0;
      
      console.log('🎯 Overall Summary:');
      console.log('═══════════════════════════════════════════════════════');
      console.log(`📈 Total Records: ${overallStats.totalRecords.toLocaleString()}`);
      console.log(`🎯 Records with Position: ${overallStats.recordsWithPosition.toLocaleString()}`);
      console.log(`✅ Formula Compliant: ${overallStats.formulaCompliant.toLocaleString()}`);
      console.log(`❌ Formula Violations: ${overallStats.formulaViolations.toLocaleString()}`);
      console.log(`📊 Overall Compliance Rate: ${overallComplianceRate}%`);
      
      if (parseFloat(overallComplianceRate) >= 93) {
        console.log('🎉 EXCELLENT: Compliance rate meets expected 93-100% range!');
      } else if (parseFloat(overallComplianceRate) >= 85) {
        console.log('⚠️  GOOD: Compliance rate is acceptable but below expected range');
      } else {
        console.log('❌ WARNING: Compliance rate is below acceptable threshold');
      }
      
    } else {
      console.log('❌ Validation failed');
    }
    
    console.log('');
    
    // Step 3: Provide recommendations
    console.log('💡 Next Steps & Recommendations:');
    console.log('═══════════════════════════════════════════════════════');
    console.log('1. ✅ Database schema has been updated with new financial fields');
    console.log('2. 📊 Existing data now has placeholders (0/null) for new fields');
    console.log('3. 🔄 Re-process source files to populate actual total_position and saleable_quantity values');
    console.log('4. 🧪 Use the validation endpoints to monitor formula compliance');
    console.log('5. 📈 Expected compliance rate should be 93-100% after re-processing');
    
    console.log('');
    console.log('🛠️  Available Commands:');
    console.log('   📋 Re-process files: node scripts/process-custody-files.js');
    console.log('   🔍 Validate data: node scripts/test-financial-fields.js');
    console.log('   📊 Check stats: curl http://localhost:3003/api/custody/stats');
    
  } catch (error) {
    console.error('❌ Migration failed with error:', error.message);
    console.error(error.stack);
  } finally {
    await schema.close();
  }
}

// Add helper function to check current schema
async function checkCurrentSchema() {
  console.log('🔍 Checking current database schema...');
  
  const pool = new Pool(config.postgresql);
  const client = await pool.connect();
  
  try {
    // Check legacy table
    const legacyCheck = await client.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'unified_custody_master'
      )
    `);
    
    if (legacyCheck.rows[0].exists) {
      const legacyColumns = await client.query(`
        SELECT column_name, data_type, is_nullable 
        FROM information_schema.columns 
        WHERE table_name = 'unified_custody_master' 
        ORDER BY ordinal_position
      `);
      
      console.log('📋 Legacy Table (unified_custody_master):');
      legacyColumns.rows.forEach(col => {
        const indicator = ['total_position', 'saleable_quantity'].includes(col.column_name) ? '🆕' : '📝';
        console.log(`   ${indicator} ${col.column_name.padEnd(25)} ${col.data_type.padEnd(20)} ${col.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`);
      });
    }
    
    // Check daily tables
    const dailyTables = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_name LIKE 'unified_custody_master_%'
      ORDER BY table_name
      LIMIT 3
    `);
    
    if (dailyTables.rows.length > 0) {
      console.log(`\n📅 Daily Tables (showing first 3 of ${dailyTables.rows.length}):`);
      
      for (const table of dailyTables.rows) {
        const columns = await client.query(`
          SELECT column_name, data_type 
          FROM information_schema.columns 
          WHERE table_name = $1 AND column_name IN ('total_position', 'saleable_quantity')
        `, [table.table_name]);
        
        const hasNewFields = columns.rows.length === 2;
        console.log(`   ${hasNewFields ? '✅' : '❌'} ${table.table_name} - ${hasNewFields ? 'Has new fields' : 'Missing new fields'}`);
      }
    }
    
  } finally {
    client.release();
    await pool.end();
  }
}

// Run the migration
if (require.main === module) {
  const command = process.argv[2];
  
  if (command === '--check') {
    checkCurrentSchema().catch(console.error);
  } else {
    migrateFinancialFields().catch(console.error);
  }
}

module.exports = { migrateFinancialFields, checkCurrentSchema }; 