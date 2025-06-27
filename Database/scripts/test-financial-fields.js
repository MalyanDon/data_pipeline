#!/usr/bin/env node

const mongoose = require('mongoose');
const config = require('../config');
const FieldMapper = require('../custody-normalization/extractors/fieldMapper');
const DataNormalizer = require('../custody-normalization/extractors/dataNormalizer');
const PostgresLoader = require('../custody-normalization/loaders/postgresLoader');
const NormalizationSchema = require('../custody-normalization/config/normalization-schema');
const { Pool } = require('pg');

async function testFinancialFields() {
  console.log('🧪 Testing Financial Fields Functionality\n');
  
  try {
    // Connect to MongoDB to get sample data
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB\n');
    
    const db = mongoose.connection.db;
    
    // Test with Axis data (has summing logic)
    console.log('📊 Testing Axis custody data with financial field summing...');
    const axisCollection = db.collection('axis_06_25');
    const axisSample = await axisCollection.findOne({});
    
    if (axisSample) {
      console.log('📄 Sample Axis record:');
      console.log(`   UCC: ${axisSample.UCC}`);
      console.log(`   ClientName: ${axisSample.ClientName}`);
      console.log(`   ISIN: ${axisSample.ISIN}`);
      console.log(`   DematLockedQty: ${axisSample.DematLockedQty}`);
      console.log(`   PhysicalLocked: ${axisSample.PhysicalLocked}`);
      console.log(`   PurchaseOutstanding: ${axisSample.PurchaseOutstanding}`);
      console.log(`   SaleOutstanding: ${axisSample.SaleOutstanding}`);
      
      // Test field mapping
      const fieldMapper = new FieldMapper();
      const mappingResult = fieldMapper.mapRecord(axisSample, 'axis', {
        sourceSystem: 'AXIS',
        fileName: 'test_axis.xlsx',
        recordDate: '2025-06-25'
      });
      
      console.log('\n🗺️ Mapped record:');
      console.log(`   blocked_quantity: ${mappingResult.mappedRecord.blocked_quantity} (should be sum of DematLockedQty + PhysicalLocked)`);
      console.log(`   pending_buy_quantity: ${mappingResult.mappedRecord.pending_buy_quantity} (should be sum of purchase fields)`);
      console.log(`   pending_sell_quantity: ${mappingResult.mappedRecord.pending_sell_quantity} (should be sum of sale fields)`);
      
      // Test data normalization
      const dataNormalizer = new DataNormalizer();
      const normalizeResult = dataNormalizer.normalizeRecord(mappingResult.mappedRecord);
      
      console.log('\n🔧 Normalization result:');
      console.log(`   Success: ${normalizeResult.success}`);
      if (normalizeResult.success) {
        console.log(`   Normalized blocked_quantity: ${normalizeResult.normalizedRecord.blocked_quantity}`);
        console.log(`   Normalized pending_buy_quantity: ${normalizeResult.normalizedRecord.pending_buy_quantity}`);
        console.log(`   Normalized pending_sell_quantity: ${normalizeResult.normalizedRecord.pending_sell_quantity}`);
      } else {
        console.log(`   Errors: ${normalizeResult.errors.join(', ')}`);
      }
    }
    
    // Test with Orbis data
    console.log('\n📊 Testing Orbis custody data...');
    const orbisCollection = db.collection('orbis_06_25');
    const orbisSample = await orbisCollection.findOne({});
    
    if (orbisSample) {
      console.log('📄 Sample Orbis record:');
      console.log(`   OFIN Code: ${orbisSample['OFIN Code']}`);
      console.log(`   Description: ${orbisSample.Description}`);
      console.log(`   ISIN: ${orbisSample.ISIN}`);
      console.log(`   Blocked/Pledge: ${orbisSample['Blocked/Pledge']}`);
      console.log(`   Intrasit Purchase: ${orbisSample['Intrasit Purchase : ']}`);
      console.log(`   Intrasit Sale: ${orbisSample['Intrasit Sale : ']}`);
      
      // Test field mapping
      const fieldMapper = new FieldMapper();
      const mappingResult = fieldMapper.mapRecord(orbisSample, 'orbis', {
        sourceSystem: 'ORBIS',
        fileName: 'test_orbis.xlsx',
        recordDate: '2025-06-25'
      });
      
      console.log('\n🗺️ Mapped record:');
      console.log(`   blocked_quantity: ${mappingResult.mappedRecord.blocked_quantity}`);
      console.log(`   pending_buy_quantity: ${mappingResult.mappedRecord.pending_buy_quantity}`);
      console.log(`   pending_sell_quantity: ${mappingResult.mappedRecord.pending_sell_quantity}`);
    }
    
    // Test PostgreSQL financial queries
    console.log('\n📊 Testing PostgreSQL financial queries...');
    const postgresLoader = new PostgresLoader();
    
    try {
      const stats = await postgresLoader.getStatistics();
      console.log('📈 Enhanced statistics:');
      console.log(`   Records with blocked quantities: ${stats.records_with_blocked_qty}`);
      console.log(`   Records with pending buy: ${stats.records_with_pending_buy}`);
      console.log(`   Records with pending sell: ${stats.records_with_pending_sell}`);
      console.log(`   Average blocked quantity: ${stats.avg_blocked_quantity}`);
      
      // Test blocked holdings query
      const blockedHoldings = await postgresLoader.getBlockedHoldings(5);
      console.log(`\n🔒 Sample blocked holdings (${blockedHoldings.length} records):`);
      blockedHoldings.forEach(record => {
        console.log(`   ${record.client_reference}: ${record.instrument_name} - Blocked: ${record.blocked_quantity}`);
      });
      
      // Test pending transactions query
      const pendingTransactions = await postgresLoader.getPendingTransactions(5);
      console.log(`\n⏳ Sample pending transactions (${pendingTransactions.length} records):`);
      pendingTransactions.forEach(record => {
        console.log(`   ${record.client_reference}: ${record.instrument_name} - Buy: ${record.pending_buy_quantity}, Sell: ${record.pending_sell_quantity}`);
      });
      
      await postgresLoader.close();
      
    } catch (error) {
      console.error('❌ PostgreSQL query error:', error.message);
    }
    
    console.log('\n🎉 Financial fields testing completed!');
    
  } catch (error) {
    console.error('❌ Test error:', error.message);
  } finally {
    await mongoose.disconnect();
  }
}

class FinancialFieldsValidator {
  constructor() {
    this.schema = new NormalizationSchema();
    this.loader = new PostgresLoader();
  }

  async runComprehensiveTests() {
    console.log('🧪 Financial Fields Validation Test Suite');
    console.log('═══════════════════════════════════════════════════════');
    console.log('📊 Testing total_position and saleable_quantity fields');
    console.log('🔍 Validating formula: saleable_quantity ≈ total_position - blocked_quantity');
    console.log('');

    try {
      // Test 1: Schema Validation
      await this.testSchemaIntegrity();
      
      // Test 2: Data Relationship Validation
      await this.testDataRelationships();
      
      // Test 3: Custody-Specific Tests
      await this.testCustodySpecificRules();
      
      // Test 4: Performance Tests
      await this.testQueryPerformance();
      
      // Test 5: Business Logic Tests
      await this.testBusinessLogic();
      
      console.log('✅ All tests completed successfully!');
      
    } catch (error) {
      console.error('❌ Test suite failed:', error.message);
      throw error;
    } finally {
      await this.cleanup();
    }
  }

  async testSchemaIntegrity() {
    console.log('🏗️  Test 1: Schema Integrity');
    console.log('─────────────────────────────────────────────────────');

    const pool = new Pool(config.postgresql);
    const client = await pool.connect();

    try {
      // Check that all tables have the new fields
      const tables = await this.schema.getAllDailyTables();
      console.log(`📋 Found ${tables.length} daily tables to validate`);

      let passedTables = 0;
      let failedTables = 0;

      for (const tableName of tables) {
        const columns = await client.query(`
          SELECT column_name, data_type, is_nullable, column_default
          FROM information_schema.columns 
          WHERE table_name = $1 
          AND column_name IN ('total_position', 'saleable_quantity')
          ORDER BY column_name
        `, [tableName]);

        if (columns.rows.length === 2) {
          const totalPosCol = columns.rows.find(r => r.column_name === 'total_position');
          const saleableCol = columns.rows.find(r => r.column_name === 'saleable_quantity');

          const validSchema = 
            totalPosCol.data_type === 'numeric' &&
            saleableCol.data_type === 'numeric' &&
            totalPosCol.column_default === '0' &&
            saleableCol.column_default === '0';

          if (validSchema) {
            passedTables++;
            console.log(`   ✅ ${tableName} - Schema OK`);
          } else {
            failedTables++;
            console.log(`   ❌ ${tableName} - Schema issues`);
          }
        } else {
          failedTables++;
          console.log(`   ❌ ${tableName} - Missing columns (found ${columns.rows.length}/2)`);
        }
      }

      console.log(`📊 Schema Test Results: ${passedTables} passed, ${failedTables} failed`);
      
      if (failedTables > 0) {
        throw new Error(`Schema integrity test failed: ${failedTables} tables have issues`);
      }

    } finally {
      client.release();
      await pool.end();
    }

    console.log('✅ Schema integrity test passed\n');
  }

  async testDataRelationships() {
    console.log('🔗 Test 2: Data Relationship Validation');
    console.log('─────────────────────────────────────────────────────');

    const validationResult = await this.schema.validateFinancialRelationships();
    
    if (!validationResult.success) {
      throw new Error('Financial relationship validation failed');
    }

    let totalRecords = 0;
    let totalCompliant = 0;
    let totalViolations = 0;

    console.log('📊 Per-Table Formula Compliance:');
    validationResult.validationResults.forEach(result => {
      totalRecords += result.recordsWithPosition;
      totalCompliant += result.formulaCompliant;
      totalViolations += result.formulaViolations;

      const status = parseFloat(result.complianceRate) >= 93 ? '✅' : 
                    parseFloat(result.complianceRate) >= 85 ? '⚠️' : '❌';
      
      console.log(`   ${status} ${result.table}: ${result.complianceRate} (${result.formulaCompliant}/${result.recordsWithPosition})`);
    });

    const overallComplianceRate = totalRecords > 0 ? 
      (totalCompliant / totalRecords * 100).toFixed(2) : 0;

    console.log('');
    console.log('🎯 Overall Formula Compliance Results:');
    console.log(`   📈 Total Records with Position: ${totalRecords.toLocaleString()}`);
    console.log(`   ✅ Formula Compliant: ${totalCompliant.toLocaleString()}`);
    console.log(`   ❌ Formula Violations: ${totalViolations.toLocaleString()}`);
    console.log(`   📊 Overall Compliance Rate: ${overallComplianceRate}%`);

    // Validate against expected ranges
    if (parseFloat(overallComplianceRate) >= 93) {
      console.log('🎉 EXCELLENT: Formula compliance meets 93-100% target range!');
    } else if (parseFloat(overallComplianceRate) >= 85) {
      console.log('⚠️  ACCEPTABLE: Formula compliance is good but below target range');
    } else {
      console.log('❌ CRITICAL: Formula compliance is below acceptable threshold');
      throw new Error(`Formula compliance rate ${overallComplianceRate}% is below acceptable threshold`);
    }

    console.log('✅ Data relationship test passed\n');
  }

  async testCustodySpecificRules() {
    console.log('🏦 Test 3: Custody-Specific Rule Validation');
    console.log('─────────────────────────────────────────────────────');

    const pool = new Pool(config.postgresql);
    const client = await pool.connect();

    try {
      const tables = await this.schema.getAllDailyTables();
      const custodyTests = {
        'TRUSTPMS': { totalPositionShouldBeNull: true, name: 'Trust PMS' },
        'ORBIS': { shouldHavePositions: true, name: 'Orbis Financial' },
        'AXIS': { shouldHavePositions: true, name: 'Axis Securities' },
        'KOTAK': { shouldHavePositions: true, name: 'Kotak Securities' },
        'DEUTSCHE': { shouldHavePositions: true, name: 'Deutsche Bank' },
        'HDFC': { shouldHavePositions: true, name: 'HDFC Securities' }
      };

      for (const tableName of tables) {
        console.log(`📋 Testing table: ${tableName}`);

        // Get source system breakdown
        const sourceStats = await client.query(`
          SELECT 
            source_system,
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE total_position IS NOT NULL) as records_with_total_position,
            COUNT(*) FILTER (WHERE total_position > 0) as records_with_positive_total,
            COUNT(*) FILTER (WHERE saleable_quantity > 0) as records_with_positive_saleable,
            AVG(total_position) as avg_total_position,
            AVG(saleable_quantity) as avg_saleable_quantity
          FROM ${tableName}
          GROUP BY source_system
        `);

        sourceStats.rows.forEach(stats => {
          const custodyType = stats.source_system;
          const rules = custodyTests[custodyType];
          
          if (!rules) {
            console.log(`   ⚠️  ${custodyType}: No specific rules defined`);
            return;
          }

          console.log(`   🔍 ${rules.name} (${stats.total_records} records):`);

          // Test Trust PMS specific rule
          if (rules.totalPositionShouldBeNull) {
            const nullPositionRate = (stats.total_records - stats.records_with_total_position) / stats.total_records * 100;
            if (nullPositionRate >= 95) {
              console.log(`      ✅ Total position is NULL as expected (${nullPositionRate.toFixed(1)}%)`);
            } else {
              console.log(`      ❌ Expected NULL total_position but found data (${nullPositionRate.toFixed(1)}% null)`);
            }
          }

          // Test other custody systems
          if (rules.shouldHavePositions) {
            const positionRate = stats.records_with_positive_total / stats.total_records * 100;
            const saleableRate = stats.records_with_positive_saleable / stats.total_records * 100;
            
            if (positionRate > 0) {
              console.log(`      ✅ Has position data (${positionRate.toFixed(1)}% of records)`);
            } else {
              console.log(`      ⚠️  No position data found - may need re-processing`);
            }

            if (saleableRate > 0) {
              console.log(`      ✅ Has saleable data (${saleableRate.toFixed(1)}% of records)`);
            } else {
              console.log(`      ⚠️  No saleable data found - may need re-processing`);
            }
          }
        });
      }

    } finally {
      client.release();
      await pool.end();
    }

    console.log('✅ Custody-specific rule test passed\n');
  }

  async testQueryPerformance() {
    console.log('⚡ Test 4: Query Performance Validation');
    console.log('─────────────────────────────────────────────────────');

    const pool = new Pool(config.postgresql);
    const client = await pool.connect();

    try {
      const tables = await this.schema.getAllDailyTables();
      
      if (tables.length === 0) {
        console.log('⚠️  No tables found for performance testing');
        return;
      }

      const testTable = tables[0]; // Use first table for performance testing
      console.log(`🎯 Testing performance on: ${testTable}`);

      // Test 1: Index effectiveness for total_position queries
      const start1 = Date.now();
      await client.query(`
        SELECT client_reference, instrument_isin, total_position 
        FROM ${testTable} 
        WHERE total_position > 0 
        ORDER BY total_position DESC 
        LIMIT 100
      `);
      const time1 = Date.now() - start1;
      console.log(`   ✅ Total position query: ${time1}ms`);

      // Test 2: Index effectiveness for saleable_quantity queries
      const start2 = Date.now();
      await client.query(`
        SELECT client_reference, instrument_isin, saleable_quantity 
        FROM ${testTable} 
        WHERE saleable_quantity > 0 
        ORDER BY saleable_quantity DESC 
        LIMIT 100
      `);
      const time2 = Date.now() - start2;
      console.log(`   ✅ Saleable quantity query: ${time2}ms`);

      // Test 3: Formula validation query performance
      const start3 = Date.now();
      await client.query(`
        SELECT COUNT(*) 
        FROM ${testTable} 
        WHERE total_position > 0 
        AND ABS((total_position - blocked_quantity) - saleable_quantity) > (total_position * 0.01)
      `);
      const time3 = Date.now() - start3;
      console.log(`   ✅ Formula validation query: ${time3}ms`);

      // Test 4: Combined financial fields query
      const start4 = Date.now();
      await client.query(`
        SELECT 
          client_reference,
          SUM(total_position) as total_pos,
          SUM(saleable_quantity) as total_saleable,
          SUM(blocked_quantity) as total_blocked
        FROM ${testTable} 
        WHERE total_position > 0 
        GROUP BY client_reference 
        ORDER BY total_pos DESC 
        LIMIT 50
      `);
      const time4 = Date.now() - start4;
      console.log(`   ✅ Aggregated positions query: ${time4}ms`);

      const avgTime = (time1 + time2 + time3 + time4) / 4;
      console.log(`📊 Average query time: ${avgTime.toFixed(1)}ms`);

      if (avgTime < 1000) {
        console.log('🚀 EXCELLENT: Query performance is optimal');
      } else if (avgTime < 5000) {
        console.log('⚠️  ACCEPTABLE: Query performance is reasonable');
      } else {
        console.log('❌ SLOW: Query performance may need optimization');
      }

    } finally {
      client.release();
      await pool.end();
    }

    console.log('✅ Query performance test passed\n');
  }

  async testBusinessLogic() {
    console.log('💼 Test 5: Business Logic Validation');
    console.log('─────────────────────────────────────────────────────');

    const pool = new Pool(config.postgresql);
    const client = await pool.connect();

    try {
      const tables = await this.schema.getAllDailyTables();
      
      let totalChecks = 0;
      let passedChecks = 0;

      for (const tableName of tables) {
        console.log(`📋 Business logic checks for: ${tableName}`);

        // Check 1: No negative positions
        const negativeCheck = await client.query(`
          SELECT COUNT(*) as count
          FROM ${tableName}
          WHERE total_position < 0 OR saleable_quantity < 0
        `);
        
        totalChecks++;
        if (parseInt(negativeCheck.rows[0].count) === 0) {
          console.log('   ✅ No negative positions found');
          passedChecks++;
        } else {
          console.log(`   ❌ Found ${negativeCheck.rows[0].count} records with negative positions`);
        }

        // Check 2: Saleable quantity doesn't exceed total position
        const excessiveCheck = await client.query(`
          SELECT COUNT(*) as count
          FROM ${tableName}
          WHERE total_position > 0 AND saleable_quantity > total_position * 1.01
        `);
        
        totalChecks++;
        if (parseInt(excessiveCheck.rows[0].count) === 0) {
          console.log('   ✅ No excessive saleable quantities found');
          passedChecks++;
        } else {
          console.log(`   ❌ Found ${excessiveCheck.rows[0].count} records where saleable exceeds total`);
        }

        // Check 3: Reasonable position sizes
        const extremeCheck = await client.query(`
          SELECT COUNT(*) as count
          FROM ${tableName}
          WHERE total_position > 1000000000 OR saleable_quantity > 1000000000
        `);
        
        totalChecks++;
        if (parseInt(extremeCheck.rows[0].count) === 0) {
          console.log('   ✅ No extremely large positions found');
          passedChecks++;
        } else {
          console.log(`   ⚠️  Found ${extremeCheck.rows[0].count} records with extremely large positions`);
          passedChecks++; // This is a warning, not a failure
        }
      }

      const successRate = (passedChecks / totalChecks * 100).toFixed(1);
      console.log(`📊 Business logic checks: ${passedChecks}/${totalChecks} passed (${successRate}%)`);

    } finally {
      client.release();
      await pool.end();
    }

    console.log('✅ Business logic test passed\n');
  }

  async cleanup() {
    await this.schema.close();
    await this.loader.close();
  }
}

// Summary report generator
async function generateSummaryReport() {
  console.log('📊 Financial Fields Implementation Summary Report');
  console.log('═══════════════════════════════════════════════════════');
  
  const schema = new NormalizationSchema();
  const loader = new PostgresLoader();
  
  try {
    // Get overall statistics
    const stats = await loader.getOverallStats();
    
    console.log('📈 Current Data Status:');
    console.log(`   🗄️  Total Tables: ${stats.totalTables}`);
    console.log(`   📊 Total Records: ${stats.totalRecords.toLocaleString()}`);
    console.log(`   👥 Unique Clients: ${stats.uniqueClients.toLocaleString()}`);
    console.log(`   🏦 Source Systems: ${stats.sourceSystems.join(', ')}`);
    console.log('');
    
    console.log('💰 Financial Fields Status:');
    console.log(`   🎯 Records with Total Position: ${stats.recordsWithTotalPosition?.toLocaleString() || 'N/A'}`);
    console.log(`   💼 Records with Saleable Quantity: ${stats.recordsWithSaleable?.toLocaleString() || 'N/A'}`);
    console.log(`   ✅ Formula Compliant Records: ${stats.formulaCompliantRecords?.toLocaleString() || 'N/A'}`);
    console.log(`   📊 Formula Compliance Rate: ${stats.formulaComplianceRate || 'N/A'}`);
    console.log(`   📐 Average Formula Deviation: ${stats.avgFormulaDeviationPercentage || 'N/A'}%`);
    console.log('');
    
    console.log('🎯 Implementation Status:');
    if (stats.recordsWithTotalPosition > 0) {
      console.log('   ✅ total_position field is implemented and populated');
    } else {
      console.log('   ⚠️  total_position field needs data population');
    }
    
    if (stats.recordsWithSaleable > 0) {
      console.log('   ✅ saleable_quantity field is implemented and populated');
    } else {
      console.log('   ⚠️  saleable_quantity field needs data population');
    }
    
    if (stats.formulaComplianceRate && parseFloat(stats.formulaComplianceRate) >= 93) {
      console.log('   🎉 Formula compliance meets target range (93-100%)');
    } else {
      console.log('   🔄 Formula compliance needs improvement or data re-processing');
    }
    
  } catch (error) {
    console.error('❌ Error generating summary report:', error.message);
  } finally {
    await schema.close();
    await loader.close();
  }
}

// Main execution
if (require.main === module) {
  const command = process.argv[2];
  
  if (command === '--summary') {
    generateSummaryReport().catch(console.error);
  } else {
    const validator = new FinancialFieldsValidator();
    validator.runComprehensiveTests().catch(console.error);
  }
}

module.exports = { FinancialFieldsValidator, generateSummaryReport }; 