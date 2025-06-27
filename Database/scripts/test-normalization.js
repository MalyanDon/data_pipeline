#!/usr/bin/env node

const path = require('path');
const fs = require('fs');
const CustodyNormalizationPipeline = require('../custody-normalization/pipeline/custodyNormalizationPipeline');
const { getSupportedCustodyTypes } = require('../custody-normalization/config/custody-mappings');

// Test configuration
const testConfig = {
  sampleSize: 5,
  testDirectories: [
    './temp_uploads',
    './test_files',
    './custody_files'
  ]
};

class NormalizationTester {
  constructor() {
    this.pipeline = new CustodyNormalizationPipeline();
    this.testResults = {
      totalTests: 0,
      passedTests: 0,
      failedTests: 0,
      warnings: [],
      errors: [],
      summary: {}
    };
  }

  /**
   * Run all normalization tests
   */
  async runAllTests() {
    console.log('🧪 Starting Custody Normalization Tests\n');

    try {
      // Test 1: Database connection
      await this.testDatabaseConnection();
      
      // Test 2: Field mappings
      await this.testFieldMappings();
      
      // Test 3: File detection
      await this.testFileDetection();
      
      // Test 4: Sample file processing
      await this.testSampleFileProcessing();
      
      // Test 5: Data validation
      await this.testDataValidation();

      // Print summary
      this.printTestSummary();

    } catch (error) {
      console.error('❌ Test suite failed:', error.message);
    } finally {
      await this.pipeline.close();
    }
  }

  /**
   * Test database connection and schema
   */
  async testDatabaseConnection() {
    console.log('🔧 Testing Database Connection...');
    this.testResults.totalTests++;

    try {
      // Test PostgreSQL connection
      const connectionTest = await this.pipeline.postgresLoader.testConnection();
      
      if (connectionTest.success) {
        console.log('   ✅ PostgreSQL connection successful');
        this.testResults.passedTests++;
      } else {
        console.log('   ❌ PostgreSQL connection failed:', connectionTest.error);
        this.testResults.failedTests++;
        this.testResults.errors.push('Database connection failed');
        return;
      }

      // Test database initialization
      const initResult = await this.pipeline.initializeDatabase();
      
      if (initResult.success) {
        console.log('   ✅ Database schema initialized');
      } else {
        console.log('   ⚠️ Database initialization issue:', initResult.error);
        this.testResults.warnings.push('Database initialization issue');
      }

    } catch (error) {
      console.log('   ❌ Database test error:', error.message);
      this.testResults.failedTests++;
      this.testResults.errors.push(`Database test: ${error.message}`);
    }

    console.log();
  }

  /**
   * Test field mappings for all custody types
   */
  async testFieldMappings() {
    console.log('🗺️ Testing Field Mappings...');
    
    const custodyTypes = getSupportedCustodyTypes();
    
    for (const custodyType of custodyTypes) {
      this.testResults.totalTests++;
      
      try {
        const mappingSummary = this.pipeline.getMappingSummary(custodyType);
        
        console.log(`   📋 ${custodyType.toUpperCase()}:`);
        
        let allMapped = true;
        Object.entries(mappingSummary.mappings).forEach(([field, config]) => {
          const status = config.possibleFields.length > 0 ? '✅' : '❌';
          const required = config.required ? '*' : '';
          console.log(`      ${status} ${field}${required}: ${config.possibleFields.join(', ') || 'NO MAPPING'}`);
          
          if (config.required && config.possibleFields.length === 0) {
            allMapped = false;
          }
        });
        
        if (allMapped) {
          this.testResults.passedTests++;
        } else {
          this.testResults.failedTests++;
          this.testResults.errors.push(`${custodyType} missing required field mappings`);
        }

      } catch (error) {
        console.log(`   ❌ ${custodyType}: ${error.message}`);
        this.testResults.failedTests++;
        this.testResults.errors.push(`${custodyType} mapping test: ${error.message}`);
      }
    }

    console.log('   (* = required field)\n');
  }

  /**
   * Test file type detection
   */
  async testFileDetection() {
    console.log('🔍 Testing File Detection...');
    
    // Test known file patterns
    const testFiles = [
      { name: 'axis_eod_custody_2025-06-25.xlsx', expectedType: 'axis' },
      { name: 'DL_164_EC0000720_25_06_2025.xlsx', expectedType: 'deutsche' },
      { name: 'End_Client_Holding_TRUSTPMS_2025.xls', expectedType: 'trustpms' },
      { name: 'hdfc_eod_custody_2025-06-25.csv', expectedType: 'hdfc' },
      { name: 'kotak_eod_custody_2025-06-25.xlsx', expectedType: 'kotak' },
      { name: 'orbisCustody25_06_2025.xlsx', expectedType: 'orbis' },
      { name: 'unknown_file.xlsx', expectedType: null }
    ];

    testFiles.forEach(testFile => {
      this.testResults.totalTests++;
      
      try {
        const { detectCustodyFileType } = require('../custody-normalization/config/custody-mappings');
        const detection = detectCustodyFileType(testFile.name);
        
        const detectedType = detection ? detection.type : null;
        
        if (detectedType === testFile.expectedType) {
          console.log(`   ✅ ${testFile.name} → ${detectedType || 'unknown'}`);
          this.testResults.passedTests++;
        } else {
          console.log(`   ❌ ${testFile.name} → expected: ${testFile.expectedType}, got: ${detectedType}`);
          this.testResults.failedTests++;
          this.testResults.errors.push(`File detection failed for ${testFile.name}`);
        }

      } catch (error) {
        console.log(`   ❌ ${testFile.name}: ${error.message}`);
        this.testResults.failedTests++;
        this.testResults.errors.push(`File detection error: ${error.message}`);
      }
    });

    console.log();
  }

  /**
   * Test sample file processing
   */
  async testSampleFileProcessing() {
    console.log('📄 Testing Sample File Processing...');
    
    // Look for sample files in test directories
    const sampleFiles = this.findSampleFiles();
    
    if (sampleFiles.length === 0) {
      console.log('   ⚠️ No sample files found for testing');
      console.log('   💡 Place sample custody files in: ./test_files/ or ./temp_uploads/');
      this.testResults.warnings.push('No sample files found for processing tests');
      console.log();
      return;
    }

    for (const filePath of sampleFiles.slice(0, 3)) { // Test max 3 files
      this.testResults.totalTests++;
      
      const fileName = path.basename(filePath);
      console.log(`   🔄 Testing: ${fileName}`);
      
      try {
        // Test preview first
        const previewResult = await this.pipeline.previewFile(filePath, 3);
        
        if (!previewResult.success) {
          console.log(`      ❌ Preview failed: ${previewResult.error}`);
          this.testResults.failedTests++;
          this.testResults.errors.push(`Preview failed for ${fileName}`);
          continue;
        }

        console.log(`      📊 Type: ${previewResult.custodyType}, Records: ${previewResult.metadata.totalRecords}`);
        
        // Test processing (skip loading)
        const processResult = await this.pipeline.processFile(filePath, {
          skipLoading: true,
          recordDate: '2025-06-25' // Use fixed date for testing
        });
        
        if (processResult.success) {
          console.log(`      ✅ Processing successful: ${processResult.stats.normalizedRecords}/${processResult.stats.totalRecords} normalized`);
          this.testResults.passedTests++;
          
          // Show sample normalized data
          if (processResult.data && processResult.data.normalizedRecords.length > 0) {
            const sample = processResult.data.normalizedRecords[0];
            console.log(`      📋 Sample normalized record:`);
            Object.entries(sample).forEach(([key, value]) => {
              if (value !== null && !['created_at', 'updated_at'].includes(key)) {
                console.log(`         ${key}: ${value}`);
              }
            });
          }
        } else {
          console.log(`      ❌ Processing failed: ${processResult.error}`);
          this.testResults.failedTests++;
          this.testResults.errors.push(`Processing failed for ${fileName}: ${processResult.error}`);
        }

      } catch (error) {
        console.log(`      ❌ Error: ${error.message}`);
        this.testResults.failedTests++;
        this.testResults.errors.push(`File processing error for ${fileName}: ${error.message}`);
      }
      
      console.log();
    }
  }

  /**
   * Test data validation rules
   */
  async testDataValidation() {
    console.log('🔧 Testing Data Validation...');
    
    const { validateRecord } = require('../custody-normalization/config/normalization-schema');
    
    // Test valid record
    this.testResults.totalTests++;
    const validRecord = {
      client_reference: 'TEST123',
      client_name: 'TEST CLIENT NAME',
      instrument_isin: 'US1234567890',
      instrument_name: 'Test Instrument',
      instrument_code: 'TEST',
      source_system: 'AXIS',
      file_name: 'test_file.xlsx',
      record_date: '2025-06-25'
    };
    
    const validationResult = validateRecord(validRecord);
    
    if (validationResult.isValid) {
      console.log('   ✅ Valid record validation passed');
      this.testResults.passedTests++;
    } else {
      console.log('   ❌ Valid record validation failed:', validationResult.errors);
      this.testResults.failedTests++;
      this.testResults.errors.push('Valid record validation failed');
    }

    // Test invalid records
    const invalidTests = [
      {
        name: 'Missing client_reference',
        record: { ...validRecord, client_reference: null },
        shouldFail: true
      },
      {
        name: 'Invalid ISIN format',
        record: { ...validRecord, instrument_isin: 'INVALID' },
        shouldFail: true
      },
      {
        name: 'Invalid source system',
        record: { ...validRecord, source_system: 'UNKNOWN' },
        shouldFail: true
      },
      {
        name: 'Future date',
        record: { ...validRecord, record_date: '2030-01-01' },
        shouldFail: true
      }
    ];

    invalidTests.forEach(test => {
      this.testResults.totalTests++;
      
      const result = validateRecord(test.record);
      
      if (test.shouldFail && !result.isValid) {
        console.log(`   ✅ ${test.name}: correctly rejected`);
        this.testResults.passedTests++;
      } else if (!test.shouldFail && result.isValid) {
        console.log(`   ✅ ${test.name}: correctly accepted`);
        this.testResults.passedTests++;
      } else {
        console.log(`   ❌ ${test.name}: validation unexpected result`);
        this.testResults.failedTests++;
        this.testResults.errors.push(`Validation test failed: ${test.name}`);
      }
    });

    console.log();
  }

  /**
   * Find sample files in test directories
   */
  findSampleFiles() {
    const sampleFiles = [];
    
    for (const dir of testConfig.testDirectories) {
      if (fs.existsSync(dir)) {
        const files = fs.readdirSync(dir);
        
        files.forEach(file => {
          const filePath = path.join(dir, file);
          const stats = fs.statSync(filePath);
          
          if (stats.isFile()) {
            const ext = path.extname(file).toLowerCase();
            if (['.xlsx', '.xls', '.csv'].includes(ext)) {
              sampleFiles.push(filePath);
            }
          }
        });
      }
    }
    
    return sampleFiles;
  }

  /**
   * Print test summary
   */
  printTestSummary() {
    console.log('📊 Test Summary');
    console.log('================');
    console.log(`Total Tests: ${this.testResults.totalTests}`);
    console.log(`Passed: ${this.testResults.passedTests} ✅`);
    console.log(`Failed: ${this.testResults.failedTests} ❌`);
    console.log(`Warnings: ${this.testResults.warnings.length} ⚠️`);
    
    const successRate = this.testResults.totalTests > 0 
      ? Math.round((this.testResults.passedTests / this.testResults.totalTests) * 100)
      : 0;
    
    console.log(`Success Rate: ${successRate}%`);

    if (this.testResults.warnings.length > 0) {
      console.log('\n⚠️ Warnings:');
      this.testResults.warnings.forEach(warning => {
        console.log(`   - ${warning}`);
      });
    }

    if (this.testResults.errors.length > 0) {
      console.log('\n❌ Errors:');
      this.testResults.errors.forEach(error => {
        console.log(`   - ${error}`);
      });
    }

    console.log('\n💡 Recommendations:');
    
    if (this.testResults.failedTests === 0) {
      console.log('   🎉 All tests passed! System is ready for production use.');
    } else {
      console.log('   🔧 Fix failing tests before using in production');
    }
    
    if (this.testResults.warnings.length > 0) {
      console.log('   📋 Review warnings to improve system reliability');
    }
    
    console.log('   📄 Place sample custody files in ./test_files/ for more comprehensive testing');
    console.log('   🗂️ Test with actual custody files using: node process-custody-files.js --preview --file <filename>');
    
    console.log('\n' + '='.repeat(50));
  }
}

// Command line execution
async function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
🧪 Custody Normalization Test Suite

USAGE:
  node test-normalization.js [OPTIONS]

OPTIONS:
  --help, -h    Show this help message

WHAT THIS TESTS:
  ✅ Database connectivity and schema
  ✅ Field mappings for all custody types  
  ✅ File type detection patterns
  ✅ Sample file processing (if available)
  ✅ Data validation rules

SAMPLE FILES:
  Place sample custody files in these directories for testing:
  - ./test_files/
  - ./temp_uploads/
  - ./custody_files/

SUPPORTED FORMATS:
  - axis_eod_custody_*.xlsx
  - DL_*EC*.xlsx  
  - End_Client_Holding_*TRUSTPMS*.xls
  - hdfc_eod_custody_*.csv
  - kotak_eod_custody_*.xlsx
  - orbisCustody*.xlsx
`);
    process.exit(0);
  }

  const tester = new NormalizationTester();
  await tester.runAllTests();
}

// Run if called directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Test suite error:', error.message);
    process.exit(1);
  });
}

module.exports = NormalizationTester; 