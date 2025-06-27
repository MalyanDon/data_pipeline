#!/usr/bin/env node

const path = require('path');
const fs = require('fs');
const CustodyNormalizationPipeline = require('../custody-normalization/pipeline/custodyNormalizationPipeline');

// Command line argument parsing
const args = process.argv.slice(2);

const options = {
  directory: null,
  file: null,
  recordDate: null,
  custodyType: null,
  preview: false,
  skipLoading: false,
  initDb: false,
  stats: false,
  help: false
};

// Parse command line arguments
for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  
  switch (arg) {
    case '--directory':
    case '-d':
      options.directory = args[++i];
      break;
    case '--file':
    case '-f':
      options.file = args[++i];
      break;
    case '--date':
      options.recordDate = args[++i];
      break;
    case '--type':
    case '-t':
      options.custodyType = args[++i];
      break;
    case '--preview':
    case '-p':
      options.preview = true;
      break;
    case '--skip-loading':
    case '-s':
      options.skipLoading = true;
      break;
    case '--init-db':
      options.initDb = true;
      break;
    case '--stats':
      options.stats = true;
      break;
    case '--help':
    case '-h':
      options.help = true;
      break;
    default:
      // Assume it's a directory or file path if no flag is provided
      if (!options.directory && !options.file && fs.existsSync(arg)) {
        const stat = fs.statSync(arg);
        if (stat.isDirectory()) {
          options.directory = arg;
        } else {
          options.file = arg;
        }
      }
  }
}

// Show help
function showHelp() {
  console.log(`
🏦 Custody File Normalization Tool

USAGE:
  node process-custody-files.js [OPTIONS] [DIRECTORY|FILE]

OPTIONS:
  -d, --directory PATH     Process all custody files in directory
  -f, --file PATH         Process single custody file
  --date YYYY-MM-DD       Override record date (auto-detected from filename if not provided)
  -t, --type TYPE         Override custody type detection (axis, hdfc, kotak, orbis, deutsche, trustpms)
  -p, --preview           Preview file without processing
  -s, --skip-loading      Process file but skip PostgreSQL loading
  --init-db               Initialize PostgreSQL database schema
  --stats                 Show database statistics
  -h, --help              Show this help message

EXAMPLES:
  # Initialize database
  node process-custody-files.js --init-db

  # Process all custody files in a directory
  node process-custody-files.js --directory ./custody_files

  # Process single file
  node process-custody-files.js --file axis_eod_custody_2025-06-25.xlsx

  # Preview file without processing
  node process-custody-files.js --preview --file hdfc_custody.csv

  # Process with custom date
  node process-custody-files.js --file custody.xlsx --date 2025-06-25

  # Show database statistics
  node process-custody-files.js --stats

SUPPORTED FILE TYPES:
  - Axis EOD Custody (.xlsx)      → axis_eod_custody_*.xlsx
  - Deutsche Bank (.xlsx)         → DL_*EC*.xlsx  
  - Trust PMS (.xls)              → End_Client_Holding_*TRUSTPMS*.xls
  - HDFC Custody (.csv)           → hdfc_eod_custody_*.csv
  - Kotak Custody (.xlsx)         → kotak_eod_custody_*.xlsx
  - Orbis Custody (.xlsx)         → orbisCustody*.xlsx
`);
}

// Main execution
async function main() {
  if (options.help || (!options.directory && !options.file && !options.initDb && !options.stats)) {
    showHelp();
    process.exit(0);
  }

  const pipeline = new CustodyNormalizationPipeline();

  try {
    // Initialize database
    if (options.initDb) {
      console.log('🔧 Initializing database...');
      const initResult = await pipeline.initializeDatabase();
      
      if (initResult.success) {
        console.log('✅ Database initialized successfully');
      } else {
        console.error('❌ Database initialization failed:', initResult.error);
        process.exit(1);
      }
      
      if (!options.directory && !options.file && !options.stats) {
        process.exit(0);
      }
    }

    // Show database statistics
    if (options.stats) {
      console.log('📊 Database Statistics:');
      const stats = await pipeline.getDatabaseStats();
      
      if (stats.success === false) {
        console.error('❌ Failed to get statistics:', stats.error);
      } else {
        console.log(`   Total Records: ${stats.total.toLocaleString()}`);
        console.log(`   Date Range: ${stats.dateRange.min_date} to ${stats.dateRange.max_date}`);
        console.log(`   Unique Dates: ${stats.dateRange.unique_dates}`);
        console.log(`   Records by Source System:`);
        
        stats.bySource.forEach(source => {
          console.log(`     ${source.source_system}: ${source.count.toLocaleString()} records`);
        });
        
        if (stats.latestBySource.length > 0) {
          console.log(`   Latest Data by Source:`);
          stats.latestBySource.forEach(latest => {
            console.log(`     ${latest.source_system}: ${latest.latest_date} (${latest.latest_count} records)`);
          });
        }
      }
      
      if (!options.directory && !options.file) {
        await pipeline.close();
        process.exit(0);
      }
    }

    // Process directory
    if (options.directory) {
      if (!fs.existsSync(options.directory)) {
        console.error(`❌ Directory not found: ${options.directory}`);
        process.exit(1);
      }

      console.log(`🗂️ Processing directory: ${options.directory}`);
      
      const result = await pipeline.processDirectory(options.directory, {
        recordDate: options.recordDate,
        skipLoading: options.skipLoading
      });

      if (result.success) {
        console.log('✅ Directory processing completed successfully');
        console.log(`📊 Summary: ${result.stats.processedFiles}/${result.stats.totalFiles} files processed`);
        console.log(`📋 Records: ${result.stats.loadedRecords.toLocaleString()} loaded`);
      } else {
        console.error('❌ Directory processing failed:', result.error);
        if (result.stats.errorFiles > 0) {
          console.error('❌ Failed files:');
          result.stats.errors.forEach(error => {
            console.error(`   ${error.fileName}: ${error.error}`);
          });
        }
        process.exit(1);
      }
    }

    // Process single file
    if (options.file) {
      if (!fs.existsSync(options.file)) {
        console.error(`❌ File not found: ${options.file}`);
        process.exit(1);
      }

      if (options.preview) {
        console.log(`👁️ Previewing file: ${options.file}`);
        
        const previewResult = await pipeline.previewFile(options.file);
        
        if (previewResult.success) {
          console.log(`✅ File Type: ${previewResult.custodyType.toUpperCase()}`);
          console.log(`📊 Sample Records: ${previewResult.sampleData.length}`);
          console.log(`📈 Total Records: ${previewResult.metadata.totalRecords}`);
          
          console.log('\n📋 Available Fields:');
          previewResult.fieldAnalysis.availableFields.forEach(field => {
            console.log(`   - ${field}`);
          });
          
          console.log('\n🗺️ Field Mapping Analysis:');
          Object.entries(previewResult.fieldAnalysis.mappingAnalysis).forEach(([field, analysis]) => {
            const status = analysis.confidence > 0 ? '✅' : '❌';
            console.log(`   ${status} ${field}: ${analysis.confidence}% confidence`);
            if (analysis.foundMappings.length > 0) {
              analysis.foundMappings.forEach(mapping => {
                console.log(`      → ${mapping.sourceField} (${mapping.exactMatch ? 'exact' : 'partial'} match)`);
              });
            }
          });
          
          if (previewResult.recommendations.length > 0) {
            console.log('\n💡 Recommendations:');
            previewResult.recommendations.forEach(rec => {
              const icon = rec.severity === 'error' ? '❌' : '⚠️';
              console.log(`   ${icon} ${rec.message}`);
            });
          }
          
          console.log('\n📄 Sample Data (first 3 records):');
          previewResult.sampleData.slice(0, 3).forEach((record, index) => {
            console.log(`   Record ${index + 1}:`);
            Object.entries(record).slice(0, 5).forEach(([key, value]) => {
              console.log(`     ${key}: ${value}`);
            });
          });
        } else {
          console.error('❌ Preview failed:', previewResult.error);
          process.exit(1);
        }
      } else {
        console.log(`📄 Processing file: ${options.file}`);
        
        const result = await pipeline.processFile(options.file, {
          recordDate: options.recordDate,
          custodyType: options.custodyType,
          skipLoading: options.skipLoading
        });

        if (result.success) {
          console.log('✅ File processing completed successfully');
          console.log(`📊 Records: ${result.stats.totalRecords} total, ${result.stats.loadedRecords} loaded`);
          
          if (options.skipLoading && result.data) {
            console.log('\n📋 Sample Normalized Data:');
            result.data.normalizedRecords.slice(0, 2).forEach((record, index) => {
              console.log(`   Record ${index + 1}:`);
              Object.entries(record).forEach(([key, value]) => {
                if (value !== null) {
                  console.log(`     ${key}: ${value}`);
                }
              });
            });
          }
        } else {
          console.error('❌ File processing failed:', result.error);
          if (result.details) {
            console.error('📋 Error details:', result.details);
          }
          process.exit(1);
        }
      }
    }

  } catch (error) {
    console.error('❌ Unexpected error:', error.message);
    console.error(error.stack);
    process.exit(1);
  } finally {
    await pipeline.close();
  }
}

// Run the main function
main().catch(error => {
  console.error('❌ Fatal error:', error.message);
  process.exit(1);
}); 