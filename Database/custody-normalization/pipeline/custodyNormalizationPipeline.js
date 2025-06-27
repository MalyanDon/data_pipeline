const path = require('path');
const CustodyFileReader = require('../extractors/custodyFileReader');
const FieldMapper = require('../extractors/fieldMapper');
const DataNormalizer = require('../extractors/dataNormalizer');
const PostgresLoader = require('../loaders/postgresLoader');
const NormalizationSchema = require('../config/normalization-schema');

class CustodyNormalizationPipeline {
  constructor(pgPool) {
    this.pgPool = pgPool;
    this.fileReader = new CustodyFileReader();
    this.fieldMapper = new FieldMapper();
    this.dataNormalizer = new DataNormalizer();
    this.postgresLoader = new PostgresLoader(pgPool);
    this.schema = new NormalizationSchema(pgPool);
    
    this.pipelineStats = {
      totalFiles: 0,
      processedFiles: 0,
      errorFiles: 0,
      totalRecords: 0,
      normalizedRecords: 0,
      loadedRecords: 0,
      processingTime: 0,
      errors: [],
      warnings: []
    };
  }

  /**
   * Process all custody files in a directory
   * @param {string} directoryPath - Directory containing custody files
   * @param {Object} options - Processing options
   * @returns {Promise<Object>} - Processing results
   */
  async processDirectory(directoryPath, options = {}) {
    console.log(`🗂️ Starting directory processing: ${directoryPath}`);
    
    const startTime = Date.now();
    this.resetStats();

    try {
      // Scan for custody files
      const custodyFiles = await this.fileReader.scanCustodyFiles(directoryPath);
      
      if (custodyFiles.length === 0) {
        return {
          success: false,
          message: 'No custody files found in directory',
          stats: this.getStats()
        };
      }

      console.log(`📊 Found ${custodyFiles.length} custody files`);
      this.pipelineStats.totalFiles = custodyFiles.length;

      // Process each file
      const fileResults = [];
      
      for (const fileInfo of custodyFiles) {
        console.log(`\n🔄 Processing: ${fileInfo.fileName} (${fileInfo.custodyType})`);
        
        const fileResult = await this.processFile(fileInfo.filePath, {
          ...options,
          recordDate: options.recordDate || this.extractDateFromFile(fileInfo.fileName)
        });
        
        fileResults.push({
          fileName: fileInfo.fileName,
          custodyType: fileInfo.custodyType,
          ...fileResult
        });

        if (fileResult.success) {
          this.pipelineStats.processedFiles++;
          this.pipelineStats.totalRecords += fileResult.stats.totalRecords || 0;
          this.pipelineStats.normalizedRecords += fileResult.stats.normalizedRecords || 0;
          this.pipelineStats.loadedRecords += fileResult.stats.loadedRecords || 0;
        } else {
          this.pipelineStats.errorFiles++;
          this.pipelineStats.errors.push({
            fileName: fileInfo.fileName,
            error: fileResult.error
          });
        }
      }

      this.pipelineStats.processingTime = Date.now() - startTime;

      console.log(`\n✅ Directory processing completed:`);
      console.log(`   📁 Files processed: ${this.pipelineStats.processedFiles}/${this.pipelineStats.totalFiles}`);
      console.log(`   📊 Records loaded: ${this.pipelineStats.loadedRecords}`);
      console.log(`   ⏱️ Processing time: ${Math.round(this.pipelineStats.processingTime / 1000)}s`);

      return {
        success: this.pipelineStats.errorFiles === 0,
        fileResults,
        stats: this.getStats()
      };

    } catch (error) {
      console.error('❌ Directory processing failed:', error.message);
      return {
        success: false,
        error: error.message,
        stats: this.getStats()
      };
    }
  }

  /**
   * Process a single custody file
   * @param {string} filePath - Path to custody file
   * @param {Object} options - Processing options
   * @returns {Promise<Object>} - Processing results
   */
  async processFile(filePath, options = {}) {
    const fileName = path.basename(filePath);
    const startTime = Date.now();

    try {
      console.log(`📖 Reading file: ${fileName}`);
      
      // Step 1: Read custody file
      const readResult = await this.fileReader.readCustodyFile(filePath);
      if (!readResult.success) {
        return {
          success: false,
          error: `File reading failed: ${readResult.error}`,
          step: 'read'
        };
      }

      const { type: custodyType, data: rawRecords, metadata } = readResult;
      console.log(`   📊 ${rawRecords.length} records read`);

      // Add record date to metadata
      metadata.recordDate = options.recordDate || this.extractDateFromFile(fileName);
      if (!metadata.recordDate) {
        return {
          success: false,
          error: 'Could not determine record date. Please provide recordDate in options.',
          step: 'date_extraction'
        };
      }

      console.log(`   📅 Record date: ${metadata.recordDate}`);

      // Step 2: Map fields to standard format
      console.log(`🗺️ Mapping fields for ${custodyType} format`);
      const mappingResult = this.fieldMapper.mapRecords(rawRecords, custodyType, metadata);
      
      if (!mappingResult.success && mappingResult.errorCount > 0) {
        return {
          success: false,
          error: `Field mapping failed: ${mappingResult.errorCount} errors`,
          step: 'mapping',
          details: mappingResult.errors
        };
      }

      console.log(`   ✅ ${mappingResult.successCount}/${mappingResult.totalRecords} records mapped`);
      if (mappingResult.warningCount > 0) {
        console.log(`   ⚠️ ${mappingResult.warningCount} warnings`);
      }

      // Step 3: Normalize and validate data
      console.log(`🔧 Normalizing data`);
      const normalizationResult = this.dataNormalizer.normalizeRecords(mappingResult.mappedRecords);
      
      if (!normalizationResult.success) {
        return {
          success: false,
          error: `Data normalization failed: ${normalizationResult.invalidCount} validation errors`,
          step: 'normalization',
          details: normalizationResult.errors
        };
      }

      console.log(`   ✅ ${normalizationResult.validCount}/${normalizationResult.totalRecords} records normalized`);

      // Step 4: Load into PostgreSQL
      if (options.skipLoading) {
        console.log(`⏭️ Skipping PostgreSQL loading (skipLoading = true)`);
        return {
          success: true,
          message: 'File processed successfully (loading skipped)',
          stats: {
            totalRecords: rawRecords.length,
            mappedRecords: mappingResult.successCount,
            normalizedRecords: normalizationResult.validCount,
            loadedRecords: 0,
            processingTime: Date.now() - startTime
          },
          data: {
            rawRecords: rawRecords.slice(0, 5), // Sample
            normalizedRecords: normalizationResult.normalizedRecords.slice(0, 5) // Sample
          }
        };
      }

      console.log(`💾 Loading ${normalizationResult.normalizedRecords.length} records into PostgreSQL`);
      const loadResult = await this.postgresLoader.loadRecords(normalizationResult.normalizedRecords);
      
      if (!loadResult.success) {
        return {
          success: false,
          error: `PostgreSQL loading failed: ${loadResult.error}`,
          step: 'loading',
          details: loadResult.stats
        };
      }

      const loadStats = loadResult.stats;
      console.log(`   ✅ ${loadStats.insertedRecords} inserted, ${loadStats.updatedRecords} updated`);

      return {
        success: true,
        message: 'File processed successfully',
        stats: {
          totalRecords: rawRecords.length,
          mappedRecords: mappingResult.successCount,
          normalizedRecords: normalizationResult.validCount,
          loadedRecords: loadStats.insertedRecords + loadStats.updatedRecords,
          insertedRecords: loadStats.insertedRecords,
          updatedRecords: loadStats.updatedRecords,
          processingTime: Date.now() - startTime
        }
      };

    } catch (error) {
      console.error(`❌ Error processing file ${fileName}:`, error.message);
      return {
        success: false,
        error: error.message,
        step: 'unknown'
      };
    }
  }

  /**
   * Get file preview without processing
   * @param {string} filePath - Path to custody file
   * @param {number} maxRows - Maximum rows to preview
   * @returns {Promise<Object>} - Preview results
   */
  async previewFile(filePath, maxRows = 10) {
    try {
      const fileName = path.basename(filePath);
      console.log(`👁️ Previewing file: ${fileName}`);

      // Read file sample
      const readResult = await this.fileReader.getFileSample(filePath, maxRows);
      if (!readResult.success) {
        return {
          success: false,
          error: readResult.error
        };
      }

      const { type: custodyType, data: rawRecords, metadata } = readResult;

      // Analyze field mappings
      const analysis = this.fieldMapper.analyzeFieldMappings(rawRecords, custodyType);

      return {
        success: true,
        fileName,
        custodyType,
        metadata,
        sampleData: rawRecords,
        fieldAnalysis: analysis,
        recommendations: analysis.recommendations
      };

    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Initialize the PostgreSQL database
   * @returns {Promise<Object>} - Initialization result
   */
  async initializeDatabase() {
    console.log('🔧 Initializing PostgreSQL database for custody data...');
    
    try {
      // Test connection first
      const connectionTest = await this.postgresLoader.testConnection();
      if (!connectionTest.success) {
        return {
          success: false,
          error: `PostgreSQL connection failed: ${connectionTest.error}`
        };
      }

      // Initialize database schema
      const initResult = await this.postgresLoader.initializeDatabase();
      return initResult;

    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Get database statistics
   * @returns {Promise<Object>} - Database statistics
   */
  async getDatabaseStats() {
    try {
      return await this.postgresLoader.getDatabaseStats();
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Query unified custody data
   * @param {Object} filters - Query filters
   * @param {Object} options - Query options
   * @returns {Promise<Object>} - Query results
   */
  async queryData(filters = {}, options = {}) {
    try {
      return await this.postgresLoader.queryRecords(filters, options);
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Extract date from filename
   * @param {string} fileName - File name
   * @returns {string|null} - Extracted date in YYYY-MM-DD format
   */
  extractDateFromFile(fileName) {
    // Try different date patterns
    const patterns = [
      /(\d{4})[_-](\d{2})[_-](\d{2})/,  // YYYY_MM_DD or YYYY-MM-DD
      /(\d{2})[_-](\d{2})[_-](\d{4})/,  // DD_MM_YYYY or DD-MM-YYYY
      /(\d{4})(\d{2})(\d{2})/,          // YYYYMMDD
      /(\d{2})(\d{2})(\d{4})/           // DDMMYYYY
    ];

    for (const pattern of patterns) {
      const match = fileName.match(pattern);
      if (match) {
        let year, month, day;
        
        if (pattern.source.includes('(\\d{4})')) {
          // YYYY format first
          if (match[1].length === 4) {
            [, year, month, day] = match;
          } else {
            [, day, month, year] = match;
          }
        } else {
          // Check which group is the year
          if (match[3].length === 4) {
            [, day, month, year] = match;
          } else if (match[1].length === 4) {
            [, year, month, day] = match;
          }
        }

        // Validate date components
        if (year && month && day) {
          const date = new Date(year, month - 1, day);
          if (date.getFullYear() == year && date.getMonth() == month - 1 && date.getDate() == day) {
            return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
          }
        }
      }
    }

    return null;
  }

  /**
   * Get field mapping summary for a custody type
   * @param {string} custodyType - Custody type
   * @returns {Object} - Mapping summary
   */
  getMappingSummary(custodyType) {
    return this.fieldMapper.getMappingSummary(custodyType);
  }

  /**
   * Reset pipeline statistics
   */
  resetStats() {
    this.pipelineStats = {
      totalFiles: 0,
      processedFiles: 0,
      errorFiles: 0,
      totalRecords: 0,
      normalizedRecords: 0,
      loadedRecords: 0,
      processingTime: 0,
      errors: [],
      warnings: []
    };
  }

  /**
   * Get pipeline statistics
   * @returns {Object} - Pipeline statistics
   */
  getStats() {
    return { ...this.pipelineStats };
  }

  /**
   * Close all connections
   */
  async close() {
    await this.postgresLoader.close();
  }

  /**
   * Process a batch of records through the normalization pipeline
   */
  async processBatch(batch, custodyType, collectionName) {
    try {
      const normalizedRecords = [];
      let validCount = 0;
      let errorCount = 0;

      // Extract date from collection name (format: custodian_MM_DD)
      const match = collectionName.match(/_(\d{2})_(\d{2})$/);
      const recordDate = match ? `2025-${match[1]}-${match[2]}` : new Date().toISOString().split('T')[0];

      // Ensure daily table exists
      const tableExists = await this.schema.dailyTableExists(recordDate);
      if (!tableExists) {
        await this.schema.createDailyTable(recordDate);
      }

      // Process each record in the batch
      for (const record of batch) {
        try {
          // Prepare metadata
          const metadata = {
            sourceSystem: custodyType,
            fileName: collectionName,
            recordDate: recordDate
          };

          // Map fields based on custody type
          const mappingResult = this.fieldMapper.mapRecord(record, custodyType.toLowerCase(), metadata);
          
          if (mappingResult.mappedRecord) {
            // Normalize the mapped record
            const normalizationResult = this.dataNormalizer.normalizeRecord(mappingResult.mappedRecord);
            
            if (normalizationResult.success) {
              normalizedRecords.push(normalizationResult.normalizedRecord);
              validCount++;
            } else {
              errorCount++;
              console.log(`❌ Normalization failed for record: ${normalizationResult.errors?.join(', ')}`);
            }
          } else {
            errorCount++;
            console.log(`❌ Field mapping failed for record: ${mappingResult.errors?.join(', ')}`);
          }
        } catch (recordError) {
          errorCount++;
          console.error(`❌ Record processing error: ${recordError.message}`);
        }
      }

      // Bulk insert normalized records if any are valid
      if (normalizedRecords.length > 0) {
        const tableName = this.schema.getTableName(recordDate);
        
        try {
          await this.postgresLoader.loadBatchToTable(
            normalizedRecords,
            tableName,
            custodyType,
            collectionName
          );
          
          console.log(`✅ Batch loaded: ${normalizedRecords.length}/${batch.length} records successful`);
          
        } catch (loadError) {
          console.error(`❌ Batch load failed: ${loadError.message}`);
          // Mark all as errors if bulk insert fails
          errorCount += validCount;
          validCount = 0;
        }
      }

      return {
        validCount,
        errorCount,
        totalProcessed: batch.length,
        recordDate
      };

    } catch (error) {
      console.error(`❌ Pipeline batch processing failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Initialize the pipeline (create required tables, etc.)
   */
  async initialize() {
    try {
      await this.schema.initializeDatabase();
      console.log('✅ Custody normalization pipeline initialized');
    } catch (error) {
      console.error('❌ Pipeline initialization failed:', error.message);
      throw error;
    }
  }

  /**
   * Close pipeline connections
   */
  async close() {
    try {
      if (this.postgresLoader) {
        await this.postgresLoader.close();
      }
      if (this.schema) {
        await this.schema.close();
      }
    } catch (error) {
      console.error('❌ Pipeline cleanup error:', error.message);
    }
  }
}

module.exports = { CustodyNormalizationPipeline }; 