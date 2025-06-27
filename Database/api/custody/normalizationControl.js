const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs').promises;

const CustodyNormalizationPipeline = require('../../custody-normalization/pipeline/custodyNormalizationPipeline');
const NormalizationSchema = require('../../custody-normalization/config/normalization-schema');
const PostgresLoader = require('../../custody-normalization/loaders/postgresLoader');

const router = express.Router();

// Configure multer for file uploads
const upload = multer({
  dest: 'temp_uploads/',
  limits: {
    fileSize: 50 * 1024 * 1024, // 50MB limit
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['.xlsx', '.xls', '.csv'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel and CSV files are allowed'), false);
    }
  }
});

// Initialize services
const pipeline = new CustodyNormalizationPipeline();
const schema = new NormalizationSchema();
const loader = new PostgresLoader();

/**
 * POST /api/custody/process-directory
 * Process all custody files in a directory
 */
router.post('/process-directory', async (req, res) => {
  try {
    const { directoryPath, recordDate, skipLoading = false } = req.body;
    
    if (!directoryPath) {
      return res.status(400).json({
        success: false,
        error: 'Directory path is required'
      });
    }

    if (!fs.promises.access(directoryPath)) {
      return res.status(400).json({
        success: false,
        error: 'Directory not found'
      });
    }

    console.log(`📂 API: Processing directory ${directoryPath}`);
    
    const result = await pipeline.processDirectory(directoryPath, {
      recordDate,
      skipLoading
    });

    res.json({
      success: result.success,
      message: result.success ? 'Directory processed successfully' : 'Directory processing failed',
      stats: result.stats,
      fileResults: result.fileResults,
      error: result.error
    });

  } catch (error) {
    console.error('❌ API Error in process-directory:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * POST /api/custody/process-file/:filename
 * Process a single custody file by filename
 */
router.post('/process-file/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const { directoryPath = './temp_uploads', recordDate, custodyType, skipLoading = false } = req.body;
    
    const filePath = path.join(directoryPath, filename);
    
    if (!fs.promises.access(filePath)) {
      return res.status(404).json({
        success: false,
        error: 'File not found'
      });
    }

    console.log(`📄 API: Processing file ${filename}`);
    
    const result = await pipeline.processFile(filePath, {
      recordDate,
      custodyType,
      skipLoading
    });

    res.json({
      success: result.success,
      message: result.success ? 'File processed successfully' : 'File processing failed',
      stats: result.stats,
      data: result.data,
      error: result.error,
      step: result.step
    });

  } catch (error) {
    console.error('❌ API Error in process-file:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * POST /api/custody/upload-and-process
 * Upload and process custody files
 */
router.post('/upload-and-process', upload.array('files'), async (req, res) => {
  try {
    const { recordDate, skipLoading = false } = req.body;
    const files = req.files;

    if (!files || files.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'No files uploaded'
      });
    }

    console.log(`📤 API: Processing ${files.length} uploaded files`);
    
    const results = [];
    
    for (const file of files) {
      try {
        const result = await pipeline.processFile(file.path, {
          recordDate,
          skipLoading
        });
        
        results.push({
          fileName: file.originalname,
          ...result
        });
        
        // Clean up uploaded file
        await fs.unlink(file.path);
        
      } catch (error) {
        results.push({
          fileName: file.originalname,
          success: false,
          error: error.message
        });
        
        // Clean up file on error
        try { await fs.unlink(file.path); } catch(e) {}
      }
    }

    const successCount = results.filter(r => r.success).length;
    const totalRecords = results.reduce((sum, r) => sum + (r.stats?.loadedRecords || 0), 0);

    res.json({
      success: successCount === files.length,
      message: `${successCount}/${files.length} files processed successfully`,
      totalRecords,
      fileResults: results
    });

  } catch (error) {
    console.error('❌ API Error in upload-and-process:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/preview/:filename
 * Preview a custody file without processing
 */
router.get('/preview/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const { directoryPath = './temp_uploads', maxRows = 10 } = req.query;
    
    const filePath = path.join(directoryPath, filename);
    
    if (!fs.promises.access(filePath)) {
      return res.status(404).json({
        success: false,
        error: 'File not found'
      });
    }

    console.log(`👁️ API: Previewing file ${filename}`);
    
    const result = await pipeline.previewFile(filePath, parseInt(maxRows));

    res.json(result);

  } catch (error) {
    console.error('❌ API Error in preview:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/unified-data
 * Query unified custody data with filters
 */
router.get('/unified-data', async (req, res) => {
  try {
    const {
      client_reference,
      instrument_isin,
      source_system,
      record_date_from,
      record_date_to,
      client_name_search,
      instrument_name_search,
      limit = 100,
      offset = 0,
      sortField = 'created_at',
      sortOrder = 'desc'
    } = req.query;

    const filters = {};
    const options = {
      limit: parseInt(limit),
      offset: parseInt(offset),
      sortField,
      sortOrder
    };

    // Build filters
    if (client_reference) filters.client_reference = client_reference;
    if (instrument_isin) filters.instrument_isin = instrument_isin;
    if (source_system) filters.source_system = source_system;
    if (record_date_from) filters.record_date_from = record_date_from;
    if (record_date_to) filters.record_date_to = record_date_to;
    if (client_name_search) filters.client_name_search = client_name_search;
    if (instrument_name_search) filters.instrument_name_search = instrument_name_search;

    console.log(`🔍 API: Querying unified data with filters:`, filters);
    
    const result = await pipeline.queryData(filters, options);

    res.json({
      success: true,
      ...result
    });

  } catch (error) {
    console.error('❌ API Error in unified-data:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/stats
 * Get processing statistics and data counts
 */
router.get('/stats', async (req, res) => {
  try {
    console.log(`📊 API: Getting database statistics`);
    
    const stats = await pipeline.getDatabaseStats();

    if (stats.success === false) {
      return res.status(500).json({
        success: false,
        error: stats.error
      });
    }

    res.json({
      success: true,
      stats
    });

  } catch (error) {
    console.error('❌ API Error in stats:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/client/:clientRef
 * Get all instruments for a specific client
 */
router.get('/client/:clientRef', async (req, res) => {
  try {
    const { clientRef } = req.params;
    const { recordDate } = req.query;

    console.log(`👤 API: Getting instruments for client ${clientRef}`);
    
    const instruments = await pipeline.postgresLoader.getInstrumentsByClient(clientRef, recordDate);

    res.json({
      success: true,
      clientReference: clientRef,
      recordDate,
      instruments,
      count: instruments.length
    });

  } catch (error) {
    console.error('❌ API Error in client query:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/instrument/:isin
 * Get all clients holding a specific instrument
 */
router.get('/instrument/:isin', async (req, res) => {
  try {
    const { isin } = req.params;
    const { recordDate } = req.query;

    console.log(`📊 API: Getting clients for instrument ${isin}`);
    
    const clients = await pipeline.postgresLoader.getClientsByInstrument(isin, recordDate);

    res.json({
      success: true,
      instrumentIsin: isin,
      recordDate,
      clients,
      count: clients.length
    });

  } catch (error) {
    console.error('❌ API Error in instrument query:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * POST /api/custody/init-database
 * Initialize the PostgreSQL database schema
 */
router.post('/init-database', async (req, res) => {
  try {
    console.log(`🔧 API: Initializing database`);
    
    const result = await pipeline.initializeDatabase();

    res.json(result);

  } catch (error) {
    console.error('❌ API Error in init-database:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/mappings/:custodyType
 * Get field mapping configuration for a custody type
 */
router.get('/mappings/:custodyType', async (req, res) => {
  try {
    const { custodyType } = req.params;

    console.log(`🗺️ API: Getting mappings for ${custodyType}`);
    
    const mappings = pipeline.getMappingSummary(custodyType);

    res.json({
      success: true,
      mappings
    });

  } catch (error) {
    console.error('❌ API Error in mappings:', error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/health
 * Health check endpoint
 */
router.get('/health', async (req, res) => {
  try {
    const overallStats = await loader.getOverallStats();
    
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      database: {
        connected: true,
        totalTables: overallStats.totalTables,
        totalRecords: overallStats.totalRecords,
        dateRange: overallStats.dateRange
      },
      architecture: 'daily-tables'
    });

  } catch (error) {
    console.error('❌ Health check failed:', error);
    res.status(500).json({
      status: 'unhealthy',
      error: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

/**
 * GET /api/custody/client/:clientRef/financial
 * Get financial data for specific client
 */
router.get('/client/:clientRef/financial', async (req, res) => {
  try {
    const { clientRef } = req.params;
    const { limit = 100, offset = 0 } = req.query;

    console.log(`🔍 API: Getting financial data for client: ${clientRef}`);

    const records = await pipeline.postgresLoader.getRecordsByClient(
      clientRef, 
      parseInt(limit), 
      parseInt(offset)
    );

    res.json({
      success: true,
      client_reference: clientRef,
      records,
      count: records.length,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (error) {
    console.error('❌ Error getting client financial data:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve client financial data',
      details: error.message
    });
  }
});

/**
 * GET /api/custody/blocked-holdings
 * Get all records with blocked quantities > 0
 */
router.get('/blocked-holdings', async (req, res) => {
  try {
    const { limit = 100, offset = 0 } = req.query;

    console.log(`🔍 API: Getting blocked holdings`);

    const records = await pipeline.postgresLoader.getBlockedHoldings(
      parseInt(limit), 
      parseInt(offset)
    );

    res.json({
      success: true,
      records,
      count: records.length,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (error) {
    console.error('❌ Error getting blocked holdings:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve blocked holdings',
      details: error.message
    });
  }
});

/**
 * GET /api/custody/pending-transactions
 * Get all records with pending buy/sell > 0
 */
router.get('/pending-transactions', async (req, res) => {
  try {
    const { limit = 100, offset = 0 } = req.query;

    console.log(`🔍 API: Getting pending transactions`);

    const records = await pipeline.postgresLoader.getPendingTransactions(
      parseInt(limit), 
      parseInt(offset)
    );

    res.json({
      success: true,
      records,
      count: records.length,
      limit: parseInt(limit),
      offset: parseInt(offset)
    });

  } catch (error) {
    console.error('❌ Error getting pending transactions:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve pending transactions',
      details: error.message
    });
  }
});

/**
 * GET /api/custody/financial-summary/:clientRef
 * Get aggregated financial summary for client
 */
router.get('/financial-summary/:clientRef', async (req, res) => {
  try {
    const { clientRef } = req.params;

    console.log(`🔍 API: Getting financial summary for client: ${clientRef}`);

    const summary = await pipeline.postgresLoader.getFinancialSummary(clientRef);

    if (!summary) {
      return res.status(404).json({
        success: false,
        error: 'Client not found',
        client_reference: clientRef
      });
    }

    res.json({
      success: true,
      client_reference: clientRef,
      summary
    });

  } catch (error) {
    console.error('❌ Error getting financial summary:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve financial summary',
      details: error.message
    });
  }
});

/**
 * GET /api/custody/enhanced-stats
 * Enhanced statistics with financial data
 */
router.get('/enhanced-stats', async (req, res) => {
  try {
    console.log('🔍 API: Getting enhanced statistics with financial data');

    const stats = await pipeline.postgresLoader.getStatistics();

    res.json({
      success: true,
      statistics: {
        total_records: parseInt(stats.total_records),
        unique_clients: parseInt(stats.unique_clients),
        unique_instruments: parseInt(stats.unique_instruments),
        source_systems: parseInt(stats.source_systems),
        date_range: {
          earliest: stats.earliest_date,
          latest: stats.latest_date
        },
        financial_data: {
          records_with_blocked_qty: parseInt(stats.records_with_blocked_qty),
          records_with_pending_buy: parseInt(stats.records_with_pending_buy),
          records_with_pending_sell: parseInt(stats.records_with_pending_sell),
          avg_blocked_quantity: parseFloat(stats.avg_blocked_quantity || 0),
          avg_pending_buy_quantity: parseFloat(stats.avg_pending_buy_quantity || 0),
          avg_pending_sell_quantity: parseFloat(stats.avg_pending_sell_quantity || 0)
        }
      },
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('❌ Error getting enhanced statistics:', error.message);
    res.status(500).json({
      success: false,
      error: 'Failed to retrieve enhanced statistics',
      details: error.message
    });
  }
});

/**
 * POST /api/custody/upload
 * Upload and process a custody file
 */
router.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const { originalname, path: filePath } = req.file;
    console.log(`📤 Processing custody file: ${originalname}`);

    const result = await pipeline.processFile(filePath, originalname);

    // Clean up uploaded file
    await fs.unlink(filePath);

    if (result.success) {
      res.json({
        success: true,
        message: 'File processed successfully',
        fileName: originalname,
        summary: {
          totalRecords: result.totalRecords,
          validRecords: result.validRecords,
          invalidRecords: result.invalidRecords,
          sourceSystem: result.sourceSystem,
          dateGroups: result.dateGroups || 1,
          recordsInserted: result.recordsInserted,
          recordsUpdated: result.recordsUpdated
        },
        errors: result.errors,
        recommendations: result.recommendations
      });
    } else {
      res.status(500).json({
        success: false,
        error: result.error,
        fileName: originalname
      });
    }

  } catch (error) {
    console.error('❌ Upload processing failed:', error);
    
    // Clean up file if it exists
    if (req.file?.path) {
      try {
        await fs.unlink(req.file.path);
      } catch (cleanupError) {
        console.error('Failed to cleanup file:', cleanupError);
      }
    }

    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/daily-stats/:date
 * Get daily statistics for a specific date
 */
router.get('/daily-stats/:date', async (req, res) => {
  try {
    const { date } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const stats = await loader.getDailyStats(date);
    res.json(stats);

  } catch (error) {
    console.error('❌ Failed to get daily stats:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/overall-stats
 * Get overall statistics across all daily tables
 */
router.get('/overall-stats', async (req, res) => {
  try {
    const stats = await loader.getOverallStats();
    res.json(stats);

  } catch (error) {
    console.error('❌ Failed to get overall stats:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/daily-tables
 * List all available daily tables
 */
router.get('/daily-tables', async (req, res) => {
  try {
    const tables = await schema.getAllDailyTables();
    
    // Extract dates from table names for easier consumption
    const tableInfo = tables.map(tableName => {
      const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
      if (match) {
        const [, year, month, day] = match;
        return {
          tableName,
          date: `${year}-${month}-${day}`,
          year: parseInt(year),
          month: parseInt(month),
          day: parseInt(day)
        };
      }
      return { tableName, date: null };
    });

    res.json({
      totalTables: tables.length,
      tables: tableInfo
    });

  } catch (error) {
    console.error('❌ Failed to list daily tables:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/query/:date
 * Query data from a specific date
 */
router.get('/query/:date', async (req, res) => {
  try {
    const { date } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const filters = {
      clientReference: req.query.client,
      instrumentIsin: req.query.isin,
      sourceSystem: req.query.source,
      hasBlocked: req.query.blocked === 'true',
      hasPending: req.query.pending === 'true',
      limit: parseInt(req.query.limit) || 1000
    };

    const result = await loader.queryByDate(date, filters);
    res.json(result);

  } catch (error) {
    console.error('❌ Failed to query data:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /api/custody/migrate-to-daily
 * Initialize/migrate to daily tables
 */
router.post('/migrate-to-daily', async (req, res) => {
  try {
    console.log('🔄 Starting migration to daily tables...');
    
    const result = await schema.migrateToDateBasedTables();
    
    res.json({
      success: true,
      message: 'Migration to daily tables completed',
      migrated: result.migrated,
      dailyTables: result.dailyTables
    });

  } catch (error) {
    console.error('❌ Migration failed:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * POST /api/custody/create-daily-table/:date
 * Create a daily table for a specific date
 */
router.post('/create-daily-table/:date', async (req, res) => {
  try {
    const { date } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const result = await schema.createDailyTable(date);
    
    res.json({
      success: true,
      message: `Daily table created for date: ${date}`,
      tableName: result.tableName
    });

  } catch (error) {
    console.error('❌ Failed to create daily table:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

/**
 * GET /api/custody/daily-financial/:date/:clientRef
 * Get financial data for a specific client on a specific date
 */
router.get('/daily-financial/:date/:clientRef', async (req, res) => {
  try {
    const { date, clientRef } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const result = await loader.queryByDate(date, {
      clientReference: clientRef,
      limit: 10000
    });

    if (!result.exists) {
      return res.status(404).json({ 
        error: `No data table exists for date: ${date}` 
      });
    }

    // Calculate financial summary
    const financialSummary = result.records.reduce((summary, record) => {
      summary.totalBlocked += parseFloat(record.blocked_quantity || 0);
      summary.totalPendingBuy += parseFloat(record.pending_buy_quantity || 0);
      summary.totalPendingSell += parseFloat(record.pending_sell_quantity || 0);
      summary.totalInstruments++;
      
      if (record.blocked_quantity > 0) summary.instrumentsWithBlocked++;
      if (record.pending_buy_quantity > 0) summary.instrumentsWithPendingBuy++;
      if (record.pending_sell_quantity > 0) summary.instrumentsWithPendingSell++;
      
      return summary;
    }, {
      totalBlocked: 0,
      totalPendingBuy: 0,
      totalPendingSell: 0,
      totalInstruments: 0,
      instrumentsWithBlocked: 0,
      instrumentsWithPendingBuy: 0,
      instrumentsWithPendingSell: 0
    });

    res.json({
      date,
      clientReference: clientRef,
      summary: financialSummary,
      holdings: result.records
    });

  } catch (error) {
    console.error('❌ Failed to get daily financial data:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/daily-blocked/:date
 * Get all blocked holdings for a specific date
 */
router.get('/daily-blocked/:date', async (req, res) => {
  try {
    const { date } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const result = await loader.queryByDate(date, {
      hasBlocked: true,
      limit: parseInt(req.query.limit) || 1000
    });

    if (!result.exists) {
      return res.status(404).json({ 
        error: `No data table exists for date: ${date}` 
      });
    }

    res.json({
      date,
      totalRecordsWithBlocked: result.records.length,
      blockedHoldings: result.records.map(record => ({
        clientReference: record.client_reference,
        clientName: record.client_name,
        instrumentIsin: record.instrument_isin,
        instrumentName: record.instrument_name,
        blockedQuantity: record.blocked_quantity,
        sourceSystem: record.source_system
      }))
    });

  } catch (error) {
    console.error('❌ Failed to get blocked holdings:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/daily-pending/:date
 * Get all pending transactions for a specific date
 */
router.get('/daily-pending/:date', async (req, res) => {
  try {
    const { date } = req.params;
    
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const result = await loader.queryByDate(date, {
      hasPending: true,
      limit: parseInt(req.query.limit) || 1000
    });

    if (!result.exists) {
      return res.status(404).json({ 
        error: `No data table exists for date: ${date}` 
      });
    }

    res.json({
      date,
      totalRecordsWithPending: result.records.length,
      pendingTransactions: result.records.map(record => ({
        clientReference: record.client_reference,
        clientName: record.client_name,
        instrumentIsin: record.instrument_isin,
        instrumentName: record.instrument_name,
        pendingBuyQuantity: record.pending_buy_quantity,
        pendingSellQuantity: record.pending_sell_quantity,
        sourceSystem: record.source_system
      }))
    });

  } catch (error) {
    console.error('❌ Failed to get pending transactions:', error);
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /api/custody/date-range-summary
 * Get date range summary
 */
router.get('/date-range-summary', async (req, res) => {
  try {
    const { startDate, endDate } = req.query;
    
    if (!startDate || !endDate) {
      return res.status(400).json({ 
        error: 'Both startDate and endDate query parameters are required (YYYY-MM-DD format)' 
      });
    }

    // Validate date formats
    if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate) || !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      return res.status(400).json({ error: 'Invalid date format. Use YYYY-MM-DD' });
    }

    const tables = await schema.getAllDailyTables();
    const dateRangeTables = tables.filter(tableName => {
      const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
      if (match) {
        const [, year, month, day] = match;
        const tableDate = `${year}-${month}-${day}`;
        return tableDate >= startDate && tableDate <= endDate;
      }
      return false;
    });

    const dailySummaries = [];
    
    for (const tableName of dateRangeTables) {
      const match = tableName.match(/unified_custody_master_(\d{4})_(\d{2})_(\d{2})/);
      if (match) {
        const [, year, month, day] = match;
        const date = `${year}-${month}-${day}`;
        const stats = await loader.getDailyStats(date);
        dailySummaries.push(stats);
      }
    }

    // Calculate overall summary for the range
    const overallSummary = dailySummaries.reduce((total, daily) => {
      total.totalRecords += daily.totalRecords;
      total.totalClients += daily.uniqueClients;
      total.totalInstruments += daily.uniqueInstruments;
      total.totalBlocked += daily.recordsWithBlocked;
      total.totalPendingBuy += daily.recordsWithPendingBuy;
      total.totalPendingSell += daily.recordsWithPendingSell;
      
      // Collect unique source systems
      daily.sourceSystems.forEach(system => {
        if (!total.sourceSystems.includes(system)) {
          total.sourceSystems.push(system);
        }
      });
      
      return total;
    }, {
      totalRecords: 0,
      totalClients: 0,
      totalInstruments: 0,
      totalBlocked: 0,
      totalPendingBuy: 0,
      totalPendingSell: 0,
      sourceSystems: []
    });

    res.json({
      dateRange: { startDate, endDate },
      daysFound: dailySummaries.length,
      overallSummary,
      dailySummaries
    });

  } catch (error) {
    console.error('❌ Failed to get date range summary:', error);
    res.status(500).json({ error: error.message });
  }
});

// Error handling middleware
router.use((err, req, res, next) => {
  console.error('❌ API Error:', err.message);
  
  if (err instanceof multer.MulterError) {
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({
        success: false,
        error: 'File size too large (max 50MB)'
      });
    }
  }
  
  res.status(500).json({
    success: false,
    error: err.message
  });
});

module.exports = router; 