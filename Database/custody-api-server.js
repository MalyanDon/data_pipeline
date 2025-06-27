const express = require('express');
const path = require('path');
const config = require('./config');

// Import API routes
const custodyNormalizationRoutes = require('./api/custody/normalizationControl');

const app = express();
const PORT = process.env.PORT || 3003;

// Middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  next();
});
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Static files (for dashboard if needed)
app.use('/static', express.static(path.join(__dirname, 'public')));

// API Routes
app.use('/api/custody', custodyNormalizationRoutes);

// Root endpoint with API documentation
app.get('/', (req, res) => {
  res.json({
    service: 'Custody Normalization API',
    version: '1.0.0',
    description: 'API for processing and normalizing custody files into PostgreSQL',
    endpoints: {
      health: 'GET /api/custody/health',
      initDatabase: 'POST /api/custody/init-database',
      processDirectory: 'POST /api/custody/process-directory',
      processFile: 'POST /api/custody/process-file/:filename',
      uploadAndProcess: 'POST /api/custody/upload-and-process',
      previewFile: 'GET /api/custody/preview/:filename',
      queryData: 'GET /api/custody/unified-data',
      getStats: 'GET /api/custody/stats',
      getClientData: 'GET /api/custody/client/:clientRef',
      getInstrumentData: 'GET /api/custody/instrument/:isin',
      getMappings: 'GET /api/custody/mappings/:custodyType'
    },
    supportedCustodyTypes: ['axis', 'deutsche', 'trustpms', 'hdfc', 'kotak', 'orbis'],
    documentation: {
      commandLine: 'node scripts/process-custody-files.js --help',
      testing: 'node scripts/test-normalization.js',
      examples: {
        initDatabase: 'POST /api/custody/init-database',
        processFile: 'POST /api/custody/process-file/axis_custody.xlsx {"recordDate": "2025-06-25"}',
        queryData: 'GET /api/custody/unified-data?source_system=AXIS&limit=10',
        getStats: 'GET /api/custody/stats'
      }
    }
  });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'custody-normalization-api',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('❌ API Error:', err.message);
  res.status(500).json({
    success: false,
    error: err.message,
    timestamp: new Date().toISOString()
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    availableEndpoints: [
      'GET /',
      'GET /health',
      'GET /api/custody/health',
      'POST /api/custody/init-database',
      'GET /api/custody/stats',
      'POST /api/custody/process-directory',
      'POST /api/custody/upload-and-process'
    ]
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🏦 Custody Normalization API Server`);
  console.log(`🚀 Server running on: http://localhost:${PORT}`);
  console.log(`📊 API Documentation: http://localhost:${PORT}`);
  console.log(`❤️ Health Check: http://localhost:${PORT}/health`);
  console.log(`🔧 Initialize DB: POST http://localhost:${PORT}/api/custody/init-database`);
  console.log(`📈 Statistics: GET http://localhost:${PORT}/api/custody/stats`);
  console.log(`📄 Process Files: POST http://localhost:${PORT}/api/custody/upload-and-process`);
  console.log('');
  console.log('🛠️ Command Line Tools:');
  console.log('   📋 Process files: node scripts/process-custody-files.js --help');
  console.log('   🧪 Run tests: node scripts/test-normalization.js');
  console.log('');
  console.log('📂 Supported File Types:');
  console.log('   • Axis EOD Custody (.xlsx)');
  console.log('   • Deutsche Bank (.xlsx)');
  console.log('   • Trust PMS (.xls)');
  console.log('   • HDFC Custody (.csv)');
  console.log('   • Kotak Custody (.xlsx)');
  console.log('   • Orbis Custody (.xlsx)');
});

module.exports = app; 