const express = require('express');
const mongoose = require('mongoose');
const { Client } = require('pg');
const config = require('./config');

const app = express();
const PORT = 3005;

app.use(express.json());

// CORS headers
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  next();
});

// PostgreSQL connection pool (more robust than single client)
const { Pool } = require('pg');
let pgPool = null;

async function initDB() {
  try {
    // Use connection pool instead of single client
    pgPool = new Pool({
      connectionString: config.postgresql.connectionString,
      max: 10, // Maximum number of clients in the pool
      idleTimeoutMillis: 30000, // Close idle clients after 30 seconds
      connectionTimeoutMillis: 10000, // Return error after 10 seconds if connection could not be established
      keepAlive: true, // Keep connection alive
      keepAliveInitialDelayMillis: 0
    });

    // Test the connection
    const testClient = await pgPool.connect();
    await testClient.query('SELECT 1');
    testClient.release();
    
    console.log('✅ Connected to PostgreSQL with connection pool');

    // Add error handling for the pool
    pgPool.on('error', (err) => {
      console.error('❌ Unexpected error on idle PostgreSQL client:', err.message);
    });

    pgPool.on('connect', () => {
      console.log('🔗 New PostgreSQL client connected');
    });
    
    // Connect to MongoDB
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB');
    
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
    // Don't exit, just log the error and continue
  }
}

// Main dashboard
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Custody Data Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #4CAF50; color: white; padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 20px; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .btn { background: #4CAF50; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        .btn:hover { background: #45a049; }
        .btn-primary { background: #007bff; }
        .btn-primary:hover { background: #0056b3; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #f0f0f0; }
        .badge { padding: 3px 8px; border-radius: 4px; font-size: 11px; }
        .badge-info { background: #17a2b8; color: white; }
        .badge-success { background: #28a745; color: white; }
        .badge-warning { background: #ffc107; color: black; }
        .pipeline { background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center; }
        .arrow { font-size: 24px; color: #007bff; margin: 0 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 Custody Data Pipeline Dashboard</h1>
            <p>MongoDB Raw Data ➜ ETL Processing ➜ PostgreSQL Normalized Data</p>
        </div>

        <div class="pipeline">
            <h3>📊 Data Flow Pipeline</h3>
            <div style="display: flex; align-items: center; justify-content: center;">
                <div style="background: white; padding: 15px; border-radius: 8px; margin: 10px;">
                    <h4>MongoDB</h4>
                    <div id="mongoCount">Loading...</div>
                    <span class="badge badge-info">Raw Files</span>
                </div>
                <span class="arrow">➜</span>
                <div style="background: white; padding: 15px; border-radius: 8px; margin: 10px;">
                    <h4>ETL Process</h4>
                    <button class="btn btn-primary" onclick="processData()">🚀 Process</button>
                </div>
                <span class="arrow">➜</span>
                <div style="background: white; padding: 15px; border-radius: 8px; margin: 10px;">
                    <h4>PostgreSQL</h4>
                    <div id="postgresCount">Loading...</div>
                    <span class="badge badge-success">Normalized</span>
                </div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h3>📊 MongoDB Raw Data</h3>
                <p><strong>Data Fields:</strong> Number of columns in each collection</p>
                <p><em>🔧 Orbis Corrections Applied: client_name="N/A", instrument_name=NULL, instrument_code=NULL</em></p>
                <button class="btn" onclick="loadMongoDB()">🔄 Refresh</button>
                <div id="mongoData">Loading...</div>
            </div>

            <div class="card">
                <h3>🗄️ PostgreSQL Processed Data</h3>
                <p><strong>Normalized:</strong> Clean, standardized custody data</p>
                <button class="btn" onclick="loadPostgreSQL()">🔄 Refresh</button>
                <div id="postgresData">Loading...</div>
            </div>
        </div>

        <div class="card">
            <h3>📈 Processing Status</h3>
            <div id="processingStatus">Ready to process data</div>
        </div>
    </div>

    <script>
        window.addEventListener('load', () => {
            loadMongoDB();
            loadPostgreSQL();
        });

        async function loadMongoDB() {
            try {
                const response = await fetch('/api/mongodb');
                const data = await response.json();
                
                if (data.success) {
                    let html = '<table><tr><th>Collection</th><th>Records</th><th>Data Fields</th><th>Type</th></tr>';
                    let totalRecords = 0;
                    
                    data.collections.forEach(collection => {
                        totalRecords += collection.recordCount;
                        const type = getTypeFromName(collection.name);
                        html += '<tr>';
                        html += '<td>' + collection.name + '</td>';
                        html += '<td>' + collection.recordCount.toLocaleString() + '</td>';
                        html += '<td><span class="badge badge-info">' + collection.fieldCount + ' fields</span></td>';
                        html += '<td><span class="badge badge-warning">' + type + '</span></td>';
                        html += '</tr>';
                    });
                    html += '</table>';
                    
                    document.getElementById('mongoData').innerHTML = html;
                    document.getElementById('mongoCount').innerHTML = totalRecords.toLocaleString() + ' records';
                } else {
                    document.getElementById('mongoData').innerHTML = 'Error: ' + data.error;
                }
            } catch (error) {
                document.getElementById('mongoData').innerHTML = 'Error: ' + error.message;
            }
        }

        async function loadPostgreSQL() {
            try {
                const response = await fetch('/api/postgresql');
                const data = await response.json();
                
                if (data.success) {
                    let html = '<table><tr><th>Table</th><th>Records</th><th>Type</th></tr>';
                    let totalRecords = 0;
                    
                    data.tables.forEach(table => {
                        totalRecords += table.recordCount;
                        html += '<tr>';
                        html += '<td>' + table.name + '</td>';
                        html += '<td>' + table.recordCount.toLocaleString() + '</td>';
                        html += '<td><span class="badge badge-success">' + table.type + '</span></td>';
                        html += '</tr>';
                    });
                    html += '</table>';
                    
                    document.getElementById('postgresData').innerHTML = html;
                    document.getElementById('postgresCount').innerHTML = totalRecords.toLocaleString() + ' records';
                } else {
                    document.getElementById('postgresData').innerHTML = 'Error: ' + data.error;
                }
            } catch (error) {
                document.getElementById('postgresData').innerHTML = 'Error: ' + error.message;
            }
        }

        async function processData() {
            document.getElementById('processingStatus').innerHTML = '🚀 Processing data... Please wait.';
            
            try {
                const response = await fetch('/api/process', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    document.getElementById('processingStatus').innerHTML = 
                        '✅ Processing complete! Processed ' + result.recordsProcessed + ' records.';
                    loadMongoDB();
                    loadPostgreSQL();
                } else {
                    document.getElementById('processingStatus').innerHTML = 
                        '❌ Processing failed: ' + result.error;
                }
            } catch (error) {
                document.getElementById('processingStatus').innerHTML = 
                    '💥 Error: ' + error.message;
            }
        }

        function getTypeFromName(name) {
            if (name.includes('axis')) return 'AXIS';
            if (name.includes('kotak')) return 'KOTAK';
            if (name.includes('orbis')) return 'ORBIS';
            if (name.includes('hdfc')) return 'HDFC';
            if (name.includes('trustpms')) return 'TRUSTPMS';
            if (name.includes('164_ec0000720')) return 'DEUTSCHE';
            return 'UNKNOWN';
        }
    </script>
</body>
</html>
  `);
});

// Status endpoint
app.get('/api/status', async (req, res) => {
  const status = {
    mongodb: false,
    postgresql: false
  };

  try {
    // Check MongoDB
    if (mongoose.connection.readyState === 1) {
      status.mongodb = true;
    }
  } catch (error) {
    console.error('MongoDB status check failed:', error.message);
  }

  try {
    // Check PostgreSQL pool
    if (pgPool) {
      const client = await pgPool.connect();
      await client.query('SELECT 1');
      client.release();
      status.postgresql = true;
    }
  } catch (error) {
    console.error('PostgreSQL status check failed:', error.message);
  }

  res.json(status);
});

// MongoDB API
app.get('/api/mongodb', async (req, res) => {
  try {
    const collections = [];
    let totalRecords = 0;

    const db = mongoose.connection.db;
    const collectionList = await db.listCollections().toArray();
    
    for (const collection of collectionList) {
      const collectionObj = db.collection(collection.name);
      const recordCount = await collectionObj.countDocuments();
      
      if (recordCount > 0) {
        const sampleDoc = await collectionObj.findOne();
        const fieldCount = sampleDoc ? Object.keys(sampleDoc).filter(key => 
          !['_id', '__v', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt'].includes(key)
        ).length : 0;

        collections.push({
          name: collection.name,
          recordCount,
          fieldCount
        });
        
        totalRecords += recordCount;
      }
    }

    res.json({
      success: true,
      collections,
      totalRecords
    });

  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// PostgreSQL API
app.get('/api/postgresql', async (req, res) => {
  let client = null;
  try {
    if (!pgPool) {
      throw new Error('PostgreSQL pool not initialized');
    }

    // Get client from pool
    client = await pgPool.connect();

    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name;
    `);

    const tables = [];
    let totalRecords = 0;

    for (const row of tablesResult.rows) {
      const tableName = row.table_name;
      
      try {
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName};`);
        const recordCount = parseInt(countResult.rows[0].count);
        
        let tableType = 'Master';
        if (tableName.includes('unified_custody_master')) {
          tableType = 'Custody';
        } else if (['trades', 'capital_flows'].includes(tableName)) {
          tableType = 'Transaction';
        }

        tables.push({
          name: tableName,
          recordCount,
          type: tableType
        });
        
        totalRecords += recordCount;
      } catch (error) {
        console.error(`Error counting ${tableName}:`, error.message);
      }
    }

    res.json({
      success: true,
      tables,
      totalRecords
    });

  } catch (error) {
    console.error('PostgreSQL API error:', error.message);
    res.json({
      success: false,
      error: error.message
    });
  } finally {
    // Always release the client back to the pool
    if (client) {
      client.release();
    }
  }
});

// Process data API - NOW USES MULTI-THREADING BY DEFAULT
app.post('/api/process', async (req, res) => {
  try {
    const startTime = Date.now();
    console.log('🚀 Starting MULTI-THREADED ETL processing from dashboard...');

    // Import and run the MULTI-THREADED processor
    const { MultiThreadedETLProcessor } = require('./multi-threaded-etl');
    
    const processor = new MultiThreadedETLProcessor();
    
    let totalProcessed = 0;
    let totalValid = 0;
    let totalErrors = 0;
    let totalCollections = 0;
    
    // Listen to events for real-time tracking
    processor.on('start', (data) => {
      console.log(`🔥 Started ${data.maxWorkers} worker threads for ${data.totalCollections} collections`);
      totalCollections = data.totalCollections;
    });

    processor.on('progress', (data) => {
      // Real-time progress updates (already logged by worker)
    });

    processor.on('complete', (data) => {
      console.log(`✅ Worker ${data.workerId} completed: ${data.collectionName} (${data.custodyType}) - ${data.result.valid}/${data.result.processed} valid`);
    });

    processor.on('finished', (data) => {
      totalProcessed = data.totalProcessed;
      totalValid = data.validRecords;
      totalErrors = data.errorRecords;
      console.log(`🎉 Multi-threaded processing complete! Success rate: ${data.successRate}%`);
    });
    
    console.log('📊 Processing all custody collections with WORKER THREADS...');
    await processor.processAllCollections();
    
    const processingTime = ((Date.now() - startTime) / 1000).toFixed(2);
    
    console.log(`✅ MULTI-THREADED ETL processing completed in ${processingTime}s`);
    
    res.json({
      success: true,
      recordsProcessed: totalValid,
      message: `Multi-threaded processing: ${totalCollections} collections with Orbis corrections applied`,
      processingTime: `${processingTime}s`,
      multiThreaded: true,
      details: {
        collections: totalCollections,
        totalRecords: totalProcessed,
        validRecords: totalValid,
        errors: totalErrors,
        maxWorkers: processor.maxWorkers,
        successRate: totalValid > 0 ? Math.round((totalValid / totalProcessed) * 100) : 0
      }
    });

  } catch (error) {
    console.error('❌ Multi-threaded ETL processing failed:', error.message);
    res.json({
      success: false,
      error: error.message,
      message: "Multi-threaded ETL processing failed - check server logs for details"
    });
  }
});

// Graceful shutdown handling
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down custody dashboard...');
  try {
    if (pgPool) {
      await pgPool.end();
      console.log('✅ PostgreSQL pool closed');
    }
    await mongoose.disconnect();
    console.log('✅ MongoDB disconnected');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during shutdown:', error.message);
    process.exit(1);
  }
});

process.on('SIGTERM', async () => {
  console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
  if (pgPool) {
    await pgPool.end();
  }
  await mongoose.disconnect();
  process.exit(0);
});

// Start server
async function startServer() {
  await initDB();
  
  app.listen(PORT, () => {
    console.log(`🚀 Custody Dashboard running at http://localhost:${PORT}`);
    console.log('📊 Shows both MongoDB raw data and PostgreSQL processed data');
    console.log('🔧 Orbis mapping corrections: client_name="N/A", instrument_name=NULL, instrument_code=NULL');
    console.log('⚡ Connection pool active for robust PostgreSQL handling');
  });
}

startServer(); 