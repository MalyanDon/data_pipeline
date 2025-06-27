const express = require('express');
const mongoose = require('mongoose');
const { Client } = require('pg');
const config = require('./config');
const { getCustodyConfig, detectCustodyType } = require('./custody-normalization/config/custody-mappings');

const app = express();
const PORT = 3005;

// PostgreSQL client
let pgClient = null;

// CORS headers
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  next();
});

app.use(express.json());

// Initialize PostgreSQL connection
async function initPostgreSQL() {
  try {
    pgClient = new Client({
      connectionString: config.postgresql.connectionString,
    });
    await pgClient.connect();
    console.log('✅ Connected to PostgreSQL');
  } catch (error) {
    console.error('❌ PostgreSQL connection failed:', error.message);
  }
}

// Main dashboard page
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Custody Data Pipeline Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; background: #f5f7fa; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; text-align: center; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0; }
        .card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .card h3 { margin-top: 0; color: #333; border-bottom: 2px solid #e1e5e9; padding-bottom: 10px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin: 15px 0; }
        .stat { background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; border-left: 4px solid #007bff; }
        .stat-number { font-size: 24px; font-weight: bold; color: #007bff; margin-bottom: 5px; }
        .stat-label { font-size: 12px; color: #666; text-transform: uppercase; }
        .btn { background: #007bff; color: white; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-size: 14px; }
        .btn:hover { background: #0056b3; }
        .btn-success { background: #28a745; }
        .btn-success:hover { background: #1e7e34; }
        .btn-warning { background: #ffc107; color: #212529; }
        .btn-warning:hover { background: #e0a800; }
        .btn-danger { background: #dc3545; }
        .btn-danger:hover { background: #c82333; }
        .table { width: 100%; border-collapse: collapse; margin: 15px 0; }
        .table th, .table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .table th { background: #f8f9fa; font-weight: 600; }
        .table tr:hover { background: #f8f9fa; }
        .badge { padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
        .badge-primary { background: #007bff; color: white; }
        .badge-success { background: #28a745; color: white; }
        .badge-warning { background: #ffc107; color: black; }
        .badge-info { background: #17a2b8; color: white; }
        .process-section { background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #2196f3; }
        .log-area { background: #1e1e1e; color: #00ff00; padding: 15px; border-radius: 8px; height: 300px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 12px; }
        .progress-bar { width: 100%; height: 20px; background: #e9ecef; border-radius: 10px; overflow: hidden; margin: 10px 0; }
        .progress-fill { height: 100%; background: linear-gradient(90deg, #007bff, #0056b3); transition: width 0.3s ease; }
        .status-indicator { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }
        .status-connected { background: #28a745; }
        .status-disconnected { background: #dc3545; }
        .status-processing { background: #ffc107; animation: pulse 1s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .pipeline-flow { display: flex; align-items: center; justify-content: space-between; margin: 20px 0; }
        .pipeline-step { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); flex: 1; margin: 0 10px; text-align: center; }
        .pipeline-arrow { font-size: 24px; color: #007bff; }
        .error { background: #f8d7da; color: #721c24; padding: 10px; border-radius: 4px; margin: 10px 0; }
        .success { background: #d4edda; color: #155724; padding: 10px; border-radius: 4px; margin: 10px 0; }
        .loading { display: none; text-align: center; padding: 20px; }
        .spinner { border: 4px solid #f3f3f3; border-top: 4px solid #3498db; border-radius: 50%; width: 40px; height: 40px; animation: spin 2s linear infinite; margin: 0 auto; }
        @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏦 Custody Data Pipeline Dashboard</h1>
        <p>Monitor MongoDB Raw Data ➜ ETL Processing ➜ PostgreSQL Normalized Data</p>
    </div>

    <div class="container">
        <!-- Connection Status -->
        <div class="card">
            <h3>🔌 Database Connections</h3>
            <div class="stats">
                <div class="stat">
                    <div class="stat-number" id="mongoStatus">
                        <span class="status-indicator status-disconnected"></span>MongoDB
                    </div>
                    <div class="stat-label">Raw Data Storage</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="postgresStatus">
                        <span class="status-indicator status-disconnected"></span>PostgreSQL
                    </div>
                    <div class="stat-label">Processed Data Storage</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="pipelineStatus">
                        <span class="status-indicator status-disconnected"></span>Pipeline
                    </div>
                    <div class="stat-label">ETL Processing</div>
                </div>
            </div>
        </div>

        <!-- Pipeline Flow Visualization -->
        <div class="pipeline-flow">
            <div class="pipeline-step">
                <h4>📊 MongoDB Raw Data</h4>
                <div id="mongoCollections">Loading...</div>
                <span class="badge badge-info" id="mongoRecords">0 records</span>
            </div>
            <div class="pipeline-arrow">➜</div>
            <div class="pipeline-step">
                <h4>⚙️ ETL Processing</h4>
                <div id="etlStatus">Ready</div>
                <button class="btn btn-warning" onclick="processAllData()">🚀 Process All</button>
            </div>
            <div class="pipeline-arrow">➜</div>
            <div class="pipeline-step">
                <h4>🗄️ PostgreSQL Clean Data</h4>
                <div id="postgresTable">No processed data</div>
                <span class="badge badge-success" id="postgresRecords">0 records</span>
            </div>
        </div>

        <div class="grid">
            <!-- MongoDB Raw Data -->
            <div class="card">
                <h3>📊 MongoDB Raw Data (Files Uploaded)</h3>
                <button class="btn" onclick="loadMongoData()">🔄 Refresh MongoDB Data</button>
                <div id="mongoData">Loading...</div>
            </div>

            <!-- PostgreSQL Processed Data -->
            <div class="card">
                <h3>🗄️ PostgreSQL Processed Data (Normalized)</h3>
                <button class="btn" onclick="loadPostgresData()">🔄 Refresh PostgreSQL Data</button>
                <div id="postgresData">Loading...</div>
            </div>
        </div>

        <!-- ETL Processing Section -->
        <div class="process-section">
            <h3>⚙️ ETL Processing Control</h3>
            <div class="stats">
                <div class="stat">
                    <div class="stat-number" id="processProgress">0%</div>
                    <div class="stat-label">Progress</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="processedFiles">0</div>
                    <div class="stat-label">Files Processed</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="processedRecords">0</div>
                    <div class="stat-label">Records Processed</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="processingTime">--</div>
                    <div class="stat-label">Processing Time</div>
                </div>
            </div>
            
            <div class="progress-bar">
                <div class="progress-fill" id="progressFill" style="width: 0%"></div>
            </div>
            
            <button class="btn btn-success" onclick="processAllData()">🚀 Process All Custody Data</button>
            <button class="btn btn-warning" onclick="processSpecificType()">📂 Process Specific Type</button>
            <button class="btn" onclick="clearProcessingLog()">🗑️ Clear Log</button>
            
            <div class="log-area" id="processingLog">
                🤖 ETL Processing Console - Ready to process custody data...
            </div>
        </div>

        <!-- Data Analysis -->
        <div class="card">
            <h3>📈 Data Analysis & Statistics</h3>
            <div class="grid">
                <div>
                    <h4>Raw Data Breakdown (MongoDB)</h4>
                    <div id="rawDataAnalysis">Loading...</div>
                </div>
                <div>
                    <h4>Processed Data Summary (PostgreSQL)</h4>
                    <div id="processedDataAnalysis">Loading...</div>
                </div>
            </div>
        </div>

        <div class="loading" id="loadingSpinner">
            <div class="spinner"></div>
            <p>Processing data...</p>
        </div>
    </div>

    <script>
        let processingActive = false;

        // Load data on page load
        window.addEventListener('load', () => {
            checkConnections();
            loadMongoData();
            loadPostgresData();
            setInterval(checkConnections, 30000); // Check every 30 seconds
        });

        async function checkConnections() {
            try {
                const response = await fetch('/api/status');
                const status = await response.json();
                
                updateConnectionStatus('mongoStatus', status.mongodb);
                updateConnectionStatus('postgresStatus', status.postgresql);
                updateConnectionStatus('pipelineStatus', status.pipeline);
                
            } catch (error) {
                console.error('Error checking connections:', error);
            }
        }

        function updateConnectionStatus(elementId, connected) {
            const element = document.getElementById(elementId);
            const indicator = element.querySelector('.status-indicator');
            
            if (connected) {
                indicator.className = 'status-indicator status-connected';
            } else {
                indicator.className = 'status-indicator status-disconnected';
            }
        }

        async function loadMongoData() {
            try {
                const response = await fetch('/api/mongodb/collections');
                const data = await response.json();
                
                if (data.success) {
                    displayMongoData(data.collections);
                    updateMongoCounts(data.totalRecords, data.collections.length);
                } else {
                    document.getElementById('mongoData').innerHTML = 
                        '<div class="error">Error loading MongoDB data: ' + data.error + '</div>';
                }
            } catch (error) {
                document.getElementById('mongoData').innerHTML = 
                    '<div class="error">Error: ' + error.message + '</div>';
            }
        }

        async function loadPostgresData() {
            try {
                const response = await fetch('/api/postgresql/tables');
                const data = await response.json();
                
                if (data.success) {
                    displayPostgresData(data.tables);
                    updatePostgresCounts(data.totalRecords, data.tables.length);
                } else {
                    document.getElementById('postgresData').innerHTML = 
                        '<div class="error">Error loading PostgreSQL data: ' + data.error + '</div>';
                }
            } catch (error) {
                document.getElementById('postgresData').innerHTML = 
                    '<div class="error">Error: ' + error.message + '</div>';
            }
        }

        function displayMongoData(collections) {
            let html = '<table class="table"><tr><th>Collection</th><th>Records</th><th>Data Fields</th><th>Custody Type</th><th>Status</th></tr>';
            
            collections.forEach(collection => {
                const custodyType = detectCustodyTypeFromName(collection.name);
                const statusBadge = collection.recordCount > 0 ? 
                    '<span class="badge badge-success">Ready</span>' : 
                    '<span class="badge badge-warning">Empty</span>';
                
                html += '<tr>';
                html += '<td>' + collection.name + '</td>';
                html += '<td>' + collection.recordCount.toLocaleString() + '</td>';
                html += '<td>' + collection.fieldCount + '</td>';
                html += '<td><span class="badge badge-info">' + custodyType.toUpperCase() + '</span></td>';
                html += '<td>' + statusBadge + '</td>';
                html += '</tr>';
            });
            
            html += '</table>';
            document.getElementById('mongoData').innerHTML = html;
        }

        function displayPostgresData(tables) {
            let html = '<table class="table"><tr><th>Table</th><th>Records</th><th>Type</th><th>Status</th></tr>';
            
            tables.forEach(table => {
                const statusBadge = table.recordCount > 0 ? 
                    '<span class="badge badge-success">Has Data</span>' : 
                    '<span class="badge badge-warning">Empty</span>';
                
                html += '<tr>';
                html += '<td>' + table.name + '</td>';
                html += '<td>' + table.recordCount.toLocaleString() + '</td>';
                html += '<td><span class="badge badge-primary">' + table.type + '</span></td>';
                html += '<td>' + statusBadge + '</td>';
                html += '</tr>';
            });
            
            html += '</table>';
            document.getElementById('postgresData').innerHTML = html;
        }

        function updateMongoCounts(totalRecords, collectionCount) {
            document.getElementById('mongoCollections').innerHTML = collectionCount + ' collections';
            document.getElementById('mongoRecords').innerHTML = totalRecords.toLocaleString() + ' records';
        }

        function updatePostgresCounts(totalRecords, tableCount) {
            const display = totalRecords > 0 ? tableCount + ' tables' : 'No processed data';
            document.getElementById('postgresTable').innerHTML = display;
            document.getElementById('postgresRecords').innerHTML = totalRecords.toLocaleString() + ' records';
        }

        async function processAllData() {
            if (processingActive) {
                alert('Processing is already active!');
                return;
            }

            processingActive = true;
            document.getElementById('loadingSpinner').style.display = 'block';
            
            const logElement = document.getElementById('processingLog');
            logElement.innerHTML = '🚀 Starting ETL processing for all custody data...\\n';
            
            try {
                const response = await fetch('/api/process/all', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    logElement.innerHTML += '✅ Processing completed successfully!\\n';
                    logElement.innerHTML += 'Files processed: ' + result.filesProcessed + '\\n';
                    logElement.innerHTML += 'Records processed: ' + result.recordsProcessed + '\\n';
                    logElement.innerHTML += 'Processing time: ' + result.processingTime + '\\n';
                    
                    // Refresh both data views
                    await loadMongoData();
                    await loadPostgresData();
                } else {
                    logElement.innerHTML += '❌ Processing failed: ' + result.error + '\\n';
                }
            } catch (error) {
                logElement.innerHTML += '💥 Error: ' + error.message + '\\n';
            } finally {
                processingActive = false;
                document.getElementById('loadingSpinner').style.display = 'none';
            }
        }

        function detectCustodyTypeFromName(collectionName) {
            const name = collectionName.toLowerCase();
            if (name.includes('axis')) return 'axis';
            if (name.includes('kotak')) return 'kotak';
            if (name.includes('orbis')) return 'orbis';
            if (name.includes('hdfc')) return 'hdfc';
            if (name.includes('trustpms')) return 'trustpms';
            if (name.includes('deutsche') || name.includes('164_ec0000720')) return 'deutsche';
            return 'unknown';
        }

        function clearProcessingLog() {
            document.getElementById('processingLog').innerHTML = '🤖 ETL Processing Console - Ready to process custody data...\\n';
        }

        // Auto-scroll log to bottom
        function scrollLogToBottom() {
            const logElement = document.getElementById('processingLog');
            logElement.scrollTop = logElement.scrollHeight;
        }

        setInterval(scrollLogToBottom, 1000);
    </script>
</body>
</html>
  `);
});

// API Routes

// Status endpoint
app.get('/api/status', async (req, res) => {
  const status = {
    mongodb: false,
    postgresql: false,
    pipeline: false
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
    // Check PostgreSQL
    if (pgClient) {
      await pgClient.query('SELECT 1');
      status.postgresql = true;
    }
  } catch (error) {
    console.error('PostgreSQL status check failed:', error.message);
  }

  status.pipeline = status.mongodb && status.postgresql;

  res.json(status);
});

// MongoDB collections endpoint
app.get('/api/mongodb/collections', async (req, res) => {
  try {
    // Connect to all financial databases
    const yearDatabases = ['financial_data_2024', 'financial_data_2025', 'financial_data'];
    const allCollections = [];
    let totalRecords = 0;

    for (const dbName of yearDatabases) {
      try {
        const connection = await mongoose.createConnection(config.mongodb.uri + dbName);
        const collections = await connection.db.listCollections().toArray();
        
        for (const collection of collections) {
          const collectionObj = connection.db.collection(collection.name);
          const recordCount = await collectionObj.countDocuments();
          
          if (recordCount > 0) {
            // Get sample document to count fields
            const sampleDoc = await collectionObj.findOne();
            const fieldCount = sampleDoc ? Object.keys(sampleDoc).filter(key => 
              !['_id', '__v', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt'].includes(key)
            ).length : 0;

            allCollections.push({
              database: dbName,
              name: collection.name,
              recordCount,
              fieldCount
            });
            
            totalRecords += recordCount;
          }
        }
        
        await connection.close();
      } catch (error) {
        console.error(`Error accessing database ${dbName}:`, error.message);
      }
    }

    res.json({
      success: true,
      collections: allCollections,
      totalRecords
    });

  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// PostgreSQL tables endpoint
app.get('/api/postgresql/tables', async (req, res) => {
  try {
    if (!pgClient) {
      throw new Error('PostgreSQL not connected');
    }

    // Get all tables
    const tablesResult = await pgClient.query(`
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
        const countResult = await pgClient.query(`SELECT COUNT(*) as count FROM ${tableName};`);
        const recordCount = parseInt(countResult.rows[0].count);
        
        let tableType = 'Master Data';
        if (tableName.includes('unified_custody_master')) {
          tableType = 'Custody Data';
        } else if (['trades', 'capital_flows', 'custody_holdings'].includes(tableName)) {
          tableType = 'Transactional';
        }

        tables.push({
          name: tableName,
          recordCount,
          type: tableType
        });
        
        totalRecords += recordCount;
      } catch (error) {
        console.error(`Error counting records in ${tableName}:`, error.message);
      }
    }

    res.json({
      success: true,
      tables,
      totalRecords
    });

  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// Process all data endpoint
app.post('/api/process/all', async (req, res) => {
  try {
    const startTime = Date.now();
    
    // Import and run the MongoDB processor
    const { SimpleCustodyProcessor } = require('./scripts/simple-mongodb-processor');
    const processor = new SimpleCustodyProcessor();
    
    await processor.connect();
    const result = await processor.processCollections();
    await processor.disconnect();
    
    const processingTime = ((Date.now() - startTime) / 1000).toFixed(2) + 's';
    
    res.json({
      success: true,
      filesProcessed: result.filesProcessed || 0,
      recordsProcessed: result.recordsProcessed || 0,
      processingTime
    });

  } catch (error) {
    res.json({
      success: false,
      error: error.message
    });
  }
});

// Start server
async function startServer() {
  try {
    await initPostgreSQL();
    
    // Connect to MongoDB
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB');
    
    app.listen(PORT, () => {
      console.log(`🚀 Custody Dashboard running at http://localhost:${PORT}`);
      console.log('📊 View MongoDB raw data and PostgreSQL processed data');
      console.log('⚙️ Control ETL processing pipeline');
    });
    
  } catch (error) {
    console.error('❌ Server startup failed:', error.message);
    process.exit(1);
  }
}

// Start the server
startServer(); 