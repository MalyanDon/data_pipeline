const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const csv = require('csv-parser');
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

const app = express();
const PORT = process.env.PORT || 3006;

// Configure multer for file uploads
const upload = multer({ dest: 'temp_uploads/' });

// Database connections
let pgPool;
let mongoConnected = false;

async function initDB() {
  // PostgreSQL connection
  try {
    pgPool = new Pool({
      connectionString: config.postgresql.connectionString,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      keepAlive: true
    });

    const testClient = await pgPool.connect();
    await testClient.query('SELECT 1');
    testClient.release();
    console.log('✅ Connected to PostgreSQL with connection pool');
  } catch (error) {
    console.error('❌ PostgreSQL connection failed:', error.message);
  }

  // MongoDB connection
  try {
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    await mongoClient.close();
    console.log('✅ Connected to MongoDB');
    mongoConnected = true;
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    mongoConnected = false;
  }
}

app.use(express.json());
app.use(express.static('public'));

// Main dashboard route
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>🚀 Simple Data Processing Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; padding: 30px 0; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .header p { font-size: 1.1em; opacity: 0.9; }
        
        .section { 
            background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); 
            border-radius: 15px; padding: 25px; margin: 25px 0; 
            border: 1px solid rgba(255,255,255,0.2);
        }
        .section h2 { margin-bottom: 20px; text-align: center; }
        
        .btn { 
            padding: 12px 25px; border: none; border-radius: 25px; font-size: 1em; font-weight: bold;
            cursor: pointer; margin: 10px; display: inline-block; text-decoration: none;
            background: linear-gradient(45deg, #4facfe, #00f2fe); color: white;
            transition: transform 0.3s ease;
        }
        .btn:hover { transform: translateY(-2px); }
        .btn-success { background: linear-gradient(45deg, #43e97b, #38f9d7); }
        .btn-warning { background: linear-gradient(45deg, #fa709a, #fee140); }
        
        .upload-area { 
            border: 2px dashed rgba(255,255,255,0.3); border-radius: 15px; 
            padding: 40px; text-align: center; margin: 20px 0;
        }
        .upload-area:hover { border-color: #43e97b; }
        
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
        .stat { background: rgba(255,255,255,0.1); padding: 20px; border-radius: 10px; text-align: center; }
        .stat-number { font-size: 2em; font-weight: bold; }
        .stat-label { font-size: 0.9em; opacity: 0.8; }
        
        .data-viewer { margin-top: 30px; }
        .data-section { margin: 20px 0; padding: 20px; background: rgba(255,255,255,0.05); border-radius: 10px; }
        
        #result { margin-top: 20px; padding: 15px; border-radius: 10px; display: none; }
        .success { background: rgba(67, 233, 123, 0.2); border: 1px solid #43e97b; }
        .error { background: rgba(255, 107, 107, 0.2); border: 1px solid #ff6b6b; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Simple Data Processing Dashboard</h1>
            <p>✅ MongoDB and PostgreSQL databases cleared - Ready for fresh uploads!</p>
        </div>

        <!-- Upload Section -->
        <div class="section">
            <h2>📤 File Upload Center</h2>
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <input type="file" id="fileInput" name="files" multiple accept=".xlsx,.xls,.csv,.json,.txt,.xml" style="display: none;">
                    <div style="font-size: 3em; margin-bottom: 10px;">📁</div>
                    <h3>Click to select files or drag & drop</h3>
                    <p>Supports: .xlsx, .xls, .csv, .json, .txt, .xml files</p>
                </div>
                <div style="text-align: center;">
                    <button type="submit" class="btn btn-success">🚀 Upload to MongoDB</button>
                    <button type="button" class="btn" onclick="clearFiles()">🗑️ Clear</button>
                </div>
            </form>
            <div id="result"></div>
        </div>

        <!-- Stats Section -->
        <div class="section">
            <h2>📊 Database Status</h2>
            <div class="stats" id="stats">
                <div class="stat">
                    <div class="stat-number" id="mongoCollections">0</div>
                    <div class="stat-label">MongoDB Collections</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="mongoRecords">0</div>
                    <div class="stat-label">MongoDB Records</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="pgTables">0</div>
                    <div class="stat-label">PostgreSQL Tables</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="pgRecords">0</div>
                    <div class="stat-label">PostgreSQL Records</div>
                </div>
            </div>
            <div style="text-align: center;">
                <button class="btn" onclick="refreshStats()">🔄 Refresh Stats</button>
                <button class="btn btn-warning" onclick="clearAllData()">🗑️ Clear All Data</button>
            </div>
        </div>

        <!-- Data Viewers -->
        <div class="section">
            <h2>🔍 Data Viewers</h2>
            <div style="text-align: center;">
                <a href="/api/mongodb/preview" target="_blank" class="btn">🍃 View MongoDB Data</a>
                <a href="/api/postgresql/preview" target="_blank" class="btn">🐘 View PostgreSQL Data</a>
                <button class="btn" onclick="loadUnifiedView()">📊 Unified View</button>
            </div>
            <div id="unifiedView" class="data-viewer"></div>
        </div>
    </div>

    <script>
        // File upload handling
        document.getElementById('uploadForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData();
            const files = document.getElementById('fileInput').files;
            
            if (files.length === 0) {
                showResult('Please select files to upload', 'error');
                return;
            }
            
            for (let i = 0; i < files.length; i++) {
                formData.append('files', files[i]);
            }
            
            showResult('Uploading files...', 'success');
            
            try {
                const response = await fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showResult('Upload successful! Files: ' + result.totalFiles + ', Records: ' + result.totalRecords, 'success');
                    refreshStats();
                } else {
                    showResult('Upload failed: ' + result.message, 'error');
                }
            } catch (error) {
                showResult('Upload error: ' + error.message, 'error');
            }
        });
        
        function showResult(message, type) {
            const result = document.getElementById('result');
            result.textContent = message;
            result.className = type;
            result.style.display = 'block';
        }
        
        function clearFiles() {
            document.getElementById('fileInput').value = '';
            document.getElementById('result').style.display = 'none';
        }
        
        async function refreshStats() {
            try {
                const response = await fetch('/api/unified-data-view');
                const data = await response.json();
                
                if (data.success) {
                    document.getElementById('mongoCollections').textContent = data.mongodb.totalCollections;
                    document.getElementById('mongoRecords').textContent = data.mongodb.totalRecords.toLocaleString();
                    document.getElementById('pgTables').textContent = data.postgresql.totalTables;
                    document.getElementById('pgRecords').textContent = data.postgresql.totalRecords.toLocaleString();
                }
            } catch (error) {
                console.error('Error refreshing stats:', error);
            }
        }
        
        async function clearAllData() {
            if (confirm('Are you sure you want to clear all data from both databases?')) {
                try {
                    const response = await fetch('/api/clear-all', { method: 'POST' });
                    const result = await response.json();
                    
                    if (result.success) {
                        showResult('All data cleared successfully!', 'success');
                        refreshStats();
                    } else {
                        showResult('Clear failed: ' + result.message, 'error');
                    }
                } catch (error) {
                    showResult('Clear error: ' + error.message, 'error');
                }
            }
        }
        
        async function loadUnifiedView() {
            try {
                const response = await fetch('/api/unified-data-view');
                const data = await response.json();
                
                if (data.success) {
                    let html = '<h3>Unified Data Summary</h3>';
                    html += '<div class="data-section">';
                    html += '<h4>MongoDB Collections (' + data.mongodb.totalCollections + ')</h4>';
                    data.mongodb.collections.forEach(col => {
                        html += '<p>• ' + col.name + ' (' + col.count + ' records)</p>';
                    });
                    html += '</div>';
                    
                    html += '<div class="data-section">';
                    html += '<h4>PostgreSQL Tables (' + data.postgresql.totalTables + ')</h4>';
                    data.postgresql.tables.forEach(table => {
                        html += '<p>• ' + table.name + ' (' + table.count + ' records)</p>';
                    });
                    html += '</div>';
                    
                    document.getElementById('unifiedView').innerHTML = html;
                }
            } catch (error) {
                document.getElementById('unifiedView').innerHTML = '<p>Error loading unified view: ' + error.message + '</p>';
            }
        }
        
        // Load stats on page load
        refreshStats();
    </script>
</body>
</html>
  `);
});

// Upload API route
app.post('/api/upload', upload.array('files'), async (req, res) => {
  try {
    const files = req.files;
    if (!files || files.length === 0) {
      return res.status(400).json({ success: false, message: 'No files uploaded' });
    }

    console.log('📤 Uploading ' + files.length + ' files to MongoDB...');
    
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    const db = mongoClient.db('financial_data_2025');
    
    let totalRecords = 0;
    const results = [];
    
    for (const file of files) {
      try {
        let data = [];
        const fileExt = path.extname(file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
          // Read CSV
          const csvData = fs.readFileSync(file.path, 'utf8');
          data = await new Promise((resolve, reject) => {
            const results = [];
            require('stream').Readable.from([csvData])
              .pipe(csv())
              .on('data', (row) => results.push(row))
              .on('end', () => resolve(results))
              .on('error', reject);
          });
        } else if (fileExt === '.xlsx' || fileExt === '.xls') {
          // Read Excel
          const workbook = XLSX.readFile(file.path);
          const sheetName = workbook.SheetNames[0];
          const worksheet = workbook.Sheets[sheetName];
          data = XLSX.utils.sheet_to_json(worksheet);
        }
        
        if (data.length > 0) {
          // Generate collection name
          const collectionName = generateCollectionName(file.originalname);
          const collection = db.collection(collectionName);
          
          // Add metadata
          const enrichedData = data.map(record => ({
            ...record,
            _fileName: file.originalname,
            _uploadTimestamp: new Date(),
            _fileSize: file.size
          }));
          
          await collection.deleteMany({ _fileName: file.originalname });
          await collection.insertMany(enrichedData);
          
          totalRecords += data.length;
          results.push({
            fileName: file.originalname,
            records: data.length,
            collectionName: collectionName
          });
          
          console.log('✅ ' + file.originalname + ': ' + data.length + ' records -> ' + collectionName);
        }
        
        // Cleanup file
        fs.unlinkSync(file.path);
        
      } catch (error) {
        console.error('❌ Error processing ' + file.originalname + ':', error.message);
        results.push({
          fileName: file.originalname,
          error: error.message,
          records: 0
        });
      }
    }
    
    await mongoClient.close();
    
    res.json({
      success: true,
      message: 'Files uploaded successfully',
      totalFiles: files.length,
      totalRecords: totalRecords,
      files: results
    });
    
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({
      success: false,
      message: 'Upload failed: ' + error.message
    });
  }
});

// Unified data view API
app.get('/api/unified-data-view', async (req, res) => {
  try {
    const mongoData = await getMongoDBSummary();
    const postgresData = await getPostgreSQLSummary();
    
    res.json({
      success: true,
      mongodb: mongoData,
      postgresql: postgresData,
      summary: {
        totalCollections: mongoData.totalCollections,
        totalMongoRecords: mongoData.totalRecords,
        totalPostgresRecords: postgresData.totalRecords,
        totalTables: postgresData.totalTables
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to load unified data view',
      details: error.message
    });
  }
});

// Clear all data API
app.post('/api/clear-all', async (req, res) => {
  try {
    const { exec } = require('child_process');
    
    exec('node clear-all-databases.js', (error, stdout, stderr) => {
      if (error) {
        console.error('Clear script error:', error);
        res.status(500).json({ success: false, message: error.message });
      } else {
        console.log('Clear script output:', stdout);
        res.json({ success: true, message: 'All databases cleared successfully' });
      }
    });
    
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// MongoDB preview (simple version)
app.get('/api/mongodb/preview', async (req, res) => {
  try {
    const mongoData = await getMongoDBSummary();
    
    let html = '<html><head><title>MongoDB Data</title><style>body{font-family:Arial;padding:20px;}</style></head><body>';
    html += '<h1>🍃 MongoDB Collections</h1>';
    html += '<p>Total Collections: ' + mongoData.totalCollections + '</p>';
    html += '<p>Total Records: ' + mongoData.totalRecords + '</p>';
    
    mongoData.collections.forEach(col => {
      html += '<div style="margin:10px 0;padding:10px;border:1px solid #ccc;">';
      html += '<h3>' + col.name + '</h3>';
      html += '<p>Records: ' + col.count + '</p>';
      html += '<p>Type: ' + col.sourceType + '</p>';
      html += '</div>';
    });
    
    html += '</body></html>';
    res.send(html);
    
  } catch (error) {
    res.status(500).send('Error: ' + error.message);
  }
});

// PostgreSQL preview (simple version)
app.get('/api/postgresql/preview', async (req, res) => {
  try {
    const pgData = await getPostgreSQLSummary();
    
    let html = '<html><head><title>PostgreSQL Data</title><style>body{font-family:Arial;padding:20px;}</style></head><body>';
    html += '<h1>🐘 PostgreSQL Tables</h1>';
    html += '<p>Total Tables: ' + pgData.totalTables + '</p>';
    html += '<p>Total Records: ' + pgData.totalRecords + '</p>';
    
    pgData.tables.forEach(table => {
      html += '<div style="margin:10px 0;padding:10px;border:1px solid #ccc;">';
      html += '<h3>' + table.name + '</h3>';
      html += '<p>Records: ' + table.count + '</p>';
      html += '<p>Type: ' + table.type + '</p>';
      html += '</div>';
    });
    
    html += '</body></html>';
    res.send(html);
    
  } catch (error) {
    res.status(500).send('Error: ' + error.message);
  }
});

// Helper functions
function generateCollectionName(fileName) {
  const name = fileName.toLowerCase();
  const timestamp = new Date().toISOString().replace(/[:.]/g, '_').slice(0, 19);
  
  if (name.includes('broker_master') || name.includes('broker master')) return 'broker_master_data_' + timestamp;
  if (name.includes('client')) return 'client_info_data_' + timestamp;
  if (name.includes('distributor')) return 'distributor_master_data_' + timestamp;
  if (name.includes('strategy')) return 'strategy_master_data_' + timestamp;
  if (name.includes('contract')) return 'contract_notes_data_' + timestamp;
  if (name.includes('cash')) return 'cash_capital_flow_data_' + timestamp;
  if (name.includes('stock')) return 'stock_capital_flow_data_' + timestamp;
  if (name.includes('mf') || name.includes('allocation')) return 'mf_allocation_data_' + timestamp;
  
  return 'general_data_' + timestamp;
}

async function getMongoDBSummary() {
  if (!mongoConnected) {
    return { totalCollections: 0, totalRecords: 0, collections: [] };
  }
  
  const { MongoClient } = require('mongodb');
  const mongoClient = new MongoClient(config.mongodb.uri, {
    serverSelectionTimeoutMS: 15000,
    connectTimeoutMS: 15000,
    tls: true,
    tlsAllowInvalidCertificates: true,
    tlsAllowInvalidHostnames: true
  });
  
  await mongoClient.connect();
  const db = mongoClient.db('financial_data_2025');
  
  const collections = await db.listCollections().toArray();
  const allCollections = [];
  let totalRecords = 0;
  
  for (const col of collections) {
    const collection = db.collection(col.name);
    const count = await collection.countDocuments();
    if (count > 0) {
      allCollections.push({
        name: col.name,
        count: count,
        sourceType: getSourceTypeFromCollection(col.name)
      });
      totalRecords += count;
    }
  }
  
  await mongoClient.close();
  
  return {
    totalCollections: allCollections.length,
    totalRecords: totalRecords,
    collections: allCollections
  };
}

async function getPostgreSQLSummary() {
  const client = await pgPool.connect();
  
  const tablesQuery = `
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
  `;
  const tablesResult = await client.query(tablesQuery);
  
  const tables = [];
  let totalRecords = 0;
  
  for (const table of tablesResult.rows) {
    const tableName = table.table_name;
    
    try {
      const countQuery = 'SELECT COUNT(*) as total FROM ' + tableName;
      const countResult = await client.query(countQuery);
      const count = parseInt(countResult.rows[0].total);
      
      tables.push({
        name: tableName,
        count: count,
        type: getTableType(tableName)
      });
      
      totalRecords += count;
    } catch (error) {
      tables.push({
        name: tableName,
        count: 0,
        type: 'Error'
      });
    }
  }
  
  client.release();
  
  return {
    totalTables: tables.length,
    totalRecords: totalRecords,
    tables: tables
  };
}

function getSourceTypeFromCollection(collectionName) {
  const name = collectionName.toLowerCase();
  if (name.includes('broker_master')) return 'Broker Master';
  if (name.includes('client')) return 'Client Master';
  if (name.includes('distributor')) return 'Distributor Master';
  if (name.includes('strategy')) return 'Strategy Master';
  if (name.includes('contract_notes')) return 'Contract Notes';
  if (name.includes('cash_capital')) return 'Cash Capital Flow';
  if (name.includes('stock_capital')) return 'Stock Capital Flow';
  if (name.includes('mf_allocation')) return 'MF Allocations';
  return 'General Data';
}

function getTableType(tableName) {
  const name = tableName.toLowerCase();
  if (name.includes('_2025_') || name.includes('_2024_')) return 'Daily Tables';
  if (name.includes('brokers') || name.includes('clients') || name.includes('distributors') || name.includes('strategies')) return 'Master Data';
  if (name.includes('contract_notes') || name.includes('capital_flow') || name.includes('allocations')) return 'Transaction Data';
  if (name.includes('custody') || name.includes('holdings')) return 'Custody Holdings';
  return 'Other';
}

// Start server
async function startServer() {
  try {
    await initDB();
    
    app.listen(PORT, () => {
      console.log('🚀 Simple Data Processing Dashboard running at http://localhost:' + PORT);
      console.log('✅ Databases cleared and ready for fresh uploads');
      console.log('📤 MongoDB-first workflow: Upload → Process → View');
    });
    
  } catch (error) {
    console.error('❌ Server startup failed:', error.message);
    process.exit(1);
  }
}

startServer(); 
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');
const csv = require('csv-parser');
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

const app = express();
const PORT = process.env.PORT || 3006;

// Configure multer for file uploads
const upload = multer({ dest: 'temp_uploads/' });

// Database connections
let pgPool;
let mongoConnected = false;

async function initDB() {
  // PostgreSQL connection
  try {
    pgPool = new Pool({
      connectionString: config.postgresql.connectionString,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
      keepAlive: true
    });

    const testClient = await pgPool.connect();
    await testClient.query('SELECT 1');
    testClient.release();
    console.log('✅ Connected to PostgreSQL with connection pool');
  } catch (error) {
    console.error('❌ PostgreSQL connection failed:', error.message);
  }

  // MongoDB connection
  try {
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    await mongoClient.close();
    console.log('✅ Connected to MongoDB');
    mongoConnected = true;
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    mongoConnected = false;
  }
}

app.use(express.json());
app.use(express.static('public'));

// Main dashboard route
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>🚀 Simple Data Processing Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; min-height: 100vh; padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; padding: 30px 0; }
        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
        .header p { font-size: 1.1em; opacity: 0.9; }
        
        .section { 
            background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); 
            border-radius: 15px; padding: 25px; margin: 25px 0; 
            border: 1px solid rgba(255,255,255,0.2);
        }
        .section h2 { margin-bottom: 20px; text-align: center; }
        
        .btn { 
            padding: 12px 25px; border: none; border-radius: 25px; font-size: 1em; font-weight: bold;
            cursor: pointer; margin: 10px; display: inline-block; text-decoration: none;
            background: linear-gradient(45deg, #4facfe, #00f2fe); color: white;
            transition: transform 0.3s ease;
        }
        .btn:hover { transform: translateY(-2px); }
        .btn-success { background: linear-gradient(45deg, #43e97b, #38f9d7); }
        .btn-warning { background: linear-gradient(45deg, #fa709a, #fee140); }
        
        .upload-area { 
            border: 2px dashed rgba(255,255,255,0.3); border-radius: 15px; 
            padding: 40px; text-align: center; margin: 20px 0;
        }
        .upload-area:hover { border-color: #43e97b; }
        
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
        .stat { background: rgba(255,255,255,0.1); padding: 20px; border-radius: 10px; text-align: center; }
        .stat-number { font-size: 2em; font-weight: bold; }
        .stat-label { font-size: 0.9em; opacity: 0.8; }
        
        .data-viewer { margin-top: 30px; }
        .data-section { margin: 20px 0; padding: 20px; background: rgba(255,255,255,0.05); border-radius: 10px; }
        
        #result { margin-top: 20px; padding: 15px; border-radius: 10px; display: none; }
        .success { background: rgba(67, 233, 123, 0.2); border: 1px solid #43e97b; }
        .error { background: rgba(255, 107, 107, 0.2); border: 1px solid #ff6b6b; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Simple Data Processing Dashboard</h1>
            <p>✅ MongoDB and PostgreSQL databases cleared - Ready for fresh uploads!</p>
        </div>

        <!-- Upload Section -->
        <div class="section">
            <h2>📤 File Upload Center</h2>
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <input type="file" id="fileInput" name="files" multiple accept=".xlsx,.xls,.csv,.json,.txt,.xml" style="display: none;">
                    <div style="font-size: 3em; margin-bottom: 10px;">📁</div>
                    <h3>Click to select files or drag & drop</h3>
                    <p>Supports: .xlsx, .xls, .csv, .json, .txt, .xml files</p>
                </div>
                <div style="text-align: center;">
                    <button type="submit" class="btn btn-success">🚀 Upload to MongoDB</button>
                    <button type="button" class="btn" onclick="clearFiles()">🗑️ Clear</button>
                </div>
            </form>
            <div id="result"></div>
        </div>

        <!-- Stats Section -->
        <div class="section">
            <h2>📊 Database Status</h2>
            <div class="stats" id="stats">
                <div class="stat">
                    <div class="stat-number" id="mongoCollections">0</div>
                    <div class="stat-label">MongoDB Collections</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="mongoRecords">0</div>
                    <div class="stat-label">MongoDB Records</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="pgTables">0</div>
                    <div class="stat-label">PostgreSQL Tables</div>
                </div>
                <div class="stat">
                    <div class="stat-number" id="pgRecords">0</div>
                    <div class="stat-label">PostgreSQL Records</div>
                </div>
            </div>
            <div style="text-align: center;">
                <button class="btn" onclick="refreshStats()">🔄 Refresh Stats</button>
                <button class="btn btn-warning" onclick="clearAllData()">🗑️ Clear All Data</button>
            </div>
        </div>

        <!-- Data Viewers -->
        <div class="section">
            <h2>🔍 Data Viewers</h2>
            <div style="text-align: center;">
                <a href="/api/mongodb/preview" target="_blank" class="btn">🍃 View MongoDB Data</a>
                <a href="/api/postgresql/preview" target="_blank" class="btn">🐘 View PostgreSQL Data</a>
                <button class="btn" onclick="loadUnifiedView()">📊 Unified View</button>
            </div>
            <div id="unifiedView" class="data-viewer"></div>
        </div>
    </div>

    <script>
        // File upload handling
        document.getElementById('uploadForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData();
            const files = document.getElementById('fileInput').files;
            
            if (files.length === 0) {
                showResult('Please select files to upload', 'error');
                return;
            }
            
            for (let i = 0; i < files.length; i++) {
                formData.append('files', files[i]);
            }
            
            showResult('Uploading files...', 'success');
            
            try {
                const response = await fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showResult('Upload successful! Files: ' + result.totalFiles + ', Records: ' + result.totalRecords, 'success');
                    refreshStats();
                } else {
                    showResult('Upload failed: ' + result.message, 'error');
                }
            } catch (error) {
                showResult('Upload error: ' + error.message, 'error');
            }
        });
        
        function showResult(message, type) {
            const result = document.getElementById('result');
            result.textContent = message;
            result.className = type;
            result.style.display = 'block';
        }
        
        function clearFiles() {
            document.getElementById('fileInput').value = '';
            document.getElementById('result').style.display = 'none';
        }
        
        async function refreshStats() {
            try {
                const response = await fetch('/api/unified-data-view');
                const data = await response.json();
                
                if (data.success) {
                    document.getElementById('mongoCollections').textContent = data.mongodb.totalCollections;
                    document.getElementById('mongoRecords').textContent = data.mongodb.totalRecords.toLocaleString();
                    document.getElementById('pgTables').textContent = data.postgresql.totalTables;
                    document.getElementById('pgRecords').textContent = data.postgresql.totalRecords.toLocaleString();
                }
            } catch (error) {
                console.error('Error refreshing stats:', error);
            }
        }
        
        async function clearAllData() {
            if (confirm('Are you sure you want to clear all data from both databases?')) {
                try {
                    const response = await fetch('/api/clear-all', { method: 'POST' });
                    const result = await response.json();
                    
                    if (result.success) {
                        showResult('All data cleared successfully!', 'success');
                        refreshStats();
                    } else {
                        showResult('Clear failed: ' + result.message, 'error');
                    }
                } catch (error) {
                    showResult('Clear error: ' + error.message, 'error');
                }
            }
        }
        
        async function loadUnifiedView() {
            try {
                const response = await fetch('/api/unified-data-view');
                const data = await response.json();
                
                if (data.success) {
                    let html = '<h3>Unified Data Summary</h3>';
                    html += '<div class="data-section">';
                    html += '<h4>MongoDB Collections (' + data.mongodb.totalCollections + ')</h4>';
                    data.mongodb.collections.forEach(col => {
                        html += '<p>• ' + col.name + ' (' + col.count + ' records)</p>';
                    });
                    html += '</div>';
                    
                    html += '<div class="data-section">';
                    html += '<h4>PostgreSQL Tables (' + data.postgresql.totalTables + ')</h4>';
                    data.postgresql.tables.forEach(table => {
                        html += '<p>• ' + table.name + ' (' + table.count + ' records)</p>';
                    });
                    html += '</div>';
                    
                    document.getElementById('unifiedView').innerHTML = html;
                }
            } catch (error) {
                document.getElementById('unifiedView').innerHTML = '<p>Error loading unified view: ' + error.message + '</p>';
            }
        }
        
        // Load stats on page load
        refreshStats();
    </script>
</body>
</html>
  `);
});

// Upload API route
app.post('/api/upload', upload.array('files'), async (req, res) => {
  try {
    const files = req.files;
    if (!files || files.length === 0) {
      return res.status(400).json({ success: false, message: 'No files uploaded' });
    }

    console.log('📤 Uploading ' + files.length + ' files to MongoDB...');
    
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    const db = mongoClient.db('financial_data_2025');
    
    let totalRecords = 0;
    const results = [];
    
    for (const file of files) {
      try {
        let data = [];
        const fileExt = path.extname(file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
          // Read CSV
          const csvData = fs.readFileSync(file.path, 'utf8');
          data = await new Promise((resolve, reject) => {
            const results = [];
            require('stream').Readable.from([csvData])
              .pipe(csv())
              .on('data', (row) => results.push(row))
              .on('end', () => resolve(results))
              .on('error', reject);
          });
        } else if (fileExt === '.xlsx' || fileExt === '.xls') {
          // Read Excel
          const workbook = XLSX.readFile(file.path);
          const sheetName = workbook.SheetNames[0];
          const worksheet = workbook.Sheets[sheetName];
          data = XLSX.utils.sheet_to_json(worksheet);
        }
        
        if (data.length > 0) {
          // Generate collection name
          const collectionName = generateCollectionName(file.originalname);
          const collection = db.collection(collectionName);
          
          // Add metadata
          const enrichedData = data.map(record => ({
            ...record,
            _fileName: file.originalname,
            _uploadTimestamp: new Date(),
            _fileSize: file.size
          }));
          
          await collection.deleteMany({ _fileName: file.originalname });
          await collection.insertMany(enrichedData);
          
          totalRecords += data.length;
          results.push({
            fileName: file.originalname,
            records: data.length,
            collectionName: collectionName
          });
          
          console.log('✅ ' + file.originalname + ': ' + data.length + ' records -> ' + collectionName);
        }
        
        // Cleanup file
        fs.unlinkSync(file.path);
        
      } catch (error) {
        console.error('❌ Error processing ' + file.originalname + ':', error.message);
        results.push({
          fileName: file.originalname,
          error: error.message,
          records: 0
        });
      }
    }
    
    await mongoClient.close();
    
    res.json({
      success: true,
      message: 'Files uploaded successfully',
      totalFiles: files.length,
      totalRecords: totalRecords,
      files: results
    });
    
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({
      success: false,
      message: 'Upload failed: ' + error.message
    });
  }
});

// Unified data view API
app.get('/api/unified-data-view', async (req, res) => {
  try {
    const mongoData = await getMongoDBSummary();
    const postgresData = await getPostgreSQLSummary();
    
    res.json({
      success: true,
      mongodb: mongoData,
      postgresql: postgresData,
      summary: {
        totalCollections: mongoData.totalCollections,
        totalMongoRecords: mongoData.totalRecords,
        totalPostgresRecords: postgresData.totalRecords,
        totalTables: postgresData.totalTables
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Failed to load unified data view',
      details: error.message
    });
  }
});

// Clear all data API
app.post('/api/clear-all', async (req, res) => {
  try {
    const { exec } = require('child_process');
    
    exec('node clear-all-databases.js', (error, stdout, stderr) => {
      if (error) {
        console.error('Clear script error:', error);
        res.status(500).json({ success: false, message: error.message });
      } else {
        console.log('Clear script output:', stdout);
        res.json({ success: true, message: 'All databases cleared successfully' });
      }
    });
    
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// MongoDB preview (simple version)
app.get('/api/mongodb/preview', async (req, res) => {
  try {
    const mongoData = await getMongoDBSummary();
    
    let html = '<html><head><title>MongoDB Data</title><style>body{font-family:Arial;padding:20px;}</style></head><body>';
    html += '<h1>🍃 MongoDB Collections</h1>';
    html += '<p>Total Collections: ' + mongoData.totalCollections + '</p>';
    html += '<p>Total Records: ' + mongoData.totalRecords + '</p>';
    
    mongoData.collections.forEach(col => {
      html += '<div style="margin:10px 0;padding:10px;border:1px solid #ccc;">';
      html += '<h3>' + col.name + '</h3>';
      html += '<p>Records: ' + col.count + '</p>';
      html += '<p>Type: ' + col.sourceType + '</p>';
      html += '</div>';
    });
    
    html += '</body></html>';
    res.send(html);
    
  } catch (error) {
    res.status(500).send('Error: ' + error.message);
  }
});

// PostgreSQL preview (simple version)
app.get('/api/postgresql/preview', async (req, res) => {
  try {
    const pgData = await getPostgreSQLSummary();
    
    let html = '<html><head><title>PostgreSQL Data</title><style>body{font-family:Arial;padding:20px;}</style></head><body>';
    html += '<h1>🐘 PostgreSQL Tables</h1>';
    html += '<p>Total Tables: ' + pgData.totalTables + '</p>';
    html += '<p>Total Records: ' + pgData.totalRecords + '</p>';
    
    pgData.tables.forEach(table => {
      html += '<div style="margin:10px 0;padding:10px;border:1px solid #ccc;">';
      html += '<h3>' + table.name + '</h3>';
      html += '<p>Records: ' + table.count + '</p>';
      html += '<p>Type: ' + table.type + '</p>';
      html += '</div>';
    });
    
    html += '</body></html>';
    res.send(html);
    
  } catch (error) {
    res.status(500).send('Error: ' + error.message);
  }
});

// Helper functions
function generateCollectionName(fileName) {
  const name = fileName.toLowerCase();
  const timestamp = new Date().toISOString().replace(/[:.]/g, '_').slice(0, 19);
  
  if (name.includes('broker_master') || name.includes('broker master')) return 'broker_master_data_' + timestamp;
  if (name.includes('client')) return 'client_info_data_' + timestamp;
  if (name.includes('distributor')) return 'distributor_master_data_' + timestamp;
  if (name.includes('strategy')) return 'strategy_master_data_' + timestamp;
  if (name.includes('contract')) return 'contract_notes_data_' + timestamp;
  if (name.includes('cash')) return 'cash_capital_flow_data_' + timestamp;
  if (name.includes('stock')) return 'stock_capital_flow_data_' + timestamp;
  if (name.includes('mf') || name.includes('allocation')) return 'mf_allocation_data_' + timestamp;
  
  return 'general_data_' + timestamp;
}

async function getMongoDBSummary() {
  if (!mongoConnected) {
    return { totalCollections: 0, totalRecords: 0, collections: [] };
  }
  
  const { MongoClient } = require('mongodb');
  const mongoClient = new MongoClient(config.mongodb.uri, {
    serverSelectionTimeoutMS: 15000,
    connectTimeoutMS: 15000,
    tls: true,
    tlsAllowInvalidCertificates: true,
    tlsAllowInvalidHostnames: true
  });
  
  await mongoClient.connect();
  const db = mongoClient.db('financial_data_2025');
  
  const collections = await db.listCollections().toArray();
  const allCollections = [];
  let totalRecords = 0;
  
  for (const col of collections) {
    const collection = db.collection(col.name);
    const count = await collection.countDocuments();
    if (count > 0) {
      allCollections.push({
        name: col.name,
        count: count,
        sourceType: getSourceTypeFromCollection(col.name)
      });
      totalRecords += count;
    }
  }
  
  await mongoClient.close();
  
  return {
    totalCollections: allCollections.length,
    totalRecords: totalRecords,
    collections: allCollections
  };
}

async function getPostgreSQLSummary() {
  const client = await pgPool.connect();
  
  const tablesQuery = `
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE'
    ORDER BY table_name
  `;
  const tablesResult = await client.query(tablesQuery);
  
  const tables = [];
  let totalRecords = 0;
  
  for (const table of tablesResult.rows) {
    const tableName = table.table_name;
    
    try {
      const countQuery = 'SELECT COUNT(*) as total FROM ' + tableName;
      const countResult = await client.query(countQuery);
      const count = parseInt(countResult.rows[0].total);
      
      tables.push({
        name: tableName,
        count: count,
        type: getTableType(tableName)
      });
      
      totalRecords += count;
    } catch (error) {
      tables.push({
        name: tableName,
        count: 0,
        type: 'Error'
      });
    }
  }
  
  client.release();
  
  return {
    totalTables: tables.length,
    totalRecords: totalRecords,
    tables: tables
  };
}

function getSourceTypeFromCollection(collectionName) {
  const name = collectionName.toLowerCase();
  if (name.includes('broker_master')) return 'Broker Master';
  if (name.includes('client')) return 'Client Master';
  if (name.includes('distributor')) return 'Distributor Master';
  if (name.includes('strategy')) return 'Strategy Master';
  if (name.includes('contract_notes')) return 'Contract Notes';
  if (name.includes('cash_capital')) return 'Cash Capital Flow';
  if (name.includes('stock_capital')) return 'Stock Capital Flow';
  if (name.includes('mf_allocation')) return 'MF Allocations';
  return 'General Data';
}

function getTableType(tableName) {
  const name = tableName.toLowerCase();
  if (name.includes('_2025_') || name.includes('_2024_')) return 'Daily Tables';
  if (name.includes('brokers') || name.includes('clients') || name.includes('distributors') || name.includes('strategies')) return 'Master Data';
  if (name.includes('contract_notes') || name.includes('capital_flow') || name.includes('allocations')) return 'Transaction Data';
  if (name.includes('custody') || name.includes('holdings')) return 'Custody Holdings';
  return 'Other';
}

// Start server
async function startServer() {
  try {
    await initDB();
    
    app.listen(PORT, () => {
      console.log('🚀 Simple Data Processing Dashboard running at http://localhost:' + PORT);
      console.log('✅ Databases cleared and ready for fresh uploads');
      console.log('📤 MongoDB-first workflow: Upload → Process → View');
    });
    
  } catch (error) {
    console.error('❌ Server startup failed:', error.message);
    process.exit(1);
  }
}

startServer(); 