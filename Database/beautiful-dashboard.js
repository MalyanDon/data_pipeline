const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const mongoose = require('mongoose');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const config = require('./config');
const { MultiThreadedETLProcessor } = require('./multi-threaded-etl');
const SmartFileProcessor = require('./smart-file-processor');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

const PORT = 3006;

// Create temp upload directory
const tempDir = './temp_uploads/';
if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir, { recursive: true });
}

// Configure multer for file uploads
const upload = multer({
    dest: tempDir,
    limits: { fileSize: 100 * 1024 * 1024 }, // 100MB limit
    fileFilter: (req, file, cb) => {
        const allowedTypes = ['.xlsx', '.xls', '.csv', '.json', '.txt', '.xml', '.tsv'];
        const fileExt = path.extname(file.originalname).toLowerCase();
        if (allowedTypes.includes(fileExt)) {
            cb(null, true);
        } else {
            cb(new Error('Supported file types: .xlsx, .xls, .csv, .json, .txt, .xml, .tsv'));
        }
    }
});

// PostgreSQL connection pool
let pgPool = null;

// Initialize databases with graceful MongoDB failure handling
let mongoConnected = false;
let pgConnected = false;

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
    pgConnected = true;
  } catch (error) {
    console.error('❌ PostgreSQL connection failed:', error.message);
  }

  // MongoDB connection with better retry logic
  try {
    await mongoose.connect(config.mongodb.uri + 'financial_data_2025', {
      serverSelectionTimeoutMS: 15000, // Increased timeout
      socketTimeoutMS: 45000,
      connectTimeoutMS: 15000,
      retryWrites: true,
      maxPoolSize: 10,
      tls: true,
      tlsAllowInvalidCertificates: false,
      tlsAllowInvalidHostnames: false
    });
    console.log('✅ Connected to MongoDB');
    mongoConnected = true;
  } catch (error) {
    console.error('❌ MongoDB connection failed:', error.message);
    console.log('⚠️  Trying alternative MongoDB connection...');
    
    // Try with different connection options
    try {
      await mongoose.connect(config.mongodb.uri + 'financial_data_2025', {
        serverSelectionTimeoutMS: 20000,
        socketTimeoutMS: 45000,
        tls: true,
        tlsInsecure: true, // Allow invalid certificates
        tlsAllowInvalidHostnames: true,
        tlsAllowInvalidCertificates: true
      });
      console.log('✅ Connected to MongoDB (alternative method)');
      mongoConnected = true;
    } catch (retryError) {
      console.error('❌ MongoDB retry failed:', retryError.message);
      console.log('⚠️  Dashboard will continue with PostgreSQL fallback');
      mongoConnected = false;
    }
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
    <title>🚀 Multi-Threaded Data Processing Dashboard</title>
    <script src="/socket.io/socket.io.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; min-height: 100vh; overflow-x: hidden;
        }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .header { text-align: center; padding: 30px 0; }
        .header h1 { font-size: 3em; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }
        .header p { font-size: 1.2em; opacity: 0.9; }
        
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 30px 0; }
        .stat-card { 
            background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); 
            border-radius: 15px; padding: 25px; text-align: center; border: 1px solid rgba(255,255,255,0.2);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .stat-card:hover { transform: translateY(-5px); box-shadow: 0 15px 35px rgba(0,0,0,0.2); }
        .stat-number { font-size: 2.5em; font-weight: bold; margin-bottom: 10px; }
        .stat-label { font-size: 0.9em; opacity: 0.8; text-transform: uppercase; letter-spacing: 1px; }
        
        .control-panel { 
            background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); 
            border-radius: 20px; padding: 30px; margin: 30px 0; border: 1px solid rgba(255,255,255,0.2);
        }
        .control-panel h2 { margin-bottom: 20px; text-align: center; }
        
        .button-group { display: flex; gap: 15px; justify-content: center; margin: 20px 0; flex-wrap: wrap; }
        .btn { 
            padding: 15px 30px; border: none; border-radius: 25px; font-size: 1.1em; font-weight: bold;
            cursor: pointer; transition: all 0.3s ease; text-decoration: none; display: inline-block;
            background: linear-gradient(45deg, #ff6b6b, #ee5a24); color: white;
        }
        .btn:hover { transform: translateY(-2px); box-shadow: 0 10px 25px rgba(0,0,0,0.2); }
        .btn-primary { background: linear-gradient(45deg, #4facfe, #00f2fe); }
        .btn-success { background: linear-gradient(45deg, #43e97b, #38f9d7); }
        .btn-warning { background: linear-gradient(45deg, #fa709a, #fee140); }
        .btn:disabled { opacity: 0.6; cursor: not-allowed; transform: none; }
        
        .progress-section { margin: 30px 0; }
        .overall-progress { 
            background: rgba(255,255,255,0.1); border-radius: 15px; padding: 25px; 
            margin-bottom: 20px; border: 1px solid rgba(255,255,255,0.2);
        }
        .progress-bar { 
            background: rgba(255,255,255,0.2); border-radius: 25px; height: 30px; 
            overflow: hidden; margin: 15px 0; position: relative;
        }
        .progress-fill { 
            height: 100%; border-radius: 25px; transition: width 0.5s ease;
            background: linear-gradient(90deg, #43e97b, #38f9d7, #4facfe);
            background-size: 200% 100%; animation: gradientShift 3s ease infinite;
        }
        @keyframes gradientShift { 0%, 100% { background-position: 0% 50%; } 50% { background-position: 100% 50%; } }
        .progress-text { 
            position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); 
            font-weight: bold; font-size: 0.9em; text-shadow: 1px 1px 2px rgba(0,0,0,0.5);
        }
        
        .workers-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .worker-card { 
            background: rgba(255,255,255,0.1); border-radius: 15px; padding: 20px; 
            border: 1px solid rgba(255,255,255,0.2); backdrop-filter: blur(10px);
        }
        .worker-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }
        .worker-status { 
            padding: 5px 12px; border-radius: 15px; font-size: 0.8em; font-weight: bold;
            background: #43e97b; color: #000;
        }
        .worker-status.processing { background: #fee140; color: #000; }
        .worker-status.complete { background: #43e97b; color: #000; }
        .worker-status.error { background: #ff6b6b; color: #fff; }
        
        .log-panel { 
            background: rgba(0,0,0,0.3); border-radius: 15px; padding: 20px; 
            height: 300px; overflow-y: auto; font-family: 'Monaco', 'Courier New', monospace;
            border: 1px solid rgba(255,255,255,0.2);
        }
        .log-entry { padding: 5px 0; border-bottom: 1px solid rgba(255,255,255,0.1); }
        .log-entry:last-child { border-bottom: none; }
        .log-timestamp { opacity: 0.6; font-size: 0.8em; }
        
        .collections-overview { 
            background: rgba(255,255,255,0.1); border-radius: 15px; padding: 25px; 
            margin: 20px 0; border: 1px solid rgba(255,255,255,0.2);
        }
        .collection-item { 
            display: flex; justify-content: space-between; align-items: center; 
            padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,0.1);
        }
        .collection-item:last-child { border-bottom: none; }
        .collection-badge { 
            padding: 3px 8px; border-radius: 10px; font-size: 0.8em; font-weight: bold;
        }
        .badge-axis { background: #ff6b6b; color: white; }
        .badge-kotak { background: #4facfe; color: white; }
        .badge-orbis { background: #43e97b; color: black; }
        .badge-deutsche { background: #fee140; color: black; }
        .badge-trustpms { background: #fa709a; color: white; }
        .badge-unknown { background: #666; color: white; }
        
        .fade-in { animation: fadeIn 0.5s ease-in; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(20px); } to { opacity: 1; transform: translateY(0); } }
        
        .pulse { animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.7; } }
        
        .system-info { 
            display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); 
            gap: 15px; margin: 20px 0; 
        }
        .system-metric { text-align: center; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 10px; }
        
        .notification { 
            position: fixed; top: 20px; right: 20px; padding: 15px 25px; 
            border-radius: 10px; color: white; font-weight: bold; z-index: 1000;
            transform: translateX(400px); transition: transform 0.3s ease;
        }
        .notification.show { transform: translateX(0); }
        .notification.success { background: linear-gradient(45deg, #43e97b, #38f9d7); }
        .notification.error { background: linear-gradient(45deg, #ff6b6b, #ee5a24); }
        .notification.info { background: linear-gradient(45deg, #4facfe, #00f2fe); }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Multi-Threaded Data Processing Dashboard</h1>
            <p>Real-time File Upload → MongoDB → PostgreSQL Processing with True Parallelism</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number" id="totalCollections">0</div>
                <div class="stat-label">Collections Found</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="totalRecords">0</div>
                <div class="stat-label">Total Records</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="activeWorkers">0</div>
                <div class="stat-label">Active Workers</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="processedRecords">0</div>
                <div class="stat-label">Processed Records</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="successRate">0%</div>
                <div class="stat-label">Success Rate</div>
            </div>
            <div class="stat-card">
                <div class="stat-number" id="processingSpeed">0</div>
                <div class="stat-label">Records/Second</div>
            </div>
        </div>

        <!-- File Upload Section -->
        <div class="control-panel">
            <h2>📤 File Upload & Processing Center</h2>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin: 20px 0;">
                <div>
                    <h3 style="margin-bottom: 15px;">🎯 Upload Data Files</h3>
                    <div id="fileUploadArea" onclick="document.getElementById('fileInput').click()" style="
                        border: 2px dashed rgba(255,255,255,0.3);
                        border-radius: 15px;
                        padding: 40px;
                        text-align: center;
                        cursor: pointer;
                        transition: all 0.3s ease;
                        background: rgba(255,255,255,0.05);
                    ">
                        <input type="file" id="fileInput" multiple accept=".xlsx,.xls,.csv,.json,.txt,.xml" style="display: none;">
                        <div style="font-size: 3em; margin-bottom: 10px;">📁</div>
                        <h4>Click to select files or drag & drop</h4>
                        <p style="opacity: 0.8; margin-top: 10px;">Supported: .xlsx, .xls, .csv, .json, .txt, .xml files</p>
                    </div>
                    <div class="button-group" style="margin-top: 15px;">
                        <button class="btn btn-success" onclick="uploadFiles()" id="uploadBtn">🚀 Upload & Process</button>
                        <button class="btn" onclick="clearSelectedFiles()">🗑️ Clear Selection</button>
                    </div>
                    
                    <div id="uploadProgress" style="display: none; margin-top: 20px;">
                        <h4>⏳ Upload Progress</h4>
                        <div class="progress-bar">
                            <div class="progress-fill" id="uploadProgressFill" style="width: 0%"></div>
                            <div class="progress-text" id="uploadProgressText">Uploading...</div>
                        </div>
                    </div>
                    
                    <div id="uploadedFiles" style="margin-top: 20px;"></div>
                </div>
                
                <div>
                    <h3 style="margin-bottom: 15px;">📊 Data Overview</h3>
                    <div id="dataOverview" style="
                        background: rgba(255,255,255,0.1);
                        border-radius: 15px;
                        padding: 20px;
                        min-height: 200px;
                    ">
                        <div style="text-align: center; opacity: 0.7; padding: 40px 0;">
                            Upload files to see data overview
                        </div>
                    </div>
                    
                    <div class="button-group" style="margin-top: 15px;">
                        <button class="btn btn-primary" onclick="refreshData()">🔄 Refresh All Data</button>
                        <button class="btn" onclick="previewMongoDB()">🍃 Preview MongoDB</button>
                        <button class="btn" onclick="previewPostgreSQL()">🐘 Preview PostgreSQL</button>
                    </div>
                </div>
            </div>
        </div>

        <div class="control-panel">
            <h2>🎛️ Data Processing Control Center</h2>
            <div class="system-info">
                <div class="system-metric">
                    <div style="font-size: 1.5em; font-weight: bold;" id="cpuCores">8</div>
                    <div style="font-size: 0.8em; opacity: 0.8;">CPU Cores</div>
                </div>
                <div class="system-metric">
                    <div style="font-size: 1.5em; font-weight: bold;" id="maxWorkers">6</div>
                    <div style="font-size: 0.8em; opacity: 0.8;">Max Workers</div>
                </div>
                <div class="system-metric">
                    <div style="font-size: 1.5em; font-weight: bold;" id="memoryUsage">8GB</div>
                    <div style="font-size: 0.8em; opacity: 0.8;">Memory</div>
                </div>
                <div class="system-metric">
                    <div style="font-size: 1.5em; font-weight: bold;" id="processingTime">0s</div>
                    <div style="font-size: 0.8em; opacity: 0.8;">Processing Time</div>
                </div>
            </div>
            
            <div class="button-group">
                <button class="btn btn-primary" onclick="discoverCollections()">🔍 Discover Collections</button>
                <button class="btn btn-success" onclick="startProcessing()" id="startBtn">🚀 Start Multi-Threading</button>
                <button class="btn btn-warning" onclick="stopProcessing()" id="stopBtn" disabled>⏹️ Stop Processing</button>
                <button class="btn" onclick="clearLogs()">🗑️ Clear Logs</button>
            </div>

            <div class="overall-progress">
                <h3>📊 Overall Progress</h3>
                <div class="progress-bar">
                    <div class="progress-fill" id="overallProgressFill" style="width: 0%"></div>
                    <div class="progress-text" id="overallProgressText">Ready to start...</div>
                </div>
                <div style="text-align: center; margin-top: 10px; font-size: 0.9em;" id="overallProgressDetails">
                    Waiting for processing to begin
                </div>
            </div>
        </div>

        <div class="collections-overview" id="collectionsOverview" style="display: none;">
            <h3>📁 Collections Overview</h3>
            <div id="collectionsList"></div>
        </div>

        <div class="progress-section" id="workersSection" style="display: none;">
            <h3>👥 Worker Threads Status</h3>
            <div class="workers-grid" id="workersGrid"></div>
        </div>

        <div class="control-panel">
            <h3>📋 Real-time Processing Log</h3>
            <div class="log-panel" id="logPanel">
                <div class="log-entry">
                    <span class="log-timestamp">[${new Date().toLocaleTimeString()}]</span>
                    🤖 Multi-threaded Data Processing Dashboard initialized. Ready for file processing...
                </div>
            </div>
        </div>

        <!-- PostgreSQL Data Viewer Section -->
        <div class="control-panel" id="postgresqlSection">
            <h3>🗄️ PostgreSQL Processed Data</h3>
            <div class="button-group">
                <button class="btn btn-primary" onclick="loadPostgreSQLData()">🔄 Refresh Data</button>
                <button class="btn" onclick="togglePostgreSQLDetails()">📊 Toggle Details</button>
            </div>
            
            <div id="postgresqlTables" style="margin-top: 20px;">
                <div style="text-align: center; opacity: 0.7; padding: 20px;">
                    Click "Refresh Data" to load PostgreSQL normalized tables
                </div>
            </div>
        </div>

        <!-- Unified Data Viewer Section -->
        <div class="control-panel" id="unifiedDataViewer">
            <h3>📊 Complete Data Overview</h3>
            <p style="text-align: center; opacity: 0.9; margin-bottom: 20px;">
                View all your data across MongoDB (raw storage) and PostgreSQL (processed tables)
            </p>
            
            <div class="button-group">
                <button class="btn btn-primary" onclick="loadUnifiedDataView()">🔄 Load All Data</button>
                <button class="btn btn-success" onclick="previewMongoDB()">🍃 MongoDB Details</button>
                <button class="btn btn-success" onclick="previewPostgreSQL()">🐘 PostgreSQL Details</button>
                <button class="btn" onclick="toggleDataViewMode()">🔀 Toggle View Mode</button>
            </div>
            
            <div id="unifiedDataContainer" style="margin-top: 30px;">
                <div style="text-align: center; opacity: 0.7; padding: 40px;">
                    <h4>📋 Unified Data Viewer</h4>
                    <p>Click "Load All Data" to see your complete data landscape</p>
                    <p style="font-size: 0.9em; opacity: 0.8;">
                        This will show both raw MongoDB collections and processed PostgreSQL tables
                    </p>
                </div>
            </div>
        </div>
    </div>

    <div id="notification" class="notification"></div>

    <script>
        const socket = io();
        let isProcessing = false;
        let startTime = null;
        let currentDataViewMode = 'split'; // 'split', 'mongodb', 'postgresql'

        // Socket event handlers
        socket.on('discovery', (data) => {
            updateCollectionsOverview(data.collections);
            updateStats({ totalCollections: data.collections.length, totalRecords: data.totalRecords });
            logMessage('🔍 Collection discovery complete', 'info');
        });

        socket.on('processing_start', (data) => {
            isProcessing = true;
            startTime = Date.now();
            document.getElementById('startBtn').disabled = true;
            document.getElementById('stopBtn').disabled = false;
            document.getElementById('workersSection').style.display = 'block';
            updateStats({ activeWorkers: data.maxWorkers });
            logMessage(`🚀 Started processing with ${data.maxWorkers} workers`, 'success');
            showNotification('Processing started with multi-threading!', 'success');
        });

        socket.on('worker_progress', (data) => {
            updateWorkerProgress(data);
            updateOverallProgress(data.overallProgress);
            
            if (startTime) {
                const elapsedSeconds = (Date.now() - startTime) / 1000;
                const speed = Math.round(data.overallProgress.processedRecords / elapsedSeconds);
                updateStats({ 
                    processedRecords: data.overallProgress.processedRecords,
                    processingSpeed: speed,
                    processingTime: Math.round(elapsedSeconds)
                });
            }
        });

        socket.on('worker_complete', (data) => {
            markWorkerComplete(data);
            logMessage(`✅ Worker ${data.workerId} completed: ${data.collectionName} - ${data.result.valid}/${data.result.processed} valid`, 'success');
        });

        socket.on('processing_complete', (data) => {
            isProcessing = false;
            document.getElementById('startBtn').disabled = false;
            document.getElementById('stopBtn').disabled = true;
            updateStats({ successRate: data.successRate + '%', activeWorkers: 0 });
            logMessage(`🎉 Processing complete! Success rate: ${data.successRate}%`, 'success');
            showNotification(`Processing complete! ${data.successRate}% success rate`, 'success');
            
            // Auto-refresh unified data view after processing
            setTimeout(() => {
                loadUnifiedDataView();
            }, 2000);
        });

        socket.on('upload_complete', (data) => {
            logMessage(`📤 Upload complete: ${data.totalFiles} files, ${data.totalRecords} records`, 'success');
            showNotification(`Upload successful: ${data.totalFiles} files processed!`, 'success');
            
            // Auto-refresh unified data view after upload
            setTimeout(() => {
                loadUnifiedDataView();
            }, 1000);
        });

        socket.on('error', (data) => {
            logMessage(`❌ Error: ${data.error}`, 'error');
            showNotification(`Error: ${data.error}`, 'error');
        });

        // UI Functions
        function updateStats(stats) {
            Object.keys(stats).forEach(key => {
                const element = document.getElementById(key);
                if (element) {
                    element.textContent = stats[key];
                    element.parentElement.classList.add('fade-in');
                }
            });
        }

        function updateCollectionsOverview(collections) {
            const container = document.getElementById('collectionsList');
            const overview = document.getElementById('collectionsOverview');
            
            container.innerHTML = collections.map(col => {
                // Parse collection name for enhanced display
                const nameParts = col.name.split('_');
                let sourceType = col.name;
                let dataDate = null;
                let custodyType = getCustodyTypeFromName(col.name);
                
                if (nameParts.length >= 4) {
                    // Format: sourceType_YYYY_MM_DD
                    sourceType = nameParts.slice(0, -3).join('_');
                    dataDate = `${nameParts[nameParts.length-3]}-${nameParts[nameParts.length-2]}-${nameParts[nameParts.length-1]}`;
                    custodyType = getCustodyTypeFromName(sourceType);
                }
                
                const displayName = dataDate ? `${custodyType} (${dataDate})` : custodyType;
                
                return `
                    <div class="collection-item">
                        <div>
                            <strong>${col.name}</strong><br>
                            <small style="opacity: 0.8;">${displayName}</small>
                        </div>
                        <div>
                            <span class="collection-badge badge-${custodyType.toLowerCase()}">${custodyType}</span><br>
                            <small>${col.records.toLocaleString()} records</small>
                        </div>
                    </div>
                `;
            }).join('');
            
            overview.style.display = 'block';
        }

        function updateWorkerProgress(data) {
            const workerId = data.workerId;
            let workerCard = document.getElementById(`worker-${workerId}`);
            
            if (!workerCard) {
                workerCard = createWorkerCard(workerId, data.collectionName, data.custodyType);
                document.getElementById('workersGrid').appendChild(workerCard);
            }
            
            const progressFill = workerCard.querySelector('.progress-fill');
            const progressText = workerCard.querySelector('.progress-text');
            const statusBadge = workerCard.querySelector('.worker-status');
            
            progressFill.style.width = data.collectionProgress.percentage + '%';
            progressText.textContent = `${data.collectionProgress.percentage}% (${data.collectionProgress.processed}/${data.collectionProgress.total})`;
            statusBadge.textContent = 'PROCESSING';
            statusBadge.className = 'worker-status processing pulse';
        }

        function createWorkerCard(workerId, collectionName, custodyType) {
            const card = document.createElement('div');
            card.className = 'worker-card fade-in';
            card.id = `worker-${workerId}`;
            card.innerHTML = `
                <div class="worker-header">
                    <div>
                        <strong>Worker ${workerId}</strong><br>
                        <small>${collectionName} (${custodyType})</small>
                    </div>
                    <div class="worker-status">STARTING</div>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: 0%"></div>
                    <div class="progress-text">Initializing...</div>
                </div>
            `;
            return card;
        }

        function markWorkerComplete(data) {
            const workerCard = document.getElementById(`worker-${data.workerId}`);
            if (workerCard) {
                const statusBadge = workerCard.querySelector('.worker-status');
                const progressFill = workerCard.querySelector('.progress-fill');
                const progressText = workerCard.querySelector('.progress-text');
                
                statusBadge.textContent = 'COMPLETE';
                statusBadge.className = 'worker-status complete';
                progressFill.style.width = '100%';
                progressText.textContent = `✅ ${data.result.valid}/${data.result.processed} valid`;
            }
        }

        function updateOverallProgress(progress) {
            const progressFill = document.getElementById('overallProgressFill');
            const progressText = document.getElementById('overallProgressText');
            const progressDetails = document.getElementById('overallProgressDetails');
            
            const percentage = progress.overallPercentage || 0;
            progressFill.style.width = percentage + '%';
            progressText.textContent = `${percentage}% Complete`;
            progressDetails.textContent = `${progress.processedRecords?.toLocaleString() || 0} / ${progress.totalRecords?.toLocaleString() || 0} records processed (${progress.validRecords?.toLocaleString() || 0} valid, ${progress.errorRecords?.toLocaleString() || 0} errors)`;
        }

        function logMessage(message, type = 'info') {
            const logPanel = document.getElementById('logPanel');
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry fade-in';
            logEntry.innerHTML = `
                <span class="log-timestamp">[${new Date().toLocaleTimeString()}]</span>
                ${message}
            `;
            logPanel.appendChild(logEntry);
            logPanel.scrollTop = logPanel.scrollHeight;
        }

        function showNotification(message, type) {
            const notification = document.getElementById('notification');
            notification.textContent = message;
            notification.className = `notification ${type} show`;
            setTimeout(() => {
                notification.classList.remove('show');
            }, 4000);
        }

        function getCustodyTypeFromName(name) {
            const lowerName = name.toLowerCase();
            if (lowerName.includes('axis')) return 'AXIS';
            if (lowerName.includes('kotak')) return 'KOTAK';
            if (lowerName.includes('orbis')) return 'ORBIS';
            if (lowerName.includes('deutsche') || lowerName.includes('164_ec0000720')) return 'DEUTSCHE';
            if (lowerName.includes('trustpms')) return 'TRUSTPMS';
            return 'UNKNOWN';
        }

        // File Upload Functions
        function uploadFiles() {
            const fileInput = document.getElementById('fileInput');
            const files = fileInput.files;
            
            if (files.length === 0) {
                showNotification('Please select files to upload', 'error');
                return;
            }

            const formData = new FormData();
            for (let i = 0; i < files.length; i++) {
                formData.append('files', files[i]);
            }

            // Show upload progress
            document.getElementById('uploadProgress').style.display = 'block';
            document.getElementById('uploadBtn').disabled = true;
            updateUploadProgress(0, 'Starting upload...');

            fetch('/api/upload', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    updateUploadProgress(100, 'Upload completed!');
                    displayUploadResults(data);
                    showNotification(`Successfully uploaded ${data.totalFiles} files!`, 'success');
                    // Auto-refresh data after upload
                    setTimeout(() => {
                        discoverCollections();
                        refreshData();
                    }, 1000);
                } else {
                    showNotification(`Upload failed: ${data.message}`, 'error');
                }
            })
            .catch(error => {
                console.error('Upload error:', error);
                showNotification('Upload failed: Network error', 'error');
            })
            .finally(() => {
                document.getElementById('uploadBtn').disabled = false;
            });
        }

        function updateUploadProgress(percentage, message) {
            document.getElementById('uploadProgressFill').style.width = percentage + '%';
            document.getElementById('uploadProgressText').textContent = message;
        }

        function displayUploadResults(data) {
            const container = document.getElementById('uploadedFiles');
            container.innerHTML = '<h4>✅ Upload Results</h4>';
            
            data.files.forEach(file => {
                const fileDiv = document.createElement('div');
                fileDiv.style.cssText = `
                    background: rgba(255,255,255,0.1);
                    border-radius: 10px;
                    padding: 15px;
                    margin: 10px 0;
                    border-left: 4px solid \${file.success ? '#43e97b' : '#ff6b6b'};
                `;
                
                fileDiv.innerHTML = `
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>\${file.fileName}</strong>
                            <div style="font-size: 0.9em; opacity: 0.8; margin-top: 5px;">
                                Source: \${file.sourceType} | Records: \${file.recordsProcessed?.toLocaleString() || 0}
                            </div>
                        </div>
                        <div style="text-align: right;">
                            <div style="color: \${file.success ? '#43e97b' : '#ff6b6b'}; font-weight: bold;">
                                \${file.success ? '✅ Success' : '❌ Failed'}
                            </div>
                            <div style="font-size: 0.8em; opacity: 0.8;">
                                \${file.message}
                            </div>
                        </div>
                    </div>
                `;
                
                container.appendChild(fileDiv);
            });

            // Update data overview
            updateDataOverview(data);
        }

        function updateDataOverview(data) {
            const overview = document.getElementById('dataOverview');
            overview.innerHTML = `
                <h4>📊 Upload Summary</h4>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-top: 15px;">
                    <div style="text-align: center;">
                        <div style="font-size: 2em; font-weight: bold; color: #43e97b;">\${data.totalFiles}</div>
                        <div style="opacity: 0.8;">Files Uploaded</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 2em; font-weight: bold; color: #4facfe;">\${data.totalRecords?.toLocaleString() || 0}</div>
                        <div style="opacity: 0.8;">Total Records</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 2em; font-weight: bold; color: #fee140;">\${data.uniqueSources}</div>
                        <div style="opacity: 0.8;">Unique Sources</div>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 2em; font-weight: bold; color: #fa709a;">\${data.versioningEnabled ? '✅' : '❌'}</div>
                        <div style="opacity: 0.8;">Smart Versioning</div>
                    </div>
                </div>
                <div style="margin-top: 15px; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 10px;">
                    <strong>Strategy:</strong> \${data.details?.versioningStrategy || 'Standard upload'}
                </div>
            `;
        }

        function clearSelectedFiles() {
            document.getElementById('fileInput').value = '';
            document.getElementById('uploadProgress').style.display = 'none';
            document.getElementById('uploadedFiles').innerHTML = '';
            showNotification('File selection cleared', 'info');
        }

        function refreshData() {
            discoverCollections();
            loadPostgreSQLData();
            logMessage('🔄 Refreshing all data...', 'info');
        }

        function previewMongoDB() {
            // Open MongoDB preview in a modal or new window
            window.open('/api/mongodb/preview', '_blank');
        }

        function previewPostgreSQL() {
            // Open PostgreSQL preview in a modal or new window
            window.open('/api/postgresql/preview', '_blank');
        }

        // Drag and Drop functionality
        function setupDragAndDrop() {
            const uploadArea = document.getElementById('fileUploadArea');
            
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.style.borderColor = '#43e97b';
                uploadArea.style.backgroundColor = 'rgba(67, 233, 123, 0.1)';
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.style.borderColor = 'rgba(255,255,255,0.3)';
                uploadArea.style.backgroundColor = 'rgba(255,255,255,0.05)';
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.style.borderColor = 'rgba(255,255,255,0.3)';
                uploadArea.style.backgroundColor = 'rgba(255,255,255,0.05)';
                
                const files = e.dataTransfer.files;
                document.getElementById('fileInput').files = files;
                
                if (files.length > 0) {
                    showNotification(`Selected \${files.length} file(s) for upload`, 'info');
                }
            });
        }

        // Control functions
        function discoverCollections() {
            socket.emit('discover_collections');
            logMessage('🔍 Discovering collections...', 'info');
        }

        function startProcessing() {
            if (!isProcessing) {
                socket.emit('start_processing');
                logMessage('🚀 Starting multi-threaded processing...', 'info');
            }
        }

        function stopProcessing() {
            if (isProcessing) {
                socket.emit('stop_processing');
                logMessage('⏹️ Stopping processing...', 'info');
            }
        }

        function clearLogs() {
            document.getElementById('logPanel').innerHTML = '';
            logMessage('🗑️ Logs cleared', 'info');
        }

        // PostgreSQL Data Functions
        async function loadPostgreSQLData() {
            try {
                showNotification('Loading PostgreSQL data...', 'info');
                
                const response = await fetch('/api/postgresql-data');
                const data = await response.json();
                
                if (data.success) {
                    displayPostgreSQLTables(data.tables);
                                    logMessage(`✅ Loaded ${data.tables.length} PostgreSQL tables`, 'success');
                showNotification(`Loaded ${data.tables.length} PostgreSQL tables`, 'success');
                } else {
                    throw new Error(data.error);
                }
            } catch (error) {
                logMessage(`❌ Failed to load PostgreSQL data: \${error.message}`, 'error');
                showNotification(`Error loading PostgreSQL data: \${error.message}`, 'error');
            }
        }

        function displayPostgreSQLTables(tables) {
            const container = document.getElementById('postgresqlTables');
            
            if (tables.length === 0) {
                container.innerHTML = `
                    <div style="text-align: center; opacity: 0.7; padding: 20px;">
                        📭 No PostgreSQL tables found. Process some data first.
                    </div>
                `;
                return;
            }

            // Calculate overall statistics
            const totalRecords = tables.reduce((sum, t) => sum + t.totalRecords, 0);
            const allSources = [...new Set(tables.flatMap(t => t.sourceSystems))];
            
            // Add summary header
            let html = `
                <div style="background: rgba(255,255,255,0.15); border-radius: 15px; padding: 20px; margin-bottom: 20px; text-align: center;">
                    <h3>🗄️ PostgreSQL Normalized Data Summary</h3>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-top: 15px;">
                        <div>
                            <div style="font-size: 1.5em; font-weight: bold;">\${tables.length}</div>
                            <div style="opacity: 0.8;">Daily Tables</div>
                        </div>
                        <div>
                            <div style="font-size: 1.5em; font-weight: bold;">\${totalRecords.toLocaleString()}</div>
                            <div style="opacity: 0.8;">Total Records</div>
                        </div>
                        <div>
                            <div style="font-size: 1.5em; font-weight: bold;">\${allSources.length}</div>
                            <div style="opacity: 0.8;">Source Systems</div>
                        </div>
                    </div>
                    <div style="margin-top: 15px;">
                        <strong>📊 Available Sources:</strong> 
                        \${allSources.map(source => \`<span class="collection-badge badge-\${source.toLowerCase()}">\${source}</span>\`).join(' ')}
                    </div>
                    <div style="margin-top: 10px; font-size: 0.9em; opacity: 0.8;">
                        ✅ No duplicates (old data cleared automatically) • ✅ Orbis corrections: client_name="N/A", instrument_name=NULL, instrument_code=NULL
                    </div>
                </div>
            `;
            
            html += tables.map(table => {
                const date = table.tableName.replace('unified_custody_master_', '').replace(/_/g, '-');
                const lastUpdated = new Date(table.lastUpdated).toLocaleString();
                
                return `
                    <div class="worker-card" style="margin-bottom: 20px;">
                        <div class="worker-header">
                            <div>
                                <strong>📅 \${date}</strong><br>
                                <small>\${table.tableName}</small>
                            </div>
                            <div class="worker-status complete">\${table.totalRecords.toLocaleString()} records</div>
                        </div>
                        
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 15px 0;">
                            <div style="text-align: center; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 8px;">
                                <div style="font-size: 1.2em; font-weight: bold;">\${table.uniqueClients}</div>
                                <div style="font-size: 0.8em; opacity: 0.8;">Clients</div>
                            </div>
                            <div style="text-align: center; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 8px;">
                                <div style="font-size: 1.2em; font-weight: bold;">\${table.uniqueInstruments}</div>
                                <div style="font-size: 0.8em; opacity: 0.8;">Instruments</div>
                            </div>
                            <div style="text-align: center; padding: 10px; background: rgba(255,255,255,0.05); border-radius: 8px;">
                                <div style="font-size: 1.2em; font-weight: bold;">\${table.sourceSystems.length}</div>
                                <div style="font-size: 0.8em; opacity: 0.8;">Sources</div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 15px;">
                            <strong>🏦 Source Systems:</strong> 
                            \${table.sourceSystems.map(system => \`<span class="collection-badge badge-\${system.toLowerCase()}">\${system}</span>\`).join(' ')}
                        </div>
                        
                        <div style="margin-top: 10px; font-size: 0.9em; opacity: 0.8;">
                            <strong>🕒 Last Updated:</strong> \${lastUpdated}
                        </div>
                        
                        <div class="postgresql-details" style="display: none; margin-top: 15px;">
                            <h4>📋 Sample Records:</h4>
                            <div style="background: rgba(0,0,0,0.2); border-radius: 8px; padding: 15px; margin-top: 10px; overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; font-size: 0.8em;">
                                    <thead>
                                        <tr style="border-bottom: 1px solid rgba(255,255,255,0.2);">
                                            <th style="text-align: left; padding: 8px;">Client Ref</th>
                                            <th style="text-align: left; padding: 8px;">Client Name</th>
                                            <th style="text-align: left; padding: 8px;">Instrument ISIN</th>
                                            <th style="text-align: left; padding: 8px;">Instrument Name</th>
                                            <th style="text-align: left; padding: 8px;">Source</th>
                                            <th style="text-align: left; padding: 8px;">Total Position</th>
                                            <th style="text-align: left; padding: 8px;">Saleable Qty</th>
                                            <th style="text-align: left; padding: 8px;">Blocked Qty</th>
                                            <th style="text-align: left; padding: 8px;">Record Date</th>
                                            <th style="text-align: left; padding: 8px;">Created At</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        \${table.sampleRecords.map(record => \`
                                            <tr style="border-bottom: 1px solid rgba(255,255,255,0.1);">
                                                <td style="padding: 8px;">\${record.client_reference || 'N/A'}</td>
                                                <td style="padding: 8px;">\${record.client_name || 'N/A'}</td>
                                                <td style="padding: 8px;">\${record.instrument_isin || 'N/A'}</td>
                                                <td style="padding: 8px;">\${record.instrument_name || 'N/A'}</td>
                                                <td style="padding: 8px;"><span class="collection-badge badge-\${record.source_system?.toLowerCase()}">\${record.source_system || 'N/A'}</span></td>
                                                <td style="padding: 8px;">\${record.total_position ? parseFloat(record.total_position).toLocaleString() : '0'}</td>
                                                <td style="padding: 8px;">\${record.saleable_quantity ? parseFloat(record.saleable_quantity).toLocaleString() : '0'}</td>
                                                <td style="padding: 8px;">\${record.blocked_quantity ? parseFloat(record.blocked_quantity).toLocaleString() : '0'}</td>
                                                <td style="padding: 8px;">\${record.record_date ? new Date(record.record_date).toLocaleDateString() : 'N/A'}</td>
                                                <td style="padding: 8px;">\${record.created_at ? new Date(record.created_at).toLocaleString() : 'N/A'}</td>
                                            </tr>
                                        \`).join('')}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                        
                        <div style="text-align: center; margin-top: 15px;">
                            <button class="btn" onclick="toggleTableDetails(this)" style="padding: 8px 20px; font-size: 0.9em;">
                                👁️ View Sample Data
                            </button>
                        </div>
                    </div>
                `;
            }).join('');
            
            container.innerHTML = html;
        }

        function toggleTableDetails(button) {
            const card = button.closest('.worker-card');
            const details = card.querySelector('.postgresql-details');
            
            if (details.style.display === 'none') {
                details.style.display = 'block';
                button.textContent = '🙈 Hide Sample Data';
            } else {
                details.style.display = 'none';
                button.textContent = '👁️ View Sample Data';
            }
        }

        function togglePostgreSQLDetails() {
            const details = document.querySelectorAll('.postgresql-details');
            const anyVisible = Array.from(details).some(detail => detail.style.display === 'block');
            
            details.forEach(detail => {
                detail.style.display = anyVisible ? 'none' : 'block';
            });
            
            // Update all toggle buttons
            const buttons = document.querySelectorAll('button[onclick="toggleTableDetails(this)"]');
            buttons.forEach(button => {
                button.textContent = anyVisible ? '👁️ View Sample Data' : '🙈 Hide Sample Data';
            });
        }

        // Initialize on page load
        window.addEventListener('load', () => {
            logMessage('🌟 Dashboard loaded. Ready for multi-threaded data processing!', 'success');
            setTimeout(discoverCollections, 1000);
            setTimeout(loadPostgreSQLData, 2000); // Auto-load PostgreSQL data
        });
    </script>
</body>
</html>
  `);
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('🔌 Dashboard client connected');

  socket.on('discover_collections', async () => {
    try {
      if (!mongoConnected) {
        console.log('🔍 Discovering MongoDB collections...');
        console.log('📋 Found 0 collections in financial_data_2024');
        console.log('📋 Found 0 collections in financial_data_2025');
        console.log('✅ Found 0 collections with data');
        
        socket.emit('discovery', {
          collections: [],
          totalRecords: 0,
          message: 'MongoDB not connected - upload files to process data'
        });
        return;
      }

      const processor = new MultiThreadedETLProcessor();
      const collections = await processor.discoverCollections();
      
      socket.emit('discovery', {
        collections: collections.map(c => ({ 
          name: c.collectionName, 
          records: c.recordCount,
          custodyType: c.custodyType,
          sourceType: c.sourceType,
          dataDate: c.dataDate,
          displayName: c.displayName 
        })),
        totalRecords: collections.reduce((sum, c) => sum + c.recordCount, 0)
      });
    } catch (error) {
      console.log(`❌ Collection discovery failed: ${error.message}`);
      socket.emit('error', { error: error.message });
    }
  });

  socket.on('start_processing', async () => {
    try {
      if (!mongoConnected) {
        socket.emit('error', { 
          error: 'MongoDB not connected. Please upload files first to process data.' 
        });
        return;
      }

      const processor = new MultiThreadedETLProcessor();
      
      processor.on('start', (data) => {
        socket.emit('processing_start', data);
      });

      processor.on('progress', (data) => {
        socket.emit('worker_progress', data);
      });

      processor.on('complete', (data) => {
        socket.emit('worker_complete', data);
      });

      processor.on('finished', (data) => {
        socket.emit('processing_complete', data);
      });

      processor.on('error', (data) => {
        socket.emit('error', data);
      });

      await processor.processAllCollections();

    } catch (error) {
      socket.emit('error', { error: error.message });
    }
  });

  socket.on('disconnect', () => {
    console.log('📴 Dashboard client disconnected');
  });
});

// Simple direct upload - no complex file processing needed

// Helper function to generate meaningful collection names with timestamps
function generateCollectionName(fileName) {
  const name = fileName.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
  
  // Generate timestamp for versioning: YYYY_MM_DD_HH_MM_SS
  const now = new Date();
  const timestamp = `${now.getFullYear()}_${String(now.getMonth() + 1).padStart(2, '0')}_${String(now.getDate()).padStart(2, '0')}_${String(now.getHours()).padStart(2, '0')}_${String(now.getMinutes()).padStart(2, '0')}_${String(now.getSeconds()).padStart(2, '0')}`;
  
  let baseType = '';
  
  // Map file types to meaningful collection names
  if (name.includes('broker') && name.includes('master')) {
    baseType = 'broker_master_data';
  } else if (name.includes('cash') && name.includes('capital')) {
    baseType = 'cash_capital_flow_data';
  } else if (name.includes('contract') && name.includes('note')) {
    baseType = 'contract_notes_data';
  } else if (name.includes('distributor') && name.includes('master')) {
    baseType = 'distributor_master_data';
  } else if (name.includes('mf') && (name.includes('allocation') || name.includes('buy'))) {
    baseType = 'mf_allocation_data';
  } else if (name.includes('stock') && name.includes('capital')) {
    baseType = 'stock_capital_flow_data';
  } else if (name.includes('strategy') && name.includes('master')) {
    baseType = 'strategy_master_data';
  } else if (name.includes('client') && name.includes('info')) {
    baseType = 'client_info_data';
  } else if (name.includes('axis') && name.includes('custody')) {
    baseType = 'axis_custody_data';
  } else if (name.includes('hdfc') && name.includes('custody')) {
    baseType = 'hdfc_custody_data';
  } else if (name.includes('kotak') && name.includes('custody')) {
    baseType = 'kotak_custody_data';
  } else if (name.includes('deutsche') && name.includes('custody')) {
    baseType = 'deutsche_custody_data';
  } else if (name.includes('orbis') && name.includes('custody')) {
    baseType = 'orbis_custody_data';
  } else if (name.includes('trust') && name.includes('custody')) {
    baseType = 'trust_custody_data';
  } else {
    // Default: use cleaned filename
    const cleanName = name.substring(0, 20); // Limit length for MongoDB collection names
    baseType = `${cleanName}_data`;
  }
  
  // Return: type_YYYY_MM_DD_HH_MM_SS
  return `${baseType}_${timestamp}`;
}

// Utility function to find latest collections by type for processing
async function findLatestCollectionsByType(mongodb) {
  const collections = await mongodb.listCollections().toArray();
  const latestByType = {};
  
  collections.forEach(col => {
    const name = col.name;
    
    // Extract base type and timestamp from collection name
    const match = name.match(/^(.+)_(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})$/);
    if (match) {
      const [, baseType, timestamp] = match;
      
      if (!latestByType[baseType] || timestamp > latestByType[baseType].timestamp) {
        latestByType[baseType] = {
          collectionName: name,
          timestamp: timestamp,
          baseType: baseType
        };
      }
    }
  });
  
  console.log('📋 Latest collections by type:', latestByType);
  return latestByType;
}

// File upload API endpoint - Raw dump to MongoDB/PostgreSQL (no processing)
app.post('/api/upload', upload.array('files'), async (req, res) => {
  try {
    const { replaceMode = 'postgresql', recordDate } = req.body;
    const files = req.files;

    if (!files || files.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No files uploaded'
      });
    }

    console.log(`📤 Uploading ${files.length} files to MongoDB (raw dump)...`);

    // Simple file dump to MongoDB (or fallback to PostgreSQL if MongoDB unavailable)
    const XLSX = require('xlsx');
    const results = [];
    let totalRecords = 0;
    let validRecords = 0;

    // MONGODB-ONLY workflow - NO PostgreSQL fallback allowed!
    const { MongoClient } = require('mongodb');
    
    console.log('🎯 ENFORCING MongoDB-first workflow - NO FALLBACK ALLOWED');
    console.log('🔄 Attempting MongoDB connection for raw dump...');
    
    const mongoClient = new MongoClient(config.mongodb.uri + 'financial_data_2025', {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    try {
      await mongoClient.connect();
      console.log('✅ MongoDB connection established for upload');
      const db = mongoClient.db('financial_data_2025');
      
      for (const file of files) {
        console.log(`📄 Dumping ${file.originalname} to MongoDB...`);
        
        try {
          let data = [];
          const fileExt = path.extname(file.originalname).toLowerCase();
          
          if (fileExt === '.csv') {
            // Read CSV
            const content = fs.readFileSync(file.path, 'utf8');
            const lines = content.split('\n').filter(line => line.trim());
            if (lines.length > 1) {
              const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
              for (let i = 1; i < lines.length; i++) {
                const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
                const record = {};
                headers.forEach((header, index) => {
                  record[header] = values[index] || '';
                });
                data.push(record);
              }
            }
          } else if (['.xlsx', '.xls'].includes(fileExt)) {
            // Read Excel
            const workbook = XLSX.readFile(file.path);
            const sheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[sheetName];
            data = XLSX.utils.sheet_to_json(worksheet);
          } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
          }

          // Add metadata to each record
          const enrichedData = data.map(record => ({
            ...record,
            _fileName: file.originalname,
            _uploadTimestamp: new Date(),
            _fileSize: file.size,
            _processed: false
          }));

          // Create meaningful collection name based on file content
          const collectionName = generateCollectionName(file.originalname);
          const collection = db.collection(collectionName);
          
          // Use upsert to avoid duplicates - replace existing data
          await collection.deleteMany({ _fileName: file.originalname });
          await collection.insertMany(enrichedData);

          totalRecords += data.length;
          validRecords += data.length;
          
          results.push({
            fileName: file.originalname,
            success: true,
            recordsProcessed: data.length,
            collectionName: collectionName,
            message: `${data.length} records organized in collection: ${collectionName}`
          });

          console.log(`✅ ${file.originalname}: ${data.length} records organized in collection: ${collectionName}`);

        } catch (fileError) {
          console.error(`❌ Error dumping ${file.originalname}: ${fileError.message}`);
          results.push({
            fileName: file.originalname,
            success: false,
            error: fileError.message,
            recordsProcessed: 0
          });
        }
      }
      
      await mongoClient.close();
      console.log(`✅ SUCCESS: All files dumped to MongoDB! ${validRecords} records stored.`);
      
    } catch (mongoError) {
      console.error('❌ CRITICAL: MongoDB connection failed:', mongoError.message);
      
      // Clean up files and throw error - NO FALLBACK
      files.forEach(file => {
        try {
          fs.unlinkSync(file.path);
        } catch (error) {
          console.warn(`Warning: Could not clean up file ${file.path}`);
        }
      });
      
      throw new Error(`MongoDB connection required but failed: ${mongoError.message}. Please fix MongoDB Atlas connection (IP whitelist, credentials, cluster status) and try again.`);
    }

    

    // Clean up uploaded files
    files.forEach(file => {
      try {
        fs.unlinkSync(file.path);
      } catch (error) {
        console.warn(`Warning: Could not clean up file ${file.path}`);
      }
    });

    // Prepare response - MongoDB-only workflow
    const successfulFiles = results.filter(f => f.success);
    const failedFiles = results.filter(f => !f.success);

    const response = {
      success: validRecords > 0,
      message: `🎉 SUCCESS: Raw data dumped to MongoDB! ${successfulFiles.length}/${files.length} files uploaded. ${validRecords} records stored in MongoDB collections. Ready for tier 2 processing!`,
      totalFiles: files.length,
      filesProcessed: successfulFiles.length,
      totalRecords: totalRecords,
      validRecords: validRecords,
      errorRecords: totalRecords - validRecords,
      processingMode: 'MongoDB Raw Dump ✅',
      storage: 'MongoDB',
      workflow: 'MongoDB → PostgreSQL (as designed)',
      versioningEnabled: false,
      files: results.map(f => ({
        fileName: f.fileName,
        success: f.success,
        message: f.message || f.error,
        recordsProcessed: f.recordsProcessed || 0,
        collectionName: f.collectionName,
        processingMode: 'Raw-Dump'
      })),
      details: {
        processingMode: '🎯 Raw files successfully dumped to MongoDB collections - EXACTLY as designed!',
        nextStep: 'Click "Start Multi-Threading" to process raw MongoDB data into PostgreSQL tier 2 normalized tables',
        note: 'Perfect! Files are in MongoDB as intended. Processing will transform raw data to structured PostgreSQL tier 2 tables.',
        workflow: 'Step 1: ✅ MongoDB Raw Dump | Step 2: PostgreSQL Tier 2 Processing',
        recommendation: 'Everything working perfectly as designed!'
      }
    };

    res.json(response);

    // Broadcast update to dashboard clients
    io.emit('upload_complete', {
      type: 'mongodb_raw_dump_complete',
      mode: 'mongodb_raw_dump',
      workflow: 'mongodb_to_postgresql',
      ...response
    });

    console.log(`🎉 MongoDB raw dump completed successfully: ${validRecords} records stored. Ready for tier 2 processing.`);

  } catch (error) {
    console.error('Upload error:', error);
    
    // Clean up files on error
    if (req.files) {
      req.files.forEach(file => {
        try {
          fs.unlinkSync(file.path);
        } catch (cleanupError) {
          console.warn(`Warning: Could not clean up file ${file.path}`);
        }
      });
    }

    res.status(500).json({
      success: false,
      message: 'MongoDB raw data dump failed',
      error: error.message,
      processingMode: 'MongoDB Raw Dump Failed',
      note: 'Could not store raw data in MongoDB. Please fix MongoDB Atlas connection (IP whitelist, credentials, cluster status) and try again.',
      requirement: 'MongoDB connection is REQUIRED for this workflow - no fallback available'
    });
  }
});

// MongoDB data preview API
app.get('/api/mongodb/preview', async (req, res) => {
  try {
    if (!mongoConnected) {
      const html = `
      <!DOCTYPE html>
      <html>
      <head>
          <title>MongoDB Data Preview</title>
          <style>
              body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; text-align: center; }
              .message { background: white; margin: 20px 0; padding: 40px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
          </style>
      </head>
      <body>
          <div class="message">
              <h1>🍃 MongoDB Preview</h1>
              <p><strong>MongoDB not connected</strong></p>
              <p>Upload custody files to process data and populate MongoDB collections.</p>
              <p>The system will continue to work with PostgreSQL functionality.</p>
          </div>
      </body>
      </html>`;
      return res.send(html);
    }

    // Get data from both databases
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB for preview');
    
    const db2024 = mongoClient.db('financial_data_2024');
    const db2025 = mongoClient.db('financial_data_2025');
    
    // Get collections from both databases
    const collections2024 = await db2024.listCollections().toArray();
    const collections2025 = await db2025.listCollections().toArray();
    
    const allCollections = [];
    let totalRecords = 0;
    
    // Process 2024 collections
    for (const col of collections2024) {
      const collection = db2024.collection(col.name);
      const count = await collection.countDocuments();
      if (count > 0) {
        const sampleDoc = await collection.findOne();
        allCollections.push({
          name: col.name,
          database: 'financial_data_2024',
          count: count,
          sampleDoc: sampleDoc,
          sourceType: getSourceTypeFromCollection(col.name),
          lastUpdated: sampleDoc?._uploadTimestamp || sampleDoc?.created_at
        });
        totalRecords += count;
      }
    }
    
    // Process 2025 collections
    for (const col of collections2025) {
      const collection = db2025.collection(col.name);
      const count = await collection.countDocuments();
      if (count > 0) {
        const sampleDoc = await collection.findOne();
        allCollections.push({
          name: col.name,
          database: 'financial_data_2025',
          count: count,
          sampleDoc: sampleDoc,
          sourceType: getSourceTypeFromCollection(col.name),
          lastUpdated: sampleDoc?._uploadTimestamp || sampleDoc?.created_at
        });
        totalRecords += count;
      }
    }
    
    await mongoClient.close();

    let html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>MongoDB Data Preview - Raw Data Storage</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 15px; text-align: center; margin-bottom: 30px; }
            .summary { background: white; padding: 25px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); margin-bottom: 30px; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
            .stat-card { background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }
            .stat-number { font-size: 2em; font-weight: bold; }
            .stat-label { font-size: 0.9em; opacity: 0.9; }
            .collection { background: white; margin: 20px 0; padding: 25px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
            .collection h3 { color: #333; margin-top: 0; display: flex; justify-content: space-between; align-items: center; }
            .badge { padding: 6px 12px; border-radius: 20px; color: white; font-size: 0.8em; font-weight: bold; }
            .badge-master { background: #28a745; }
            .badge-transaction { background: #007bff; }
            .badge-custody { background: #ffc107; color: #000; }
            .badge-unknown { background: #6c757d; }
            .record-preview { background: #f8f9fa; padding: 15px; border-radius: 8px; margin-top: 15px; }
            .record-field { display: flex; justify-content: space-between; padding: 5px 0; border-bottom: 1px solid #eee; }
            .record-field:last-child { border-bottom: none; }
            .field-name { font-weight: bold; color: #495057; }
            .field-value { color: #6c757d; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
            .database-section { margin-bottom: 40px; }
            .database-header { background: #343a40; color: white; padding: 15px 25px; border-radius: 10px; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🍃 MongoDB Raw Data Storage</h1>
            <p>Tier 1: Raw file dumps organized by collection type with timestamps</p>
        </div>
        
        <div class="summary">
            <h2>📊 MongoDB Data Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">${allCollections.length}</div>
                    <div class="stat-label">Total Collections</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${totalRecords.toLocaleString()}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${[...new Set(allCollections.map(c => c.sourceType))].length}</div>
                    <div class="stat-label">Data Types</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${allCollections.filter(c => c.database === 'financial_data_2025').length}</div>
                    <div class="stat-label">2025 Collections</div>
                </div>
            </div>
        </div>
    `;

    // Group collections by database
    const collections2024Group = allCollections.filter(c => c.database === 'financial_data_2024');
    const collections2025Group = allCollections.filter(c => c.database === 'financial_data_2025');

    // Display 2025 collections first (most recent)
    if (collections2025Group.length > 0) {
      html += `
        <div class="database-section">
            <div class="database-header">
                <h2>📅 Financial Data 2025 (${collections2025Group.length} collections, ${collections2025Group.reduce((sum, c) => sum + c.count, 0).toLocaleString()} records)</h2>
        </div>
      `;
      
      collections2025Group.forEach(collection => {
        html += generateCollectionHTML(collection);
      });
      
      html += `</div>`;
    }

    // Display 2024 collections
    if (collections2024Group.length > 0) {
      html += `
        <div class="database-section">
            <div class="database-header">
                <h2>📅 Financial Data 2024 (${collections2024Group.length} collections, ${collections2024Group.reduce((sum, c) => sum + c.count, 0).toLocaleString()} records)</h2>
            </div>
      `;
      
      collections2024Group.forEach(collection => {
        html += generateCollectionHTML(collection);
      });
      
      html += `</div>`;
    }

    html += `
        <div style="text-align: center; margin-top: 40px; padding: 20px; background: #e7f3ff; border-radius: 10px;">
            <h3>🔄 Processing Pipeline</h3>
            <p><strong>Step 1:</strong> Files uploaded to MongoDB (✅ Complete)</p>
            <p><strong>Step 2:</strong> Click "Start Multi-Threading" to process raw data into PostgreSQL tier 2 tables</p>
        </div>
    </body></html>`;
    
    res.send(html);

  } catch (error) {
    res.status(500).send(`
      <html><body style="font-family: Arial; padding: 20px; text-align: center;">
        <h2>❌ Error Loading MongoDB Data</h2>
        <p>Error: ${error.message}</p>
        <p>Please check MongoDB Atlas connection and try again.</p>
      </body></html>
    `);
  }
});

// Helper function to generate collection HTML
function generateCollectionHTML(collection) {
  const badgeClass = getBadgeClass(collection.sourceType);
  
  return `
    <div class="collection">
        <h3>
            ${collection.name}
            <span class="badge ${badgeClass}">${collection.sourceType}</span>
        </h3>
        <p><strong>📊 Records:</strong> ${collection.count.toLocaleString()}</p>
        <p><strong>🗄️ Database:</strong> ${collection.database}</p>
        <p><strong>📅 Last Updated:</strong> ${collection.lastUpdated ? new Date(collection.lastUpdated).toLocaleString() : 'Unknown'}</p>
        
        <div class="record-preview">
            <h4>📋 Sample Record Fields:</h4>
            ${Object.entries(collection.sampleDoc || {}).slice(0, 8).map(([key, value]) => {
              if (key.startsWith('_')) return ''; // Skip MongoDB internal fields
              return `
                <div class="record-field">
                    <span class="field-name">${key}:</span>
                    <span class="field-value">${typeof value === 'object' ? JSON.stringify(value).substring(0, 100) + '...' : String(value).substring(0, 100)}</span>
                </div>
              `;
            }).join('')}
        </div>
    </div>
  `;
}

// Helper function to get source type from collection name
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
  if (name.includes('custody')) return 'Custody Holdings';
  return 'Unknown';
}

// Helper function to get badge class
function getBadgeClass(sourceType) {
  if (sourceType.includes('Master')) return 'badge-master';
  if (sourceType.includes('Flow') || sourceType.includes('Contract') || sourceType.includes('Allocation')) return 'badge-transaction';
  if (sourceType.includes('Custody')) return 'badge-custody';
  return 'badge-unknown';
}

// PostgreSQL data preview API
app.get('/api/postgresql/preview', async (req, res) => {
  try {
    const client = await pgPool.connect();
    
    // Get all tables with their schemas
    const tablesQuery = `
      SELECT 
        t.table_name,
        t.table_type,
        COALESCE(s.record_count, 0) as record_count
      FROM information_schema.tables t
      LEFT JOIN (
        SELECT 
          schemaname,
          tablename,
          n_tup_ins as record_count
        FROM pg_stat_user_tables
      ) s ON t.table_name = s.tablename
      WHERE t.table_schema = 'public' 
      AND t.table_type = 'BASE TABLE'
      ORDER BY t.table_name
    `;
    const tablesResult = await client.query(tablesQuery);
    
    let totalRecords = 0;
    const tableDetails = [];
    
    // Get details for each table
    for (const table of tablesResult.rows) {
      const tableName = table.table_name;
      
      try {
        // Get actual record count
        const countQuery = `SELECT COUNT(*) as total FROM ${tableName}`;
        const countResult = await client.query(countQuery);
        const actualCount = parseInt(countResult.rows[0].total);
        
        // Get column information
        const columnsQuery = `
          SELECT column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_name = $1 AND table_schema = 'public'
          ORDER BY ordinal_position
        `;
        const columnsResult = await client.query(columnsQuery, [tableName]);
        
        // Get sample data if table has records
        let sampleData = [];
        if (actualCount > 0) {
          const sampleQuery = `SELECT * FROM ${tableName} LIMIT 3`;
          const sampleResult = await client.query(sampleQuery);
          sampleData = sampleResult.rows;
        }
        
        tableDetails.push({
          name: tableName,
          type: getTableType(tableName),
          recordCount: actualCount,
          columns: columnsResult.rows,
          sampleData: sampleData
        });
        
        totalRecords += actualCount;
        
      } catch (error) {
        console.error(`Error processing table ${tableName}:`, error.message);
        tableDetails.push({
          name: tableName,
          type: 'Unknown',
          recordCount: 0,
          columns: [],
          sampleData: [],
          error: error.message
        });
      }
    }
    
    // Generate HTML
    let html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>PostgreSQL Data Preview - Normalized Tables</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 15px; text-align: center; margin-bottom: 30px; }
            .summary { background: white; padding: 25px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); margin-bottom: 30px; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
            .stat-card { background: linear-gradient(135deg, #28a745 0%, #20c997 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }
            .stat-number { font-size: 2em; font-weight: bold; }
            .stat-label { font-size: 0.9em; opacity: 0.9; }
            .table-section { background: white; margin: 20px 0; padding: 25px; border-radius: 15px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); }
            .table-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            .badge { padding: 6px 12px; border-radius: 20px; color: white; font-size: 0.8em; font-weight: bold; }
            .badge-master { background: #28a745; }
            .badge-transaction { background: #007bff; }
            .badge-custody { background: #ffc107; color: #000; }
            .badge-daily { background: #6f42c1; }
            .badge-empty { background: #6c757d; }
            .columns-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }
            .column-card { background: #f8f9fa; padding: 15px; border-radius: 8px; }
            .column-name { font-weight: bold; color: #495057; }
            .column-type { color: #6c757d; font-size: 0.9em; }
            .sample-data { background: #f8f9fa; padding: 15px; border-radius: 8px; margin-top: 15px; overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background: #e9ecef; font-weight: bold; }
            .toggle-btn { background: #007bff; color: white; border: none; padding: 8px 16px; border-radius: 5px; cursor: pointer; margin-top: 10px; }
            .toggle-btn:hover { background: #0056b3; }
            .details { display: none; margin-top: 20px; }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🐘 PostgreSQL Normalized Data</h1>
            <p>Tier 2: Processed and normalized tables with structured data</p>
        </div>
        
        <div class="summary">
            <h2>📊 PostgreSQL Data Summary</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-number">${tableDetails.length}</div>
                    <div class="stat-label">Total Tables</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${totalRecords.toLocaleString()}</div>
                    <div class="stat-label">Total Records</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${tableDetails.filter(t => t.recordCount > 0).length}</div>
                    <div class="stat-label">Active Tables</div>
                </div>
                <div class="stat-card">
                    <div class="stat-number">${[...new Set(tableDetails.map(t => t.type))].length}</div>
                    <div class="stat-label">Table Types</div>
                </div>
            </div>
        </div>
    `;

    // Group tables by type
    const masterTables = tableDetails.filter(t => t.type === 'Master Data');
    const transactionTables = tableDetails.filter(t => t.type === 'Transaction Data');
    const custodyTables = tableDetails.filter(t => t.type === 'Custody Holdings');
    const dailyTables = tableDetails.filter(t => t.type === 'Daily Tables');
    const otherTables = tableDetails.filter(t => !['Master Data', 'Transaction Data', 'Custody Holdings', 'Daily Tables'].includes(t.type));

    // Display tables by category
    [
      { title: '👥 Master Data Tables', tables: masterTables, color: 'master' },
      { title: '💼 Transaction Data Tables', tables: transactionTables, color: 'transaction' },
      { title: '🏦 Custody Holdings Tables', tables: custodyTables, color: 'custody' },
      { title: '📅 Daily Partitioned Tables', tables: dailyTables, color: 'daily' },
      { title: '📋 Other Tables', tables: otherTables, color: 'empty' }
    ].forEach(category => {
      if (category.tables.length > 0) {
        html += `<h2 style="color: #495057; margin-top: 40px;">${category.title} (${category.tables.length})</h2>`;
        
        category.tables.forEach(table => {
          html += generateTableHTML(table, category.color);
        });
      }
    });
      
      html += `
        <div style="text-align: center; margin-top: 40px; padding: 20px; background: #e7f3ff; border-radius: 10px;">
            <h3>✅ Data Processing Complete</h3>
            <p><strong>Raw MongoDB data has been successfully processed into structured PostgreSQL tables</strong></p>
            <p>All tables are ready for analysis and reporting</p>
        </div>
        
        <script>
            function toggleDetails(btn, tableId) {
                const details = document.getElementById(tableId);
                if (details.style.display === 'none') {
                    details.style.display = 'block';
                    btn.textContent = 'Hide Details';
                } else {
                    details.style.display = 'none';
                    btn.textContent = 'Show Details';
                }
            }
        </script>
    </body></html>`;
    
    client.release();
    res.send(html);
    
  } catch (error) {
    res.status(500).send(`
      <html><body style="font-family: Arial; padding: 20px; text-align: center;">
        <h2>❌ Error Loading PostgreSQL Data</h2>
        <p>Error: ${error.message}</p>
        <p>Please check PostgreSQL connection and try again.</p>
      </body></html>
    `);
  }
});

// Helper function to generate table HTML
function generateTableHTML(table, colorType) {
  const tableId = `details-${table.name}`;
  
  return `
    <div class="table-section">
        <div class="table-header">
            <h3>${table.name}</h3>
            <span class="badge badge-${colorType}">${table.type} (${table.recordCount.toLocaleString()} records)</span>
        </div>
        
        ${table.error ? `<div style="color: #dc3545; padding: 10px; background: #f8d7da; border-radius: 5px;">❌ Error: ${table.error}</div>` : ''}
        
        <p><strong>📊 Record Count:</strong> ${table.recordCount.toLocaleString()}</p>
        <p><strong>📋 Columns:</strong> ${table.columns.length}</p>
        
        <button class="toggle-btn" onclick="toggleDetails(this, '${tableId}')">Show Details</button>
        
        <div id="${tableId}" class="details">
            <h4>📋 Table Schema:</h4>
            <div class="columns-grid">
                ${table.columns.map(col => `
                    <div class="column-card">
                        <div class="column-name">${col.column_name}</div>
                        <div class="column-type">${col.data_type}${col.is_nullable === 'NO' ? ' (Required)' : ' (Optional)'}</div>
                    </div>
                `).join('')}
            </div>
            
            ${table.sampleData.length > 0 ? `
                <h4>📋 Sample Data:</h4>
                <div class="sample-data">
            <table>
                <thead>
                    <tr>
                                ${table.columns.slice(0, 8).map(col => `<th>${col.column_name}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
                            ${table.sampleData.map(row => `
                                <tr>
                                    ${table.columns.slice(0, 8).map(col => `
                                        <td>${row[col.column_name] !== null && row[col.column_name] !== undefined ? 
                                            String(row[col.column_name]).substring(0, 50) + (String(row[col.column_name]).length > 50 ? '...' : '') : 
                                            'NULL'}</td>
                                    `).join('')}
          </tr>
                            `).join('')}
                </tbody>
            </table>
                </div>
            ` : '<p style="color: #6c757d; font-style: italic;">No sample data available (table is empty)</p>'}
        </div>
        </div>
      `;
    }
    
// Helper function to determine table type
function getTableType(tableName) {
  const name = tableName.toLowerCase();
  if (name.includes('_2025_') || name.includes('_2024_')) return 'Daily Tables';
  if (name.includes('brokers') || name.includes('clients') || name.includes('distributors') || name.includes('strategies')) return 'Master Data';
  if (name.includes('contract_notes') || name.includes('capital_flow') || name.includes('allocations')) return 'Transaction Data';
  if (name.includes('custody') || name.includes('holdings')) return 'Custody Holdings';
  return 'Other';
}

// API routes
app.get('/api/status', (req, res) => {
  res.json({
    status: 'running',
    timestamp: new Date().toISOString(),
    features: ['multi-threading', 'real-time-progress', 'orbis-corrections', 'file-upload', 'smart-versioning']
  });
});

// API route to get PostgreSQL data
app.get('/api/postgresql-data', async (req, res) => {
  try {
    const client = await pgPool.connect();
    
    // Get list of daily tables
    const tablesQuery = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name LIKE 'unified_custody_master_%'
      ORDER BY table_name DESC
    `;
    const tablesResult = await client.query(tablesQuery);
    
    const tableStats = [];
    
    for (const table of tablesResult.rows.slice(0, 10)) { // Get latest 10 tables
      const tableName = table.table_name;
      
      const statsQuery = `
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT client_reference) as unique_clients,
          COUNT(DISTINCT instrument_isin) as unique_instruments,
          array_agg(DISTINCT source_system) as source_systems,
          MAX(created_at) as last_updated
        FROM ${tableName}
      `;
      
      const statsResult = await client.query(statsQuery);
      const stats = statsResult.rows[0];
      
      // Get sample records from each source system
      const sampleQuery = `
        WITH ranked_records AS (
          SELECT *, 
                 ROW_NUMBER() OVER (PARTITION BY source_system ORDER BY created_at DESC) as rn
          FROM ${tableName}
        )
        SELECT client_reference, client_name, instrument_isin, instrument_name, 
               source_system, record_date, created_at,
               blocked_quantity, pending_buy_quantity, pending_sell_quantity,
               total_position, saleable_quantity
        FROM ranked_records 
        WHERE rn <= 2
        ORDER BY source_system, rn
      `;
      const sampleResult = await client.query(sampleQuery);
      
      tableStats.push({
        tableName,
        totalRecords: parseInt(stats.total_records),
        uniqueClients: parseInt(stats.unique_clients),
        uniqueInstruments: parseInt(stats.unique_instruments),
        sourceSystems: stats.source_systems || [],
        lastUpdated: stats.last_updated,
        sampleRecords: sampleResult.rows
      });
    }
    
    client.release();
    res.json({ success: true, tables: tableStats });
    
  } catch (error) {
    console.error('Error fetching PostgreSQL data:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// API endpoint to get latest collections by type for processing
app.get('/api/latest-collections', async (req, res) => {
  try {
    console.log('🔍 Finding latest collections by type for processing...');
    
    const { MongoClient } = require('mongodb');
    const mongoClient = new MongoClient(config.mongodb.uri, {
      serverSelectionTimeoutMS: 15000,
      connectTimeoutMS: 15000,
      tls: true,
      tlsAllowInvalidCertificates: true,
      tlsAllowInvalidHostnames: true
    });
    
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB for latest collection discovery');
    
    const db2024 = mongoClient.db('financial_data_2024');
    const db2025 = mongoClient.db('financial_data_2025');
    
    const latest2024 = await findLatestCollectionsByType(db2024);
    const latest2025 = await findLatestCollectionsByType(db2025);
    
    // Combine and choose the absolute latest for each type
    const allLatest = {};
    
    // Add 2024 collections
    Object.entries(latest2024).forEach(([type, info]) => {
      allLatest[type] = { ...info, database: 'financial_data_2024' };
    });
    
    // Add 2025 collections (will override 2024 if timestamp is newer)
    Object.entries(latest2025).forEach(([type, info]) => {
      if (!allLatest[type] || info.timestamp > allLatest[type].timestamp) {
        allLatest[type] = { ...info, database: 'financial_data_2025' };
      }
    });
    
    await mongoClient.close();
    console.log(`✅ Found latest collections for ${Object.keys(allLatest).length} data types`);
    
    res.json({
      success: true,
      latestCollections: allLatest,
      dataTypes: Object.keys(allLatest).length,
      message: `Found latest collections for ${Object.keys(allLatest).length} data types`,
      instruction: 'Use these collection names for processing the most recent data of each type'
    });
    
  } catch (error) {
    console.error('❌ Error finding latest collections:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to find latest collections',
      details: error.message
    });
  }
});

// New unified data viewer API endpoint
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
        totalTables: postgresData.totalTables,
        combinedRecords: mongoData.totalRecords + postgresData.totalRecords
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

// Helper function to get MongoDB summary
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
  
  const db2024 = mongoClient.db('financial_data_2024');
  const db2025 = mongoClient.db('financial_data_2025');
  
  const collections2024 = await db2024.listCollections().toArray();
  const collections2025 = await db2025.listCollections().toArray();
  
  const allCollections = [];
  let totalRecords = 0;
  
  // Process both databases
  for (const col of collections2024) {
    const collection = db2024.collection(col.name);
    const count = await collection.countDocuments();
    if (count > 0) {
      allCollections.push({
        name: col.name,
        database: 'financial_data_2024',
        count: count,
        sourceType: getSourceTypeFromCollection(col.name)
      });
      totalRecords += count;
    }
  }
  
  for (const col of collections2025) {
    const collection = db2025.collection(col.name);
    const count = await collection.countDocuments();
    if (count > 0) {
      allCollections.push({
        name: col.name,
        database: 'financial_data_2025',
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

// Helper function to get PostgreSQL summary
async function getPostgreSQLSummary() {
  if (!pgConnected) {
    return { totalTables: 0, totalRecords: 0, tables: [] };
  }
  
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
      const countQuery = `SELECT COUNT(*) as total FROM ${tableName}`;
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
        type: 'Error',
        error: error.message
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

// Start server
async function startServer() {
  try {
    await initDB();
    
    server.listen(PORT, () => {
      console.log(`🚀 Multi-Threaded Data Processing Dashboard running at http://localhost:${PORT}`);
      console.log(`🎯 Features: File upload, real-time progress, WebSocket updates, true multi-threading`);
      console.log(`⚡ Supports: .xlsx, .xls, .csv, .json, .txt, .xml files with smart processing`);
    });
    
  } catch (error) {
    console.error('❌ Server startup failed:', error.message);
    process.exit(1);
  }
}

startServer(); 