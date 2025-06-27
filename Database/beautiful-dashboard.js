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

// Initialize databases
async function initDB() {
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

    await mongoose.connect(config.mongodb.uri + 'financial_data_2025');
    console.log('✅ Connected to MongoDB');
    
  } catch (error) {
    console.error('❌ Database connection failed:', error.message);
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
    </div>

    <div id="notification" class="notification"></div>

    <script>
        const socket = io();
        let isProcessing = false;
        let startTime = null;

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
            logMessage(\`🚀 Started processing with \${data.maxWorkers} workers\`, 'success');
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
            logMessage(\`✅ Worker \${data.workerId} completed: \${data.collectionName} - \${data.result.valid}/\${data.result.processed} valid\`, 'success');
        });

        socket.on('processing_complete', (data) => {
            isProcessing = false;
            document.getElementById('startBtn').disabled = false;
            document.getElementById('stopBtn').disabled = true;
            updateStats({ successRate: data.successRate + '%', activeWorkers: 0 });
            logMessage(\`🎉 Processing complete! Success rate: \${data.successRate}%\`, 'success');
            showNotification(\`Processing complete! \${data.successRate}% success rate\`, 'success');
        });

        socket.on('error', (data) => {
            logMessage(\`❌ Error: \${data.error}\`, 'error');
            showNotification(\`Error: \${data.error}\`, 'error');
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
                    dataDate = \`\${nameParts[nameParts.length-3]}-\${nameParts[nameParts.length-2]}-\${nameParts[nameParts.length-1]}\`;
                    custodyType = getCustodyTypeFromName(sourceType);
                }
                
                const displayName = dataDate ? \`\${custodyType} (\${dataDate})\` : custodyType;
                
                return \`
                    <div class="collection-item">
                        <div>
                            <strong>\${col.name}</strong><br>
                            <small style="opacity: 0.8;">\${displayName}</small>
                        </div>
                        <div>
                            <span class="collection-badge badge-\${custodyType.toLowerCase()}">\${custodyType}</span><br>
                            <small>\${col.records.toLocaleString()} records</small>
                        </div>
                    </div>
                \`;
            }).join('');
            
            overview.style.display = 'block';
        }

        function updateWorkerProgress(data) {
            const workerId = data.workerId;
            let workerCard = document.getElementById(\`worker-\${workerId}\`);
            
            if (!workerCard) {
                workerCard = createWorkerCard(workerId, data.collectionName, data.custodyType);
                document.getElementById('workersGrid').appendChild(workerCard);
            }
            
            const progressFill = workerCard.querySelector('.progress-fill');
            const progressText = workerCard.querySelector('.progress-text');
            const statusBadge = workerCard.querySelector('.worker-status');
            
            progressFill.style.width = data.collectionProgress.percentage + '%';
            progressText.textContent = \`\${data.collectionProgress.percentage}% (\${data.collectionProgress.processed}/\${data.collectionProgress.total})\`;
            statusBadge.textContent = 'PROCESSING';
            statusBadge.className = 'worker-status processing pulse';
        }

        function createWorkerCard(workerId, collectionName, custodyType) {
            const card = document.createElement('div');
            card.className = 'worker-card fade-in';
            card.id = \`worker-\${workerId}\`;
            card.innerHTML = \`
                <div class="worker-header">
                    <div>
                        <strong>Worker \${workerId}</strong><br>
                        <small>\${collectionName} (\${custodyType})</small>
                    </div>
                    <div class="worker-status">STARTING</div>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: 0%"></div>
                    <div class="progress-text">Initializing...</div>
                </div>
            \`;
            return card;
        }

        function markWorkerComplete(data) {
            const workerCard = document.getElementById(\`worker-\${data.workerId}\`);
            if (workerCard) {
                const statusBadge = workerCard.querySelector('.worker-status');
                const progressFill = workerCard.querySelector('.progress-fill');
                const progressText = workerCard.querySelector('.progress-text');
                
                statusBadge.textContent = 'COMPLETE';
                statusBadge.className = 'worker-status complete';
                progressFill.style.width = '100%';
                progressText.textContent = \`✅ \${data.result.valid}/\${data.result.processed} valid\`;
            }
        }

        function updateOverallProgress(progress) {
            const progressFill = document.getElementById('overallProgressFill');
            const progressText = document.getElementById('overallProgressText');
            const progressDetails = document.getElementById('overallProgressDetails');
            
            const percentage = progress.overallPercentage || 0;
            progressFill.style.width = percentage + '%';
            progressText.textContent = \`\${percentage}% Complete\`;
            progressDetails.textContent = \`\${progress.processedRecords?.toLocaleString() || 0} / \${progress.totalRecords?.toLocaleString() || 0} records processed (\${progress.validRecords?.toLocaleString() || 0} valid, \${progress.errorRecords?.toLocaleString() || 0} errors)\`;
        }

        function logMessage(message, type = 'info') {
            const logPanel = document.getElementById('logPanel');
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry fade-in';
            logEntry.innerHTML = \`
                <span class="log-timestamp">[\${new Date().toLocaleTimeString()}]</span>
                \${message}
            \`;
            logPanel.appendChild(logEntry);
            logPanel.scrollTop = logPanel.scrollHeight;
        }

        function showNotification(message, type) {
            const notification = document.getElementById('notification');
            notification.textContent = message;
            notification.className = \`notification \${type} show\`;
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
                    showNotification(\`Successfully uploaded \${data.totalFiles} files!\`, 'success');
                    // Auto-refresh data after upload
                    setTimeout(() => {
                        discoverCollections();
                        refreshData();
                    }, 1000);
                } else {
                    showNotification(\`Upload failed: \${data.message}\`, 'error');
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
                fileDiv.style.cssText = \`
                    background: rgba(255,255,255,0.1);
                    border-radius: 10px;
                    padding: 15px;
                    margin: 10px 0;
                    border-left: 4px solid \${file.success ? '#43e97b' : '#ff6b6b'};
                \`;
                
                fileDiv.innerHTML = \`
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
                \`;
                
                container.appendChild(fileDiv);
            });

            // Update data overview
            updateDataOverview(data);
        }

        function updateDataOverview(data) {
            const overview = document.getElementById('dataOverview');
            overview.innerHTML = \`
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
            \`;
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
                    showNotification(\`Selected \${files.length} file(s) for upload\`, 'info');
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
                    logMessage(\`✅ Loaded \${data.tables.length} PostgreSQL tables\`, 'success');
                    showNotification(\`Loaded \${data.tables.length} PostgreSQL tables\`, 'success');
                } else {
                    throw new Error(data.error);
                }
            } catch (error) {
                logMessage(\`❌ Failed to load PostgreSQL data: \${error.message}\`, 'error');
                showNotification(\`Error loading PostgreSQL data: \${error.message}\`, 'error');
            }
        }

        function displayPostgreSQLTables(tables) {
            const container = document.getElementById('postgresqlTables');
            
            if (tables.length === 0) {
                container.innerHTML = \`
                    <div style="text-align: center; opacity: 0.7; padding: 20px;">
                        📭 No PostgreSQL tables found. Process some data first.
                    </div>
                \`;
                return;
            }

            // Calculate overall statistics
            const totalRecords = tables.reduce((sum, t) => sum + t.totalRecords, 0);
            const allSources = [...new Set(tables.flatMap(t => t.sourceSystems))];
            
            // Add summary header
            let html = \`
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
                        ✅ No duplicates (old data cleared automatically) • ✅ Orbis corrections applied
                    </div>
                </div>
            \`;
            
            html += tables.map(table => {
                const date = table.tableName.replace('unified_custody_master_', '').replace(/_/g, '-');
                const lastUpdated = new Date(table.lastUpdated).toLocaleString();
                
                return \`
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
                \`;
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
      socket.emit('error', { error: error.message });
    }
  });

  socket.on('start_processing', async () => {
    try {
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

// File upload API endpoint
app.post('/api/upload', upload.array('files'), async (req, res) => {
  try {
    const { replaceMode = 'timestamp', recordDate } = req.body;
    const files = req.files;

    if (!files || files.length === 0) {
      return res.status(400).json({
        success: false,
        message: 'No files uploaded'
      });
    }

    console.log(`📤 Processing ${files.length} files with smart deduplication...`);

    // Use SmartFileProcessor with intelligent versioning
    const processor = new SmartFileProcessor();
    const result = await processor.processFiles(files, {
      versioningMode: replaceMode || 'timestamp', // Default to timestamp versioning
      recordDate
    });

    // Clean up uploaded files
    files.forEach(file => {
      try {
        fs.unlinkSync(file.path);
      } catch (error) {
        console.warn(`Warning: Could not clean up file ${file.path}`);
      }
    });

    await processor.disconnect();

    // Prepare response with versioning details
    const response = {
      success: result.success,
      message: result.summary.message,
      totalFiles: result.totalFiles,
      filesProcessed: result.filesProcessed,
      totalRecords: result.totalRecords,
      uniqueSources: result.uniqueSources,
      versioningMode: result.versioningMode,
      versioningEnabled: result.summary.versioningEnabled,
      historicalDataPreserved: result.summary.historicalDataPreserved,
      details: {
        sourcesUpdated: result.summary.sourcesUpdated,
        versioningStrategy: result.versioningMode === 'timestamp' ? 
          'Intelligent versioning - previous data archived, latest version active' : 
          'Complete replacement - previous data deleted',
        dataLossProtection: result.versioningMode === 'timestamp'
      },
      files: result.results.map(r => ({
        fileName: r.fileName,
        sourceType: r.sourceType,
        success: r.success,
        message: r.message,
        recordsProcessed: r.recordsProcessed,
        recordDate: r.recordDate,
        versionId: r.versionId,
        uploadTimestamp: r.uploadTimestamp,
        isActive: r.isActive,
        hasHistoricalVersions: r.hasHistoricalVersions
      }))
    };

    res.json(response);

    // Broadcast update to dashboard clients
    io.emit('upload_complete', {
      type: 'upload_complete',
      ...response
    });

  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({
      success: false,
      message: 'Upload failed',
      error: error.message
    });
  }
});

// MongoDB data preview API
app.get('/api/mongodb/preview', async (req, res) => {
  try {
    const processor = new SmartFileProcessor();
    const collections = await processor.getCollectionsInfo();
    await processor.disconnect();

    let html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>MongoDB Data Preview</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .collection { background: white; margin: 20px 0; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
            .collection h3 { color: #333; margin-top: 0; }
            .badge { padding: 4px 8px; border-radius: 4px; color: white; font-size: 0.8em; }
            .badge-active { background: #28a745; }
            .badge-historical { background: #6c757d; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background: #f8f9fa; }
        </style>
    </head>
    <body>
        <h1>🍃 MongoDB Collections Overview</h1>
        <p>Total Collections: ${collections.length} | Active Records: ${collections.reduce((sum, c) => sum + c.count, 0).toLocaleString()}</p>
    `;

    collections.forEach(collection => {
      html += `
        <div class="collection">
            <h3>${collection.name} 
                <span class="badge ${collection.status.includes('✅') ? 'badge-active' : 'badge-historical'}">
                    ${collection.status.includes('✅') ? 'Active' : 'Historical'}
                </span>
            </h3>
            <p><strong>Records:</strong> ${collection.count.toLocaleString()}</p>
            <p><strong>Source Type:</strong> ${collection.sourceType}</p>
            <p><strong>Last Updated:</strong> ${collection.lastUpdated ? new Date(collection.lastUpdated).toLocaleString() : 'Unknown'}</p>
            ${collection.versioningEnabled ? 
              `<p><strong>Versions Available:</strong> ${collection.versionsAvailable}</p>` : ''}
        </div>
      `;
    });

    html += `</body></html>`;
    res.send(html);

  } catch (error) {
    res.status(500).send(`Error loading MongoDB data: ${error.message}`);
  }
});

// PostgreSQL data preview API
app.get('/api/postgresql/preview', async (req, res) => {
  try {
    const client = await pgPool.connect();
    
    const tablesQuery = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name LIKE 'unified_custody_master_%'
      ORDER BY table_name DESC
      LIMIT 5
    `;
    const tablesResult = await client.query(tablesQuery);
    
    let html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>PostgreSQL Data Preview</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .table-preview { background: white; margin: 20px 0; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
            .table-preview h3 { color: #333; margin-top: 0; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 0.9em; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background: #f8f9fa; font-weight: bold; }
            .summary { background: #e7f3ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <h1>🐘 PostgreSQL Normalized Data Preview</h1>
        <div class="summary">
            <h3>📊 Unified Custody Master Data Summary</h3>
            <p><strong>✅ All Custody Systems Present:</strong> Daily tables containing clean, normalized custody data with enhanced financial fields</p>
            <p><strong>📈 Financial Fields:</strong> total_position, saleable_quantity, blocked_quantity, pending_buy_quantity, pending_sell_quantity</p>
            <p><strong>🔗 Formula Validation:</strong> saleable_quantity ≈ total_position - blocked_quantity</p>
        </div>
    `;

    for (const table of tablesResult.rows) {
      const tableName = table.table_name;
      const date = tableName.replace('unified_custody_master_', '').replace(/_/g, '-');
      
      const countQuery = `SELECT COUNT(*) as total FROM ${tableName}`;
      const countResult = await client.query(countQuery);
      const totalRecords = countResult.rows[0].total;
      
      const sampleQuery = `
        SELECT client_reference, client_name, instrument_isin, instrument_name, source_system, record_date 
        FROM ${tableName} 
        ORDER BY source_system, client_reference, instrument_isin
        LIMIT 10
      `;
      const sampleResult = await client.query(sampleQuery);
      
      html += `
        <div class="table-preview">
            <h3>📅 ${date} (${totalRecords.toLocaleString()} records)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Client Ref</th>
                        <th>Client Name</th>
                        <th>Instrument ISIN</th>
                        <th>Instrument Name</th>
                        <th>Source System</th>
                        <th>Total Position</th>
                        <th>Saleable Qty</th>
                        <th>Blocked Qty</th>
                        <th>Record Date</th>
                        <th>Created At</th>
                    </tr>
                </thead>
                <tbody>
      `;
      
      sampleResult.rows.forEach(row => {
        html += `
          <tr>
              <td>${row.client_reference || 'N/A'}</td>
              <td>${row.client_name || 'N/A'}</td>
              <td>${row.instrument_isin || 'N/A'}</td>
              <td>${row.instrument_name || 'N/A'}</td>
              <td><span style="background: #4facfe; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold;">${row.source_system || 'N/A'}</span></td>
              <td>${row.total_position ? parseFloat(row.total_position).toLocaleString() : '0'}</td>
              <td>${row.saleable_quantity ? parseFloat(row.saleable_quantity).toLocaleString() : '0'}</td>
              <td>${row.blocked_quantity ? parseFloat(row.blocked_quantity).toLocaleString() : '0'}</td>
              <td>${row.record_date ? new Date(row.record_date).toLocaleDateString() : 'N/A'}</td>
              <td>${row.created_at ? new Date(row.created_at).toLocaleString() : 'N/A'}</td>
          </tr>
        `;
      });
      
      html += `
                </tbody>
            </table>
        </div>
      `;
    }
    
    html += `
      <script>
        function getSourceColor(source) {
          const colors = {
            'ORBIS': '#ff6b6b',
            'KOTAK': '#4ecdc4', 
            'AXIS': '#45b7d1',
            'TRUSTPMS': '#96ceb4',
            'HDFC': '#ffeaa7',
            'DEUTSCHE': '#a29bfe'
          };
          return colors[source] || '#6c757d';
        }
      </script>
    </body></html>`;
    client.release();
    res.send(html);
    
  } catch (error) {
    res.status(500).send(`Error loading PostgreSQL data: ${error.message}`);
  }
});

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