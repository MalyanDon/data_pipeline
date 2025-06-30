const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const http = require('http');
const { Server } = require('socket.io');
const os = require('os');
const cors = require('cors');
const { MultiThreadedETLProcessor } = require('./multi-threaded-etl');
const SmartFileProcessor = require('./smart-file-processor');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static('public'));

// Database connections
let pgPool;
let mongoClient;

// Initialize database connections
async function initializeConnections() {
    try {
        // PostgreSQL connection (Neon cloud)
        pgPool = new Pool({
            connectionString: 'postgresql://neondb_owner:npg_0jJAfrLxdRM7@ep-falling-union-a15mokzs-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require',
            max: 10,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 10000,
            keepAlive: true
        });
        
        await pgPool.connect();
        console.log('✅ Connected to PostgreSQL (Neon cloud) with connection pool');

        // MongoDB connection (Atlas cloud)
        mongoClient = new MongoClient('mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/');
        await mongoClient.connect();
        console.log('✅ Connected to MongoDB (Atlas cloud)');

    } catch (error) {
        console.error('❌ Database connection failed:', error);
        process.exit(1);
    }
}

// File upload configuration
const storage = multer.diskStorage({
    destination: './temp_uploads/',
    filename: (req, file, cb) => {
        cb(null, file.fieldname + '-' + Date.now() + path.extname(file.originalname));
    }
});

const upload = multer({ 
    storage: storage,
    limits: { fileSize: 50 * 1024 * 1024 }, // 50MB limit
    fileFilter: (req, file, cb) => {
        const allowedTypes = ['.xlsx', '.xls', '.csv'];
        const ext = path.extname(file.originalname).toLowerCase();
        if (allowedTypes.includes(ext)) {
            cb(null, true);
        } else {
            cb(new Error('Invalid file type. Only .xlsx, .xls, and .csv files are allowed.'));
        }
    }
});

// Main HTML interface
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🏦 Unified Custody Data Management System</title>
    <script src="/socket.io/socket.io.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 30px;
            margin-bottom: 30px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .header h1 {
            color: white;
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }

        .header p {
            color: rgba(255, 255, 255, 0.9);
            font-size: 1.2em;
        }

        .tabs {
            display: flex;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            padding: 5px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }

        .tab {
            flex: 1;
            padding: 15px 20px;
            background: transparent;
            border: none;
            color: white;
            font-size: 16px;
            cursor: pointer;
            border-radius: 10px;
            transition: all 0.3s ease;
        }

        .tab.active {
            background: rgba(255, 255, 255, 0.2);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }

        .tab:hover {
            background: rgba(255, 255, 255, 0.15);
        }

        .tab-content {
            display: none;
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            padding: 30px;
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .tab-content.active {
            display: block;
        }

        .card {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 20px;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .card h3 {
            color: white;
            margin-bottom: 15px;
            font-size: 1.5em;
        }

        .upload-area {
            border: 2px dashed rgba(255, 255, 255, 0.3);
            border-radius: 15px;
            padding: 40px;
            text-align: center;
            cursor: pointer;
            transition: all 0.3s ease;
            background: rgba(255, 255, 255, 0.05);
        }

        .upload-area:hover {
            border-color: rgba(255, 255, 255, 0.6);
            background: rgba(255, 255, 255, 0.1);
        }

        .upload-area.dragover {
            border-color: #4CAF50;
            background: rgba(76, 175, 80, 0.1);
        }

        .btn {
            background: linear-gradient(45deg, #4CAF50, #45a049);
            color: white;
            border: none;
            padding: 12px 25px;
            border-radius: 25px;
            cursor: pointer;
            font-size: 16px;
            transition: all 0.3s ease;
            margin: 5px;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }

        .btn-danger {
            background: linear-gradient(45deg, #f44336, #da190b);
        }

        .btn-info {
            background: linear-gradient(45deg, #2196F3, #0b7dda);
        }

        .btn-warning {
            background: linear-gradient(45deg, #ff9800, #e68900);
        }

        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .status-card {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            padding: 20px;
            text-align: center;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .status-card h4 {
            color: white;
            margin-bottom: 10px;
        }

        .status-card .value {
            font-size: 2em;
            font-weight: bold;
            color: #4CAF50;
        }

        .progress-container {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
        }

        .progress-bar {
            width: 100%;
            height: 20px;
            background: rgba(255, 255, 255, 0.2);
            border-radius: 10px;
            overflow: hidden;
        }

        .progress-fill {
            height: 100%;
            background: linear-gradient(45deg, #4CAF50, #45a049);
            width: 0%;
            transition: width 0.3s ease;
        }

        .log-container {
            background: rgba(0, 0, 0, 0.3);
            border-radius: 10px;
            padding: 20px;
            height: 300px;
            overflow-y: auto;
            font-family: 'Courier New', monospace;
            font-size: 14px;
        }

        .log-entry {
            margin: 5px 0;
            padding: 5px;
            border-radius: 5px;
        }

        .log-info { color: #4CAF50; }
        .log-error { color: #f44336; }
        .log-warning { color: #ff9800; }

        .control-panel {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }

        .file-info {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 15px;
            margin: 10px 0;
        }

        .file-info h4 {
            color: white;
            margin-bottom: 10px;
        }

        .file-info p {
            color: rgba(255, 255, 255, 0.8);
            margin: 5px 0;
        }

        .connection-status {
            display: flex;
            justify-content: space-around;
            margin-bottom: 20px;
        }

        .connection-indicator {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .connection-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #4CAF50;
            animation: pulse 2s infinite;
        }

        .connection-dot.disconnected {
            background: #f44336;
        }

        @keyframes pulse {
            0% { opacity: 1; }
            50% { opacity: 0.5; }
            100% { opacity: 1; }
        }

        .worker-card {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 15px;
            padding: 20px;
            margin: 10px 0;
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
        }

        .worker-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }

        .worker-status {
            padding: 5px 10px;
            border-radius: 20px;
            font-size: 0.8em;
            font-weight: bold;
        }

        .worker-status.processing {
            background: #ff9800;
            color: white;
        }

        .worker-status.complete {
            background: #4CAF50;
            color: white;
        }

        .collection-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: bold;
            margin: 2px;
        }

        .badge-axis { background: #2196F3; color: white; }
        .badge-kotak { background: #ff9800; color: white; }
        .badge-orbis { background: #9c27b0; color: white; }
        .badge-deutsche { background: #795548; color: white; }
        .badge-trustpms { background: #607d8b; color: white; }
        .badge-unknown { background: #9e9e9e; color: white; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏦 Unified Custody Data Management System</h1>
            <p>Complete solution for file upload, data processing, and visualization</p>
        </div>

        <div class="connection-status">
            <div class="connection-indicator">
                <div class="connection-dot" id="mongoStatus"></div>
                <span style="color: white;">MongoDB</span>
            </div>
            <div class="connection-indicator">
                <div class="connection-dot" id="pgStatus"></div>
                <span style="color: white;">PostgreSQL</span>
            </div>
            <div class="connection-indicator">
                <div class="connection-dot" id="socketStatus"></div>
                <span style="color: white;">Socket.IO</span>
            </div>
        </div>

        <div class="tabs">
            <button class="tab active" onclick="showTab('upload', this)">📤 File Upload</button>
            <button class="tab" onclick="showTab('mongodb', this)">🍃 MongoDB Data</button>
            <button class="tab" onclick="showTab('postgresql', this)">🐘 PostgreSQL Data</button>
            <button class="tab" onclick="showTab('processing', this)">⚡ ETL Processing</button>
            <button class="tab" onclick="showTab('system', this)">🖥️ System Status</button>
        </div>

        <!-- File Upload Tab -->
        <div id="upload" class="tab-content active">
            <div class="card">
                <h3>📤 Upload Custody Files</h3>
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <input type="file" id="fileInput" multiple accept=".xlsx,.xls,.csv" style="display: none;">
                    <p style="color: white; font-size: 1.2em;">🎯 Click to select files or drag & drop</p>
                    <p style="color: rgba(255,255,255,0.8);">Supported: .xlsx, .xls, .csv files</p>
                </div>
                <div class="control-panel">
                    <button class="btn" onclick="uploadFiles()">🚀 Upload & Process</button>
                    <button class="btn btn-info" onclick="refreshData()">🔄 Refresh Data</button>
                    <button class="btn btn-warning" onclick="clearTempFiles()">🧹 Clear Temp Files</button>
                </div>
                <div id="uploadProgress" class="progress-container" style="display: none;">
                    <h4 style="color: white; margin-bottom: 10px;">Upload Progress</h4>
                    <div class="progress-bar">
                        <div class="progress-fill" id="uploadProgressFill"></div>
                    </div>
                    <p id="uploadStatus" style="color: white; margin-top: 10px;"></p>
                </div>
                <div id="uploadedFiles"></div>
            </div>
        </div>

        <!-- MongoDB Data Tab -->
        <div id="mongodb" class="tab-content">
            <div class="card">
                <h3>🍃 MongoDB Collections</h3>
                <div class="control-panel">
                    <button class="btn btn-info" onclick="loadMongoData()">🔄 Refresh Collections</button>
                    <button class="btn btn-danger" onclick="clearMongoData()">🗑️ Clear All MongoDB</button>
                </div>
                <div id="mongoCollections"></div>
            </div>
        </div>

        <!-- PostgreSQL Data Tab -->
        <div id="postgresql" class="tab-content">
            <div class="card">
                <h3>🐘 PostgreSQL Tables</h3>
                <div class="control-panel">
                    <button class="btn btn-info" onclick="loadPostgresData()">🔄 Refresh Tables</button>
                    <button class="btn btn-danger" onclick="clearPostgresData()">🗑️ Clear All PostgreSQL</button>
                </div>
                <div id="postgresTables"></div>
            </div>
        </div>

        <!-- ETL Processing Tab -->
        <div id="processing" class="tab-content">
            <div class="card">
                <h3>⚡ Multi-Threaded ETL Processing</h3>
                <div class="status-grid">
                    <div class="status-card">
                        <h4>Total Records</h4>
                        <div class="value" id="totalRecords">0</div>
                    </div>
                    <div class="status-card">
                        <h4>Processed</h4>
                        <div class="value" id="processedRecords">0</div>
                    </div>
                    <div class="status-card">
                        <h4>Success Rate</h4>
                        <div class="value" id="successRate">0%</div>
                    </div>
                    <div class="status-card">
                        <h4>Active Workers</h4>
                        <div class="value" id="activeWorkers">0</div>
                    </div>
                </div>
                <div class="control-panel">
                    <button class="btn" onclick="startETLProcessing()">🚀 Start Multi-Threaded ETL</button>
                    <button class="btn btn-warning" onclick="stopETLProcessing()">⏸️ Stop Processing</button>
                    <button class="btn btn-info" onclick="discoverCollections()">🔍 Discover Collections</button>
                </div>
                <div id="etlProgress" class="progress-container" style="display: none;">
                    <h4 style="color: white; margin-bottom: 10px;">Processing Progress</h4>
                    <div class="progress-bar">
                        <div class="progress-fill" id="etlProgressFill"></div>
                    </div>
                    <p id="etlStatus" style="color: white; margin-top: 10px;"></p>
                </div>
                <div id="workersGrid"></div>
                <div class="log-container" id="etlLogs"></div>
            </div>
        </div>

        <!-- System Status Tab -->
        <div id="system" class="tab-content">
            <div class="card">
                <h3>🖥️ System Information</h3>
                <div class="status-grid">
                    <div class="status-card">
                        <h4>CPU Cores</h4>
                        <div class="value" id="cpuCores">Loading...</div>
                    </div>
                    <div class="status-card">
                        <h4>Memory</h4>
                        <div class="value" id="memoryUsage">0GB</div>
                    </div>
                    <div class="status-card">
                        <h4>Uptime</h4>
                        <div class="value" id="systemUptime">0h</div>
                    </div>
                    <div class="status-card">
                        <h4>Platform</h4>
                        <div class="value" id="systemPlatform">Loading...</div>
                    </div>
                </div>
                <div class="control-panel">
                    <button class="btn btn-info" onclick="refreshSystemStats()">🔄 Refresh Stats</button>
                    <button class="btn btn-danger" onclick="clearAllData()">🗑️ Clear All Data</button>
                    <button class="btn btn-warning" onclick="restartServices()">🔄 Restart Services</button>
                </div>
                <div id="systemLogs" class="log-container"></div>
            </div>
        </div>
    </div>

    <script>
        let socket = null;
        let processingActive = false;

        // Initialize Socket.IO connection
        function initSocket() {
            socket = io();
            
            socket.on('connect', () => {
                console.log('Socket.IO connected');
                document.getElementById('socketStatus').classList.remove('disconnected');
                addLogEntry('info', 'Connected to server');
            });
            
            socket.on('disconnect', () => {
                console.log('Socket.IO disconnected');
                document.getElementById('socketStatus').classList.add('disconnected');
                addLogEntry('warning', 'Disconnected from server');
            });

            // ETL processing events
            socket.on('processing_start', (data) => {
                processingActive = true;
                document.getElementById('etlProgress').style.display = 'block';
                document.getElementById('etlStatus').textContent = 'Multi-threaded processing started...';
                addLogEntry('info', 'Multi-threaded ETL processing started');
            });

            socket.on('worker_progress', (data) => {
                updateWorkerProgress(data);
                updateOverallProgress(data.overallProgress);
            });

            socket.on('worker_complete', (data) => {
                markWorkerComplete(data);
                addLogEntry('success', 'Worker ' + data.workerId + ' completed: ' + data.collectionName);
            });

            socket.on('processing_complete', (data) => {
                processingActive = false;
                document.getElementById('etlStatus').textContent = 'Processing completed!';
                document.getElementById('etlProgressFill').style.width = '100%';
                addLogEntry('success', 'Multi-threaded processing complete! Success rate: ' + data.successRate + '%');
            });

            socket.on('error', (data) => {
                addLogEntry('error', 'Error: ' + data.error);
            });
        }

        // Tab switching
        function showTab(tabName, element) {
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            element.classList.add('active');
            document.getElementById(tabName).classList.add('active');
            
            // Load data for the active tab
            switch(tabName) {
                case 'mongodb':
                    loadMongoData();
                    break;
                case 'postgresql':
                    loadPostgresData();
                    break;
                case 'system':
                    refreshSystemStats();
                    break;
                case 'processing':
                    discoverCollections();
                    break;
            }
        }

        // File upload functionality
        function uploadFiles() {
            const fileInput = document.getElementById('fileInput');
            const files = fileInput.files;
            
            if (files.length === 0) {
                alert('Please select files to upload');
                return;
            }

            const formData = new FormData();
            for (let i = 0; i < files.length; i++) {
                formData.append('files', files[i]);
            }

            document.getElementById('uploadProgress').style.display = 'block';
            document.getElementById('uploadStatus').textContent = 'Uploading...';

            fetch('/api/upload', {
                method: 'POST',
                body: formData
            })
            .then(response => response.json())
            .then(data => {
                document.getElementById('uploadStatus').textContent = 'Upload completed!';
                document.getElementById('uploadProgressFill').style.width = '100%';
                displayUploadedFiles(data.files);
                addLogEntry('success', 'Files uploaded successfully');
            })
            .catch(error => {
                console.error('Upload error:', error);
                document.getElementById('uploadStatus').textContent = 'Upload failed!';
                addLogEntry('error', 'Upload failed: ' + error.message);
            });
        }

        // Load MongoDB data
        async function loadMongoData() {
            try {
                const response = await fetch('/api/mongodb/collections');
                const data = await response.json();
                displayMongoCollections(data.collections);
            } catch (error) {
                console.error('Error loading MongoDB data:', error);
                addLogEntry('error', 'Failed to load MongoDB data');
            }
        }

        // Load PostgreSQL data
        async function loadPostgresData() {
            try {
                const response = await fetch('/api/postgresql/tables');
                const data = await response.json();
                displayPostgresTables(data.tables);
            } catch (error) {
                console.error('Error loading PostgreSQL data:', error);
                addLogEntry('error', 'Failed to load PostgreSQL data');
            }
        }

        // ETL Processing functions
        function discoverCollections() {
            if (socket) {
                socket.emit('discover_collections');
                addLogEntry('info', 'Discovering MongoDB collections...');
            }
        }

        function startETLProcessing() {
            if (processingActive) {
                alert('Processing is already active');
                return;
            }

            if (socket) {
                socket.emit('start_processing');
                addLogEntry('info', 'Starting multi-threaded ETL processing...');
            }
        }

        function stopETLProcessing() {
            if (socket) {
                socket.emit('stop_processing');
                processingActive = false;
                addLogEntry('warning', 'Processing stopped');
            }
        }

        // Display functions
        function displayUploadedFiles(files) {
            const container = document.getElementById('uploadedFiles');
            container.innerHTML = '';
            
            files.forEach(file => {
                const fileDiv = document.createElement('div');
                fileDiv.className = 'file-info';
                fileDiv.innerHTML = '<h4>' + file.originalName + '</h4>' +
                    '<p>Size: ' + (file.size / 1024 / 1024).toFixed(2) + ' MB</p>' +
                    '<p>Type: ' + file.type + '</p>' +
                    '<p>Uploaded: ' + new Date(file.uploadTime).toLocaleString() + '</p>';
                container.appendChild(fileDiv);
            });
        }

        function displayMongoCollections(collections) {
            const container = document.getElementById('mongoCollections');
            container.innerHTML = '';
            
            if (collections.length === 0) {
                container.innerHTML = '<p style="color: white;">No collections found</p>';
                return;
            }

            collections.forEach(collection => {
                const collectionDiv = document.createElement('div');
                collectionDiv.className = 'file-info';
                
                const versionInfo = collection.versioningEnabled ? 
                    '<span class="collection-badge badge-info">📚 ' + collection.versionsAvailable + ' versions</span>' :
                    '<span class="collection-badge badge-unknown">📝 Single version</span>';
                
                const statusBadge = collection.status.includes('✅') ?
                    '<span class="collection-badge badge-axis">✅ Active</span>' :
                    '<span class="collection-badge badge-warning">⚠️ Inactive</span>';
                
                collectionDiv.innerHTML = '<h4>' + collection.name + ' ' + statusBadge + '</h4>' +
                    '<p><strong>Active Records:</strong> ' + collection.count.toLocaleString() + '</p>' +
                    (collection.historicalCount > 0 ? 
                        '<p><strong>Historical Records:</strong> ' + collection.historicalCount.toLocaleString() + '</p>' : '') +
                    '<p><strong>Version:</strong> ' + (collection.currentVersion || 'v1') + ' ' + versionInfo + '</p>' +
                    '<p><strong>Database:</strong> ' + collection.database + '</p>' +
                    '<p><strong>Last Updated:</strong> ' + (collection.lastUpdated ? new Date(collection.lastUpdated).toLocaleString() : 'Unknown') + '</p>' +
                    '<div style="margin-top: 10px;">' +
                    '<button class="btn btn-success" onclick="previewMongoCollection(\'' + collection.name + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">👁️ Preview Data</button>' +
                    (collection.versioningEnabled ? 
                        '<button class="btn btn-info" onclick="viewVersionHistory(\'' + collection.sourceType + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">📋 View Versions</button>' : '') +
                    '<button class="btn btn-warning" onclick="exportCollection(\'' + collection.name + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">📥 Export</button>' +
                    '</div>';
                
                container.appendChild(collectionDiv);
            });
        }

        // Function to view version history
        async function viewVersionHistory(sourceType) {
            try {
                const response = await fetch('/api/versions/' + sourceType + '?limit=20');
                const data = await response.json();
                
                if (data.success && data.versions.length > 0) {
                    let html = '<div style="background: rgba(0,0,0,0.3); padding: 20px; border-radius: 10px; margin: 10px 0;">';
                    html += '<h4>📚 Version History: ' + sourceType + '</h4>';
                    html += '<table style="width: 100%; border-collapse: collapse; margin-top: 10px;">';
                    html += '<tr style="background: rgba(255,255,255,0.1);"><th style="padding: 8px; text-align: left;">Version</th><th style="padding: 8px; text-align: left;">Status</th><th style="padding: 8px; text-align: left;">Records</th><th style="padding: 8px; text-align: left;">Upload Time</th><th style="padding: 8px; text-align: left;">Actions</th></tr>';
                    
                    data.versions.forEach(version => {
                        const isActive = version.isActive;
                        const status = isActive ? '✅ Active' : '📦 Historical';
                        const statusColor = isActive ? '#4CAF50' : '#9e9e9e';
                        
                        html += '<tr style="border-bottom: 1px solid rgba(255,255,255,0.1);">';
                        html += '<td style="padding: 8px;">' + (version.uploadVersion || 'v' + version._id) + '</td>';
                        html += '<td style="padding: 8px; color: ' + statusColor + ';">' + status + '</td>';
                        html += '<td style="padding: 8px;">' + version.recordCount.toLocaleString() + '</td>';
                        html += '<td style="padding: 8px;">' + new Date(version.uploadTimestamp).toLocaleString() + '</td>';
                        html += '<td style="padding: 8px;">';
                        if (!isActive) {
                            html += '<button class="btn btn-warning" onclick="activateVersion(\'' + sourceType + '\', ' + version._id + ')" style="padding: 4px 8px; font-size: 11px;">🔄 Activate</button>';
                        }
                        html += '</td>';
                        html += '</tr>';
                    });
                    
                    html += '</table>';
                    html += '<button class="btn btn-danger" onclick="closeVersionHistory()" style="margin-top: 10px; padding: 5px 10px; font-size: 12px;">❌ Close</button>';
                    html += '</div>';
                    
                    // Add to page
                    const existingHistory = document.getElementById('versionHistory');
                    if (existingHistory) {
                        existingHistory.remove();
                    }
                    
                    const historyDiv = document.createElement('div');
                    historyDiv.id = 'versionHistory';
                    historyDiv.innerHTML = html;
                    document.getElementById('mongoCollections').appendChild(historyDiv);
                    
                } else {
                    alert('No version history found for ' + sourceType);
                }
            } catch (error) {
                console.error('Error viewing version history:', error);
                alert('Error loading version history');
            }
        }

        // Function to activate a version
        async function activateVersion(sourceType, versionId) {
            if (confirm('Are you sure you want to activate this version? Current active data will become historical.')) {
                try {
                    const response = await fetch('/api/versions/' + sourceType + '/activate', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ versionId: versionId })
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        alert('Version activated successfully! ' + result.recordsActivated + ' records activated.');
                        closeVersionHistory();
                        loadMongoData(); // Refresh data
                    } else {
                        alert('Failed to activate version: ' + result.message);
                    }
                } catch (error) {
                    console.error('Error activating version:', error);
                    alert('Error activating version');
                }
            }
        }

        // Function to close version history
        function closeVersionHistory() {
            const historyDiv = document.getElementById('versionHistory');
            if (historyDiv) {
                historyDiv.remove();
            }
        }

        // Preview MongoDB collection data
        async function previewMongoCollection(collectionName) {
            try {
                const response = await fetch('/api/mongodb/preview/' + collectionName + '?page=1&limit=20');
                const data = await response.json();
                
                if (data.success) {
                    showDataPreviewModal({
                        title: '🍃 MongoDB Collection: ' + collectionName,
                        data: data.data,
                        schema: data.schema,
                        pagination: data.pagination,
                        type: 'mongodb',
                        source: collectionName,
                        filter: data.filter
                    });
                } else {
                    alert('Failed to load collection data');
                }
            } catch (error) {
                console.error('Error previewing MongoDB collection:', error);
                alert('Error loading collection data');
            }
        }

        // Preview PostgreSQL table data
        async function previewPostgresTable(tableName) {
            try {
                const response = await fetch('/api/postgresql/preview/' + tableName + '?page=1&limit=20');
                const data = await response.json();
                
                if (data.success) {
                    showDataPreviewModal({
                        title: '🐘 PostgreSQL Table: ' + tableName,
                        data: data.data,
                        schema: data.schema,
                        pagination: data.pagination,
                        type: 'postgresql',
                        source: tableName
                    });
                } else {
                    alert('Failed to load table data');
                }
            } catch (error) {
                console.error('Error previewing PostgreSQL table:', error);
                alert('Error loading table data');
            }
        }

        // Show data preview modal
        function showDataPreviewModal(config) {
            // Remove existing modal
            const existingModal = document.getElementById('dataPreviewModal');
            if (existingModal) {
                existingModal.remove();
            }

            // Create modal
            const modal = document.createElement('div');
            modal.id = 'dataPreviewModal';
            modal.style.cssText = `
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0,0,0,0.8);
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 1000;
            `;

            const modalContent = document.createElement('div');
            modalContent.style.cssText = `
                background: linear-gradient(135deg, #1e3c72, #2a5298);
                border-radius: 15px;
                padding: 20px;
                width: 90%;
                max-width: 1200px;
                height: 80%;
                overflow: auto;
                position: relative;
                box-shadow: 0 10px 30px rgba(0,0,0,0.5);
            `;

            // Generate table HTML
            let tableHtml = `
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                    <h3 style="color: white; margin: 0;">${config.title}</h3>
                    <button onclick="closeDataPreview()" style="background: #f44336; color: white; border: none; padding: 8px 12px; border-radius: 5px; cursor: pointer;">❌ Close</button>
                </div>
                
                <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 10px; margin-bottom: 15px;">
                    <h4 style="color: white; margin: 0 0 10px 0;">📊 Data Summary</h4>
                    <p style="color: white; margin: 5px 0;"><strong>Total Records:</strong> ${config.pagination.totalRecords.toLocaleString()}</p>
                    <p style="color: white; margin: 5px 0;"><strong>Showing:</strong> Page ${config.pagination.currentPage} of ${config.pagination.totalPages}</p>
                    <p style="color: white; margin: 5px 0;"><strong>Fields/Columns:</strong> ${config.type === 'mongodb' ? config.schema.totalFields : config.schema.totalColumns}</p>
                    ${config.filter ? `<p style="color: white; margin: 5px 0;"><strong>Filter:</strong> ${config.filter}</p>` : ''}
                </div>
            `;

            if (config.data && config.data.length > 0) {
                tableHtml += `
                    <div style="overflow-x: auto; background: rgba(255,255,255,0.1); border-radius: 10px; padding: 10px;">
                        <table style="width: 100%; border-collapse: collapse; color: white; font-size: 12px;">
                            <thead>
                                <tr style="background: rgba(255,255,255,0.2);">
                `;

                // Table headers
                if (config.type === 'mongodb') {
                    const sampleRecord = config.data[0];
                    Object.keys(sampleRecord).filter(key => !key.startsWith('_')).slice(0, 10).forEach(key => {
                        tableHtml += `<th style="padding: 8px; border: 1px solid rgba(255,255,255,0.3); text-align: left;">${key}</th>`;
                    });
                } else {
                    config.schema.columns.slice(0, 10).forEach(col => {
                        tableHtml += `<th style="padding: 8px; border: 1px solid rgba(255,255,255,0.3); text-align: left;">${col.column_name}</th>`;
                    });
                }

                tableHtml += `
                                </tr>
                            </thead>
                            <tbody>
                `;

                // Table rows
                config.data.slice(0, 20).forEach(record => {
                    tableHtml += '<tr style="border-bottom: 1px solid rgba(255,255,255,0.1);">';
                    
                    if (config.type === 'mongodb') {
                        Object.keys(record).filter(key => !key.startsWith('_')).slice(0, 10).forEach(key => {
                            let value = record[key];
                            if (typeof value === 'object' && value !== null) {
                                value = JSON.stringify(value).substring(0, 50) + '...';
                            } else if (typeof value === 'string' && value.length > 50) {
                                value = value.substring(0, 50) + '...';
                            }
                            tableHtml += `<td style="padding: 6px; border: 1px solid rgba(255,255,255,0.1);">${value || 'N/A'}</td>`;
                        });
                    } else {
                        config.schema.columns.slice(0, 10).forEach(col => {
                            let value = record[col.column_name];
                            if (typeof value === 'string' && value.length > 50) {
                                value = value.substring(0, 50) + '...';
                            }
                            tableHtml += `<td style="padding: 6px; border: 1px solid rgba(255,255,255,0.1);">${value || 'N/A'}</td>`;
                        });
                    }
                    
                    tableHtml += '</tr>';
                });

                tableHtml += `
                            </tbody>
                        </table>
                    </div>
                `;

                // Pagination controls
                if (config.pagination.totalPages > 1) {
                    const prevBtn = config.pagination.hasPrev ? 
                        '<button onclick="loadDataPage(\'' + config.type + '\', \'' + config.source + '\', ' + (config.pagination.currentPage - 1) + ')" style="background: #2196F3; color: white; border: none; padding: 8px 12px; margin: 2px; border-radius: 5px; cursor: pointer;">← Previous</button>' : '';
                    const nextBtn = config.pagination.hasNext ? 
                        '<button onclick="loadDataPage(\'' + config.type + '\', \'' + config.source + '\', ' + (config.pagination.currentPage + 1) + ')" style="background: #2196F3; color: white; border: none; padding: 8px 12px; margin: 2px; border-radius: 5px; cursor: pointer;">Next →</button>' : '';
                    
                    tableHtml += '<div style="margin-top: 15px; text-align: center;">' +
                        prevBtn +
                        '<span style="color: white; margin: 0 10px;">Page ' + config.pagination.currentPage + ' of ' + config.pagination.totalPages + '</span>' +
                        nextBtn +
                        '</div>';
                }
            } else {
                tableHtml += '<p style="color: white; text-align: center; padding: 40px;">No data available</p>';
            }

            modalContent.innerHTML = tableHtml;
            modal.appendChild(modalContent);
            document.body.appendChild(modal);
        }

        // Load specific page of data
        async function loadDataPage(type, source, page) {
            try {
                const endpoint = type === 'mongodb' ? 
                    `/api/mongodb/preview/${source}?page=${page}&limit=20` :
                    `/api/postgresql/preview/${source}?page=${page}&limit=20`;
                
                const response = await fetch(endpoint);
                const data = await response.json();
                
                if (data.success) {
                    const title = type === 'mongodb' ? 
                        `🍃 MongoDB Collection: ${source}` : 
                        `🐘 PostgreSQL Table: ${source}`;
                    
                    showDataPreviewModal({
                        title: title,
                        data: data.data,
                        schema: data.schema,
                        pagination: data.pagination,
                        type: type,
                        source: source,
                        filter: data.filter
                    });
                }
            } catch (error) {
                console.error('Error loading data page:', error);
                alert('Error loading data page');
            }
        }

        // Close data preview modal
        function closeDataPreview() {
            const modal = document.getElementById('dataPreviewModal');
            if (modal) {
                modal.remove();
            }
        }

        // Placeholder functions for other features
        function exportCollection(collectionName) {
            alert('Export functionality for ' + collectionName + ' would be implemented here');
        }

        function analyzeTable(tableName) {
            alert('Table analysis for ' + tableName + ' would be implemented here');
        }

        function exportTable(tableName) {
            alert('Export functionality for ' + tableName + ' would be implemented here');
        }

        function displayPostgresTables(tables) {
            const container = document.getElementById('postgresTables');
            container.innerHTML = '';
            
            if (tables.length === 0) {
                container.innerHTML = '<p style="color: white;">No tables found</p>';
                return;
            }

            tables.forEach(table => {
                const tableDiv = document.createElement('div');
                tableDiv.className = 'file-info';
                tableDiv.innerHTML = '<h4>' + table.name + '</h4>' +
                    '<p><strong>Records:</strong> ' + table.count.toLocaleString() + '</p>' +
                    '<p><strong>Columns:</strong> ' + table.columns + '</p>' +
                    '<div style="margin-top: 10px;">' +
                    '<button class="btn btn-success" onclick="previewPostgresTable(\'' + table.name + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">👁️ Preview Data</button>' +
                    '<button class="btn btn-info" onclick="analyzeTable(\'' + table.name + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">📊 Analyze</button>' +
                    '<button class="btn btn-warning" onclick="exportTable(\'' + table.name + '\')" style="margin: 2px; padding: 5px 10px; font-size: 12px;">📥 Export</button>' +
                    '</div>';
                container.appendChild(tableDiv);
            });
        }

        function updateWorkerProgress(data) {
            const workerId = data.workerId;
            let workerCard = document.getElementById('worker-' + workerId);
            
            if (!workerCard) {
                workerCard = createWorkerCard(workerId, data.collectionName, data.custodyType);
                document.getElementById('workersGrid').appendChild(workerCard);
            }
            
            const progressFill = workerCard.querySelector('.progress-fill');
            const progressText = workerCard.querySelector('.progress-text');
            const statusBadge = workerCard.querySelector('.worker-status');
            
            progressFill.style.width = data.collectionProgress.percentage + '%';
            progressText.textContent = data.collectionProgress.percentage + '% (' + 
                data.collectionProgress.processed + '/' + data.collectionProgress.total + ')';
            statusBadge.textContent = 'PROCESSING';
            statusBadge.className = 'worker-status processing';
        }

        function createWorkerCard(workerId, collectionName, custodyType) {
            const card = document.createElement('div');
            card.className = 'worker-card';
            card.id = 'worker-' + workerId;
            card.innerHTML = '<div class="worker-header">' +
                '<div><strong>Worker ' + workerId + '</strong><br>' +
                '<small>' + collectionName + ' (' + custodyType + ')</small></div>' +
                '<div class="worker-status">STARTING</div></div>' +
                '<div class="progress-bar"><div class="progress-fill" style="width: 0%"></div></div>' +
                '<div class="progress-text">Initializing...</div>';
            return card;
        }

        function markWorkerComplete(data) {
            const workerCard = document.getElementById('worker-' + data.workerId);
            if (workerCard) {
                const statusBadge = workerCard.querySelector('.worker-status');
                const progressFill = workerCard.querySelector('.progress-fill');
                const progressText = workerCard.querySelector('.progress-text');
                
                statusBadge.textContent = 'COMPLETE';
                statusBadge.className = 'worker-status complete';
                progressFill.style.width = '100%';
                progressText.textContent = '✅ ' + data.result.valid + '/' + data.result.processed + ' valid';
            }
        }

        function updateOverallProgress(progress) {
            if (progress) {
                const percentage = progress.overallPercentage || 0;
                document.getElementById('etlProgressFill').style.width = percentage + '%';
                document.getElementById('etlStatus').textContent = 'Processing: ' + percentage + '% complete';
                
                document.getElementById('totalRecords').textContent = (progress.totalRecords || 0).toLocaleString();
                document.getElementById('processedRecords').textContent = (progress.processedRecords || 0).toLocaleString();
                document.getElementById('successRate').textContent = 
                    Math.round((progress.validRecords / progress.processedRecords) * 100) + '%';
            }
        }

        function addLogEntry(type, message) {
            const logContainer = document.getElementById('etlLogs');
            const logEntry = document.createElement('div');
            logEntry.className = 'log-entry log-' + type;
            logEntry.textContent = '[' + new Date().toLocaleTimeString() + '] ' + message;
            
            logContainer.appendChild(logEntry);
            logContainer.scrollTop = logContainer.scrollHeight;
        }

        function refreshSystemStats() {
            fetch('/api/system/stats')
            .then(response => response.json())
            .then(data => {
                document.getElementById('cpuCores').textContent = data.cpuCores;
                document.getElementById('memoryUsage').textContent = 
                    (data.memoryUsage / 1024 / 1024 / 1024).toFixed(1) + 'GB';
                document.getElementById('systemUptime').textContent = 
                    (data.uptime / 3600).toFixed(1) + 'h';
                document.getElementById('systemPlatform').textContent = data.platform;
            })
            .catch(error => console.error('Error refreshing stats:', error));
        }

        function clearAllData() {
            if (confirm('Are you sure you want to clear ALL data? This action cannot be undone.')) {
                fetch('/api/clear-all', { method: 'POST' })
                .then(response => response.json())
                .then(data => {
                    alert('All data cleared successfully');
                    refreshData();
                })
                .catch(error => {
                    console.error('Error clearing data:', error);
                    alert('Error clearing data');
                });
            }
        }

        function refreshData() {
            loadMongoData();
            loadPostgresData();
            refreshSystemStats();
        }

        function clearTempFiles() {
            fetch('/api/clear-temp', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                alert('Temporary files cleared');
                addLogEntry('info', 'Temporary files cleared');
            })
            .catch(error => {
                console.error('Error clearing temp files:', error);
                alert('Error clearing temp files');
            });
        }

        function restartServices() {
            if (confirm('Are you sure you want to restart all services?')) {
                alert('Service restart would be implemented here');
            }
        }

        // Initialize on page load
        document.addEventListener('DOMContentLoaded', () => {
            initSocket();
            refreshData();
            
            // File drag and drop
            const uploadArea = document.querySelector('.upload-area');
            
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('dragover');
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('dragover');
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');
                
                const files = e.dataTransfer.files;
                document.getElementById('fileInput').files = files;
            });

            // Auto-refresh every 30 seconds
            setInterval(refreshData, 30000);
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
                collections: collections.map(c => ({ name: c.collectionName, records: c.recordCount })),
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

// API Routes
app.post('/api/upload', upload.array('files'), async (req, res) => {
    try {
        const { replaceMode = 'source', recordDate } = req.body;
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

app.get('/api/mongodb/collections', async (req, res) => {
    try {
        // Use SmartFileProcessor to get clean collection info
        const processor = new SmartFileProcessor();
        const collections = await processor.getCollectionsInfo();
        await processor.disconnect();

        // Format for dashboard display with versioning info
        const formattedCollections = collections.map(collection => ({
            name: collection.name,
            database: collection.database,
            count: collection.count,
            totalCount: collection.totalCount,
            historicalCount: collection.historicalCount,
            sourceType: collection.sourceType,
            lastUpdated: collection.lastUpdated,
            latestDate: collection.latestDate,
            currentVersion: collection.currentVersion,
            versioningEnabled: collection.versioningEnabled,
            versionsAvailable: collection.versionsAvailable,
            status: collection.status,
            dateRange: collection.dateRange
        }));

        // Calculate summary stats
        const totalActiveRecords = formattedCollections.reduce((sum, c) => sum + c.count, 0);
        const totalHistoricalRecords = formattedCollections.reduce((sum, c) => sum + (c.historicalCount || 0), 0);
        const versionsedCollections = formattedCollections.filter(c => c.versioningEnabled).length;

        res.json({ 
            collections: formattedCollections,
            summary: {
                totalCollections: formattedCollections.length,
                totalActiveRecords: totalActiveRecords,
                totalHistoricalRecords: totalHistoricalRecords,
                versionedCollections: versionsedCollections,
                versioningEnabled: true,
                message: `${formattedCollections.length} sources with intelligent versioning (${totalActiveRecords} active, ${totalHistoricalRecords} historical)`
            }
        });
    } catch (error) {
        console.error('Error getting MongoDB collections:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/postgresql/tables', async (req, res) => {
    try {
        const client = await pgPool.connect();
        
        const result = await client.query(`
            SELECT 
                tablename as name,
                schemaname as schema
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        `);

        const tables = [];
        for (const row of result.rows) {
            try {
                const countResult = await client.query(`SELECT COUNT(*) FROM ${row.name}`);
                const columnsResult = await client.query(`
                    SELECT COUNT(*) as column_count 
                    FROM information_schema.columns 
                    WHERE table_name = $1
                `, [row.name]);

                tables.push({
                    name: row.name,
                    count: parseInt(countResult.rows[0].count),
                    columns: parseInt(columnsResult.rows[0].column_count),
                    size: 'N/A'
                });
            } catch (error) {
                console.error(`Error getting info for table ${row.name}:`, error);
            }
        }

        client.release();
        res.json({ tables });
    } catch (error) {
        console.error('Error getting PostgreSQL tables:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/system/stats', (req, res) => {
    const stats = {
        cpuCores: os.cpus().length,
        memoryUsage: process.memoryUsage().rss,
        uptime: process.uptime(),
        platform: os.platform(),
        nodeVersion: process.version
    };
    
    res.json(stats);
});

app.post('/api/clear-all', async (req, res) => {
    try {
        // Clear MongoDB
        const databases = ['financial_data_2024', 'financial_data_2025'];
        for (const dbName of databases) {
            try {
                const db = mongoClient.db(dbName);
                await db.dropDatabase();
            } catch (error) {
                console.error(`Error clearing MongoDB database ${dbName}:`, error);
            }
        }

        // Clear PostgreSQL
        const client = await pgPool.connect();
        await client.query('DROP SCHEMA public CASCADE');
        await client.query('CREATE SCHEMA public');
        client.release();

        res.json({ success: true, message: 'All data cleared successfully' });
    } catch (error) {
        console.error('Error clearing data:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/clear-temp', (req, res) => {
    try {
        const tempDir = './temp_uploads/';
        if (fs.existsSync(tempDir)) {
            const files = fs.readdirSync(tempDir);
            files.forEach(file => {
                fs.unlinkSync(path.join(tempDir, file));
            });
        }
        res.json({ success: true, message: 'Temporary files cleared' });
    } catch (error) {
        console.error('Error clearing temp files:', error);
        res.status(500).json({ error: error.message });
    }
});

// Version management endpoints
app.get('/api/versions/:sourceType', async (req, res) => {
    try {
        const { sourceType } = req.params;
        const { limit = 10 } = req.query;
        
        const processor = new SmartFileProcessor();
        const versions = await processor.getVersionHistory(sourceType, parseInt(limit));
        await processor.disconnect();
        
        res.json({
            success: true,
            sourceType,
            versions,
            totalVersions: versions.length
        });
    } catch (error) {
        console.error('Error getting version history:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/versions/:sourceType/activate', async (req, res) => {
    try {
        const { sourceType } = req.params;
        const { versionId } = req.body;
        
        if (!versionId) {
            return res.status(400).json({ error: 'versionId is required' });
        }
        
        const processor = new SmartFileProcessor();
        const result = await processor.activateVersion(sourceType, parseInt(versionId));
        await processor.disconnect();
        
        // Broadcast version change to connected clients
        io.emit('version_activated', {
            type: 'version_activated',
            sourceType,
            versionId,
            result
        });
        
        res.json(result);
    } catch (error) {
        console.error('Error activating version:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/versions/:sourceType/cleanup', async (req, res) => {
    try {
        const { sourceType } = req.params;
        const { keepVersions = 5 } = req.body;
        
        const processor = new SmartFileProcessor();
        const result = await processor.cleanupOldVersions(sourceType, parseInt(keepVersions));
        await processor.disconnect();
        
        res.json(result);
    } catch (error) {
        console.error('Error cleaning up versions:', error);
        res.status(500).json({ error: error.message });
    }
});

// Add new API endpoints for data preview

// Preview MongoDB collection data
app.get('/api/mongodb/preview/:collection', async (req, res) => {
    try {
        const { collection } = req.params;
        const { page = 1, limit = 50, showAll = false } = req.query;
        
        const processor = new SmartFileProcessor();
        const db = processor.mongoClient.db('financial_data_2025');
        
        const skip = (page - 1) * limit;
        const query = showAll === 'true' ? {} : { isActive: true };
        
        const records = await db.collection(collection)
            .find(query)
            .sort({ uploadTimestamp: -1 })
            .skip(skip)
            .limit(parseInt(limit))
            .toArray();
            
        const totalCount = await db.collection(collection).countDocuments(query);
        const totalPages = Math.ceil(totalCount / limit);
        
        // Get schema info (field names from first record)
        const sampleRecord = records[0];
        const fields = sampleRecord ? Object.keys(sampleRecord).filter(key => !key.startsWith('_')) : [];
        
        await processor.disconnect();
        
        res.json({
            success: true,
            collection: collection,
            data: records,
            pagination: {
                currentPage: parseInt(page),
                totalPages: totalPages,
                totalRecords: totalCount,
                recordsPerPage: parseInt(limit),
                hasNext: page < totalPages,
                hasPrev: page > 1
            },
            schema: {
                fields: fields.slice(0, 20), // Show first 20 fields
                totalFields: fields.length
            },
            filter: showAll === 'true' ? 'All versions' : 'Active only'
        });
    } catch (error) {
        console.error('Error previewing MongoDB collection:', error);
        res.status(500).json({ error: error.message });
    }
});

// Preview PostgreSQL table data
app.get('/api/postgresql/preview/:table', async (req, res) => {
    try {
        const { table } = req.params;
        const { page = 1, limit = 50 } = req.query;
        
        const offset = (page - 1) * limit;
        const client = await pgPool.connect();
        
        // Get data with pagination
        const dataResult = await client.query(
            `SELECT * FROM ${table} ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
            [limit, offset]
        );
        
        // Get total count
        const countResult = await client.query(`SELECT COUNT(*) FROM ${table}`);
        const totalCount = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalCount / limit);
        
        // Get column info
        const columnsResult = await client.query(`
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = $1 
            ORDER BY ordinal_position
        `, [table]);
        
        client.release();
        
        res.json({
            success: true,
            table: table,
            data: dataResult.rows,
            pagination: {
                currentPage: parseInt(page),
                totalPages: totalPages,
                totalRecords: totalCount,
                recordsPerPage: parseInt(limit),
                hasNext: page < totalPages,
                hasPrev: page > 1
            },
            schema: {
                columns: columnsResult.rows,
                totalColumns: columnsResult.rows.length
            }
        });
    } catch (error) {
        console.error('Error previewing PostgreSQL table:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get MongoDB collections with detailed statistics
app.get('/api/mongodb/collections/detailed', async (req, res) => {
    try {
        const processor = new SmartFileProcessor();
        const collections = await processor.getCollectionsInfo();
        
        // Get additional stats for each collection
        const detailedCollections = [];
        for (const collection of collections) {
            const db = processor.mongoClient.db(collection.database);
            const coll = db.collection(collection.name);
            
            // Get version breakdown
            const versionStats = await coll.aggregate([
                {
                    $group: {
                        _id: { isActive: "$isActive", versionId: "$versionId" },
                        count: { $sum: 1 },
                        latestUpload: { $max: "$uploadTimestamp" }
                    }
                }
            ]).toArray();
            
            // Get sample fields
            const sampleRecord = await coll.findOne({ isActive: true });
            const fields = sampleRecord ? Object.keys(sampleRecord).filter(key => !key.startsWith('_')) : [];
            
            detailedCollections.push({
                ...collection,
                versionStats: versionStats,
                sampleFields: fields.slice(0, 10),
                totalFields: fields.length,
                hasPreview: true
            });
        }
        
        await processor.disconnect();
        
        res.json({
            success: true,
            collections: detailedCollections,
            totalCollections: detailedCollections.length,
            totalActiveRecords: detailedCollections.reduce((sum, c) => sum + c.count, 0)
        });
    } catch (error) {
        console.error('Error getting detailed collections:', error);
        res.status(500).json({ error: error.message });
    }
});

// Start server
async function startServer() {
    await initializeConnections();
    
    const PORT = 3007;
    server.listen(PORT, () => {
        console.log(`🚀 Unified Custody Dashboard running at http://localhost:${PORT}`);
        console.log(`🎯 All features in one place: Upload, MongoDB, PostgreSQL, ETL, System Status`);
        console.log(`⚡ Multi-threaded processing with real-time updates`);
        console.log(`🔧 Orbis corrections: client_name="N/A", instrument_name=NULL, instrument_code=NULL`);
        console.log(`🌟 Socket.IO enabled for real-time communication`);
    });
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n🔄 Shutting down gracefully...');
    
    if (pgPool) {
        await pgPool.end();
        console.log('✅ PostgreSQL pool closed');
    }
    
    if (mongoClient) {
        await mongoClient.close();
        console.log('✅ MongoDB connection closed');
    }
    
    process.exit(0);
});

startServer().catch(console.error); 

startServer().catch(console.error); 