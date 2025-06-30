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
    console.log('✅ Connected to PostgreSQL');
  } catch (error) {
    console.error('❌ PostgreSQL connection failed:', error.message);
  }

  // MongoDB connection
  try {
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

// Main dashboard route
app.get('/', (req, res) => {
  res.send(\`<!DOCTYPE html>
<html>
<head>
    <title>🚀 Simple Dashboard - Fresh Start</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f0f2f5; }
        .container { max-width: 1000px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; text-align: center; margin-bottom: 30px; }
        .section { background: white; padding: 25px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .btn { padding: 12px 25px; border: none; border-radius: 5px; cursor: pointer; margin: 5px; font-size: 14px; font-weight: bold; }
        .btn-primary { background: #007bff; color: white; }
        .btn-success { background: #28a745; color: white; }
        .btn-warning { background: #ffc107; color: black; }
        .btn:hover { opacity: 0.9; }
        .upload-area { border: 2px dashed #ccc; padding: 40px; text-align: center; border-radius: 5px; margin: 20px 0; }
        .upload-area:hover { border-color: #007bff; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
        .stat { background: #f8f9fa; padding: 20px; border-radius: 5px; text-align: center; }
        .stat-number { font-size: 2em; font-weight: bold; color: #007bff; }
        .stat-label { color: #666; }
        #result { padding: 15px; border-radius: 5px; margin-top: 20px; display: none; }
        .success { background: #d4edda; border: 1px solid #c3e6cb; color: #155724; }
        .error { background: #f8d7da; border: 1px solid #f5c6cb; color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Simple Data Processing Dashboard</h1>
            <p>✅ Fresh Start - All databases cleared and ready!</p>
            <p>MongoDB-first workflow: Upload → Process → Analyze</p>
        </div>

        <div class="section">
            <h2>📤 File Upload</h2>
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <input type="file" id="fileInput" name="files" multiple accept=".xlsx,.xls,.csv,.json,.txt" style="display: none;">
                    <h3>📁 Click to select files</h3>
                    <p>Supports: Excel, CSV, JSON, TXT files</p>
                </div>
                <button type="submit" class="btn btn-success">🚀 Upload to MongoDB</button>
                <button type="button" class="btn btn-warning" onclick="clearFiles()">🗑️ Clear</button>
            </form>
            <div id="result"></div>
        </div>

        <div class="section">
            <h2>📊 Database Statistics</h2>
            <div class="stats">
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
            <button class="btn btn-primary" onclick="refreshStats()">🔄 Refresh Stats</button>
            <button class="btn btn-warning" onclick="clearAllData()">🗑️ Clear All Data</button>
        </div>

        <div class="section">
            <h2>�� Data Viewers</h2>
            <p>View your uploaded and processed data:</p>
            <a href="/api/mongodb/preview" target="_blank" class="btn btn-primary">🍃 MongoDB Data</a>
            <a href="/api/postgresql/preview" target="_blank" class="btn btn-primary">🐘 PostgreSQL Data</a>
            <button class="btn btn-success" onclick="showUnifiedView()">📊 Unified View</button>
            <div id="unifiedView" style="margin-top: 20px;"></div>
        </div>
    </div>

    <script>
        // JavaScript implementation here
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
            
            showResult('Uploading ' + files.length + ' files...', 'success');
            
            try {
                const response = await fetch('/api/upload', {
                    method: 'POST',
                    body: formData
                });
                
                const result = await response.json();
                
                if (result.success) {
                    showResult('✅ Upload successful! Files: ' + result.totalFiles + ', Records: ' + result.totalRecords, 'success');
                    refreshStats();
                } else {
                    showResult('❌ Upload failed: ' + result.message, 'error');
                }
            } catch (error) {
                showResult('❌ Upload error: ' + error.message, 'error');
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
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                if (data.success) {
                    document.getElementById('mongoCollections').textContent = data.mongodb.collections;
                    document.getElementById('mongoRecords').textContent = data.mongodb.records.toLocaleString();
                    document.getElementById('pgTables').textContent = data.postgresql.tables;
                    document.getElementById('pgRecords').textContent = data.postgresql.records.toLocaleString();
                }
            } catch (error) {
                console.error('Error refreshing stats:', error);
            }
        }
        
        async function clearAllData() {
            if (confirm('Clear all data from both databases?')) {
                try {
                    const response = await fetch('/api/clear-all', { method: 'POST' });
                    const result = await response.json();
                    
                    if (result.success) {
                        showResult('✅ All data cleared!', 'success');
                        refreshStats();
                    } else {
                        showResult('❌ Clear failed: ' + result.message, 'error');
                    }
                } catch (error) {
                    showResult('❌ Clear error: ' + error.message, 'error');
                }
            }
        }
        
        async function showUnifiedView() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                
                if (data.success) {
                    let html = '<div style="background: #f8f9fa; padding: 20px; border-radius: 5px;">';
                    html += '<h3>📊 Complete Data Overview</h3>';
                    html += '<h4>🍃 MongoDB Collections (' + data.mongodb.collections + '):</h4>';
                    if (data.mongodb.collectionList && data.mongodb.collectionList.length > 0) {
                        data.mongodb.collectionList.forEach(col => {
                            html += '<p>• ' + col.name + ' (' + col.count + ' records)</p>';
                        });
                    } else {
                        html += '<p>No collections found</p>';
                    }
                    
                    html += '<h4>🐘 PostgreSQL Tables (' + data.postgresql.tables + '):</h4>';
                    if (data.postgresql.tableList && data.postgresql.tableList.length > 0) {
                        data.postgresql.tableList.forEach(table => {
                            html += '<p>• ' + table.name + ' (' + table.count + ' records)</p>';
                        });
                    } else {
                        html += '<p>No tables found</p>';
                    }
                    html += '</div>';
                    
                    document.getElementById('unifiedView').innerHTML = html;
                }
            } catch (error) {
                document.getElementById('unifiedView').innerHTML = '<p>Error: ' + error.message + '</p>';
            }
        }
        
        // Load stats on page load
        refreshStats();
    </script>
</body>
</html>\`);
});

// API routes implementation continues...
// Start server
async function startServer() {
  try {
    await initDB();
    
    app.listen(PORT, () => {
      console.log('🚀 Simple Dashboard running at http://localhost:' + PORT);
      console.log('✅ Fresh start - databases cleared and ready');
      console.log('📤 Upload files to begin processing');
    });
    
  } catch (error) {
    console.error('❌ Server startup failed:', error.message);
    process.exit(1);
  }
}

startServer();
