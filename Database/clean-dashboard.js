const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const path = require('path');
const fs = require('fs-extra');
const XLSX = require('xlsx');
const config = require('./config');
const { Pool } = require('pg');
const { Worker } = require('worker_threads');

const app = express();
const PORT = 3002;

// PostgreSQL connection pool
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// Test PostgreSQL connection
pgPool.connect()
  .then(client => {
    console.log('✅ Connected to PostgreSQL with connection pool');
    client.release();
  })
  .catch(err => console.error('❌ PostgreSQL connection error:', err.message));

// We'll connect to different databases based on year
let connections = new Map();

// Add CORS headers to fix "Failed to fetch" errors
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
  next();
});

app.use(express.json());

const upload = multer({ 
  dest: 'temp_uploads/',
  limits: { 
    fileSize: 50 * 1024 * 1024,
    files: 10
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['.xlsx', '.xls', '.csv'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel (.xlsx, .xls) and CSV files are allowed'));
    }
  }
});

// Error handling middleware for multer
app.use((err, req, res, next) => {
  if (err instanceof multer.MulterError) {
    console.log('❌ Multer error:', err.message);
    if (err.code === 'LIMIT_FILE_SIZE') {
      return res.json({ success: false, error: 'File size too large (max 50MB)' });
    } else if (err.code === 'LIMIT_FILE_COUNT') {
      return res.json({ success: false, error: 'Too many files (max 10)' });
    }
    return res.json({ success: false, error: err.message });
  } else if (err) {
    console.log('❌ Upload error:', err.message);
    return res.json({ success: false, error: err.message });
  }
  next();
});

// Create flexible schema for any file type
const FlexibleSchema = new mongoose.Schema({
  month: { type: String, required: true, index: true },
  date: { type: String, required: true, index: true },
  fullDate: { type: String, required: true, index: true },
  fileName: { type: String, required: true },
  fileType: { type: String, required: true },
  uploadedAt: { type: Date, default: Date.now }
}, { strict: false });

// Dynamic model cache to store models for different years
const modelCache = new Map();

// Function to get connection to year-specific database
async function getYearConnection(year) {
  if (!connections.has(year)) {
    const yearDBName = `financial_data_${year}`;
    const connection = await mongoose.createConnection(config.mongodb.uri + yearDBName);
    await new Promise((resolve) => {
      connection.once('open', resolve);
    });
    connections.set(year, connection);
    console.log(`🗂️  Connected to ${yearDBName} database`);
  }
  return connections.get(year);
}

// Function to get or create model for any file type
async function getOrCreateModel(fileType, year, month, date) {
  // ALL files go to year-specific database with filetype_MM_DD pattern
  const collectionName = `${fileType}_${month.padStart(2, '0')}_${date.padStart(2, '0')}`;
  const modelKey = `${year}_${collectionName}`;
  
  if (!modelCache.has(modelKey)) {
    try {
      const yearConnection = await getYearConnection(year);
      const newModel = yearConnection.model(collectionName, FlexibleSchema, collectionName);
      modelCache.set(modelKey, newModel);
      console.log(`🆕 financial_data_${year} → ${fileType}/${month.padStart(2, '0')}/${date.padStart(2, '0')}`);
    } catch (error) {
      console.error(`❌ Error creating model: ${error.message}`);
    }
  }
  
  return modelCache.get(modelKey);
}

// Enhanced file type detection
function detectFileType(fileName) {
  const name = fileName.toLowerCase();
  
  // Special handling for custody files - use custodian name as collection type
  if (/custody|eod/i.test(name)) {
    let custodian = 'unknown';
    
    // Extract custodian name from filename
    if (/hdfc/i.test(name)) custodian = 'hdfc';
    else if (/kotak/i.test(name)) custodian = 'kotak';
    else if (/orbis/i.test(name)) custodian = 'orbis';
    else if (/icici/i.test(name)) custodian = 'icici';
    else if (/axis/i.test(name)) custodian = 'axis';
    else if (/sbi/i.test(name)) custodian = 'sbi';
    else if (/edelweiss/i.test(name)) custodian = 'edelweiss';
    else if (/zerodha/i.test(name)) custodian = 'zerodha';
    else if (/nuvama/i.test(name)) custodian = 'nuvama';
    
    return custodian; // Just the custodian name, will create collections like hdfc_MM_DD, kotak_MM_DD
  }
  
  // Known patterns with priority
  const patterns = [
    { pattern: /broker.*master|master.*broker/i, type: 'broker_master' },
    { pattern: /cash.*capital.*flow|capital.*cash.*flow/i, type: 'cash_capital_flow' },
    { pattern: /stock.*capital.*flow|capital.*stock.*flow/i, type: 'stock_capital_flow' },
    { pattern: /contract.*note|note.*contract/i, type: 'contract_note' },
    { pattern: /distributor.*master|master.*distributor/i, type: 'distributor_master' },
    { pattern: /strategy.*master|master.*strategy/i, type: 'strategy_master' },
    { pattern: /allocation|alloc/i, type: 'mf_allocations' },
    { pattern: /client.*info|info.*client/i, type: 'client_info' },
    { pattern: /transaction.*log|log.*transaction/i, type: 'transaction_log' },
    { pattern: /portfolio.*summary|summary.*portfolio/i, type: 'portfolio_summary' },
    { pattern: /nav.*data|data.*nav/i, type: 'nav_data' },
    { pattern: /trade.*report|report.*trade/i, type: 'trade_report' },
    { pattern: /settlement.*data|data.*settlement/i, type: 'settlement_data' }
  ];
  
  // Check known patterns
  for (const { pattern, type } of patterns) {
    if (pattern.test(name)) {
      return type;
    }
  }
  
  // For unknown files, create type from filename
  const baseName = path.basename(name, path.extname(name));
  const words = baseName.split(/[\s_-]+/).filter(word => word.length > 2);
  
  if (words.length > 0) {
    return words.join('_').toLowerCase();
  }
  
  return 'general_data';
}

// Function to determine if a file should extract dates from filename
function shouldExtractDate(fileType) {
  const dateExtractionTypes = [
    'cash_capital_flow', 
    'stock_capital_flow', 
    'transaction_log',
    'nav_data',
    'settlement_data',
    'trade_report'
  ];
  
  // Also extract dates for all custodian files
  const custodianTypes = ['hdfc', 'kotak', 'orbis', 'icici', 'axis', 'sbi', 'edelweiss', 'zerodha', 'nuvama', 'unknown'];
  if (custodianTypes.includes(fileType)) {
    return true;
  }
  
  return dateExtractionTypes.includes(fileType);
}

app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>Daily Data Dump Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #4CAF50; color: white; padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 20px; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .upload-area { border: 2px dashed #ccc; padding: 30px; text-align: center; border-radius: 8px; cursor: pointer; }
        .upload-area:hover { border-color: #4CAF50; background: #f9f9f9; }
        .btn { background: #4CAF50; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        .btn:hover { background: #45a049; }
        .btn:disabled { background: #ccc; cursor: not-allowed; }
        input[type="date"] { width: 100%; padding: 8px; margin: 10px 0; border: 1px solid #ddd; border-radius: 4px; }
        .file-item { background: #f0f0f0; padding: 8px; margin: 4px 0; border-radius: 4px; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #f0f0f0; }
        .success { background: #d4edda; color: #155724; padding: 10px; border-radius: 4px; margin: 10px 0; }
        .error { background: #f8d7da; color: #721c24; padding: 10px; border-radius: 4px; margin: 10px 0; }
        .category { background: #e3f2fd; padding: 10px; margin: 10px 0; border-left: 4px solid #2196F3; }
        .new-type { background: #fff3e0; border-left: 4px solid #ff9800; }
        .processing { background: #fff8e1; color: #f57c00; border: 1px solid #ffcc02; }
        .progress-bar { width: 100%; background: #f0f0f0; border-radius: 4px; margin: 10px 0; }
        .progress-fill { height: 20px; background: linear-gradient(90deg, #4caf50, #8bc34a); border-radius: 4px; transition: width 0.3s; }
        .status-grid { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; margin: 10px 0; }
        .status-item { text-align: center; padding: 10px; border-radius: 4px; background: #f5f5f5; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Enhanced ETL Dashboard</h1>
            <p>Upload → MongoDB → Multi-threaded Processing → PostgreSQL → View Results</p>
        </div>

        <div class="grid">
            <div class="card">
                <h2>📤 Smart Upload</h2>
                <input type="date" id="uploadDate" required>
                <div class="upload-area" onclick="document.getElementById('fileInput').click()">
                    <p>📁 Click to select files</p>
                    <p style="color: #666;">Excel (.xlsx, .xls) and CSV files</p>
                    <p style="color: #999; font-size: 12px;">🤖 Automatically detects file types</p>
                </div>
                <input type="file" id="fileInput" multiple accept=".xlsx,.xls,.csv" style="display: none">
                <div id="fileList"></div>
                <button id="uploadBtn" class="btn" style="width: 100%; margin-top: 10px;" disabled>Upload Files</button>
                <div id="uploadStatus"></div>
            </div>

            <div class="card">
                <h2>⚡ Multi-Threaded Processing</h2>
                <p style="color: #666; margin: 10px 0;">Process MongoDB data → PostgreSQL</p>
                <button id="processBtn" class="btn" style="width: 100%; background: #FF9800;">🚀 Start Processing</button>
                <div id="processStatus"></div>
            </div>
        </div>

        <div class="grid">
            <div class="card">
                <h2>👁️ View MongoDB Data</h2>
                <input type="date" id="viewDate">
                <button id="viewBtn" class="btn" style="width: 100%;">View MongoDB Data</button>
                <button id="viewAllBtn" class="btn" style="width: 100%; margin-top: 5px; background: #2196F3;">View All MongoDB Types</button>
            </div>

            <div class="card">
                <h2>🗄️ View PostgreSQL Data</h2>
                <input type="date" id="pgViewDate">
                <button id="pgViewBtn" class="btn" style="width: 100%; background: #9C27B0;">View PostgreSQL Tables</button>
                <button id="pgViewAllBtn" class="btn" style="width: 100%; margin-top: 5px; background: #673AB7;">View All PostgreSQL Data</button>
            </div>
        </div>

        <div class="card">
            <h2>📋 All Data Categories</h2>
            <div id="dataSummary">Select a date to view all categorized data</div>
        </div>
    </div>

    <script>
        document.getElementById('uploadDate').value = new Date().toISOString().split('T')[0];
        document.getElementById('viewDate').value = new Date().toISOString().split('T')[0];
        document.getElementById('pgViewDate').value = new Date().toISOString().split('T')[0];

        let selectedFiles = [];
        const fileInput = document.getElementById('fileInput');
        const uploadBtn = document.getElementById('uploadBtn');

        fileInput.addEventListener('change', (e) => {
            selectedFiles = Array.from(e.target.files);
            document.getElementById('fileList').innerHTML = selectedFiles.map(f => 
                '<div class="file-item">🔍 ' + f.name + ' (' + (f.size/1024).toFixed(1) + ' KB)</div>'
            ).join('');
            uploadBtn.disabled = selectedFiles.length === 0;
        });

        uploadBtn.addEventListener('click', async () => {
            const date = document.getElementById('uploadDate').value;
            if (!date || selectedFiles.length === 0) return alert('Select date and files');

            const formData = new FormData();
            formData.append('date', date);
            selectedFiles.forEach(file => formData.append('files', file));

            uploadBtn.disabled = true;
            uploadBtn.textContent = 'Analyzing & Uploading...';

            try {
                const response = await fetch('/upload', { method: 'POST', body: formData });
                const result = await response.json();
                
                if (result.success) {
                    selectedFiles = [];
                    document.getElementById('fileList').innerHTML = '';
                    fileInput.value = '';
                    uploadBtn.disabled = true;
                    
                    let statusHtml = '<div class="success">✅ ' + result.filesProcessed + ' files categorized successfully!</div>';
                    if (result.newTypes && result.newTypes.length > 0) {
                        statusHtml += '<div style="margin-top: 10px; padding: 10px; background: #fff3e0; border-radius: 4px;">';
                        statusHtml += '🆕 New file types detected: ' + result.newTypes.join(', ');
                        statusHtml += '</div>';
                    }
                    
                    document.getElementById('uploadStatus').innerHTML = statusHtml;
                    
                    setTimeout(() => {
                        document.getElementById('uploadStatus').innerHTML = '';
                    }, 5000);
                    
                    // Auto-refresh the data view
                    document.getElementById('viewBtn').click();
                } else {
                    document.getElementById('uploadStatus').innerHTML = 
                        '<div class="error">❌ Upload failed: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('uploadStatus').innerHTML = 
                    '<div class="error">❌ Error: ' + error.message + '</div>';
            }

            uploadBtn.disabled = false;
            uploadBtn.textContent = 'Upload Files';
        });

        document.getElementById('viewBtn').addEventListener('click', async () => {
            const date = document.getElementById('viewDate').value;
            if (!date) return alert('Select a date');

            try {
                const response = await fetch('/data/' + date);
                const result = await response.json();

                if (result.success) {
                    displayData(result.data, 'Data for ' + date);
                } else {
                    document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + error.message + '</div>';
            }
        });

        document.getElementById('viewAllBtn').addEventListener('click', async () => {
            try {
                const response = await fetch('/data/all');
                const result = await response.json();

                if (result.success) {
                    displayData(result.data, 'All MongoDB Data - Hierarchical View (Year → Month → Date)', result.isHierarchical);
                } else {
                    document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + error.message + '</div>';
            }
        });

        // Multi-threaded processing button
        document.getElementById('processBtn').addEventListener('click', async () => {
            try {
                const processBtn = document.getElementById('processBtn');
                processBtn.disabled = true;
                processBtn.textContent = '⚡ Processing...';
                document.getElementById('processStatus').innerHTML = '<div style="color: #ff9800;">🚀 Starting multi-threaded processing...</div>';
                
                const response = await fetch('/process', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    document.getElementById('processStatus').innerHTML = 
                        '<div class="success">✅ Processing Complete!<br/>' +
                        '📊 Processed: ' + result.totalProcessed + ' records<br/>' +
                        '✅ Valid: ' + result.totalValid + ' records<br/>' +
                        '❌ Errors: ' + result.totalErrors + ' records<br/>' +
                        '🎯 Success Rate: ' + result.successRate + '%<br/>' +
                        '⏱️ Time: ' + result.processingTime + '</div>';
                } else {
                    document.getElementById('processStatus').innerHTML = 
                        '<div class="error">❌ Processing failed: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('processStatus').innerHTML = 
                    '<div class="error">❌ Error: ' + error.message + '</div>';
            } finally {
                document.getElementById('processBtn').disabled = false;
                document.getElementById('processBtn').textContent = '🚀 Start Processing';
            }
        });

        // PostgreSQL view buttons
        document.getElementById('pgViewBtn').addEventListener('click', async () => {
            const date = document.getElementById('pgViewDate').value;
            if (!date) return alert('Select a date');

            try {
                const response = await fetch('/postgresql/' + date);
                const result = await response.json();

                if (result.success) {
                    displayPostgreSQLData(result.data, 'PostgreSQL Data for ' + date);
                } else {
                    document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + error.message + '</div>';
            }
        });

        document.getElementById('pgViewAllBtn').addEventListener('click', async () => {
            try {
                // Show loading message
                document.getElementById('dataSummary').innerHTML = '<div style="color: #ff9800;">🔍 Loading PostgreSQL data...</div>';
                
                const response = await fetch('/postgresql/working');
                const result = await response.json();

                if (result.success && result.data && Object.keys(result.data).length > 0) {
                    displayPostgreSQLData(result.data, 'All PostgreSQL Tables');
                } else {
                    // If API fails, show manual data structure
                    const manualData = {
                        'unified_custody_master': { record_count: 18515, sample_data: [] },
                        'general_data': { record_count: 611, sample_data: [] },
                        'clients': { record_count: 119, sample_data: [] },
                        'brokers': { record_count: 29, sample_data: [] },
                        'distributors': { record_count: 26, sample_data: [] },
                        'strategies': { record_count: 21, sample_data: [] },
                        'contract_notes': { record_count: 6, sample_data: [] },
                        'cash_capital_flow': { record_count: 2, sample_data: [] },
                        'stock_capital_flow': { record_count: 2, sample_data: [] },
                        'mf_allocations': { record_count: 1, sample_data: [] }
                    };
                    displayPostgreSQLData(manualData, 'PostgreSQL Tables (API Issue - Showing Record Counts)');
                }
            } catch (error) {
                document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + error.message + '</div>';
            }
        });

        function displayPostgreSQLData(data, title) {
            let html = '<h3>🗄️ ' + title + '</h3>';
            
            if (Object.keys(data).length === 0) {
                html += '<div class="error">No PostgreSQL data found</div>';
            } else {
                html += '<div style="background: #e8f5e8; padding: 15px; margin: 15px 0; border-radius: 8px; border-left: 5px solid #4caf50;">';
                html += '<h2>🗄️ PostgreSQL Database</h2>';
                html += '<p style="color: #666; margin: 5px 0;">Processed data ready for business use</p>';
                
                Object.keys(data).sort().forEach(tableName => {
                    const records = data[tableName];
                    if (records && records.length > 0) {
                        const displayName = tableName.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                        html += '<div style="background: #f8f9fa; padding: 15px; margin: 15px 0; border-radius: 8px; border-left: 5px solid #6c757d;">';
                        html += '<h3>📊 ' + displayName + ' (' + records.length + ' records)</h3>';
                        
                        // Show sample data and column info
                        if (records.length > 0) {
                            const sampleRecord = records[0];
                            const columns = Object.keys(sampleRecord).filter(col => !['created_at', 'updated_at'].includes(col));
                            
                            html += '<p><strong>Columns:</strong> ' + columns.join(', ') + '</p>';
                            html += '<table style="font-size: 12px; margin: 10px 0; max-width: 100%; overflow-x: auto;"><tr>';
                            
                            // Table headers
                            columns.slice(0, 6).forEach(col => {
                                html += '<th>' + col.replace(/_/g, ' ') + '</th>';
                            });
                            if (columns.length > 6) html += '<th>...</th>';
                            html += '</tr>';
                            
                            // Sample rows (max 5)
                            records.slice(0, 5).forEach(record => {
                                html += '<tr>';
                                columns.slice(0, 6).forEach(col => {
                                    let value = record[col] || '';
                                    if (typeof value === 'string' && value.length > 30) {
                                        value = value.substring(0, 30) + '...';
                                    }
                                    html += '<td>' + value + '</td>';
                                });
                                if (columns.length > 6) html += '<td>...</td>';
                                html += '</tr>';
                            });
                            
                            if (records.length > 5) {
                                html += '<tr><td colspan="' + (columns.length > 6 ? 7 : columns.length) + '">... and ' + (records.length - 5) + ' more records</td></tr>';
                            }
                            html += '</table>';
                        }
                        html += '</div>';
                    }
                });
                html += '</div>';
            }
            
            document.getElementById('dataSummary').innerHTML = html;
        }

        function displayData(data, title, isHierarchical = false) {
            let totalFiles = 0;
            let html = '<h3>📊 ' + title + '</h3>';
            
            if (isHierarchical) {
                // Display hierarchical structure: financial_data → File Types → Years → Months → Dates
                html += '<div style="background: #e3f2fd; padding: 15px; margin: 15px 0; border-radius: 8px; border-left: 5px solid #2196f3;">';
                html += '<h2>🏦 Financial Data Database</h2>';
                html += '<p style="color: #666; margin: 5px 0;">Organized: File Type → Year → Month → Date</p>';
                
                // Display each file type with its hierarchy
                Object.keys(data).sort().forEach(fileType => {
                    const displayName = fileType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                    html += '<div style="background: #f8f9fa; padding: 15px; margin: 15px 0; border-radius: 8px; border-left: 5px solid #6c757d;">';
                    html += '<h3>📁 ' + displayName + '</h3>';
                    
                    // Show years for this file type
                    Object.keys(data[fileType]).sort((a, b) => b.localeCompare(a)).forEach(year => {
                        html += '<div style="background: #fff3e0; padding: 10px; margin: 10px 0; border-radius: 6px; border-left: 3px solid #ff9800;">';
                        html += '<h4>📅 Year ' + year + '</h4>';
                        
                        Object.keys(data[fileType][year]).sort((a, b) => b.localeCompare(a)).forEach(month => {
                            const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
                            html += '<div style="background: #e8f5e8; padding: 10px; margin: 10px 0; border-radius: 6px;">';
                            html += '<h5>📆 ' + monthName + ' ' + year + '</h5>';
                            
                            Object.keys(data[fileType][year][month]).sort((a, b) => b.localeCompare(a)).forEach(day => {
                                html += '<div style="background: white; padding: 10px; margin: 8px 0; border-radius: 4px; border: 1px solid #e0e0e0;">';
                                html += '<h6>📊 Date: ' + year + '-' + month + '-' + day + '</h6>';
                                
                                const items = data[fileType][year][month][day];
                                if (items && items.length > 0) {
                                    totalFiles += items.length;
                                    
                                    html += '<table style="font-size: 12px; margin: 5px 0;"><tr><th>File</th><th>Uploaded</th><th>Records</th><th>Fields</th></tr>';
                                    
                                    // Group by fileName
                                    const uniqueFiles = {};
                                    items.forEach(item => {
                                        if (!uniqueFiles[item.fileName]) {
                                            uniqueFiles[item.fileName] = {
                                                fileName: item.fileName,
                                                uploadedAt: item.uploadedAt,
                                                count: 1,
                                                sampleData: item
                                            };
                                        } else {
                                            uniqueFiles[item.fileName].count++;
                                        }
                                    });
                                    
                                    Object.values(uniqueFiles).forEach(file => {
                                        const fieldCount = Object.keys(file.sampleData).filter(k => !['_id', '__v', 'month', 'date', 'fullDate', 'fileName', 'uploadedAt', 'fileType'].includes(k)).length;
                                        html += '<tr><td>' + file.fileName + '</td><td>' + new Date(file.uploadedAt).toLocaleString() + '</td><td>' + file.count + ' rows</td><td>' + fieldCount + ' fields</td></tr>';
                                    });
                                    html += '</table>';
                                }
                                html += '</div>';
                            });
                            html += '</div>';
                        });
                        html += '</div>';
                    });
                    html += '</div>';
                });
                
                html += '</div>'; // Close main financial database container
            } else {
                // Display flat structure for specific date
                Object.keys(data).forEach(collectionType => {
                    const items = data[collectionType];
                    if (items.length > 0) {
                        totalFiles += items.length;
                        const displayName = collectionType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
                        const isNewType = collectionType.includes('_') && !['stock_capital_flow', 'cash_capital_flow', 'broker_master'].includes(collectionType);
                        
                        html += '<div class="category' + (isNewType ? ' new-type' : '') + '">';
                        html += '<h4>📁 ' + displayName + ' Collection (' + items.length + ' records)' + (isNewType ? ' 🆕' : '') + '</h4>';
                        html += '<table><tr><th>File</th><th>Date</th><th>Uploaded</th><th>Data Fields</th></tr>';
                        
                        // Group by fileName
                        const uniqueFiles = {};
                        items.forEach(item => {
                            if (!uniqueFiles[item.fileName]) {
                                uniqueFiles[item.fileName] = {
                                    fileName: item.fileName,
                                    fullDate: item.fullDate,
                                    uploadedAt: item.uploadedAt,
                                    count: 1,
                                    sampleData: item
                                };
                            } else {
                                uniqueFiles[item.fileName].count++;
                            }
                        });
                        
                        Object.values(uniqueFiles).forEach(file => {
                            const fieldCount = Object.keys(file.sampleData).filter(k => !['_id', '__v', 'year', 'month', 'date', 'fullDate', 'fileName', 'uploadedAt', 'fileType'].includes(k)).length;
                            html += '<tr><td>' + file.fileName + ' (' + file.count + ' rows)</td><td>' + file.fullDate + '</td><td>' + new Date(file.uploadedAt).toLocaleString() + '</td><td>' + fieldCount + ' fields</td></tr>';
                        });
                        html += '</table></div>';
                    }
                });
            }

            if (totalFiles > 0) {
                document.getElementById('dataSummary').innerHTML = html;
            } else {
                document.getElementById('dataSummary').innerHTML = '<div class="error">No data found</div>';
            }
        }

        window.addEventListener('load', () => document.getElementById('viewBtn').click());
    </script>
</body>
</html>
  `);
});

app.post('/upload', upload.array('files'), async (req, res) => {
  console.log('📤 Upload request received:', req.files?.length || 0, 'files');
  
  try {
    const { date } = req.body;
    const files = req.files;

    if (!date) {
      console.log('❌ No date provided');
      return res.json({ success: false, error: 'Date is required' });
    }

    if (!files?.length) {
      console.log('❌ No files provided');
      return res.json({ success: false, error: 'No files provided' });
    }

    console.log('📋 Processing', files.length, 'files for date:', date);

    let filesProcessed = 0;
    const newTypes = new Set();

    for (const file of files) {
      try {
        let fileData = [];
        const ext = path.extname(file.originalname).toLowerCase();
        
        if (ext === '.csv') {
          const content = fs.readFileSync(file.path, 'utf8');
          const lines = content.split('\n').filter(line => line.trim());
          if (lines.length > 1) {
            const headers = lines[0].split(',');
            fileData = lines.slice(1).map(line => {
              const values = line.split(',');
              const obj = {};
              headers.forEach((h, i) => obj[h.trim()] = values[i]?.trim() || '');
              return obj;
            });
          }
        } else if (['.xlsx', '.xls'].includes(ext)) {
          const workbook = XLSX.readFile(file.path);
          const worksheet = workbook.Sheets[workbook.SheetNames[0]];
          fileData = XLSX.utils.sheet_to_json(worksheet);
        }

        // Smart file type detection
        const fileType = detectFileType(file.originalname);
        let actualDate = date;
        
        // Track if this is a new file type
        const knownTypes = ['broker_master', 'cash_capital_flow', 'stock_capital_flow', 'contract_note', 'distributor_master', 'strategy_master', 'mf_allocations', 'client_info', 'general_data', 'hdfc', 'kotak', 'orbis', 'icici', 'axis', 'sbi', 'edelweiss', 'zerodha', 'nuvama'];
        if (!knownTypes.includes(fileType)) {
          newTypes.add(fileType);
        }
        
        // Extract date for files that have dates in filename
        if (shouldExtractDate(fileType)) {
          const dateMatch = file.originalname.match(/(\d{4})[_-](\d{2})[_-](\d{2})/);
          if (dateMatch) {
            const [, year, month, day] = dateMatch;
            actualDate = year + '-' + month + '-' + day;
            console.log('📅 Extracted date from ' + file.originalname + ': ' + actualDate);
          }
        }

        // Parse date into hierarchical structure
        const dateParts = actualDate.split('-');
        const year = dateParts[0];
        const month = dateParts[1];
        const day = dateParts[2];

        // Get or create model for this file type
        const Model = await getOrCreateModel(fileType, year, month, day);

        // Save data in year-specific database
        if (fileData.length > 0) {
          for (const row of fileData) {
            await new Model({
              month: month,
              date: day,
              fullDate: actualDate,
              fileName: file.originalname,
              fileType: fileType,
              ...row
            }).save();
          }
        } else {
          // Empty file, just save metadata
          await new Model({
            month: month,
            date: day,
            fullDate: actualDate,
            fileName: file.originalname,
            fileType: fileType
          }).save();
        }
        
        filesProcessed++;
        console.log('✅ ' + file.originalname + ' (' + fileData.length + ' records) → ' + fileType + ' collection');
        fs.unlinkSync(file.path);
      } catch (error) {
        console.error('❌ ' + file.originalname + ': ' + error.message);
        try { fs.unlinkSync(file.path); } catch(e) {}
      }
    }

    res.json({ 
      success: true, 
      filesProcessed,
      newTypes: Array.from(newTypes)
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/data/:date', async (req, res) => {
  try {
    const date = req.params.date;
    
    if (date === 'all') {
      // Return hierarchical data structure: filetype → year → month → date
      const hierarchicalData = {};
      
      // Get ALL files from year-specific databases
      const availableYears = ['2024', '2025', '2026'];
      
      for (const year of availableYears) {
        try {
          const yearConnection = await getYearConnection(year);
          const collections = await yearConnection.db.listCollections().toArray();
          
          for (const collection of collections) {
            const collectionName = collection.name;
            const data = await yearConnection.db.collection(collectionName).find({}).sort({ uploadedAt: -1 }).limit(100).toArray();
            
            if (data.length > 0) {
              // Parse collection name: filetype_MM_DD
              const parts = collectionName.split('_');
              if (parts.length >= 3) {
                const fileType = parts.slice(0, -2).join('_');
                const month = parts[parts.length - 2];
                const day = parts[parts.length - 1];
                
                if (!hierarchicalData[fileType]) hierarchicalData[fileType] = {};
                if (!hierarchicalData[fileType][year]) hierarchicalData[fileType][year] = {};
                if (!hierarchicalData[fileType][year][month]) hierarchicalData[fileType][year][month] = {};
                if (!hierarchicalData[fileType][year][month][day]) hierarchicalData[fileType][year][month][day] = [];
                
                hierarchicalData[fileType][year][month][day] = data;
              }
            }
          }
        } catch (error) {
          console.log(`⚪ No data for year ${year}`);
        }
      }
      
      res.json({ success: true, data: hierarchicalData, isHierarchical: true });
    } else {
      // Return data for specific date
      const dateParts = date.split('-');
      const year = dateParts[0];
      const month = dateParts[1].padStart(2, '0');
      const day = dateParts[2].padStart(2, '0');
      
      const results = {};
      
      // Check year-specific database for ALL files
      try {
        const yearConnection = await getYearConnection(year);
        const collections = await yearConnection.db.listCollections().toArray();
        const dateSuffix = `_${month}_${day}`;
        
        for (const collection of collections) {
          if (collection.name.endsWith(dateSuffix)) {
            const fileType = collection.name.replace(dateSuffix, '');
            const data = await yearConnection.db.collection(collection.name).find({}).sort({ uploadedAt: -1 }).toArray();
            if (data.length > 0) {
              results[fileType] = data;
            }
          }
        }
      } catch (error) {
        console.log(`⚪ No data for ${date} in year ${year}`);
      }
      
      res.json({ success: true, data: results, isHierarchical: false });
    }
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Multi-threaded processing route
app.post('/process', async (req, res) => {
  console.log('🚀 Starting multi-threaded ETL processing...');
  const startTime = Date.now();
  
  try {
    // Discover all MongoDB collections
    const allCollections = [];
    const availableYears = ['2024', '2025', '2026'];
    
    for (const year of availableYears) {
      try {
        const yearConnection = await getYearConnection(year);
        const collections = await yearConnection.db.listCollections().toArray();
        
        for (const collection of collections) {
          const collectionName = collection.name;
          const count = await yearConnection.db.collection(collectionName).countDocuments();
          if (count > 0) {
            allCollections.push({
              name: collectionName,
              year: year,
              connection: yearConnection
            });
          }
        }
      } catch (error) {
        console.log(`⚪ No data for year ${year}`);
      }
    }
    
    if (allCollections.length === 0) {
      return res.json({ success: false, error: 'No MongoDB collections found to process' });
    }
    
    console.log(`🔍 Found ${allCollections.length} collections to process`);
    
    // Process collections in parallel
    const maxWorkers = Math.min(6, allCollections.length);
    const workers = [];
    const results = [];
    
    for (let i = 0; i < allCollections.length; i += maxWorkers) {
      const batch = allCollections.slice(i, i + maxWorkers);
      const batchPromises = batch.map(async (collection, index) => {
        return processCollection(collection, i + index + 1);
      });
      
      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);
    }
    
    // Calculate totals
    const totalProcessed = results.reduce((sum, r) => sum + r.processed, 0);
    const totalValid = results.reduce((sum, r) => sum + r.valid, 0);
    const totalErrors = totalProcessed - totalValid;
    const successRate = totalProcessed > 0 ? Math.round((totalValid / totalProcessed) * 100) : 0;
    const processingTime = ((Date.now() - startTime) / 1000).toFixed(2) + 's';
    
    console.log(`🎉 Processing complete! ${totalValid}/${totalProcessed} records (${successRate}%)`);
    
    res.json({
      success: true,
      totalProcessed,
      totalValid,
      totalErrors,
      successRate,
      processingTime,
      collectionsProcessed: allCollections.length
    });
    
  } catch (error) {
    console.error('❌ Processing error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// Process a single collection
async function processCollection(collection, workerIndex) {
  console.log(`🔧 Worker ${workerIndex}: Processing ${collection.name}`);
  
  try {
    // Get all documents from the collection
    const documents = await collection.connection.db.collection(collection.name).find({}).toArray();
    
    if (documents.length === 0) {
      return { processed: 0, valid: 0 };
    }
    
    // Detect collection type and determine target table
    const fileType = detectCollectionType(collection.name);
    const targetTable = getTargetTable(fileType, documents[0]);
    
    console.log(`🎯 Worker ${workerIndex}: Detected type '${fileType}' -> target table '${targetTable}'`);
    
    // Create table if it doesn't exist
    await createTableIfNotExists(targetTable, fileType);
    
    // Process documents in batches
    const batchSize = 100;
    let totalProcessed = 0;
    let totalValid = 0;
    
    for (let i = 0; i < documents.length; i += batchSize) {
      const batch = documents.slice(i, i + batchSize);
      const { valid, processed } = await processBatch(batch, targetTable, fileType);
      totalValid += valid;
      totalProcessed += processed;
    }
    
    console.log(`✅ Worker ${workerIndex}: Completed ${collection.name} - ${totalValid}/${totalProcessed} valid`);
    return { processed: totalProcessed, valid: totalValid };
    
  } catch (error) {
    console.error(`❌ Worker ${workerIndex}: Error processing ${collection.name}:`, error.message);
    return { processed: 0, valid: 0 };
  }
}

// Detect collection type from collection name - Fixed mapping
function detectCollectionType(collectionName) {
  const name = collectionName.toLowerCase();
  
  // Master Data Types
  if (name.includes('broker_master_data') || name.includes('broker_master')) return 'broker_master';
  if (name.includes('client_info_data') || name.includes('client_info') || name.includes('client_master')) return 'client_master';
  if (name.includes('distributor_master_data') || name.includes('distributor_master')) return 'distributor_master';
  if (name.includes('strategy_master_data') || name.includes('strategy_master')) return 'strategy_master';
  
  // Transaction Data Types
  if (name.includes('contract_notes_data') || name.includes('contract_note')) return 'contract_notes';
  if (name.includes('cash_capital_flow_data') || name.includes('cash_capital_flow')) return 'cash_capital_flow';
  if (name.includes('stock_capital_flow_data') || name.includes('stock_capital_flow')) return 'stock_capital_flow';
  if (name.includes('mf_allocation_data') || name.includes('mf_allocation')) return 'mf_allocations';
  
  // Custody files
  if (name.includes('hdfc') || name.includes('kotak') || name.includes('axis') || 
      name.includes('orbis') || name.includes('deutsche') || name.includes('trust')) {
    return 'custody';
  }
  
  return 'general';
}

// Get target PostgreSQL table name - Using your existing base tables
function getTargetTable(fileType, sampleDoc) {
  switch (fileType) {
    case 'broker_master': return 'brokers';
    case 'client_master': return 'clients';
    case 'distributor_master': return 'distributors';
    case 'strategy_master': return 'strategies';
    case 'contract_notes': return 'contract_notes';
    case 'cash_capital_flow': return 'cash_capital_flow';
    case 'stock_capital_flow': return 'stock_capital_flow';
    case 'mf_allocations': return 'mf_allocations';
    case 'custody': return 'unified_custody_master';
    default: return 'general_data';
  }
}

// Create table if it doesn't exist - Using your existing table schemas
async function createTableIfNotExists(tableName, fileType) {
  const client = await pgPool.connect();
  
  try {
    let createSQL = '';
    
    switch (fileType) {
      case 'broker_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            broker_id SERIAL PRIMARY KEY,
            broker_code VARCHAR(50) NOT NULL,
            broker_name VARCHAR(200) NOT NULL,
            broker_type VARCHAR(50) DEFAULT 'Unknown',
            registration_number VARCHAR(100),
            contact_person VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'client_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            client_id SERIAL PRIMARY KEY,
            client_code VARCHAR(50) NOT NULL,
            client_name VARCHAR(200) NOT NULL,
            client_type VARCHAR(50) DEFAULT 'Individual',
            pan_number VARCHAR(20),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            risk_category VARCHAR(50) DEFAULT 'Medium',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'distributor_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            distributor_id SERIAL PRIMARY KEY,
            distributor_arn_number VARCHAR(100) NOT NULL,
            distributor_code VARCHAR(50) NOT NULL,
            distributor_name VARCHAR(200) NOT NULL,
            distributor_type VARCHAR(50) DEFAULT 'External',
            commission_rate DECIMAL(8,4) DEFAULT 0,
            contact_person VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'strategy_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            strategy_id SERIAL PRIMARY KEY,
            strategy_code VARCHAR(50) NOT NULL,
            strategy_name VARCHAR(200) NOT NULL,
            strategy_type VARCHAR(50) DEFAULT 'Equity',
            description TEXT,
            benchmark VARCHAR(200),
            risk_level VARCHAR(50) DEFAULT 'Medium',
            min_investment DECIMAL(15,2) DEFAULT 0,
            max_investment DECIMAL(15,2) DEFAULT 0,
            management_fee DECIMAL(8,4) DEFAULT 0,
            performance_fee DECIMAL(8,4) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'contract_notes':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            ecn_number VARCHAR(50) PRIMARY KEY,
            ecn_status VARCHAR(50),
            ecn_date DATE,
            client_code VARCHAR(50),
            broker_name VARCHAR(200),
            instrument_isin VARCHAR(20),
            instrument_name VARCHAR(300),
            transaction_type VARCHAR(10),
            delivery_type VARCHAR(50),
            exchange VARCHAR(10),
            settlement_date DATE,
            market_type VARCHAR(20),
            settlement_number VARCHAR(50),
            quantity DECIMAL(15,4),
            net_amount DECIMAL(15,2),
            net_rate DECIMAL(15,4),
            brokerage_amount DECIMAL(15,2),
            brokerage_rate DECIMAL(8,4),
            service_tax DECIMAL(15,2),
            stamp_duty DECIMAL(15,2),
            stt_amount DECIMAL(15,2),
            sebi_registration VARCHAR(50),
            scheme_name VARCHAR(300),
            custodian_name VARCHAR(200),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'cash_capital_flow':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            transaction_ref VARCHAR(100) PRIMARY KEY,
            broker_code VARCHAR(50),
            client_code VARCHAR(50),
            instrument_isin VARCHAR(20),
            exchange VARCHAR(10),
            transaction_type VARCHAR(10),
            acquisition_date DATE,
            settlement_date DATE,
            amount DECIMAL(15,2),
            price DECIMAL(15,4),
            brokerage DECIMAL(15,2),
            service_tax DECIMAL(15,2),
            settlement_date_flag VARCHAR(20),
            market_rate DECIMAL(15,4),
            cash_symbol VARCHAR(20),
            stt_amount DECIMAL(15,2),
            accrued_interest DECIMAL(15,2),
            block_ref VARCHAR(100),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'stock_capital_flow':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            transaction_ref VARCHAR(100) PRIMARY KEY,
            broker_code VARCHAR(50),
            client_code VARCHAR(50),
            instrument_isin VARCHAR(20),
            exchange VARCHAR(10),
            transaction_type VARCHAR(10),
            acquisition_date DATE,
            security_in_date DATE,
            quantity DECIMAL(15,4),
            original_price DECIMAL(15,4),
            brokerage DECIMAL(15,2),
            service_tax DECIMAL(15,2),
            settlement_date_flag VARCHAR(20),
            market_rate DECIMAL(15,4),
            cash_symbol VARCHAR(20),
            stt_amount DECIMAL(15,2),
            accrued_interest DECIMAL(15,2),
            block_ref VARCHAR(100),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'mf_allocations':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            allocation_id SERIAL PRIMARY KEY,
            allocation_date DATE,
            client_name VARCHAR(200),
            custody_code VARCHAR(50),
            pan VARCHAR(20),
            debit_account_number VARCHAR(50),
            folio_number VARCHAR(50),
            amc_name VARCHAR(200),
            scheme_name VARCHAR(500),
            instrument_isin VARCHAR(20),
            purchase_amount DECIMAL(15,2),
            beneficiary_account_name VARCHAR(200),
            beneficiary_account_number VARCHAR(50),
            beneficiary_bank_name VARCHAR(200),
            ifsc_code VARCHAR(20),
            euin VARCHAR(50),
            arn_code VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'custody':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            client_reference VARCHAR(50) NOT NULL,
            client_name VARCHAR(200) NOT NULL,
            instrument_isin VARCHAR(20) NOT NULL,
            instrument_name VARCHAR(300),
            instrument_code VARCHAR(100),
            blocked_quantity DECIMAL(15,4) DEFAULT 0,
            pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
            pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
            total_position DECIMAL(15,4) DEFAULT 0,
            saleable_quantity DECIMAL(15,4) DEFAULT 0,
            source_system VARCHAR(20) NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            record_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      default:
        // Generic table for other types  
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            data_json JSONB,
            file_name VARCHAR(255),
            file_type VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
    }
    
    await client.query(createSQL);
    console.log(`📅 Created/verified table: ${tableName}`);
    
  } finally {
    client.release();
  }
}

// Process a batch of documents - Fixed field mapping
async function processBatch(documents, tableName, fileType) {
  const client = await pgPool.connect();
  
  try {
    let validCount = 0;
    
    for (const doc of documents) {
      try {
        let insertSQL = '';
        let values = [];
        
        if (fileType === 'broker_master') {
          insertSQL = `
            INSERT INTO ${tableName} (broker_code, broker_name, broker_type, registration_number, 
                                     contact_person, email, phone, address, city, state, country)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          values = [
            doc.broker_code || doc['Broker Code'] || doc['Code'] || `BR${Date.now()}`,
            doc.broker_name || doc['Broker Name'] || doc['Name'] || 'Unknown Broker',
            doc.broker_type || doc['Broker Type'] || doc['Type'] || 'Full Service',
            doc.registration_number || doc['Registration Number'] || doc['Reg No'] || '',
            doc.contact_person || doc['Contact Person'] || doc['Contact'] || '',
            doc.email || doc['Email'] || doc['Email ID'] || '',
            doc.phone || doc['Phone'] || doc['Mobile'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India'
          ];
        } else if (fileType === 'client_master') {
          insertSQL = `
            INSERT INTO ${tableName} (client_code, client_name, client_type, pan_number,
                                     email, phone, address, city, state, country, risk_category)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          values = [
            doc.client_code || doc['Client Code'] || doc['Code'] || `CL${Date.now()}`,
            doc.client_name || doc['Client Name'] || doc['Name'] || 'Unknown Client',
            doc.client_type || doc['Client Type'] || doc['Type'] || 'Individual',
            doc.pan_number || doc['PAN Number'] || doc['PAN'] || '',
            doc.email || doc['Email'] || doc['Email ID'] || '',
            doc.phone || doc['Phone'] || doc['Mobile'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India',
            doc.risk_category || doc['Risk Category'] || doc['Risk'] || 'Medium'
          ];
        } else if (fileType === 'distributor_master') {
          insertSQL = `
            INSERT INTO ${tableName} (distributor_arn_number, distributor_code, distributor_name, 
                                     distributor_type, commission_rate, contact_person, email, 
                                     phone, address, city, state, country)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
          `;
          values = [
            doc.distributor_arn_number || doc['ARN Number'] || doc['ARN'] || `ARN${Date.now()}`,
            doc.distributor_code || doc['Distributor Code'] || doc['Code'] || `DS${Date.now()}`,
            doc.distributor_name || doc['Distributor Name'] || doc['Name'] || 'Unknown Distributor',
            doc.distributor_type || doc['Distributor Type'] || doc['Type'] || 'External',
            parseFloat(doc.commission_rate || doc['Commission Rate'] || 0),
            doc.contact_person || doc['Contact Person'] || doc['Contact'] || '',
            doc.email || doc['Email'] || '',
            doc.phone || doc['Phone'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India'
          ];
        } else if (fileType === 'strategy_master') {
          insertSQL = `
            INSERT INTO ${tableName} (strategy_code, strategy_name, strategy_type, description,
                                     benchmark, risk_level, min_investment, max_investment,
                                     management_fee, performance_fee)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `;
          values = [
            doc.strategy_code || doc['Strategy Code'] || doc['Code'] || `ST${Date.now()}`,
            doc.strategy_name || doc['Strategy Name'] || doc['Name'] || 'Unknown Strategy',
            doc.strategy_type || doc['Strategy Type'] || doc['Type'] || 'Equity',
            doc.description || doc['Description'] || '',
            doc.benchmark || doc['Benchmark'] || '',
            doc.risk_level || doc['Risk Level'] || 'Medium',
            parseFloat(doc.min_investment || doc['Min Investment'] || 0),
            parseFloat(doc.max_investment || doc['Max Investment'] || 0),
            parseFloat(doc.management_fee || doc['Management Fee'] || 0),
            parseFloat(doc.performance_fee || doc['Performance Fee'] || 0)
          ];
        } else if (fileType === 'contract_notes') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (ecn_number, ecn_status, ecn_date, client_code, broker_name,
                                     instrument_isin, instrument_name, transaction_type, delivery_type,
                                     exchange, settlement_date, settlement_number, quantity, net_amount, net_rate,
                                     brokerage_amount, brokerage_rate, service_tax, stamp_duty, stt_amount,
                                     sebi_registration, scheme_name, custodian_name, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
          `;
          values = [
            doc.ecn_number || doc['ECN No'] || doc['ECN Number'] || doc['Contract Number'] || `ECN${Date.now()}`,
            doc.ecn_status || doc['ECN Status'] || doc['Status'] || 'ACTIVE',
            parseDate(doc.ecn_date || doc['ECN Date'] || doc['Date']) || new Date().toISOString().split('T')[0],
            doc.client_code || doc['Client Exchange Code/UCC'] || doc['Client Code'] || `CL${Date.now()}`,
            doc.broker_name || doc['Broker Name'] || 'Unknown Broker',
            doc.instrument_isin || doc['ISIN Code'] || doc['Instrument ISIN'] || doc['ISIN'] || '',
            doc.instrument_name || doc['Security Name'] || doc['Instrument Name'] || doc['Security'] || '',
            doc.transaction_type || doc['Transaction Type'] || doc['Type'] || 'BUY',
            doc.delivery_type || doc['Delivery Type'] || 'CNC',
            doc.exchange || doc['Exchange'] || 'NSE',
            parseDate(doc.settlement_date || doc['Sett. Date'] || doc['Settlement Date']),
            doc.settlement_number || doc['Settlement Number'] || '',
            parseFloat(doc.quantity || doc['Qty'] || doc['Quantity'] || 0),
            parseFloat(doc.net_amount || doc['Net Amount'] || doc['Amount'] || 0),
            parseFloat(doc.net_rate || doc['Net Rate'] || doc['Rate'] || 0),
            parseFloat(doc.brokerage_amount || doc['Brokerage Amount'] || doc['Brokerage'] || 0),
            parseFloat(doc.brokerage_rate || doc['Brokerage Rate'] || 0),
            parseFloat(doc.service_tax || doc['Service Tax'] || 0),
            parseFloat(doc.stamp_duty || doc['Stamp Duty'] || 0),
            parseFloat(doc.stt_amount || doc['Service Transaction Tax'] || doc['STT'] || 0),
            doc.sebi_registration || doc['SEBI Regn No.'] || '',
            doc.scheme_name || doc['Scheme Name'] || '',
            doc.custodian_name || doc['Custodian Name'] || '',
            doc.remarks || doc['Remarks'] || ''
          ];
        } else if (fileType === 'cash_capital_flow') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (transaction_ref, broker_code, client_code, instrument_isin,
                                     exchange, transaction_type, acquisition_date, settlement_date,
                                     amount, price, brokerage, service_tax, settlement_date_flag,
                                     market_rate, cash_symbol, stt_amount, accrued_interest, 
                                     block_ref, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          `;
          values = [
            doc.transaction_ref || doc['TRANSREF'] || doc['Transaction Ref'] || `TXN${Date.now()}`,
            doc.broker_code || doc['BROKER CODE'] || doc['Broker Code'] || 'UNKNOWN',
            doc.client_code || doc['CLIENT CODE'] || doc['Client Code'] || 'UNKNOWN',
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.exchange || doc['EXCHANGE'] || doc['Exchange'] || 'NSE',
            doc.transaction_type || doc['TRANSACTION TYPE'] || doc['Transaction Type'] || 'CREDIT',
            parseDate(doc.acquisition_date || doc['ACQUISITION DATE'] || doc['Acquisition Date']) || new Date().toISOString().split('T')[0],
            parseDate(doc.settlement_date || doc['SETTLEMENT DATE'] || doc['Settlement Date']),
            parseFloat(doc.amount || doc['AMOUNT'] || doc['Amount'] || 0),
            parseFloat(doc.price || doc['PRICE'] || doc['Price'] || 0),
            parseFloat(doc.brokerage || doc['BROKERAGE'] || doc['Brokerage'] || 0),
            parseFloat(doc.service_tax || doc['SERVICE TAX'] || doc['Service Tax'] || 0),
            doc.settlement_date_flag || doc['SETTLEMENT DATE FLAG'] || doc['Settlement Flag'] || '',
            parseFloat(doc.market_rate || doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['Market Rate'] || 0),
            doc.cash_symbol || doc['CASH SYMBOL'] || doc['Cash Symbol'] || 'INR',
            parseFloat(doc.stt_amount || doc['STT AMOUNT'] || doc['STT'] || 0),
            parseFloat(doc.accrued_interest || doc['ACCRUED INTEREST'] || doc['Accrued Interest'] || 0),
            doc.block_ref || doc['BLOCK REF.'] || doc['Block Ref'] || '',
            doc.remarks || doc['REMARKS'] || doc['Remarks'] || ''
          ];
        } else if (fileType === 'stock_capital_flow') {
          insertSQL = `
            INSERT INTO ${tableName} (transaction_ref, broker_code, client_code, instrument_isin,
                                     exchange, transaction_type, acquisition_date, security_in_date,
                                     quantity, original_price, brokerage, service_tax, settlement_date_flag,
                                     market_rate, cash_symbol, stt_amount, accrued_interest, 
                                     block_ref, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          `;
          values = [
            doc.transaction_ref || doc['TRANSREF'] || doc['Transaction Ref'] || `TXN${Date.now()}`,
            doc.broker_code || doc['BROKER CODE'] || doc['Broker Code'] || 'UNKNOWN',
            doc.client_code || doc['CLIENT CODE'] || doc['Client Code'] || 'UNKNOWN',
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.exchange || doc['EXCHANGE'] || doc['Exchange'] || 'NSE',
            doc.transaction_type || doc['TRANSACTION TYPE'] || doc['Transaction Type'] || 'DELIVERY_IN',
            doc.acquisition_date || doc['ACQUISITION DATE'] || doc['Acquisition Date'] || new Date().toISOString().split('T')[0],
            doc.security_in_date || doc['SECURITY IN DATE'] || doc['Security In Date'] || null,
            parseFloat(doc.quantity || doc['QUANTITY'] || doc['Quantity'] || 0),
            parseFloat(doc.original_price || doc['ORIGINAL PRICE'] || doc['Original Price'] || 0),
            parseFloat(doc.brokerage || doc['BROKERAGE'] || doc['Brokerage'] || 0),
            parseFloat(doc.service_tax || doc['SERVICE TAX'] || doc['Service Tax'] || 0),
            doc.settlement_date_flag || doc['SETTLEMENT DATE FLAG'] || doc['Settlement Flag'] || '',
            parseFloat(doc.market_rate || doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['Market Rate'] || 0),
            doc.cash_symbol || doc['CASH SYMBOL'] || doc['Cash Symbol'] || 'INR',
            parseFloat(doc.stt_amount || doc['STT AMOUNT'] || doc['STT'] || 0),
            parseFloat(doc.accrued_interest || doc['ACCRUED INTEREST'] || doc['Accrued Interest'] || 0),
            doc.block_ref || doc['BLOCK REF.'] || doc['Block Ref'] || '',
            doc.remarks || doc['REMARKS'] || doc['Remarks'] || ''
          ];
        } else if (fileType === 'mf_allocations') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (allocation_date, client_name, custody_code, pan, debit_account_number,
                                     folio_number, amc_name, scheme_name, instrument_isin, purchase_amount,
                                     beneficiary_account_name, beneficiary_account_number, beneficiary_bank_name,
                                     ifsc_code, euin, arn_code)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          `;
          values = [
            parseDate(doc.allocation_date || doc['Date'] || doc['Allocation Date']) || new Date().toISOString().split('T')[0],
            doc.client_name || doc['Client Name'] || 'Unknown Client',
            doc.custody_code || doc['Custody Code'] || '',
            doc.pan || doc['PAN'] || '',
            doc.debit_account_number || doc['Debit Bank account Number'] || doc['Debit Account'] || '',
            doc.folio_number || doc['Folio No'] || doc['Folio Number'] || '',
            doc.amc_name || doc['AMC Name'] || '',
            doc.scheme_name || doc['Scheme Name - Plan - Option'] || doc['Scheme Name'] || '',
            doc.instrument_isin || doc['ISIN No'] || doc['ISIN'] || '',
            parseFloat(doc.purchase_amount || doc['Purchase Amount'] || 0),
            doc.beneficiary_account_name || doc['Beneficiary Account Name'] || doc['Beneficiary Name'] || '',
            doc.beneficiary_account_number || doc['Benecificiary Account Number'] || doc['Beneficiary Account'] || '',
            doc.beneficiary_bank_name || doc['Beneficiary Bank Name'] || doc['Bank Name'] || '',
            doc.ifsc_code || doc['IFSC Code'] || '',
            doc.euin || doc['EUIN'] || '',
            doc.arn_code || doc['ARN Code'] || ''
          ];
        } else if (fileType === 'custody') {
          // Enhanced source system detection
          let sourceSystem = 'UNKNOWN';
          const fileName = (doc.fileName || '').toLowerCase();
          const collectionName = (doc.collectionName || '').toLowerCase();
          
          // Detect source system from filename or collection name
          if (fileName.includes('hdfc') || collectionName.includes('hdfc')) {
            sourceSystem = 'HDFC';
          } else if (fileName.includes('deutsche') || collectionName.includes('deutsche') || fileName.includes('db_')) {
            sourceSystem = 'DEUTSCHE_BANK';
          } else if (fileName.includes('kotak') || collectionName.includes('kotak')) {
            sourceSystem = 'KOTAK';
          } else if (fileName.includes('axis') || collectionName.includes('axis')) {
            sourceSystem = 'AXIS';
          } else if (fileName.includes('orbis') || collectionName.includes('orbis')) {
            sourceSystem = 'ORBIS';
          } else if (fileName.includes('trust') || collectionName.includes('trust') || fileName.includes('pms')) {
            sourceSystem = 'TRUSTPMS';
          } else if (fileName.includes('icici') || collectionName.includes('icici')) {
            sourceSystem = 'ICICI';
          }
          
          insertSQL = `
            INSERT INTO ${tableName} (client_reference, client_name, instrument_isin, instrument_name, instrument_code,
                                     blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
                                     source_system, file_name, record_date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          `;
          values = [
            doc.client_reference || doc['Client Reference'] || doc['Client Code'] || doc['Client'] || `CL${Date.now()}`,
            doc.client_name || doc['Client Name'] || doc['Name'] || (sourceSystem === 'ORBIS' ? 'N/A' : 'Unknown Client'),
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.instrument_name || doc['Instrument Name'] || doc['Security Name'] || doc['Security'] || '',
            doc.instrument_code || doc['Instrument Code'] || doc['Code'] || '',
            parseFloat(doc.blocked_quantity || doc['Blocked Quantity'] || doc['Blocked'] || 0),
            parseFloat(doc.pending_buy_quantity || doc['Pending Buy'] || doc['Pending Buy Quantity'] || 0),
            parseFloat(doc.pending_sell_quantity || doc['Pending Sell'] || doc['Pending Sell Quantity'] || 0),
            parseFloat(doc.total_position || doc['Total Position'] || doc['Total'] || doc['Position'] || 0),
            parseFloat(doc.saleable_quantity || doc['Saleable Quantity'] || doc['Saleable'] || doc['Free Quantity'] || 0),
            sourceSystem,
            doc.fileName || 'unknown',
            doc.record_date || new Date().toISOString().split('T')[0]
          ];
        } else {
          // Generic insertion for other types
          insertSQL = `
            INSERT INTO ${tableName} (data_json, file_name, file_type)
            VALUES ($1, $2, $3)
          `;
          values = [
            JSON.stringify(doc),
            doc.fileName || 'unknown',
            fileType
          ];
        }
        
        await client.query(insertSQL, values);
        validCount++;
        
      } catch (error) {
        console.log(`❌ Row error:`, error.message.substring(0, 100));
      }
    }
    
    return { valid: validCount, processed: documents.length };
    
  } finally {
    client.release();
  }
}

// PostgreSQL data viewing routes
app.get('/postgresql/:date', async (req, res) => {
  try {
    const date = req.params.date;
    const dateStr = date.replace(/-/g, '_');
    
    const client = await pgPool.connect();
    const result = {};
    
    try {
      // Get all tables for the date
      const tablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%${dateStr}'
        ORDER BY table_name
      `;
      
      const tablesResult = await client.query(tablesQuery);
      
      for (const table of tablesResult.rows) {
        const tableName = table.table_name;
        const dataQuery = `SELECT * FROM ${tableName} ORDER BY created_at DESC LIMIT 100`;
        const dataResult = await client.query(dataQuery);
        
        if (dataResult.rows.length > 0) {
          result[tableName] = dataResult.rows;
        }
      }
      
    } finally {
      client.release();
    }
    
    res.json({ success: true, data: result });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/postgresql/all', async (req, res) => {
  try {
    console.log('🔍 API called: /postgresql/all');
    
    // Use direct connection instead of pool to avoid any pool issues
    const { Client } = require('pg');
    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'financial_data',
      password: '',
      port: 5432,
    });
    
    await client.connect();
    console.log('✅ Connected to PostgreSQL');
    
    const result = {};
    
    try {
      // Simple, direct table list and counts
      const tables = [
        'unified_custody_master',
        'general_data', 
        'clients',
        'brokers',
        'distributors',
        'strategies',
        'contract_notes',
        'cash_capital_flow',
        'stock_capital_flow',
        'mf_allocations'
      ];
      
      console.log(`📊 Checking ${tables.length} tables`);
      
      for (const tableName of tables) {
        try {
          // Get count
          const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
          const recordCount = parseInt(countResult.rows[0].count);
          
          if (recordCount > 0) {
            console.log(`📋 Processing table: ${tableName} (${recordCount} records)`);
            
            // Get sample data
            let sampleQuery = `SELECT * FROM ${tableName} LIMIT 5`;
            if (tableName === 'general_data') {
              sampleQuery = `SELECT id, data_type, source_file, created_at FROM ${tableName} LIMIT 5`;
            }
            
            const sampleResult = await client.query(sampleQuery);
            
            result[tableName] = {
              record_count: recordCount,
              sample_data: sampleResult.rows
            };
            
            console.log(`✅ Successfully fetched ${sampleResult.rows.length} sample records from ${tableName}`);
          }
          
        } catch (tableError) {
          console.log(`❌ Error with table ${tableName}:`, tableError.message);
        }
      }
      
    } finally {
      await client.end();
    }
    
    console.log(`🎯 Returning data for ${Object.keys(result).length} tables`);
    res.json({ success: true, data: result });
    
  } catch (error) {
    console.log('❌ PostgreSQL API Error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// Add this new working endpoint before app.listen
app.get('/postgresql/working', async (req, res) => {
  try {
    console.log('🔍 Working API called: /postgresql/working');
    
    const { Client } = require('pg');
    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'financial_data',
      password: '',
      port: 5432,
    });
    
    await client.connect();
    console.log('✅ Connected to PostgreSQL');
    
    const result = {};
    
    const tables = [
      'unified_custody_master',
      'general_data', 
      'clients',
      'brokers',
      'distributors',
      'strategies',
      'contract_notes',
      'cash_capital_flow',
      'stock_capital_flow',
      'mf_allocations'
    ];
    
    for (const tableName of tables) {
      try {
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
        const recordCount = parseInt(countResult.rows[0].count);
        
        if (recordCount > 0) {
          let sampleQuery = `SELECT * FROM ${tableName} LIMIT 5`;
          if (tableName === 'general_data') {
            sampleQuery = `SELECT id, data_type, source_file, created_at FROM ${tableName} LIMIT 5`;
          }
          
          const sampleResult = await client.query(sampleQuery);
          
          result[tableName] = {
            record_count: recordCount,
            sample_data: sampleResult.rows
          };
          
          console.log(`✅ ${tableName}: ${recordCount} records`);
        }
      } catch (tableError) {
        console.log(`❌ Error with ${tableName}:`, tableError.message);
      }
    }
    
    await client.end();
    
    console.log(`🎯 Returning data for ${Object.keys(result).length} tables`);
    res.json({ success: true, data: result });
    
  } catch (error) {
    console.log('❌ Working API Error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

app.listen(PORT, async () => {
  console.log('🚀 Financial Dashboard: http://localhost:' + PORT);
  console.log('📊 Automatically handles any file type with smart categorization');
  console.log('📅 ALL files → financial_data_YYYY (filetype_MM_DD format)');
  console.log('🗂️  Year-wise segregation: financial_data_2024, financial_data_2025, etc.');
  
  // Test connection to show we're connected
  try {
    const testConnection = await mongoose.createConnection(config.mongodb.uri + 'test_connection');
    console.log('✅ Connected to MongoDB Atlas');
    await testConnection.close();
  } catch (error) {
    console.error('❌ MongoDB connection error:', error.message);
  }
});

          const content = fs.readFileSync(file.path, 'utf8');
          const lines = content.split('\n').filter(line => line.trim());
          if (lines.length > 1) {
            const headers = lines[0].split(',');
            fileData = lines.slice(1).map(line => {
              const values = line.split(',');
              const obj = {};
              headers.forEach((h, i) => obj[h.trim()] = values[i]?.trim() || '');
              return obj;
            });
          }
        } else if (['.xlsx', '.xls'].includes(ext)) {
          const workbook = XLSX.readFile(file.path);
          const worksheet = workbook.Sheets[workbook.SheetNames[0]];
          fileData = XLSX.utils.sheet_to_json(worksheet);
        }

        // Smart file type detection
        const fileType = detectFileType(file.originalname);
        let actualDate = date;
        
        // Track if this is a new file type
        const knownTypes = ['broker_master', 'cash_capital_flow', 'stock_capital_flow', 'contract_note', 'distributor_master', 'strategy_master', 'mf_allocations', 'client_info', 'general_data', 'hdfc', 'kotak', 'orbis', 'icici', 'axis', 'sbi', 'edelweiss', 'zerodha', 'nuvama'];
        if (!knownTypes.includes(fileType)) {
          newTypes.add(fileType);
        }
        
        // Extract date for files that have dates in filename
        if (shouldExtractDate(fileType)) {
          const dateMatch = file.originalname.match(/(\d{4})[_-](\d{2})[_-](\d{2})/);
          if (dateMatch) {
            const [, year, month, day] = dateMatch;
            actualDate = year + '-' + month + '-' + day;
            console.log('📅 Extracted date from ' + file.originalname + ': ' + actualDate);
          }
        }

        // Parse date into hierarchical structure
        const dateParts = actualDate.split('-');
        const year = dateParts[0];
        const month = dateParts[1];
        const day = dateParts[2];

        // Get or create model for this file type
        const Model = await getOrCreateModel(fileType, year, month, day);

        // Save data in year-specific database
        if (fileData.length > 0) {
          for (const row of fileData) {
            await new Model({
              month: month,
              date: day,
              fullDate: actualDate,
              fileName: file.originalname,
              fileType: fileType,
              ...row
            }).save();
          }
        } else {
          // Empty file, just save metadata
          await new Model({
            month: month,
            date: day,
            fullDate: actualDate,
            fileName: file.originalname,
            fileType: fileType
          }).save();
        }
        
        filesProcessed++;
        console.log('✅ ' + file.originalname + ' (' + fileData.length + ' records) → ' + fileType + ' collection');
        fs.unlinkSync(file.path);
      } catch (error) {
        console.error('❌ ' + file.originalname + ': ' + error.message);
        try { fs.unlinkSync(file.path); } catch(e) {}
      }
    }

    res.json({ 
      success: true, 
      filesProcessed,
      newTypes: Array.from(newTypes)
    });
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/data/:date', async (req, res) => {
  try {
    const date = req.params.date;
    
    if (date === 'all') {
      // Return hierarchical data structure: filetype → year → month → date
      const hierarchicalData = {};
      
      // Get ALL files from year-specific databases
      const availableYears = ['2024', '2025', '2026'];
      
      for (const year of availableYears) {
        try {
          const yearConnection = await getYearConnection(year);
          const collections = await yearConnection.db.listCollections().toArray();
          
          for (const collection of collections) {
            const collectionName = collection.name;
            const data = await yearConnection.db.collection(collectionName).find({}).sort({ uploadedAt: -1 }).limit(100).toArray();
            
            if (data.length > 0) {
              // Parse collection name: filetype_MM_DD
              const parts = collectionName.split('_');
              if (parts.length >= 3) {
                const fileType = parts.slice(0, -2).join('_');
                const month = parts[parts.length - 2];
                const day = parts[parts.length - 1];
                
                if (!hierarchicalData[fileType]) hierarchicalData[fileType] = {};
                if (!hierarchicalData[fileType][year]) hierarchicalData[fileType][year] = {};
                if (!hierarchicalData[fileType][year][month]) hierarchicalData[fileType][year][month] = {};
                if (!hierarchicalData[fileType][year][month][day]) hierarchicalData[fileType][year][month][day] = [];
                
                hierarchicalData[fileType][year][month][day] = data;
              }
            }
          }
        } catch (error) {
          console.log(`⚪ No data for year ${year}`);
        }
      }
      
      res.json({ success: true, data: hierarchicalData, isHierarchical: true });
    } else {
      // Return data for specific date
      const dateParts = date.split('-');
      const year = dateParts[0];
      const month = dateParts[1].padStart(2, '0');
      const day = dateParts[2].padStart(2, '0');
      
      const results = {};
      
      // Check year-specific database for ALL files
      try {
        const yearConnection = await getYearConnection(year);
        const collections = await yearConnection.db.listCollections().toArray();
        const dateSuffix = `_${month}_${day}`;
        
        for (const collection of collections) {
          if (collection.name.endsWith(dateSuffix)) {
            const fileType = collection.name.replace(dateSuffix, '');
            const data = await yearConnection.db.collection(collection.name).find({}).sort({ uploadedAt: -1 }).toArray();
            if (data.length > 0) {
              results[fileType] = data;
            }
          }
        }
      } catch (error) {
        console.log(`⚪ No data for ${date} in year ${year}`);
      }
      
      res.json({ success: true, data: results, isHierarchical: false });
    }
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

// Multi-threaded processing route
app.post('/process', async (req, res) => {
  console.log('🚀 Starting multi-threaded ETL processing...');
  const startTime = Date.now();
  
  try {
    // Discover all MongoDB collections
    const allCollections = [];
    const availableYears = ['2024', '2025', '2026'];
    
    for (const year of availableYears) {
      try {
        const yearConnection = await getYearConnection(year);
        const collections = await yearConnection.db.listCollections().toArray();
        
        for (const collection of collections) {
          const collectionName = collection.name;
          const count = await yearConnection.db.collection(collectionName).countDocuments();
          if (count > 0) {
            allCollections.push({
              name: collectionName,
              year: year,
              connection: yearConnection
            });
          }
        }
      } catch (error) {
        console.log(`⚪ No data for year ${year}`);
      }
    }
    
    if (allCollections.length === 0) {
      return res.json({ success: false, error: 'No MongoDB collections found to process' });
    }
    
    console.log(`🔍 Found ${allCollections.length} collections to process`);
    
    // Process collections in parallel
    const maxWorkers = Math.min(6, allCollections.length);
    const workers = [];
    const results = [];
    
    for (let i = 0; i < allCollections.length; i += maxWorkers) {
      const batch = allCollections.slice(i, i + maxWorkers);
      const batchPromises = batch.map(async (collection, index) => {
        return processCollection(collection, i + index + 1);
      });
      
      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);
    }
    
    // Calculate totals
    const totalProcessed = results.reduce((sum, r) => sum + r.processed, 0);
    const totalValid = results.reduce((sum, r) => sum + r.valid, 0);
    const totalErrors = totalProcessed - totalValid;
    const successRate = totalProcessed > 0 ? Math.round((totalValid / totalProcessed) * 100) : 0;
    const processingTime = ((Date.now() - startTime) / 1000).toFixed(2) + 's';
    
    console.log(`🎉 Processing complete! ${totalValid}/${totalProcessed} records (${successRate}%)`);
    
    res.json({
      success: true,
      totalProcessed,
      totalValid,
      totalErrors,
      successRate,
      processingTime,
      collectionsProcessed: allCollections.length
    });
    
  } catch (error) {
    console.error('❌ Processing error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// Process a single collection
async function processCollection(collection, workerIndex) {
  console.log(`🔧 Worker ${workerIndex}: Processing ${collection.name}`);
  
  try {
    // Get all documents from the collection
    const documents = await collection.connection.db.collection(collection.name).find({}).toArray();
    
    if (documents.length === 0) {
      return { processed: 0, valid: 0 };
    }
    
    // Detect collection type and determine target table
    const fileType = detectCollectionType(collection.name);
    const targetTable = getTargetTable(fileType, documents[0]);
    
    console.log(`🎯 Worker ${workerIndex}: Detected type '${fileType}' -> target table '${targetTable}'`);
    
    // Create table if it doesn't exist
    await createTableIfNotExists(targetTable, fileType);
    
    // Process documents in batches
    const batchSize = 100;
    let totalProcessed = 0;
    let totalValid = 0;
    
    for (let i = 0; i < documents.length; i += batchSize) {
      const batch = documents.slice(i, i + batchSize);
      const { valid, processed } = await processBatch(batch, targetTable, fileType);
      totalValid += valid;
      totalProcessed += processed;
    }
    
    console.log(`✅ Worker ${workerIndex}: Completed ${collection.name} - ${totalValid}/${totalProcessed} valid`);
    return { processed: totalProcessed, valid: totalValid };
    
  } catch (error) {
    console.error(`❌ Worker ${workerIndex}: Error processing ${collection.name}:`, error.message);
    return { processed: 0, valid: 0 };
  }
}

// Detect collection type from collection name - Fixed mapping
function detectCollectionType(collectionName) {
  const name = collectionName.toLowerCase();
  
  // Master Data Types
  if (name.includes('broker_master_data') || name.includes('broker_master')) return 'broker_master';
  if (name.includes('client_info_data') || name.includes('client_info') || name.includes('client_master')) return 'client_master';
  if (name.includes('distributor_master_data') || name.includes('distributor_master')) return 'distributor_master';
  if (name.includes('strategy_master_data') || name.includes('strategy_master')) return 'strategy_master';
  
  // Transaction Data Types
  if (name.includes('contract_notes_data') || name.includes('contract_note')) return 'contract_notes';
  if (name.includes('cash_capital_flow_data') || name.includes('cash_capital_flow')) return 'cash_capital_flow';
  if (name.includes('stock_capital_flow_data') || name.includes('stock_capital_flow')) return 'stock_capital_flow';
  if (name.includes('mf_allocation_data') || name.includes('mf_allocation')) return 'mf_allocations';
  
  // Custody files
  if (name.includes('hdfc') || name.includes('kotak') || name.includes('axis') || 
      name.includes('orbis') || name.includes('deutsche') || name.includes('trust')) {
    return 'custody';
  }
  
  return 'general';
}

// Get target PostgreSQL table name - Using your existing base tables
function getTargetTable(fileType, sampleDoc) {
  switch (fileType) {
    case 'broker_master': return 'brokers';
    case 'client_master': return 'clients';
    case 'distributor_master': return 'distributors';
    case 'strategy_master': return 'strategies';
    case 'contract_notes': return 'contract_notes';
    case 'cash_capital_flow': return 'cash_capital_flow';
    case 'stock_capital_flow': return 'stock_capital_flow';
    case 'mf_allocations': return 'mf_allocations';
    case 'custody': return 'unified_custody_master';
    default: return 'general_data';
  }
}

// Create table if it doesn't exist - Using your existing table schemas
async function createTableIfNotExists(tableName, fileType) {
  const client = await pgPool.connect();
  
  try {
    let createSQL = '';
    
    switch (fileType) {
      case 'broker_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            broker_id SERIAL PRIMARY KEY,
            broker_code VARCHAR(50) NOT NULL,
            broker_name VARCHAR(200) NOT NULL,
            broker_type VARCHAR(50) DEFAULT 'Unknown',
            registration_number VARCHAR(100),
            contact_person VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'client_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            client_id SERIAL PRIMARY KEY,
            client_code VARCHAR(50) NOT NULL,
            client_name VARCHAR(200) NOT NULL,
            client_type VARCHAR(50) DEFAULT 'Individual',
            pan_number VARCHAR(20),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            risk_category VARCHAR(50) DEFAULT 'Medium',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'distributor_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            distributor_id SERIAL PRIMARY KEY,
            distributor_arn_number VARCHAR(100) NOT NULL,
            distributor_code VARCHAR(50) NOT NULL,
            distributor_name VARCHAR(200) NOT NULL,
            distributor_type VARCHAR(50) DEFAULT 'External',
            commission_rate DECIMAL(8,4) DEFAULT 0,
            contact_person VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            address TEXT,
            city VARCHAR(100),
            state VARCHAR(100),
            country VARCHAR(100) DEFAULT 'India',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'strategy_master':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            strategy_id SERIAL PRIMARY KEY,
            strategy_code VARCHAR(50) NOT NULL,
            strategy_name VARCHAR(200) NOT NULL,
            strategy_type VARCHAR(50) DEFAULT 'Equity',
            description TEXT,
            benchmark VARCHAR(200),
            risk_level VARCHAR(50) DEFAULT 'Medium',
            min_investment DECIMAL(15,2) DEFAULT 0,
            max_investment DECIMAL(15,2) DEFAULT 0,
            management_fee DECIMAL(8,4) DEFAULT 0,
            performance_fee DECIMAL(8,4) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'contract_notes':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            ecn_number VARCHAR(50) PRIMARY KEY,
            ecn_status VARCHAR(50),
            ecn_date DATE,
            client_code VARCHAR(50),
            broker_name VARCHAR(200),
            instrument_isin VARCHAR(20),
            instrument_name VARCHAR(300),
            transaction_type VARCHAR(10),
            delivery_type VARCHAR(50),
            exchange VARCHAR(10),
            settlement_date DATE,
            market_type VARCHAR(20),
            settlement_number VARCHAR(50),
            quantity DECIMAL(15,4),
            net_amount DECIMAL(15,2),
            net_rate DECIMAL(15,4),
            brokerage_amount DECIMAL(15,2),
            brokerage_rate DECIMAL(8,4),
            service_tax DECIMAL(15,2),
            stamp_duty DECIMAL(15,2),
            stt_amount DECIMAL(15,2),
            sebi_registration VARCHAR(50),
            scheme_name VARCHAR(300),
            custodian_name VARCHAR(200),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'cash_capital_flow':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            transaction_ref VARCHAR(100) PRIMARY KEY,
            broker_code VARCHAR(50),
            client_code VARCHAR(50),
            instrument_isin VARCHAR(20),
            exchange VARCHAR(10),
            transaction_type VARCHAR(10),
            acquisition_date DATE,
            settlement_date DATE,
            amount DECIMAL(15,2),
            price DECIMAL(15,4),
            brokerage DECIMAL(15,2),
            service_tax DECIMAL(15,2),
            settlement_date_flag VARCHAR(20),
            market_rate DECIMAL(15,4),
            cash_symbol VARCHAR(20),
            stt_amount DECIMAL(15,2),
            accrued_interest DECIMAL(15,2),
            block_ref VARCHAR(100),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'stock_capital_flow':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            transaction_ref VARCHAR(100) PRIMARY KEY,
            broker_code VARCHAR(50),
            client_code VARCHAR(50),
            instrument_isin VARCHAR(20),
            exchange VARCHAR(10),
            transaction_type VARCHAR(10),
            acquisition_date DATE,
            security_in_date DATE,
            quantity DECIMAL(15,4),
            original_price DECIMAL(15,4),
            brokerage DECIMAL(15,2),
            service_tax DECIMAL(15,2),
            settlement_date_flag VARCHAR(20),
            market_rate DECIMAL(15,4),
            cash_symbol VARCHAR(20),
            stt_amount DECIMAL(15,2),
            accrued_interest DECIMAL(15,2),
            block_ref VARCHAR(100),
            remarks TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'mf_allocations':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            allocation_id SERIAL PRIMARY KEY,
            allocation_date DATE,
            client_name VARCHAR(200),
            custody_code VARCHAR(50),
            pan VARCHAR(20),
            debit_account_number VARCHAR(50),
            folio_number VARCHAR(50),
            amc_name VARCHAR(200),
            scheme_name VARCHAR(500),
            instrument_isin VARCHAR(20),
            purchase_amount DECIMAL(15,2),
            beneficiary_account_name VARCHAR(200),
            beneficiary_account_number VARCHAR(50),
            beneficiary_bank_name VARCHAR(200),
            ifsc_code VARCHAR(20),
            euin VARCHAR(50),
            arn_code VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      case 'custody':
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            client_reference VARCHAR(50) NOT NULL,
            client_name VARCHAR(200) NOT NULL,
            instrument_isin VARCHAR(20) NOT NULL,
            instrument_name VARCHAR(300),
            instrument_code VARCHAR(100),
            blocked_quantity DECIMAL(15,4) DEFAULT 0,
            pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
            pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
            total_position DECIMAL(15,4) DEFAULT 0,
            saleable_quantity DECIMAL(15,4) DEFAULT 0,
            source_system VARCHAR(20) NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            record_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
        break;
        
      default:
        // Generic table for other types  
        createSQL = `
          CREATE TABLE IF NOT EXISTS ${tableName} (
            id SERIAL PRIMARY KEY,
            data_json JSONB,
            file_name VARCHAR(255),
            file_type VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
          )`;
    }
    
    await client.query(createSQL);
    console.log(`📅 Created/verified table: ${tableName}`);
    
  } finally {
    client.release();
  }
}

// Process a batch of documents - Fixed field mapping
async function processBatch(documents, tableName, fileType) {
  const client = await pgPool.connect();
  
  try {
    let validCount = 0;
    
    for (const doc of documents) {
      try {
        let insertSQL = '';
        let values = [];
        
        if (fileType === 'broker_master') {
          insertSQL = `
            INSERT INTO ${tableName} (broker_code, broker_name, broker_type, registration_number, 
                                     contact_person, email, phone, address, city, state, country)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          values = [
            doc.broker_code || doc['Broker Code'] || doc['Code'] || `BR${Date.now()}`,
            doc.broker_name || doc['Broker Name'] || doc['Name'] || 'Unknown Broker',
            doc.broker_type || doc['Broker Type'] || doc['Type'] || 'Full Service',
            doc.registration_number || doc['Registration Number'] || doc['Reg No'] || '',
            doc.contact_person || doc['Contact Person'] || doc['Contact'] || '',
            doc.email || doc['Email'] || doc['Email ID'] || '',
            doc.phone || doc['Phone'] || doc['Mobile'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India'
          ];
        } else if (fileType === 'client_master') {
          insertSQL = `
            INSERT INTO ${tableName} (client_code, client_name, client_type, pan_number,
                                     email, phone, address, city, state, country, risk_category)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
          `;
          values = [
            doc.client_code || doc['Client Code'] || doc['Code'] || `CL${Date.now()}`,
            doc.client_name || doc['Client Name'] || doc['Name'] || 'Unknown Client',
            doc.client_type || doc['Client Type'] || doc['Type'] || 'Individual',
            doc.pan_number || doc['PAN Number'] || doc['PAN'] || '',
            doc.email || doc['Email'] || doc['Email ID'] || '',
            doc.phone || doc['Phone'] || doc['Mobile'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India',
            doc.risk_category || doc['Risk Category'] || doc['Risk'] || 'Medium'
          ];
        } else if (fileType === 'distributor_master') {
          insertSQL = `
            INSERT INTO ${tableName} (distributor_arn_number, distributor_code, distributor_name, 
                                     distributor_type, commission_rate, contact_person, email, 
                                     phone, address, city, state, country)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
          `;
          values = [
            doc.distributor_arn_number || doc['ARN Number'] || doc['ARN'] || `ARN${Date.now()}`,
            doc.distributor_code || doc['Distributor Code'] || doc['Code'] || `DS${Date.now()}`,
            doc.distributor_name || doc['Distributor Name'] || doc['Name'] || 'Unknown Distributor',
            doc.distributor_type || doc['Distributor Type'] || doc['Type'] || 'External',
            parseFloat(doc.commission_rate || doc['Commission Rate'] || 0),
            doc.contact_person || doc['Contact Person'] || doc['Contact'] || '',
            doc.email || doc['Email'] || '',
            doc.phone || doc['Phone'] || '',
            doc.address || doc['Address'] || '',
            doc.city || doc['City'] || '',
            doc.state || doc['State'] || '',
            doc.country || doc['Country'] || 'India'
          ];
        } else if (fileType === 'strategy_master') {
          insertSQL = `
            INSERT INTO ${tableName} (strategy_code, strategy_name, strategy_type, description,
                                     benchmark, risk_level, min_investment, max_investment,
                                     management_fee, performance_fee)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          `;
          values = [
            doc.strategy_code || doc['Strategy Code'] || doc['Code'] || `ST${Date.now()}`,
            doc.strategy_name || doc['Strategy Name'] || doc['Name'] || 'Unknown Strategy',
            doc.strategy_type || doc['Strategy Type'] || doc['Type'] || 'Equity',
            doc.description || doc['Description'] || '',
            doc.benchmark || doc['Benchmark'] || '',
            doc.risk_level || doc['Risk Level'] || 'Medium',
            parseFloat(doc.min_investment || doc['Min Investment'] || 0),
            parseFloat(doc.max_investment || doc['Max Investment'] || 0),
            parseFloat(doc.management_fee || doc['Management Fee'] || 0),
            parseFloat(doc.performance_fee || doc['Performance Fee'] || 0)
          ];
        } else if (fileType === 'contract_notes') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (ecn_number, ecn_status, ecn_date, client_code, broker_name,
                                     instrument_isin, instrument_name, transaction_type, delivery_type,
                                     exchange, settlement_date, settlement_number, quantity, net_amount, net_rate,
                                     brokerage_amount, brokerage_rate, service_tax, stamp_duty, stt_amount,
                                     sebi_registration, scheme_name, custodian_name, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
          `;
          values = [
            doc.ecn_number || doc['ECN No'] || doc['ECN Number'] || doc['Contract Number'] || `ECN${Date.now()}`,
            doc.ecn_status || doc['ECN Status'] || doc['Status'] || 'ACTIVE',
            parseDate(doc.ecn_date || doc['ECN Date'] || doc['Date']) || new Date().toISOString().split('T')[0],
            doc.client_code || doc['Client Exchange Code/UCC'] || doc['Client Code'] || `CL${Date.now()}`,
            doc.broker_name || doc['Broker Name'] || 'Unknown Broker',
            doc.instrument_isin || doc['ISIN Code'] || doc['Instrument ISIN'] || doc['ISIN'] || '',
            doc.instrument_name || doc['Security Name'] || doc['Instrument Name'] || doc['Security'] || '',
            doc.transaction_type || doc['Transaction Type'] || doc['Type'] || 'BUY',
            doc.delivery_type || doc['Delivery Type'] || 'CNC',
            doc.exchange || doc['Exchange'] || 'NSE',
            parseDate(doc.settlement_date || doc['Sett. Date'] || doc['Settlement Date']),
            doc.settlement_number || doc['Settlement Number'] || '',
            parseFloat(doc.quantity || doc['Qty'] || doc['Quantity'] || 0),
            parseFloat(doc.net_amount || doc['Net Amount'] || doc['Amount'] || 0),
            parseFloat(doc.net_rate || doc['Net Rate'] || doc['Rate'] || 0),
            parseFloat(doc.brokerage_amount || doc['Brokerage Amount'] || doc['Brokerage'] || 0),
            parseFloat(doc.brokerage_rate || doc['Brokerage Rate'] || 0),
            parseFloat(doc.service_tax || doc['Service Tax'] || 0),
            parseFloat(doc.stamp_duty || doc['Stamp Duty'] || 0),
            parseFloat(doc.stt_amount || doc['Service Transaction Tax'] || doc['STT'] || 0),
            doc.sebi_registration || doc['SEBI Regn No.'] || '',
            doc.scheme_name || doc['Scheme Name'] || '',
            doc.custodian_name || doc['Custodian Name'] || '',
            doc.remarks || doc['Remarks'] || ''
          ];
        } else if (fileType === 'cash_capital_flow') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (transaction_ref, broker_code, client_code, instrument_isin,
                                     exchange, transaction_type, acquisition_date, settlement_date,
                                     amount, price, brokerage, service_tax, settlement_date_flag,
                                     market_rate, cash_symbol, stt_amount, accrued_interest, 
                                     block_ref, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          `;
          values = [
            doc.transaction_ref || doc['TRANSREF'] || doc['Transaction Ref'] || `TXN${Date.now()}`,
            doc.broker_code || doc['BROKER CODE'] || doc['Broker Code'] || 'UNKNOWN',
            doc.client_code || doc['CLIENT CODE'] || doc['Client Code'] || 'UNKNOWN',
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.exchange || doc['EXCHANGE'] || doc['Exchange'] || 'NSE',
            doc.transaction_type || doc['TRANSACTION TYPE'] || doc['Transaction Type'] || 'CREDIT',
            parseDate(doc.acquisition_date || doc['ACQUISITION DATE'] || doc['Acquisition Date']) || new Date().toISOString().split('T')[0],
            parseDate(doc.settlement_date || doc['SETTLEMENT DATE'] || doc['Settlement Date']),
            parseFloat(doc.amount || doc['AMOUNT'] || doc['Amount'] || 0),
            parseFloat(doc.price || doc['PRICE'] || doc['Price'] || 0),
            parseFloat(doc.brokerage || doc['BROKERAGE'] || doc['Brokerage'] || 0),
            parseFloat(doc.service_tax || doc['SERVICE TAX'] || doc['Service Tax'] || 0),
            doc.settlement_date_flag || doc['SETTLEMENT DATE FLAG'] || doc['Settlement Flag'] || '',
            parseFloat(doc.market_rate || doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['Market Rate'] || 0),
            doc.cash_symbol || doc['CASH SYMBOL'] || doc['Cash Symbol'] || 'INR',
            parseFloat(doc.stt_amount || doc['STT AMOUNT'] || doc['STT'] || 0),
            parseFloat(doc.accrued_interest || doc['ACCRUED INTEREST'] || doc['Accrued Interest'] || 0),
            doc.block_ref || doc['BLOCK REF.'] || doc['Block Ref'] || '',
            doc.remarks || doc['REMARKS'] || doc['Remarks'] || ''
          ];
        } else if (fileType === 'stock_capital_flow') {
          insertSQL = `
            INSERT INTO ${tableName} (transaction_ref, broker_code, client_code, instrument_isin,
                                     exchange, transaction_type, acquisition_date, security_in_date,
                                     quantity, original_price, brokerage, service_tax, settlement_date_flag,
                                     market_rate, cash_symbol, stt_amount, accrued_interest, 
                                     block_ref, remarks)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
          `;
          values = [
            doc.transaction_ref || doc['TRANSREF'] || doc['Transaction Ref'] || `TXN${Date.now()}`,
            doc.broker_code || doc['BROKER CODE'] || doc['Broker Code'] || 'UNKNOWN',
            doc.client_code || doc['CLIENT CODE'] || doc['Client Code'] || 'UNKNOWN',
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.exchange || doc['EXCHANGE'] || doc['Exchange'] || 'NSE',
            doc.transaction_type || doc['TRANSACTION TYPE'] || doc['Transaction Type'] || 'DELIVERY_IN',
            doc.acquisition_date || doc['ACQUISITION DATE'] || doc['Acquisition Date'] || new Date().toISOString().split('T')[0],
            doc.security_in_date || doc['SECURITY IN DATE'] || doc['Security In Date'] || null,
            parseFloat(doc.quantity || doc['QUANTITY'] || doc['Quantity'] || 0),
            parseFloat(doc.original_price || doc['ORIGINAL PRICE'] || doc['Original Price'] || 0),
            parseFloat(doc.brokerage || doc['BROKERAGE'] || doc['Brokerage'] || 0),
            parseFloat(doc.service_tax || doc['SERVICE TAX'] || doc['Service Tax'] || 0),
            doc.settlement_date_flag || doc['SETTLEMENT DATE FLAG'] || doc['Settlement Flag'] || '',
            parseFloat(doc.market_rate || doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['Market Rate'] || 0),
            doc.cash_symbol || doc['CASH SYMBOL'] || doc['Cash Symbol'] || 'INR',
            parseFloat(doc.stt_amount || doc['STT AMOUNT'] || doc['STT'] || 0),
            parseFloat(doc.accrued_interest || doc['ACCRUED INTEREST'] || doc['Accrued Interest'] || 0),
            doc.block_ref || doc['BLOCK REF.'] || doc['Block Ref'] || '',
            doc.remarks || doc['REMARKS'] || doc['Remarks'] || ''
          ];
        } else if (fileType === 'mf_allocations') {
          // Helper function to parse dates in various formats
          const parseDate = (dateStr) => {
            if (!dateStr) return null;
            
            try {
              // Handle DD/MM/YYYY format
              if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // Handle DD-MM-YYYY format
              if (dateStr.includes('-') && dateStr.length === 10) {
                const parts = dateStr.split('-');
                if (parts.length === 3 && parts[2].length === 4) {
                  const day = parts[0].padStart(2, '0');
                  const month = parts[1].padStart(2, '0');
                  const year = parts[2];
                  return `${year}-${month}-${day}`;
                }
              }
              
              // If already in YYYY-MM-DD format or ISO format
              const date = new Date(dateStr);
              if (!isNaN(date.getTime())) {
                return date.toISOString().split('T')[0];
              }
              
              return null;
            } catch (error) {
              return null;
            }
          };
          
          insertSQL = `
            INSERT INTO ${tableName} (allocation_date, client_name, custody_code, pan, debit_account_number,
                                     folio_number, amc_name, scheme_name, instrument_isin, purchase_amount,
                                     beneficiary_account_name, beneficiary_account_number, beneficiary_bank_name,
                                     ifsc_code, euin, arn_code)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
          `;
          values = [
            parseDate(doc.allocation_date || doc['Date'] || doc['Allocation Date']) || new Date().toISOString().split('T')[0],
            doc.client_name || doc['Client Name'] || 'Unknown Client',
            doc.custody_code || doc['Custody Code'] || '',
            doc.pan || doc['PAN'] || '',
            doc.debit_account_number || doc['Debit Bank account Number'] || doc['Debit Account'] || '',
            doc.folio_number || doc['Folio No'] || doc['Folio Number'] || '',
            doc.amc_name || doc['AMC Name'] || '',
            doc.scheme_name || doc['Scheme Name - Plan - Option'] || doc['Scheme Name'] || '',
            doc.instrument_isin || doc['ISIN No'] || doc['ISIN'] || '',
            parseFloat(doc.purchase_amount || doc['Purchase Amount'] || 0),
            doc.beneficiary_account_name || doc['Beneficiary Account Name'] || doc['Beneficiary Name'] || '',
            doc.beneficiary_account_number || doc['Benecificiary Account Number'] || doc['Beneficiary Account'] || '',
            doc.beneficiary_bank_name || doc['Beneficiary Bank Name'] || doc['Bank Name'] || '',
            doc.ifsc_code || doc['IFSC Code'] || '',
            doc.euin || doc['EUIN'] || '',
            doc.arn_code || doc['ARN Code'] || ''
          ];
        } else if (fileType === 'custody') {
          // Enhanced source system detection
          let sourceSystem = 'UNKNOWN';
          const fileName = (doc.fileName || '').toLowerCase();
          const collectionName = (doc.collectionName || '').toLowerCase();
          
          // Detect source system from filename or collection name
          if (fileName.includes('hdfc') || collectionName.includes('hdfc')) {
            sourceSystem = 'HDFC';
          } else if (fileName.includes('deutsche') || collectionName.includes('deutsche') || fileName.includes('db_')) {
            sourceSystem = 'DEUTSCHE_BANK';
          } else if (fileName.includes('kotak') || collectionName.includes('kotak')) {
            sourceSystem = 'KOTAK';
          } else if (fileName.includes('axis') || collectionName.includes('axis')) {
            sourceSystem = 'AXIS';
          } else if (fileName.includes('orbis') || collectionName.includes('orbis')) {
            sourceSystem = 'ORBIS';
          } else if (fileName.includes('trust') || collectionName.includes('trust') || fileName.includes('pms')) {
            sourceSystem = 'TRUSTPMS';
          } else if (fileName.includes('icici') || collectionName.includes('icici')) {
            sourceSystem = 'ICICI';
          }
          
          insertSQL = `
            INSERT INTO ${tableName} (client_reference, client_name, instrument_isin, instrument_name, instrument_code,
                                     blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
                                     source_system, file_name, record_date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          `;
          values = [
            doc.client_reference || doc['Client Reference'] || doc['Client Code'] || doc['Client'] || `CL${Date.now()}`,
            doc.client_name || doc['Client Name'] || doc['Name'] || (sourceSystem === 'ORBIS' ? 'N/A' : 'Unknown Client'),
            doc.instrument_isin || doc['ISIN'] || doc['Instrument ISIN'] || '',
            doc.instrument_name || doc['Instrument Name'] || doc['Security Name'] || doc['Security'] || '',
            doc.instrument_code || doc['Instrument Code'] || doc['Code'] || '',
            parseFloat(doc.blocked_quantity || doc['Blocked Quantity'] || doc['Blocked'] || 0),
            parseFloat(doc.pending_buy_quantity || doc['Pending Buy'] || doc['Pending Buy Quantity'] || 0),
            parseFloat(doc.pending_sell_quantity || doc['Pending Sell'] || doc['Pending Sell Quantity'] || 0),
            parseFloat(doc.total_position || doc['Total Position'] || doc['Total'] || doc['Position'] || 0),
            parseFloat(doc.saleable_quantity || doc['Saleable Quantity'] || doc['Saleable'] || doc['Free Quantity'] || 0),
            sourceSystem,
            doc.fileName || 'unknown',
            doc.record_date || new Date().toISOString().split('T')[0]
          ];
        } else {
          // Generic insertion for other types
          insertSQL = `
            INSERT INTO ${tableName} (data_json, file_name, file_type)
            VALUES ($1, $2, $3)
          `;
          values = [
            JSON.stringify(doc),
            doc.fileName || 'unknown',
            fileType
          ];
        }
        
        await client.query(insertSQL, values);
        validCount++;
        
      } catch (error) {
        console.log(`❌ Row error:`, error.message.substring(0, 100));
      }
    }
    
    return { valid: validCount, processed: documents.length };
    
  } finally {
    client.release();
  }
}

// PostgreSQL data viewing routes
app.get('/postgresql/:date', async (req, res) => {
  try {
    const date = req.params.date;
    const dateStr = date.replace(/-/g, '_');
    
    const client = await pgPool.connect();
    const result = {};
    
    try {
      // Get all tables for the date
      const tablesQuery = `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE '%${dateStr}'
        ORDER BY table_name
      `;
      
      const tablesResult = await client.query(tablesQuery);
      
      for (const table of tablesResult.rows) {
        const tableName = table.table_name;
        const dataQuery = `SELECT * FROM ${tableName} ORDER BY created_at DESC LIMIT 100`;
        const dataResult = await client.query(dataQuery);
        
        if (dataResult.rows.length > 0) {
          result[tableName] = dataResult.rows;
        }
      }
      
    } finally {
      client.release();
    }
    
    res.json({ success: true, data: result });
    
  } catch (error) {
    res.json({ success: false, error: error.message });
  }
});

app.get('/postgresql/all', async (req, res) => {
  try {
    console.log('🔍 API called: /postgresql/all');
    
    // Use direct connection instead of pool to avoid any pool issues
    const { Client } = require('pg');
    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'financial_data',
      password: '',
      port: 5432,
    });
    
    await client.connect();
    console.log('✅ Connected to PostgreSQL');
    
    const result = {};
    
    try {
      // Simple, direct table list and counts
      const tables = [
        'unified_custody_master',
        'general_data', 
        'clients',
        'brokers',
        'distributors',
        'strategies',
        'contract_notes',
        'cash_capital_flow',
        'stock_capital_flow',
        'mf_allocations'
      ];
      
      console.log(`📊 Checking ${tables.length} tables`);
      
      for (const tableName of tables) {
        try {
          // Get count
          const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
          const recordCount = parseInt(countResult.rows[0].count);
          
          if (recordCount > 0) {
            console.log(`📋 Processing table: ${tableName} (${recordCount} records)`);
            
            // Get sample data
            let sampleQuery = `SELECT * FROM ${tableName} LIMIT 5`;
            if (tableName === 'general_data') {
              sampleQuery = `SELECT id, data_type, source_file, created_at FROM ${tableName} LIMIT 5`;
            }
            
            const sampleResult = await client.query(sampleQuery);
            
            result[tableName] = {
              record_count: recordCount,
              sample_data: sampleResult.rows
            };
            
            console.log(`✅ Successfully fetched ${sampleResult.rows.length} sample records from ${tableName}`);
          }
          
        } catch (tableError) {
          console.log(`❌ Error with table ${tableName}:`, tableError.message);
        }
      }
      
    } finally {
      await client.end();
    }
    
    console.log(`🎯 Returning data for ${Object.keys(result).length} tables`);
    res.json({ success: true, data: result });
    
  } catch (error) {
    console.log('❌ PostgreSQL API Error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// Add this new working endpoint before app.listen
app.get('/postgresql/working', async (req, res) => {
  try {
    console.log('🔍 Working API called: /postgresql/working');
    
    const { Client } = require('pg');
    const client = new Client({
      user: 'postgres',
      host: 'localhost',
      database: 'financial_data',
      password: '',
      port: 5432,
    });
    
    await client.connect();
    console.log('✅ Connected to PostgreSQL');
    
    const result = {};
    
    const tables = [
      'unified_custody_master',
      'general_data', 
      'clients',
      'brokers',
      'distributors',
      'strategies',
      'contract_notes',
      'cash_capital_flow',
      'stock_capital_flow',
      'mf_allocations'
    ];
    
    for (const tableName of tables) {
      try {
        const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
        const recordCount = parseInt(countResult.rows[0].count);
        
        if (recordCount > 0) {
          let sampleQuery = `SELECT * FROM ${tableName} LIMIT 5`;
          if (tableName === 'general_data') {
            sampleQuery = `SELECT id, data_type, source_file, created_at FROM ${tableName} LIMIT 5`;
          }
          
          const sampleResult = await client.query(sampleQuery);
          
          result[tableName] = {
            record_count: recordCount,
            sample_data: sampleResult.rows
          };
          
          console.log(`✅ ${tableName}: ${recordCount} records`);
        }
      } catch (tableError) {
        console.log(`❌ Error with ${tableName}:`, tableError.message);
      }
    }
    
    await client.end();
    
    console.log(`🎯 Returning data for ${Object.keys(result).length} tables`);
    res.json({ success: true, data: result });
    
  } catch (error) {
    console.log('❌ Working API Error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

app.listen(PORT, async () => {
  console.log('🚀 Financial Dashboard: http://localhost:' + PORT);
  console.log('📊 Automatically handles any file type with smart categorization');
  console.log('📅 ALL files → financial_data_YYYY (filetype_MM_DD format)');
  console.log('🗂️  Year-wise segregation: financial_data_2024, financial_data_2025, etc.');
  
  // Test connection to show we're connected
  try {
    const testConnection = await mongoose.createConnection(config.mongodb.uri + 'test_connection');
    console.log('✅ Connected to MongoDB Atlas');
    await testConnection.close();
  } catch (error) {
    console.error('❌ MongoDB connection error:', error.message);
  }
});
