const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const path = require('path');
const fs = require('fs-extra');
const XLSX = require('xlsx');
const config = require('./config');

const app = express();
const PORT = 3002;

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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Flexible Data Dashboard</h1>
            <p>Automatically categorizes any file type - handles new files intelligently</p>
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
                <h2>👁️ View All Data Types</h2>
                <input type="date" id="viewDate">
                <button id="viewBtn" class="btn" style="width: 100%;">View Data</button>
                <button id="viewAllBtn" class="btn" style="width: 100%; margin-top: 5px; background: #2196F3;">View All Types</button>
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
                    displayData(result.data, 'All Data - Hierarchical View (Year → Month → Date)', result.isHierarchical);
                } else {
                    document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + result.error + '</div>';
                }
            } catch (error) {
                document.getElementById('dataSummary').innerHTML = '<div class="error">Error: ' + error.message + '</div>';
            }
        });

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
