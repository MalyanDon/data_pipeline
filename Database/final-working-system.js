const express = require('express');
const multer = require('multer');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const app = express();
const PORT = 3000; // STAYING ON PORT 3000

// Storage configuration
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = './temp_uploads';
        if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        const timestamp = Date.now();
        const sanitized = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
        cb(null, `${timestamp}_${sanitized}`);
    }
});

const upload = multer({ 
    storage: storage,
    fileFilter: (req, file, cb) => {
        const allowedTypes = ['.xlsx', '.xls', '.csv'];
        const fileExt = path.extname(file.originalname).toLowerCase();
        if (allowedTypes.includes(fileExt)) {
            cb(null, true);
        } else {
            cb(new Error(`Invalid file type. Only ${allowedTypes.join(', ')} files are allowed.`), false);
        }
    },
    limits: { fileSize: 100 * 1024 * 1024, files: 1 }
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    if (req.method === 'OPTIONS') { res.sendStatus(200); } else { next(); }
});

const mongoUri = config.mongodb.uri + config.mongodb.database;

const DATA_CATEGORIES = {
    custody_files: { displayName: 'Custody Files', subcategories: ['hdfc', 'axis', 'kotak', 'orbis', 'others'], icon: '🏛️', hasSubcategories: true },
    stock_capital_flow: { displayName: 'Stock Capital Flow', subcategories: ['stock_capital_flow'], icon: '📊', hasSubcategories: false },
    cash_capital_flow: { displayName: 'Cash Capital Flow', subcategories: ['cash_capital_flow'], icon: '💰', hasSubcategories: false },
    distributor_master: { displayName: 'Distributor Master', subcategories: ['distributor_master'], icon: '🤝', hasSubcategories: false },
    contract_notes: { displayName: 'Contract Notes', subcategories: ['contract_notes'], icon: '📋', hasSubcategories: false },
    mf_allocations: { displayName: 'MF Allocations', subcategories: ['mf_allocations'], icon: '📈', hasSubcategories: false },
    strategy_master: { displayName: 'Strategy Master', subcategories: ['strategy_master'], icon: '🎯', hasSubcategories: false },
    client_info: { displayName: 'Client Info', subcategories: ['client_info'], icon: '👥', hasSubcategories: false },
    trades: { displayName: 'Trades', subcategories: ['trades'], icon: '💹', hasSubcategories: false }
};

// Helper functions
function extractDateFromFilename(filename) {
    try {
        const nameWithoutExt = filename.replace(/\.(xlsx?|csv)$/i, '');
        const cleanName = nameWithoutExt.toLowerCase();
        
        const datePatterns = [
            { pattern: /(\d{1,2})[-\/._](\d{1,2})[-\/._](\d{4})/, format: 'DMY' },
            { pattern: /(\d{4})[-\/._](\d{1,2})[-\/._](\d{1,2})/, format: 'YMD' },
            { pattern: /(\d{4})(\d{2})(\d{2})(?![0-9])/, format: 'YMD_COMPACT' },
        ];
        
        for (const { pattern, format } of datePatterns) {
            const match = cleanName.match(pattern);
            if (match) {
                let day, month, year;
                switch (format) {
                    case 'DMY': day = parseInt(match[1]); month = parseInt(match[2]); year = parseInt(match[3]); break;
                    case 'YMD': year = parseInt(match[1]); month = parseInt(match[2]); day = parseInt(match[3]); break;
                    case 'YMD_COMPACT': year = parseInt(match[1]); month = parseInt(match[2]); day = parseInt(match[3]); break;
                }
                
                if (year >= 2020 && year <= 2030 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                    return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
                }
            }
        }
        return new Date().toISOString().split('T')[0];
    } catch (error) {
        return new Date().toISOString().split('T')[0];
    }
}

async function processCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv({ skipEmptyLines: true, skipLinesWithError: true }))
            .on('data', (data) => {
                const cleanData = {};
                for (const [key, value] of Object.entries(data)) {
                    const cleanKey = key.trim();
                    const cleanValue = typeof value === 'string' ? value.trim() : value;
                    if (cleanKey && cleanValue !== '') {
                        cleanData[cleanKey] = cleanValue;
                    }
                }
                if (Object.keys(cleanData).length > 0) {
                    results.push(cleanData);
                }
            })
            .on('end', () => resolve(results))
            .on('error', (error) => reject(new Error(`CSV parsing failed: ${error.message}`)));
    });
}

async function processExcelFile(filePath) {
    try {
        const workbook = xlsx.readFile(filePath, { cellDates: true, cellNF: false, cellText: false });
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) throw new Error('No sheets found in Excel file');
        
        const worksheet = workbook.Sheets[sheetName];
        const jsonData = xlsx.utils.sheet_to_json(worksheet, { raw: false, defval: '', blankrows: false });
        
        return jsonData.map(row => {
            const cleanRow = {};
            for (const [key, value] of Object.entries(row)) {
                const cleanKey = key.trim();
                const cleanValue = typeof value === 'string' ? value.trim() : value;
                if (cleanKey && cleanValue !== '') {
                    cleanRow[cleanKey] = cleanValue;
                }
            }
            return cleanRow;
        }).filter(row => Object.keys(row).length > 0);
    } catch (error) {
        throw new Error(`Excel parsing failed: ${error.message}`);
    }
}

// Main page route
app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🚀 Complete ETL System</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                   background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .header { text-align: center; color: white; margin-bottom: 30px; }
            .header h1 { font-size: 2.5rem; margin-bottom: 10px; }
            .tabs { display: flex; justify-content: center; margin-bottom: 20px; }
            .tab { background: rgba(255,255,255,0.2); color: white; border: none; padding: 15px 30px; 
                   margin: 0 5px; border-radius: 10px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
            .tab.active { background: white; color: #667eea; }
            .tab:hover { background: rgba(255,255,255,0.3); }
            .content { background: white; border-radius: 15px; padding: 30px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); }
            .category-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }
            .category-card { background: #f8f9fa; border: 2px solid #e9ecef; border-radius: 10px; padding: 20px; cursor: pointer; transition: all 0.3s; }
            .category-card:hover { border-color: #667eea; background: #f0f4ff; }
            .category-card.selected { border-color: #667eea; background: #e3f2fd; }
            .subcategory-container { margin-top: 15px; display: none; }
            .subcategory-container.show { display: block; }
            .subcategory-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }
            .subcategory-btn { background: #e9ecef; border: 1px solid #ced4da; border-radius: 5px; padding: 8px 12px; cursor: pointer; text-align: center; transition: all 0.3s; }
            .subcategory-btn:hover { background: #667eea; color: white; }
            .subcategory-btn.selected { background: #667eea; color: white; }
            .upload-area { border: 2px dashed #ced4da; border-radius: 10px; padding: 40px; text-align: center; margin: 20px 0; transition: all 0.3s; }
            .upload-area.drag-over { border-color: #667eea; background: #f0f4ff; }
            .btn { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
            .btn:hover { background: #5a6fd8; }
            .btn:disabled { background: #6c757d; cursor: not-allowed; }
            .table-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }
            .table-card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; cursor: pointer; transition: all 0.3s; }
            .table-card:hover { background: #e9ecef; }
            .table-card.selected { background: #e3f2fd; border-color: #667eea; }
            .mapping-section { margin: 20px 0; }
            .column-mapping { background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0; }
            .form-group { margin: 15px 0; }
            .form-group label { display: block; margin-bottom: 5px; font-weight: bold; }
            .form-group input, .form-group select { width: 100%; padding: 10px; border: 1px solid #ced4da; border-radius: 5px; }
            .status { padding: 15px; border-radius: 8px; margin: 15px 0; }
            .status.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .status.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
            .status.info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
            .hidden { display: none !important; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Complete ETL System</h1>
                <p>Universal File Upload & Advanced Table Mapping</p>
            </div>
            
            <div class="tabs">
                <button class="tab active" onclick="switchTab('upload')">📤 Upload Files</button>
                <button class="tab" onclick="switchTab('mapping')">🔄 ETL Mapping</button>
            </div>
            
            <div class="content">
                <!-- Upload Tab -->
                <div id="uploadTab" class="tab-content">
                    <h2>📤 Upload Files to MongoDB</h2>
                    
                    <div class="category-grid" id="categoryGrid">
                        <!-- Categories will be loaded here -->
                    </div>
                    
                    <div class="upload-area" id="uploadArea">
                        <div class="upload-content">
                            <h3>📁 Drop your file here or click to browse</h3>
                            <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                            <input type="file" id="fileInput" accept=".xlsx,.xls,.csv" style="display: none;">
                            <button class="btn" onclick="document.getElementById('fileInput').click()">Choose File</button>
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="dateInput">Processing Date (optional - auto-extracted from filename):</label>
                        <input type="date" id="dateInput">
                    </div>
                    
                    <button class="btn" id="uploadBtn" onclick="uploadFile()" disabled>Upload File</button>
                    
                    <div id="uploadStatus"></div>
                </div>
                
                <!-- Mapping Tab -->
                <div id="mappingTab" class="tab-content hidden">
                    <h2>🔄 ETL Table Mapping</h2>
                    
                    <div class="form-group">
                        <label for="targetTableName">Target PostgreSQL Table Name:</label>
                        <input type="text" id="targetTableName" placeholder="e.g., unified_custody_master">
                    </div>
                    
                    <h3>📊 Available MongoDB Tables</h3>
                    <button class="btn" onclick="loadAvailableTables()">🔄 Refresh Tables</button>
                    
                    <div id="availableTables" class="table-grid">
                        <!-- Available tables will be loaded here -->
                    </div>
                    
                    <div id="mappingSection" class="mapping-section hidden">
                        <h3>🎯 Column Mapping</h3>
                        <button class="btn" onclick="addColumnMapping()">➕ Add Column Mapping</button>
                        <div id="columnMappings"></div>
                        
                        <button class="btn" onclick="processMapping()" style="margin-top: 20px;">🚀 Process to PostgreSQL</button>
                    </div>
                    
                    <div id="mappingStatus"></div>
                </div>
            </div>
        </div>
        
        <script>
            let selectedCategory = null;
            let selectedSubcategory = null;
            let selectedFile = null;
            let availableTablesData = {};
            let selectedTables = [];
            let columnMappings = {};
            
            // Initialize
            loadCategories();
            
            function switchTab(tabName) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
                
                event.target.classList.add('active');
                document.getElementById(tabName + 'Tab').classList.remove('hidden');
                
                if (tabName === 'mapping') {
                    loadAvailableTables();
                }
            }
            
            async function loadCategories() {
                try {
                    const response = await fetch('/api/upload/categories');
                    const data = await response.json();
                    
                    if (data.success) {
                        renderCategories(data.categories);
                    }
                } catch (error) {
                    console.error('Error loading categories:', error);
                }
            }
            
            function renderCategories(categories) {
                const grid = document.getElementById('categoryGrid');
                grid.innerHTML = '';
                
                Object.entries(categories).forEach(([key, category]) => {
                    const card = document.createElement('div');
                    card.className = 'category-card';
                    card.onclick = () => selectCategory(key, category);
                    
                    card.innerHTML = \`
                        <h3>\${category.icon} \${category.displayName}</h3>
                        <p>Click to select this category</p>
                        
                        \${category.hasSubcategories ? \`
                        <div class="subcategory-container" id="sub_\${key}">
                            <h4>Choose subcategory:</h4>
                            <div class="subcategory-grid">
                                \${category.subcategories.map(sub => \`
                                    <div class="subcategory-btn" onclick="selectSubcategory(event, '\${sub}')">\${sub.toUpperCase()}</div>
                                \`).join('')}
                            </div>
                        </div>
                        \` : ''}
                    \`;
                    
                    grid.appendChild(card);
                });
            }
            
            function selectCategory(key, category) {
                // Reset previous selections
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                
                // Select current category
                event.currentTarget.classList.add('selected');
                selectedCategory = key;
                
                if (category.hasSubcategories) {
                    document.getElementById(\`sub_\${key}\`).classList.add('show');
                    selectedSubcategory = null;
                } else {
                    selectedSubcategory = category.subcategories[0];
                }
                
                updateUploadButton();
            }
            
            function selectSubcategory(event, subcategory) {
                event.stopPropagation();
                
                // Reset subcategory selections
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                // Select current subcategory
                event.target.classList.add('selected');
                selectedSubcategory = subcategory;
                
                updateUploadButton();
            }
            
            function updateUploadButton() {
                const btn = document.getElementById('uploadBtn');
                btn.disabled = !(selectedCategory && selectedSubcategory && selectedFile);
            }
            
            // File handling
            const fileInput = document.getElementById('fileInput');
            const uploadArea = document.getElementById('uploadArea');
            
            fileInput.addEventListener('change', handleFileSelect);
            
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('drag-over');
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('drag-over');
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('drag-over');
                const files = e.dataTransfer.files;
                if (files.length > 0) {
                    fileInput.files = files;
                    handleFileSelect();
                }
            });
            
            function handleFileSelect() {
                const file = fileInput.files[0];
                if (file) {
                    selectedFile = file;
                    uploadArea.innerHTML = \`
                        <div class="upload-content">
                            <h3>✅ File Selected: \${file.name}</h3>
                            <p>Size: \${(file.size / (1024*1024)).toFixed(2)} MB</p>
                            <button class="btn" onclick="document.getElementById('fileInput').click()">Choose Different File</button>
                        </div>
                    \`;
                    updateUploadButton();
                }
            }
            
            async function uploadFile() {
                if (!selectedCategory || !selectedSubcategory || !selectedFile) {
                    showStatus('uploadStatus', 'Please select category and file', 'error');
                    return;
                }
                
                const formData = new FormData();
                formData.append('file', selectedFile);
                
                const processingDate = document.getElementById('dateInput').value;
                if (processingDate) {
                    formData.append('processingDate', processingDate);
                }
                
                try {
                    showStatus('uploadStatus', 'Uploading file...', 'info');
                    
                    const url = \`/api/upload/\${selectedCategory}/\${selectedSubcategory}\`;
                    const response = await fetch(url, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        showStatus('uploadStatus', \`✅ Upload successful! \${result.details.recordCount} records uploaded to \${result.details.collectionName}\`, 'success');
                        resetUploadForm();
                    } else {
                        showStatus('uploadStatus', \`❌ Upload failed: \${result.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('uploadStatus', \`❌ Upload error: \${error.message}\`, 'error');
                }
            }
            
            function resetUploadForm() {
                selectedCategory = null;
                selectedSubcategory = null;
                selectedFile = null;
                fileInput.value = '';
                document.getElementById('dateInput').value = '';
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                uploadArea.innerHTML = \`
                    <div class="upload-content">
                        <h3>📁 Drop your file here or click to browse</h3>
                        <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                        <button class="btn" onclick="document.getElementById('fileInput').click()">Choose File</button>
                    </div>
                \`;
                
                updateUploadButton();
            }
            
            // Mapping functionality
            async function loadAvailableTables() {
                try {
                    showStatus('mappingStatus', 'Loading available tables...', 'info');
                    
                    const response = await fetch('/api/mapping/available-tables');
                    const data = await response.json();
                    
                    if (data.success) {
                        availableTablesData = data.availableTables;
                        renderAvailableTables(data.availableTables);
                        showStatus('mappingStatus', \`✅ Loaded \${data.count} available tables\`, 'success');
                    } else {
                        showStatus('mappingStatus', \`❌ Error loading tables: \${data.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('mappingStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function renderAvailableTables(tables) {
                const container = document.getElementById('availableTables');
                container.innerHTML = '';
                
                Object.entries(tables).forEach(([id, table]) => {
                    const card = document.createElement('div');
                    card.className = 'table-card';
                    card.onclick = () => toggleTableSelection(id, card);
                    
                    card.innerHTML = \`
                        <h4>\${table.icon} \${table.displayName}</h4>
                        <p><strong>Records:</strong> \${table.recordCount}</p>
                        <p><strong>Date:</strong> \${table.date}</p>
                        <p><strong>Version:</strong> v\${table.latestVersion}</p>
                        <p><strong>Columns:</strong> \${table.columns.length}</p>
                    \`;
                    
                    container.appendChild(card);
                });
            }
            
            function toggleTableSelection(tableId, cardElement) {
                const index = selectedTables.indexOf(tableId);
                
                if (index === -1) {
                    selectedTables.push(tableId);
                    cardElement.classList.add('selected');
                } else {
                    selectedTables.splice(index, 1);
                    cardElement.classList.remove('selected');
                }
                
                if (selectedTables.length > 0) {
                    document.getElementById('mappingSection').classList.remove('hidden');
                    updateColumnMappings();
                } else {
                    document.getElementById('mappingSection').classList.add('hidden');
                }
            }
            
            function addColumnMapping() {
                const targetColumn = prompt('Enter target column name:');
                if (targetColumn && !columnMappings[targetColumn]) {
                    columnMappings[targetColumn] = {};
                    updateColumnMappings();
                }
            }
            
            function updateColumnMappings() {
                const container = document.getElementById('columnMappings');
                container.innerHTML = '';
                
                Object.keys(columnMappings).forEach(targetColumn => {
                    const mappingDiv = document.createElement('div');
                    mappingDiv.className = 'column-mapping';
                    
                    let html = \`<h4>Target Column: \${targetColumn}</h4>\`;
                    
                    selectedTables.forEach(tableId => {
                        const table = availableTablesData[tableId];
                        html += \`
                            <div class="form-group">
                                <label>Map from \${table.displayName}:</label>
                                <select onchange="updateMapping('\${targetColumn}', '\${tableId}', this.value)">
                                    <option value="">-- Select Column --</option>
                                    \${table.columns.map(col => \`
                                        <option value="\${col}" \${columnMappings[targetColumn][tableId] === col ? 'selected' : ''}>\${col}</option>
                                    \`).join('')}
                                </select>
                            </div>
                        \`;
                    });
                    
                    html += \`<button class="btn" onclick="removeColumnMapping('\${targetColumn}')" style="background: #dc3545;">Remove Column</button>\`;
                    
                    mappingDiv.innerHTML = html;
                    container.appendChild(mappingDiv);
                });
            }
            
            function updateMapping(targetColumn, tableId, sourceColumn) {
                columnMappings[targetColumn][tableId] = sourceColumn;
            }
            
            function removeColumnMapping(targetColumn) {
                delete columnMappings[targetColumn];
                updateColumnMappings();
            }
            
            async function processMapping() {
                const tableName = document.getElementById('targetTableName').value.trim();
                
                if (!tableName) {
                    showStatus('mappingStatus', 'Please enter a target table name', 'error');
                    return;
                }
                
                if (selectedTables.length === 0) {
                    showStatus('mappingStatus', 'Please select at least one source table', 'error');
                    return;
                }
                
                if (Object.keys(columnMappings).length === 0) {
                    showStatus('mappingStatus', 'Please add at least one column mapping', 'error');
                    return;
                }
                
                try {
                    showStatus('mappingStatus', 'Processing mapping to PostgreSQL...', 'info');
                    
                    const response = await fetch('/api/mapping/process', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            tableName: tableName,
                            mappings: columnMappings,
                            selectedTables: selectedTables
                        })
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        showStatus('mappingStatus', \`✅ Successfully processed \${result.details.processedRecords} records to table "\${result.details.tableName}"\`, 'success');
                    } else {
                        showStatus('mappingStatus', \`❌ Processing failed: \${result.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('mappingStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function showStatus(elementId, message, type) {
                const element = document.getElementById(elementId);
                element.innerHTML = \`<div class="status \${type}">\${message}</div>\`;
            }
        </script>
    </body>
    </html>
    `);
});

// API Routes
app.get('/api/upload/categories', (req, res) => {
    res.json({ success: true, categories: DATA_CATEGORIES, timestamp: new Date().toISOString() });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    let tempFilePath = null;
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        if (!DATA_CATEGORIES[category]) {
            return res.status(400).json({ success: false, error: `Invalid category: ${category}` });
        }
        
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        
        try {
            const collectionName = `${category}.${finalDate}.${finalSubcategory}`;
            
            const enhancedData = processedData.map((record, index) => ({
                ...record,
                recordIndex: index + 1,
                fileName: req.file.originalname,
                fileSize: req.file.size,
                category: category,
                subcategory: finalSubcategory,
                uploadedAt: new Date().toISOString(),
                processingDate: finalDate
            }));
            
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            
            // Update file tracker
            const trackerId = `${category}.${finalSubcategory}`;
            await db.collection('file_versions_tracker').replaceOne(
                { _id: trackerId },
                {
                    _id: trackerId,
                    category: category,
                    subcategory: finalSubcategory,
                    latestCollection: collectionName,
                    latestDate: finalDate,
                    fileName: req.file.originalname,
                    recordCount: enhancedData.length,
                    fileSize: req.file.size,
                    latestUpload: new Date().toISOString()
                },
                { upsert: true }
            );
            
        } finally {
            await client.close();
        }
        
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
        res.json({
            success: true,
            message: 'File uploaded successfully',
            details: {
                category: DATA_CATEGORIES[category].displayName,
                subcategory: finalSubcategory,
                fileName: req.file.originalname,
                recordCount: enhancedData.length,
                collectionName: collectionName,
                processingDate: finalDate
            }
        });
        
    } catch (error) {
        console.error('Upload error:', error.message);
        
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
        res.status(500).json({
            success: false,
            error: 'Upload failed',
            details: error.message
        });
    }
});

app.get('/api/mapping/available-tables', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const availableFiles = await db.collection('file_versions_tracker').find({}).toArray();
        const tableDetails = {};
        
        for (const file of availableFiles) {
            try {
                const sampleRecord = await db.collection(file.latestCollection).findOne({});
                
                if (sampleRecord) {
                    const cleanSample = { ...sampleRecord };
                    delete cleanSample._id;
                    delete cleanSample.recordIndex;
                    delete cleanSample.fileName;
                    delete cleanSample.fileSize;
                    delete cleanSample.category;
                    delete cleanSample.subcategory;
                    delete cleanSample.uploadedAt;
                    delete cleanSample.processingDate;
                    
                    const tableId = `${file.category}.${file.subcategory}`;
                    tableDetails[tableId] = {
                        id: tableId,
                        category: file.category,
                        subcategory: file.subcategory,
                        displayName: `${DATA_CATEGORIES[file.category]?.displayName || file.category} - ${file.subcategory}`,
                        fileName: file.fileName,
                        recordCount: file.recordCount,
                        latestCollection: file.latestCollection,
                        date: file.latestDate,
                        columns: Object.keys(cleanSample),
                        icon: DATA_CATEGORIES[file.category]?.icon || '📄'
                    };
                }
            } catch (error) {
                console.error(`Error processing ${file.subcategory}:`, error);
            }
        }
        
        await client.close();
        
        res.json({
            success: true,
            availableTables: tableDetails,
            count: Object.keys(tableDetails).length
        });
        
    } catch (error) {
        console.error('Error getting available tables:', error);
        res.status(500).json({ success: false, error: 'Failed to get available tables', details: error.message });
    }
});

app.post('/api/mapping/process', async (req, res) => {
    try {
        const { tableName, mappings, selectedTables } = req.body;
        
        if (!tableName || !mappings || !selectedTables || selectedTables.length === 0) {
            return res.status(400).json({ success: false, error: 'Table name, mappings, and selected tables are required' });
        }
        
        const mongoClient = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await mongoClient.connect();
        const db = mongoClient.db('financial_data_2025');
        
        const pgPool = new Pool(config.postgresql);
        
        let processedRecords = 0;
        
        try {
            // Create table
            const columnDefinitions = Object.keys(mappings).map(col => `"${col}" TEXT`).join(', ');
            const createTableSQL = `
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                    id SERIAL PRIMARY KEY,
                    ${columnDefinitions},
                    source_table TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `;
            
            await pgPool.query(createTableSQL);
            console.log(`✅ Created/verified table: ${tableName}`);
            
            // Process each selected table
            for (const tableId of selectedTables) {
                const fileTracker = await db.collection('file_versions_tracker').findOne({ _id: tableId });
                
                if (!fileTracker) {
                    console.log(`⚠️ File tracker not found for ${tableId}`);
                    continue;
                }
                
                const records = await db.collection(fileTracker.latestCollection).find({}).toArray();
                console.log(`📊 Found ${records.length} records in ${fileTracker.latestCollection}`);
                
                // Transform and insert records
                for (const record of records) {
                    const transformedRecord = {};
                    
                    Object.keys(mappings).forEach(targetColumn => {
                        const sourceColumn = mappings[targetColumn][tableId];
                        if (sourceColumn && record[sourceColumn] !== undefined) {
                            transformedRecord[targetColumn] = record[sourceColumn];
                        } else {
                            transformedRecord[targetColumn] = null;
                        }
                    });
                    
                    transformedRecord.source_table = tableId;
                    
                    const columns = Object.keys(transformedRecord);
                    const values = columns.map(col => transformedRecord[col]);
                    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
                    const columnNames = columns.map(col => `"${col}"`).join(', ');
                    
                    const insertSQL = `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`;
                    await pgPool.query(insertSQL, values);
                    processedRecords++;
                }
                
                console.log(`✅ Processed ${records.length} records from ${tableId}`);
            }
            
        } finally {
            await pgPool.end();
            await mongoClient.close();
        }
        
        res.json({
            success: true,
            message: 'Data processed successfully to PostgreSQL',
            details: {
                tableName: tableName,
                processedRecords: processedRecords,
                processedTables: selectedTables.length
            }
        });
        
    } catch (error) {
        console.error('Error processing to PostgreSQL:', error);
        res.status(500).json({ success: false, error: 'Failed to process data to PostgreSQL', details: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Complete ETL System running at http://localhost:${PORT}`);
    console.log(`✨ Features:`);
    console.log(`   📤 Universal file upload for all categories`);
    console.log(`   🔄 Advanced table selection and column mapping`);
    console.log(`   🏛️ MongoDB storage with smart categorization`);
    console.log(`   📊 PostgreSQL processing with unified tables`);
    console.log(`   🎯 Always on PORT 3000 - no more changing!`);
}); 
const multer = require('multer');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const app = express();
const PORT = 3000; // STAYING ON PORT 3000

// Storage configuration
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = './temp_uploads';
        if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        const timestamp = Date.now();
        const sanitized = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
        cb(null, `${timestamp}_${sanitized}`);
    }
});

const upload = multer({ 
    storage: storage,
    fileFilter: (req, file, cb) => {
        const allowedTypes = ['.xlsx', '.xls', '.csv'];
        const fileExt = path.extname(file.originalname).toLowerCase();
        if (allowedTypes.includes(fileExt)) {
            cb(null, true);
        } else {
            cb(new Error(`Invalid file type. Only ${allowedTypes.join(', ')} files are allowed.`), false);
        }
    },
    limits: { fileSize: 100 * 1024 * 1024, files: 1 }
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    if (req.method === 'OPTIONS') { res.sendStatus(200); } else { next(); }
});

const mongoUri = config.mongodb.uri + config.mongodb.database;

const DATA_CATEGORIES = {
    custody_files: { displayName: 'Custody Files', subcategories: ['hdfc', 'axis', 'kotak', 'orbis', 'others'], icon: '🏛️', hasSubcategories: true },
    stock_capital_flow: { displayName: 'Stock Capital Flow', subcategories: ['stock_capital_flow'], icon: '📊', hasSubcategories: false },
    cash_capital_flow: { displayName: 'Cash Capital Flow', subcategories: ['cash_capital_flow'], icon: '💰', hasSubcategories: false },
    distributor_master: { displayName: 'Distributor Master', subcategories: ['distributor_master'], icon: '🤝', hasSubcategories: false },
    contract_notes: { displayName: 'Contract Notes', subcategories: ['contract_notes'], icon: '📋', hasSubcategories: false },
    mf_allocations: { displayName: 'MF Allocations', subcategories: ['mf_allocations'], icon: '📈', hasSubcategories: false },
    strategy_master: { displayName: 'Strategy Master', subcategories: ['strategy_master'], icon: '🎯', hasSubcategories: false },
    client_info: { displayName: 'Client Info', subcategories: ['client_info'], icon: '👥', hasSubcategories: false },
    trades: { displayName: 'Trades', subcategories: ['trades'], icon: '💹', hasSubcategories: false }
};

// Helper functions
function extractDateFromFilename(filename) {
    try {
        const nameWithoutExt = filename.replace(/\.(xlsx?|csv)$/i, '');
        const cleanName = nameWithoutExt.toLowerCase();
        
        const datePatterns = [
            { pattern: /(\d{1,2})[-\/._](\d{1,2})[-\/._](\d{4})/, format: 'DMY' },
            { pattern: /(\d{4})[-\/._](\d{1,2})[-\/._](\d{1,2})/, format: 'YMD' },
            { pattern: /(\d{4})(\d{2})(\d{2})(?![0-9])/, format: 'YMD_COMPACT' },
        ];
        
        for (const { pattern, format } of datePatterns) {
            const match = cleanName.match(pattern);
            if (match) {
                let day, month, year;
                switch (format) {
                    case 'DMY': day = parseInt(match[1]); month = parseInt(match[2]); year = parseInt(match[3]); break;
                    case 'YMD': year = parseInt(match[1]); month = parseInt(match[2]); day = parseInt(match[3]); break;
                    case 'YMD_COMPACT': year = parseInt(match[1]); month = parseInt(match[2]); day = parseInt(match[3]); break;
                }
                
                if (year >= 2020 && year <= 2030 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                    return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
                }
            }
        }
        return new Date().toISOString().split('T')[0];
    } catch (error) {
        return new Date().toISOString().split('T')[0];
    }
}

async function processCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv({ skipEmptyLines: true, skipLinesWithError: true }))
            .on('data', (data) => {
                const cleanData = {};
                for (const [key, value] of Object.entries(data)) {
                    const cleanKey = key.trim();
                    const cleanValue = typeof value === 'string' ? value.trim() : value;
                    if (cleanKey && cleanValue !== '') {
                        cleanData[cleanKey] = cleanValue;
                    }
                }
                if (Object.keys(cleanData).length > 0) {
                    results.push(cleanData);
                }
            })
            .on('end', () => resolve(results))
            .on('error', (error) => reject(new Error(`CSV parsing failed: ${error.message}`)));
    });
}

async function processExcelFile(filePath) {
    try {
        const workbook = xlsx.readFile(filePath, { cellDates: true, cellNF: false, cellText: false });
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) throw new Error('No sheets found in Excel file');
        
        const worksheet = workbook.Sheets[sheetName];
        const jsonData = xlsx.utils.sheet_to_json(worksheet, { raw: false, defval: '', blankrows: false });
        
        return jsonData.map(row => {
            const cleanRow = {};
            for (const [key, value] of Object.entries(row)) {
                const cleanKey = key.trim();
                const cleanValue = typeof value === 'string' ? value.trim() : value;
                if (cleanKey && cleanValue !== '') {
                    cleanRow[cleanKey] = cleanValue;
                }
            }
            return cleanRow;
        }).filter(row => Object.keys(row).length > 0);
    } catch (error) {
        throw new Error(`Excel parsing failed: ${error.message}`);
    }
}

// Main page route
app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🚀 Complete ETL System</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                   background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; }
            .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
            .header { text-align: center; color: white; margin-bottom: 30px; }
            .header h1 { font-size: 2.5rem; margin-bottom: 10px; }
            .tabs { display: flex; justify-content: center; margin-bottom: 20px; }
            .tab { background: rgba(255,255,255,0.2); color: white; border: none; padding: 15px 30px; 
                   margin: 0 5px; border-radius: 10px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
            .tab.active { background: white; color: #667eea; }
            .tab:hover { background: rgba(255,255,255,0.3); }
            .content { background: white; border-radius: 15px; padding: 30px; box-shadow: 0 20px 40px rgba(0,0,0,0.1); }
            .category-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }
            .category-card { background: #f8f9fa; border: 2px solid #e9ecef; border-radius: 10px; padding: 20px; cursor: pointer; transition: all 0.3s; }
            .category-card:hover { border-color: #667eea; background: #f0f4ff; }
            .category-card.selected { border-color: #667eea; background: #e3f2fd; }
            .subcategory-container { margin-top: 15px; display: none; }
            .subcategory-container.show { display: block; }
            .subcategory-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }
            .subcategory-btn { background: #e9ecef; border: 1px solid #ced4da; border-radius: 5px; padding: 8px 12px; cursor: pointer; text-align: center; transition: all 0.3s; }
            .subcategory-btn:hover { background: #667eea; color: white; }
            .subcategory-btn.selected { background: #667eea; color: white; }
            .upload-area { border: 2px dashed #ced4da; border-radius: 10px; padding: 40px; text-align: center; margin: 20px 0; transition: all 0.3s; }
            .upload-area.drag-over { border-color: #667eea; background: #f0f4ff; }
            .btn { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
            .btn:hover { background: #5a6fd8; }
            .btn:disabled { background: #6c757d; cursor: not-allowed; }
            .table-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 15px; margin: 20px 0; }
            .table-card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; cursor: pointer; transition: all 0.3s; }
            .table-card:hover { background: #e9ecef; }
            .table-card.selected { background: #e3f2fd; border-color: #667eea; }
            .mapping-section { margin: 20px 0; }
            .column-mapping { background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 10px 0; }
            .form-group { margin: 15px 0; }
            .form-group label { display: block; margin-bottom: 5px; font-weight: bold; }
            .form-group input, .form-group select { width: 100%; padding: 10px; border: 1px solid #ced4da; border-radius: 5px; }
            .status { padding: 15px; border-radius: 8px; margin: 15px 0; }
            .status.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .status.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
            .status.info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
            .hidden { display: none !important; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Complete ETL System</h1>
                <p>Universal File Upload & Advanced Table Mapping</p>
            </div>
            
            <div class="tabs">
                <button class="tab active" onclick="switchTab('upload')">📤 Upload Files</button>
                <button class="tab" onclick="switchTab('mapping')">🔄 ETL Mapping</button>
            </div>
            
            <div class="content">
                <!-- Upload Tab -->
                <div id="uploadTab" class="tab-content">
                    <h2>📤 Upload Files to MongoDB</h2>
                    
                    <div class="category-grid" id="categoryGrid">
                        <!-- Categories will be loaded here -->
                    </div>
                    
                    <div class="upload-area" id="uploadArea">
                        <div class="upload-content">
                            <h3>📁 Drop your file here or click to browse</h3>
                            <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                            <input type="file" id="fileInput" accept=".xlsx,.xls,.csv" style="display: none;">
                            <button class="btn" onclick="document.getElementById('fileInput').click()">Choose File</button>
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="dateInput">Processing Date (optional - auto-extracted from filename):</label>
                        <input type="date" id="dateInput">
                    </div>
                    
                    <button class="btn" id="uploadBtn" onclick="uploadFile()" disabled>Upload File</button>
                    
                    <div id="uploadStatus"></div>
                </div>
                
                <!-- Mapping Tab -->
                <div id="mappingTab" class="tab-content hidden">
                    <h2>🔄 ETL Table Mapping</h2>
                    
                    <div class="form-group">
                        <label for="targetTableName">Target PostgreSQL Table Name:</label>
                        <input type="text" id="targetTableName" placeholder="e.g., unified_custody_master">
                    </div>
                    
                    <h3>📊 Available MongoDB Tables</h3>
                    <button class="btn" onclick="loadAvailableTables()">🔄 Refresh Tables</button>
                    
                    <div id="availableTables" class="table-grid">
                        <!-- Available tables will be loaded here -->
                    </div>
                    
                    <div id="mappingSection" class="mapping-section hidden">
                        <h3>🎯 Column Mapping</h3>
                        <button class="btn" onclick="addColumnMapping()">➕ Add Column Mapping</button>
                        <div id="columnMappings"></div>
                        
                        <button class="btn" onclick="processMapping()" style="margin-top: 20px;">🚀 Process to PostgreSQL</button>
                    </div>
                    
                    <div id="mappingStatus"></div>
                </div>
            </div>
        </div>
        
        <script>
            let selectedCategory = null;
            let selectedSubcategory = null;
            let selectedFile = null;
            let availableTablesData = {};
            let selectedTables = [];
            let columnMappings = {};
            
            // Initialize
            loadCategories();
            
            function switchTab(tabName) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
                
                event.target.classList.add('active');
                document.getElementById(tabName + 'Tab').classList.remove('hidden');
                
                if (tabName === 'mapping') {
                    loadAvailableTables();
                }
            }
            
            async function loadCategories() {
                try {
                    const response = await fetch('/api/upload/categories');
                    const data = await response.json();
                    
                    if (data.success) {
                        renderCategories(data.categories);
                    }
                } catch (error) {
                    console.error('Error loading categories:', error);
                }
            }
            
            function renderCategories(categories) {
                const grid = document.getElementById('categoryGrid');
                grid.innerHTML = '';
                
                Object.entries(categories).forEach(([key, category]) => {
                    const card = document.createElement('div');
                    card.className = 'category-card';
                    card.onclick = () => selectCategory(key, category);
                    
                    card.innerHTML = \`
                        <h3>\${category.icon} \${category.displayName}</h3>
                        <p>Click to select this category</p>
                        
                        \${category.hasSubcategories ? \`
                        <div class="subcategory-container" id="sub_\${key}">
                            <h4>Choose subcategory:</h4>
                            <div class="subcategory-grid">
                                \${category.subcategories.map(sub => \`
                                    <div class="subcategory-btn" onclick="selectSubcategory(event, '\${sub}')">\${sub.toUpperCase()}</div>
                                \`).join('')}
                            </div>
                        </div>
                        \` : ''}
                    \`;
                    
                    grid.appendChild(card);
                });
            }
            
            function selectCategory(key, category) {
                // Reset previous selections
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                
                // Select current category
                event.currentTarget.classList.add('selected');
                selectedCategory = key;
                
                if (category.hasSubcategories) {
                    document.getElementById(\`sub_\${key}\`).classList.add('show');
                    selectedSubcategory = null;
                } else {
                    selectedSubcategory = category.subcategories[0];
                }
                
                updateUploadButton();
            }
            
            function selectSubcategory(event, subcategory) {
                event.stopPropagation();
                
                // Reset subcategory selections
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                // Select current subcategory
                event.target.classList.add('selected');
                selectedSubcategory = subcategory;
                
                updateUploadButton();
            }
            
            function updateUploadButton() {
                const btn = document.getElementById('uploadBtn');
                btn.disabled = !(selectedCategory && selectedSubcategory && selectedFile);
            }
            
            // File handling
            const fileInput = document.getElementById('fileInput');
            const uploadArea = document.getElementById('uploadArea');
            
            fileInput.addEventListener('change', handleFileSelect);
            
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('drag-over');
            });
            
            uploadArea.addEventListener('dragleave', () => {
                uploadArea.classList.remove('drag-over');
            });
            
            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('drag-over');
                const files = e.dataTransfer.files;
                if (files.length > 0) {
                    fileInput.files = files;
                    handleFileSelect();
                }
            });
            
            function handleFileSelect() {
                const file = fileInput.files[0];
                if (file) {
                    selectedFile = file;
                    uploadArea.innerHTML = \`
                        <div class="upload-content">
                            <h3>✅ File Selected: \${file.name}</h3>
                            <p>Size: \${(file.size / (1024*1024)).toFixed(2)} MB</p>
                            <button class="btn" onclick="document.getElementById('fileInput').click()">Choose Different File</button>
                        </div>
                    \`;
                    updateUploadButton();
                }
            }
            
            async function uploadFile() {
                if (!selectedCategory || !selectedSubcategory || !selectedFile) {
                    showStatus('uploadStatus', 'Please select category and file', 'error');
                    return;
                }
                
                const formData = new FormData();
                formData.append('file', selectedFile);
                
                const processingDate = document.getElementById('dateInput').value;
                if (processingDate) {
                    formData.append('processingDate', processingDate);
                }
                
                try {
                    showStatus('uploadStatus', 'Uploading file...', 'info');
                    
                    const url = \`/api/upload/\${selectedCategory}/\${selectedSubcategory}\`;
                    const response = await fetch(url, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        showStatus('uploadStatus', \`✅ Upload successful! \${result.details.recordCount} records uploaded to \${result.details.collectionName}\`, 'success');
                        resetUploadForm();
                    } else {
                        showStatus('uploadStatus', \`❌ Upload failed: \${result.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('uploadStatus', \`❌ Upload error: \${error.message}\`, 'error');
                }
            }
            
            function resetUploadForm() {
                selectedCategory = null;
                selectedSubcategory = null;
                selectedFile = null;
                fileInput.value = '';
                document.getElementById('dateInput').value = '';
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                uploadArea.innerHTML = \`
                    <div class="upload-content">
                        <h3>📁 Drop your file here or click to browse</h3>
                        <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                        <button class="btn" onclick="document.getElementById('fileInput').click()">Choose File</button>
                    </div>
                \`;
                
                updateUploadButton();
            }
            
            // Mapping functionality
            async function loadAvailableTables() {
                try {
                    showStatus('mappingStatus', 'Loading available tables...', 'info');
                    
                    const response = await fetch('/api/mapping/available-tables');
                    const data = await response.json();
                    
                    if (data.success) {
                        availableTablesData = data.availableTables;
                        renderAvailableTables(data.availableTables);
                        showStatus('mappingStatus', \`✅ Loaded \${data.count} available tables\`, 'success');
                    } else {
                        showStatus('mappingStatus', \`❌ Error loading tables: \${data.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('mappingStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function renderAvailableTables(tables) {
                const container = document.getElementById('availableTables');
                container.innerHTML = '';
                
                Object.entries(tables).forEach(([id, table]) => {
                    const card = document.createElement('div');
                    card.className = 'table-card';
                    card.onclick = () => toggleTableSelection(id, card);
                    
                    card.innerHTML = \`
                        <h4>\${table.icon} \${table.displayName}</h4>
                        <p><strong>Records:</strong> \${table.recordCount}</p>
                        <p><strong>Date:</strong> \${table.date}</p>
                        <p><strong>Version:</strong> v\${table.latestVersion}</p>
                        <p><strong>Columns:</strong> \${table.columns.length}</p>
                    \`;
                    
                    container.appendChild(card);
                });
            }
            
            function toggleTableSelection(tableId, cardElement) {
                const index = selectedTables.indexOf(tableId);
                
                if (index === -1) {
                    selectedTables.push(tableId);
                    cardElement.classList.add('selected');
                } else {
                    selectedTables.splice(index, 1);
                    cardElement.classList.remove('selected');
                }
                
                if (selectedTables.length > 0) {
                    document.getElementById('mappingSection').classList.remove('hidden');
                    updateColumnMappings();
                } else {
                    document.getElementById('mappingSection').classList.add('hidden');
                }
            }
            
            function addColumnMapping() {
                const targetColumn = prompt('Enter target column name:');
                if (targetColumn && !columnMappings[targetColumn]) {
                    columnMappings[targetColumn] = {};
                    updateColumnMappings();
                }
            }
            
            function updateColumnMappings() {
                const container = document.getElementById('columnMappings');
                container.innerHTML = '';
                
                Object.keys(columnMappings).forEach(targetColumn => {
                    const mappingDiv = document.createElement('div');
                    mappingDiv.className = 'column-mapping';
                    
                    let html = \`<h4>Target Column: \${targetColumn}</h4>\`;
                    
                    selectedTables.forEach(tableId => {
                        const table = availableTablesData[tableId];
                        html += \`
                            <div class="form-group">
                                <label>Map from \${table.displayName}:</label>
                                <select onchange="updateMapping('\${targetColumn}', '\${tableId}', this.value)">
                                    <option value="">-- Select Column --</option>
                                    \${table.columns.map(col => \`
                                        <option value="\${col}" \${columnMappings[targetColumn][tableId] === col ? 'selected' : ''}>\${col}</option>
                                    \`).join('')}
                                </select>
                            </div>
                        \`;
                    });
                    
                    html += \`<button class="btn" onclick="removeColumnMapping('\${targetColumn}')" style="background: #dc3545;">Remove Column</button>\`;
                    
                    mappingDiv.innerHTML = html;
                    container.appendChild(mappingDiv);
                });
            }
            
            function updateMapping(targetColumn, tableId, sourceColumn) {
                columnMappings[targetColumn][tableId] = sourceColumn;
            }
            
            function removeColumnMapping(targetColumn) {
                delete columnMappings[targetColumn];
                updateColumnMappings();
            }
            
            async function processMapping() {
                const tableName = document.getElementById('targetTableName').value.trim();
                
                if (!tableName) {
                    showStatus('mappingStatus', 'Please enter a target table name', 'error');
                    return;
                }
                
                if (selectedTables.length === 0) {
                    showStatus('mappingStatus', 'Please select at least one source table', 'error');
                    return;
                }
                
                if (Object.keys(columnMappings).length === 0) {
                    showStatus('mappingStatus', 'Please add at least one column mapping', 'error');
                    return;
                }
                
                try {
                    showStatus('mappingStatus', 'Processing mapping to PostgreSQL...', 'info');
                    
                    const response = await fetch('/api/mapping/process', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({
                            tableName: tableName,
                            mappings: columnMappings,
                            selectedTables: selectedTables
                        })
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        showStatus('mappingStatus', \`✅ Successfully processed \${result.details.processedRecords} records to table "\${result.details.tableName}"\`, 'success');
                    } else {
                        showStatus('mappingStatus', \`❌ Processing failed: \${result.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('mappingStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function showStatus(elementId, message, type) {
                const element = document.getElementById(elementId);
                element.innerHTML = \`<div class="status \${type}">\${message}</div>\`;
            }
        </script>
    </body>
    </html>
    `);
});

// API Routes
app.get('/api/upload/categories', (req, res) => {
    res.json({ success: true, categories: DATA_CATEGORIES, timestamp: new Date().toISOString() });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    let tempFilePath = null;
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        if (!DATA_CATEGORIES[category]) {
            return res.status(400).json({ success: false, error: `Invalid category: ${category}` });
        }
        
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        
        try {
            const collectionName = `${category}.${finalDate}.${finalSubcategory}`;
            
            const enhancedData = processedData.map((record, index) => ({
                ...record,
                recordIndex: index + 1,
                fileName: req.file.originalname,
                fileSize: req.file.size,
                category: category,
                subcategory: finalSubcategory,
                uploadedAt: new Date().toISOString(),
                processingDate: finalDate
            }));
            
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            
            // Update file tracker
            const trackerId = `${category}.${finalSubcategory}`;
            await db.collection('file_versions_tracker').replaceOne(
                { _id: trackerId },
                {
                    _id: trackerId,
                    category: category,
                    subcategory: finalSubcategory,
                    latestCollection: collectionName,
                    latestDate: finalDate,
                    fileName: req.file.originalname,
                    recordCount: enhancedData.length,
                    fileSize: req.file.size,
                    latestUpload: new Date().toISOString()
                },
                { upsert: true }
            );
            
        } finally {
            await client.close();
        }
        
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
        res.json({
            success: true,
            message: 'File uploaded successfully',
            details: {
                category: DATA_CATEGORIES[category].displayName,
                subcategory: finalSubcategory,
                fileName: req.file.originalname,
                recordCount: enhancedData.length,
                collectionName: collectionName,
                processingDate: finalDate
            }
        });
        
    } catch (error) {
        console.error('Upload error:', error.message);
        
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
        res.status(500).json({
            success: false,
            error: 'Upload failed',
            details: error.message
        });
    }
});

app.get('/api/mapping/available-tables', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const availableFiles = await db.collection('file_versions_tracker').find({}).toArray();
        const tableDetails = {};
        
        for (const file of availableFiles) {
            try {
                const sampleRecord = await db.collection(file.latestCollection).findOne({});
                
                if (sampleRecord) {
                    const cleanSample = { ...sampleRecord };
                    delete cleanSample._id;
                    delete cleanSample.recordIndex;
                    delete cleanSample.fileName;
                    delete cleanSample.fileSize;
                    delete cleanSample.category;
                    delete cleanSample.subcategory;
                    delete cleanSample.uploadedAt;
                    delete cleanSample.processingDate;
                    
                    const tableId = `${file.category}.${file.subcategory}`;
                    tableDetails[tableId] = {
                        id: tableId,
                        category: file.category,
                        subcategory: file.subcategory,
                        displayName: `${DATA_CATEGORIES[file.category]?.displayName || file.category} - ${file.subcategory}`,
                        fileName: file.fileName,
                        recordCount: file.recordCount,
                        latestCollection: file.latestCollection,
                        date: file.latestDate,
                        columns: Object.keys(cleanSample),
                        icon: DATA_CATEGORIES[file.category]?.icon || '📄'
                    };
                }
            } catch (error) {
                console.error(`Error processing ${file.subcategory}:`, error);
            }
        }
        
        await client.close();
        
        res.json({
            success: true,
            availableTables: tableDetails,
            count: Object.keys(tableDetails).length
        });
        
    } catch (error) {
        console.error('Error getting available tables:', error);
        res.status(500).json({ success: false, error: 'Failed to get available tables', details: error.message });
    }
});

app.post('/api/mapping/process', async (req, res) => {
    try {
        const { tableName, mappings, selectedTables } = req.body;
        
        if (!tableName || !mappings || !selectedTables || selectedTables.length === 0) {
            return res.status(400).json({ success: false, error: 'Table name, mappings, and selected tables are required' });
        }
        
        const mongoClient = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await mongoClient.connect();
        const db = mongoClient.db('financial_data_2025');
        
        const pgPool = new Pool(config.postgresql);
        
        let processedRecords = 0;
        
        try {
            // Create table
            const columnDefinitions = Object.keys(mappings).map(col => `"${col}" TEXT`).join(', ');
            const createTableSQL = `
                CREATE TABLE IF NOT EXISTS "${tableName}" (
                    id SERIAL PRIMARY KEY,
                    ${columnDefinitions},
                    source_table TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            `;
            
            await pgPool.query(createTableSQL);
            console.log(`✅ Created/verified table: ${tableName}`);
            
            // Process each selected table
            for (const tableId of selectedTables) {
                const fileTracker = await db.collection('file_versions_tracker').findOne({ _id: tableId });
                
                if (!fileTracker) {
                    console.log(`⚠️ File tracker not found for ${tableId}`);
                    continue;
                }
                
                const records = await db.collection(fileTracker.latestCollection).find({}).toArray();
                console.log(`📊 Found ${records.length} records in ${fileTracker.latestCollection}`);
                
                // Transform and insert records
                for (const record of records) {
                    const transformedRecord = {};
                    
                    Object.keys(mappings).forEach(targetColumn => {
                        const sourceColumn = mappings[targetColumn][tableId];
                        if (sourceColumn && record[sourceColumn] !== undefined) {
                            transformedRecord[targetColumn] = record[sourceColumn];
                        } else {
                            transformedRecord[targetColumn] = null;
                        }
                    });
                    
                    transformedRecord.source_table = tableId;
                    
                    const columns = Object.keys(transformedRecord);
                    const values = columns.map(col => transformedRecord[col]);
                    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
                    const columnNames = columns.map(col => `"${col}"`).join(', ');
                    
                    const insertSQL = `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`;
                    await pgPool.query(insertSQL, values);
                    processedRecords++;
                }
                
                console.log(`✅ Processed ${records.length} records from ${tableId}`);
            }
            
        } finally {
            await pgPool.end();
            await mongoClient.close();
        }
        
        res.json({
            success: true,
            message: 'Data processed successfully to PostgreSQL',
            details: {
                tableName: tableName,
                processedRecords: processedRecords,
                processedTables: selectedTables.length
            }
        });
        
    } catch (error) {
        console.error('Error processing to PostgreSQL:', error);
        res.status(500).json({ success: false, error: 'Failed to process data to PostgreSQL', details: error.message });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Complete ETL System running at http://localhost:${PORT}`);
    console.log(`✨ Features:`);
    console.log(`   📤 Universal file upload for all categories`);
    console.log(`   🔄 Advanced table selection and column mapping`);
    console.log(`   🏛️ MongoDB storage with smart categorization`);
    console.log(`   📊 PostgreSQL processing with unified tables`);
    console.log(`   🎯 Always on PORT 3000 - no more changing!`);
}); 