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
const PORT = 3000;

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
        <title>🚀 Debugged ETL System</title>
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
                <h1>🚀 Debugged ETL System</h1>
                <p>Fixed Upload Issues & Complete Functionality</p>
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
                    <p>Coming after successful upload testing...</p>
                </div>
            </div>
        </div>
        
        <script>
            let selectedCategory = null;
            let selectedSubcategory = null;
            let selectedFile = null;
            
            // Initialize
            loadCategories();
            
            function switchTab(tabName) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
                
                event.target.classList.add('active');
                document.getElementById(tabName + 'Tab').classList.remove('hidden');
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
                console.log('Selected category:', key, category);
                
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
                console.log('Selected subcategory:', subcategory);
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
                console.log('Update button:', { selectedCategory, selectedSubcategory, selectedFile, disabled: btn.disabled });
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
                    console.log('File selected:', file.name);
                    updateUploadButton();
                }
            }
            
            async function uploadFile() {
                console.log('Upload started:', { selectedCategory, selectedSubcategory, selectedFile: selectedFile?.name });
                
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
                    console.log('Upload URL:', url);
                    
                    const response = await fetch(url, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    console.log('Upload result:', result);
                    
                    if (result.success) {
                        showStatus('uploadStatus', \`✅ Upload successful! \${result.details.recordCount} records uploaded to \${result.details.collectionName}\`, 'success');
                        resetUploadForm();
                    } else {
                        showStatus('uploadStatus', \`❌ Upload failed: \${result.error}. Details: \${result.details || 'No details'}\`, 'error');
                    }
                } catch (error) {
                    console.error('Upload error:', error);
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
    console.log('📋 Categories requested');
    res.json({ success: true, categories: DATA_CATEGORIES, timestamp: new Date().toISOString() });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    console.log('📤 Upload started:', req.params, req.file?.originalname);
    
    let tempFilePath = null;
    let collectionName = null;
    let enhancedData = [];
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        console.log('🔍 Processing:', { category, subcategory, processingDate });
        
        if (!DATA_CATEGORIES[category]) {
            console.log('❌ Invalid category:', category);
            return res.status(400).json({ success: false, error: `Invalid category: ${category}` });
        }
        
        if (!req.file) {
            console.log('❌ No file uploaded');
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        console.log('📁 Temp file path:', tempFilePath);
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        console.log('📅 Date processing:', { extracted: extractedDate, final: finalDate, subcategory: finalSubcategory });
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        console.log('🔧 Processing file type:', fileExt);
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
        }
        
        console.log('📊 Processed data:', processedData.length, 'records');
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        // Define collection name before MongoDB operations
        collectionName = `${category}.${finalDate}.${finalSubcategory}`;
        console.log('🗂️ Collection name:', collectionName);
        
        // Enhance data before MongoDB operations
        enhancedData = processedData.map((record, index) => ({
            ...record,
            recordIndex: index + 1,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            category: category,
            subcategory: finalSubcategory,
            uploadedAt: new Date().toISOString(),
            processingDate: finalDate
        }));
        
        console.log('✨ Enhanced data created:', enhancedData.length, 'records');
        
        // MongoDB operations
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        console.log('🔌 MongoDB connected');
        
        try {
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            console.log('✅ Data inserted into MongoDB');
            
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
            console.log('📋 File tracker updated');
            
        } finally {
            await client.close();
            console.log('🔌 MongoDB disconnected');
        }
        
        // Cleanup temp file
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
            console.log('🧹 Temp file cleaned up');
        }
        
        // Success response
        const response = {
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
        };
        
        console.log('✅ Upload successful:', response.details);
        res.json(response);
        
    } catch (error) {
        console.error('❌ Upload error:', error.message);
        console.error('❌ Stack trace:', error.stack);
        
        // Cleanup temp file on error
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
            console.log('🧹 Temp file cleaned up (error case)');
        }
        
        res.status(500).json({
            success: false,
            error: 'Upload failed',
            details: error.message,
            stack: error.stack
        });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Debugged ETL System running at http://localhost:${PORT}`);
    console.log(`🔍 Debug mode: All upload steps will be logged`);
    console.log(`✨ Features:`);
    console.log(`   📤 Universal file upload with detailed logging`);
    console.log(`   🔧 All variable scoping issues fixed`);
    console.log(`   🎯 Enhanced error handling and debugging`);
    console.log(`   📋 Step-by-step process logging`);
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
const PORT = 3000;

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
        <title>🚀 Debugged ETL System</title>
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
                <h1>🚀 Debugged ETL System</h1>
                <p>Fixed Upload Issues & Complete Functionality</p>
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
                    <p>Coming after successful upload testing...</p>
                </div>
            </div>
        </div>
        
        <script>
            let selectedCategory = null;
            let selectedSubcategory = null;
            let selectedFile = null;
            
            // Initialize
            loadCategories();
            
            function switchTab(tabName) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
                
                event.target.classList.add('active');
                document.getElementById(tabName + 'Tab').classList.remove('hidden');
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
                console.log('Selected category:', key, category);
                
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
                console.log('Selected subcategory:', subcategory);
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
                console.log('Update button:', { selectedCategory, selectedSubcategory, selectedFile, disabled: btn.disabled });
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
                    console.log('File selected:', file.name);
                    updateUploadButton();
                }
            }
            
            async function uploadFile() {
                console.log('Upload started:', { selectedCategory, selectedSubcategory, selectedFile: selectedFile?.name });
                
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
                    console.log('Upload URL:', url);
                    
                    const response = await fetch(url, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    console.log('Upload result:', result);
                    
                    if (result.success) {
                        showStatus('uploadStatus', \`✅ Upload successful! \${result.details.recordCount} records uploaded to \${result.details.collectionName}\`, 'success');
                        resetUploadForm();
                    } else {
                        showStatus('uploadStatus', \`❌ Upload failed: \${result.error}. Details: \${result.details || 'No details'}\`, 'error');
                    }
                } catch (error) {
                    console.error('Upload error:', error);
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
    console.log('📋 Categories requested');
    res.json({ success: true, categories: DATA_CATEGORIES, timestamp: new Date().toISOString() });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    console.log('📤 Upload started:', req.params, req.file?.originalname);
    
    let tempFilePath = null;
    let collectionName = null;
    let enhancedData = [];
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        console.log('🔍 Processing:', { category, subcategory, processingDate });
        
        if (!DATA_CATEGORIES[category]) {
            console.log('❌ Invalid category:', category);
            return res.status(400).json({ success: false, error: `Invalid category: ${category}` });
        }
        
        if (!req.file) {
            console.log('❌ No file uploaded');
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        console.log('📁 Temp file path:', tempFilePath);
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        console.log('📅 Date processing:', { extracted: extractedDate, final: finalDate, subcategory: finalSubcategory });
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        console.log('🔧 Processing file type:', fileExt);
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
        }
        
        console.log('📊 Processed data:', processedData.length, 'records');
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        // Define collection name before MongoDB operations
        collectionName = `${category}.${finalDate}.${finalSubcategory}`;
        console.log('🗂️ Collection name:', collectionName);
        
        // Enhance data before MongoDB operations
        enhancedData = processedData.map((record, index) => ({
            ...record,
            recordIndex: index + 1,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            category: category,
            subcategory: finalSubcategory,
            uploadedAt: new Date().toISOString(),
            processingDate: finalDate
        }));
        
        console.log('✨ Enhanced data created:', enhancedData.length, 'records');
        
        // MongoDB operations
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        console.log('🔌 MongoDB connected');
        
        try {
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            console.log('✅ Data inserted into MongoDB');
            
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
            console.log('📋 File tracker updated');
            
        } finally {
            await client.close();
            console.log('🔌 MongoDB disconnected');
        }
        
        // Cleanup temp file
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
            console.log('🧹 Temp file cleaned up');
        }
        
        // Success response
        const response = {
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
        };
        
        console.log('✅ Upload successful:', response.details);
        res.json(response);
        
    } catch (error) {
        console.error('❌ Upload error:', error.message);
        console.error('❌ Stack trace:', error.stack);
        
        // Cleanup temp file on error
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
            console.log('🧹 Temp file cleaned up (error case)');
        }
        
        res.status(500).json({
            success: false,
            error: 'Upload failed',
            details: error.message,
            stack: error.stack
        });
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Debugged ETL System running at http://localhost:${PORT}`);
    console.log(`🔍 Debug mode: All upload steps will be logged`);
    console.log(`✨ Features:`);
    console.log(`   📤 Universal file upload with detailed logging`);
    console.log(`   🔧 All variable scoping issues fixed`);
    console.log(`   🎯 Enhanced error handling and debugging`);
    console.log(`   📋 Step-by-step process logging`);
}); 