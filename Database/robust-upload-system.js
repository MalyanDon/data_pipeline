const express = require('express');
const multer = require('multer');
const { MongoClient } = require('mongodb');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const app = express();
const PORT = 3004; // Different port to avoid conflicts

// Enhanced multer configuration with better error handling
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = './temp_uploads';
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
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
    limits: {
        fileSize: 100 * 1024 * 1024, // 100MB limit
        files: 1
    }
});

const mongoUri = config.mongodb.uri + config.mongodb.database;

// Simplified and robust data categories
const DATA_CATEGORIES = {
    custody_files: {
        displayName: 'Custody Files',
        description: 'Custody holding files from custodians',
        subcategories: ['hdfc', 'axis', 'kotak', 'orbis', 'others'],
        icon: '🏛️',
        hasSubcategories: true
    },
    stock_capital_flow: {
        displayName: 'Stock Capital Flow',
        description: 'Stock transactions and capital flow data',
        subcategories: ['stock_capital_flow'],
        icon: '📊',
        hasSubcategories: false
    },
    cash_capital_flow: {
        displayName: 'Cash Capital Flow', 
        description: 'Cash transactions and liquidity data',
        subcategories: ['cash_capital_flow'],
        icon: '💰',
        hasSubcategories: false
    },
    distributor_master: {
        displayName: 'Distributor Master',
        description: 'Distributor and channel partner data',
        subcategories: ['distributor_master'],
        icon: '🤝',
        hasSubcategories: false
    },
    contract_notes: {
        displayName: 'Contract Notes',
        description: 'Trade contract notes and confirmations',
        subcategories: ['contract_notes'],
        icon: '📋',
        hasSubcategories: false
    },
    mf_allocations: {
        displayName: 'MF Allocations',
        description: 'Mutual fund allocation data',
        subcategories: ['mf_allocations'],
        icon: '📈',
        hasSubcategories: false
    },
    strategy_master: {
        displayName: 'Strategy Master',
        description: 'Investment strategy configurations',
        subcategories: ['strategy_master'],
        icon: '🎯',
        hasSubcategories: false
    },
    client_info: {
        displayName: 'Client Info',
        description: 'Client master and information data',
        subcategories: ['client_info'],
        icon: '👥',
        hasSubcategories: false
    },
    trades: {
        displayName: 'Trades',
        description: 'Trade execution data',
        subcategories: ['trades'],
        icon: '💹',
        hasSubcategories: false
    }
};

// Robust date extraction with better error handling
function extractDateFromFilename(filename) {
    try {
        console.log(`🔍 Extracting date from: ${filename}`);
        
        const nameWithoutExt = filename.replace(/\.(xlsx?|csv)$/i, '');
        const cleanName = nameWithoutExt.toLowerCase();
        
        // Enhanced date patterns with priority order
        const datePatterns = [
            // DD-MM-YYYY or DD/MM/YYYY or DD_MM_YYYY (most common)
            { pattern: /(\d{1,2})[-\/._](\d{1,2})[-\/._](\d{4})/, format: 'DMY' },
            // YYYY-MM-DD (ISO format)
            { pattern: /(\d{4})[-\/._](\d{1,2})[-\/._](\d{1,2})/, format: 'YMD' },
            // YYYYMMDD (compact format)
            { pattern: /(\d{4})(\d{2})(\d{2})(?![0-9])/, format: 'YMD_COMPACT' },
            // DDMMYYYY (Indian format)
            { pattern: /(\d{2})(\d{2})(\d{4})(?![0-9])/, format: 'DMY_COMPACT' },
            // Month names: 25Dec2024, 25-Dec-2024, etc.
            { pattern: /(\d{1,2})[-\/._]?(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[-\/._]?(\d{4})/i, format: 'MONTH_NAME' }
        ];
        
        const monthMap = {
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        };
        
        for (const { pattern, format } of datePatterns) {
            const match = cleanName.match(pattern);
            if (match) {
                let day, month, year;
                
                switch (format) {
                    case 'DMY':
                        day = parseInt(match[1]);
                        month = parseInt(match[2]);
                        year = parseInt(match[3]);
                        break;
                    case 'YMD':
                        year = parseInt(match[1]);
                        month = parseInt(match[2]);
                        day = parseInt(match[3]);
                        break;
                    case 'YMD_COMPACT':
                        year = parseInt(match[1]);
                        month = parseInt(match[2]);
                        day = parseInt(match[3]);
                        break;
                    case 'DMY_COMPACT':
                        day = parseInt(match[1]);
                        month = parseInt(match[2]);
                        year = parseInt(match[3]);
                        break;
                    case 'MONTH_NAME':
                        day = parseInt(match[1]);
                        month = monthMap[match[2].toLowerCase()];
                        year = parseInt(match[3]);
                        break;
                }
                
                // Validate date components
                if (isValidDate(year, month, day)) {
                    const extractedDate = `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
                    console.log(`✅ Extracted: ${extractedDate} (format: ${format})`);
                    return extractedDate;
                }
            }
        }
        
        console.log(`⚠️ No valid date found, using current date`);
        return new Date().toISOString().split('T')[0];
        
    } catch (error) {
        console.error(`❌ Date extraction error: ${error.message}`);
        return new Date().toISOString().split('T')[0];
    }
}

function isValidDate(year, month, day) {
    if (year < 2020 || year > 2030) return false;
    if (month < 1 || month > 12) return false;
    if (day < 1 || day > 31) return false;
    
    // Check if date exists
    const date = new Date(year, month - 1, day);
    return date.getFullYear() === year && 
           date.getMonth() === month - 1 && 
           date.getDate() === day;
}

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

// Add CORS headers
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    if (req.method === 'OPTIONS') {
        res.sendStatus(200);
    } else {
        next();
    }
});

// API endpoints with better error handling
app.get('/api/upload/categories', (req, res) => {
    try {
        res.json({
            success: true,
            categories: DATA_CATEGORIES,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({
            success: false,
            error: 'Failed to load categories',
            details: error.message
        });
    }
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    let tempFilePath = null;
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate, overrideDate } = req.body;
        
        // Validation
        if (!DATA_CATEGORIES[category]) {
            return res.status(400).json({
                success: false,
                error: `Invalid category: ${category}`
            });
        }
        
        if (!req.file) {
            return res.status(400).json({
                success: false,
                error: 'No file uploaded'
            });
        }
        
        tempFilePath = req.file.path;
        
        console.log(`📤 Processing: ${category}/${subcategory || 'default'}`);
        console.log(`📄 File: ${req.file.originalname} (${req.file.size} bytes)`);
        
        // Extract date from filename
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = overrideDate || processingDate || extractedDate;
        
        console.log(`📅 Using date: ${finalDate}`);
        
        // Process file with better error handling
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        try {
            if (fileExt === '.csv') {
                processedData = await processCSVFile(req.file.path);
            } else if (['.xlsx', '.xls'].includes(fileExt)) {
                processedData = await processExcelFile(req.file.path);
            } else {
                throw new Error(`Unsupported file type: ${fileExt}`);
            }
        } catch (parseError) {
            throw new Error(`File parsing failed: ${parseError.message}`);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        // Determine subcategory
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        // Add metadata with validation
        const enhancedData = processedData.map((record, index) => ({
            ...record,
            _id: undefined, // Let MongoDB generate
            recordIndex: index + 1,
            month: finalDate.split('-')[1],
            date: finalDate.split('-')[2],
            year: finalDate.split('-')[0],
            fullDate: finalDate,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            fileType: finalSubcategory,
            category: category,
            uploadedAt: new Date().toISOString(),
            extractedDate: extractedDate,
            processingDate: finalDate,
            __v: 0
        }));
        
        // Store in MongoDB with connection pooling
        const client = new MongoClient(mongoUri, {
            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000
        });
        
        try {
            await client.connect();
            const db = client.db('financial_data_2025');
            
            const collectionName = `${category}.${finalDate}.${finalSubcategory}`;
            
            // Insert with write concern
            const result = await db.collection(collectionName).insertMany(enhancedData, {
                ordered: false,
                writeConcern: { w: 'majority', j: true }
            });
            
            console.log(`✅ Inserted ${result.insertedCount} documents into ${collectionName}`);
            
        } finally {
            await client.close();
        }
        
        // Clean up temp file
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
            tempFilePath = null;
        }
        
        res.json({
            success: true,
            message: 'File uploaded successfully',
            details: {
                category: DATA_CATEGORIES[category].displayName,
                subcategory: finalSubcategory,
                fileName: req.file.originalname,
                fileSize: req.file.size,
                recordCount: enhancedData.length,
                collectionName: collectionName,
                processingDate: finalDate,
                extractedDate: extractedDate,
                dateSource: overrideDate ? 'manual_override' : (processingDate ? 'manual' : 'filename')
            }
        });
        
    } catch (error) {
        console.error('❌ Upload error:', error.message);
        
        // Clean up temp file on error
        if (tempFilePath && fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
        res.status(500).json({
            success: false,
            error: 'Upload failed',
            details: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Helper functions with better error handling
async function processCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        const stream = fs.createReadStream(filePath);
        
        stream
            .pipe(csv({
                skipEmptyLines: true,
                skipLinesWithError: true
            }))
            .on('data', (data) => {
                // Clean up data
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
            .on('end', () => {
                console.log(`📊 CSV processed: ${results.length} records`);
                resolve(results);
            })
            .on('error', (error) => {
                console.error('CSV parsing error:', error);
                reject(new Error(`CSV parsing failed: ${error.message}`));
            });
            
        // Timeout handling
        setTimeout(() => {
            stream.destroy();
            reject(new Error('CSV processing timeout'));
        }, 30000);
    });
}

async function processExcelFile(filePath) {
    try {
        console.log(`�� Processing Excel file: ${filePath}`);
        
        const workbook = xlsx.readFile(filePath, {
            cellDates: true,
            cellNF: false,
            cellText: false
        });
        
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) {
            throw new Error('No sheets found in Excel file');
        }
        
        const worksheet = workbook.Sheets[sheetName];
        const jsonData = xlsx.utils.sheet_to_json(worksheet, {
            raw: false,
            defval: '',
            blankrows: false
        });
        
        // Clean up data
        const cleanData = jsonData.map(row => {
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
        
        console.log(`📊 Excel processed: ${cleanData.length} records from sheet "${sheetName}"`);
        return cleanData;
        
    } catch (error) {
        console.error('Excel parsing error:', error);
        throw new Error(`Excel parsing failed: ${error.message}`);
    }
}

// Health check endpoint
app.get('/api/health', (req, res) => {
    res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        version: '2.0.0'
    });
});

// Main interface with improved UI
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html><head><title>Robust Upload System v2.0</title><style>
body{font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;margin:0;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh}
.container{max-width:1200px;margin:0 auto;padding:20px}
.header{text-align:center;color:white;margin-bottom:40px}
.title{font-size:42px;margin-bottom:10px;text-shadow:2px 2px 4px rgba(0,0,0,0.3);font-weight:300}
.subtitle{font-size:18px;opacity:0.9;margin-bottom:10px}
.version{font-size:14px;opacity:0.7}
.upload-section{background:white;border-radius:16px;padding:40px;margin-bottom:30px;box-shadow:0 20px 40px rgba(0,0,0,0.1)}
.section-title{font-size:28px;color:#333;margin-bottom:30px;font-weight:500}
.category-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px;margin-bottom:40px}
.category-card{border:2px solid #e8e8e8;border-radius:16px;padding:24px;cursor:pointer;transition:all 0.3s ease;background:white;position:relative;overflow:hidden}
.category-card:hover{border-color:#667eea;transform:translateY(-4px);box-shadow:0 12px 24px rgba(102,126,234,0.15)}
.category-card.selected{border-color:#667eea;background:linear-gradient(135deg,#f8f9ff 0%,#e8ecff 100%)}
.category-icon{font-size:36px;margin-bottom:16px;display:block}
.category-name{font-size:20px;font-weight:600;margin-bottom:8px;color:#333}
.category-desc{font-size:14px;color:#666;line-height:1.5}
.form-group{margin-bottom:24px}
.form-label{display:block;margin-bottom:12px;font-weight:600;color:#333;font-size:16px}
.form-input,.form-select{width:100%;padding:16px;border:2px solid #e8e8e8;border-radius:12px;font-size:16px;transition:border-color 0.3s;font-family:inherit}
.form-input:focus,.form-select:focus{outline:none;border-color:#667eea;box-shadow:0 0 0 3px rgba(102,126,234,0.1)}
.upload-area{border:3px dashed #d1d5db;border-radius:16px;padding:60px 40px;text-align:center;transition:all 0.3s;cursor:pointer;background:#fafafa}
.upload-area:hover{border-color:#667eea;background:#f8f9ff}
.upload-area.dragover{border-color:#667eea;background:#e8ecff}
.upload-btn{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:white;border:none;padding:16px 32px;border-radius:12px;font-size:16px;font-weight:600;cursor:pointer;transition:all 0.3s;min-width:200px}
.upload-btn:hover{transform:translateY(-2px);box-shadow:0 8px 16px rgba(102,126,234,0.3)}
.upload-btn:disabled{background:#ccc;cursor:not-allowed;transform:none;box-shadow:none}
.progress-container{margin:24px 0;display:none}
.progress-bar{width:100%;height:8px;background:#f0f0f0;border-radius:4px;overflow:hidden}
.progress-fill{height:100%;background:linear-gradient(90deg,#667eea,#764ba2);width:0%;transition:width 0.3s}
.message{padding:20px;border-radius:12px;margin:24px 0;font-weight:500}
.message.success{background:#f6ffed;border:2px solid #b7eb8f;color:#389e0d}
.message.error{background:#fff2f0;border:2px solid #ffccc7;color:#cf1322}
.message.info{background:#e6f7ff;border:2px solid #91d5ff;color:#0050b3}
.feature-box{background:#e8ecff;border:1px solid #c7d2fe;border-radius:12px;padding:20px;margin:24px 0}
.feature-title{font-weight:600;color:#4338ca;margin-bottom:8px}
.feature-list{color:#6366f1;font-size:14px;line-height:1.6}
.loading{display:none;align-items:center;gap:12px;margin:20px 0}
.spinner{width:20px;height:20px;border:2px solid #f3f3f3;border-top:2px solid #667eea;border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
</style></head><body>
<div class="container">
    <div class="header">
        <h1 class="title">🚀 Robust Upload System</h1>
        <p class="subtitle">Enhanced file upload with smart date extraction & error handling</p>
        <p class="version">Version 2.0 - MongoDB Raw Data Storage</p>
    </div>
    
    <div class="upload-section">
        <h2 class="section-title">📤 Upload Data Files</h2>
        
        <div class="feature-box">
            <div class="feature-title">🧠 Smart Features</div>
            <div class="feature-list">
                ✅ Automatic date extraction from filenames (multiple formats)<br>
                ✅ Enhanced error handling and file validation<br>
                ✅ Support for large files (up to 100MB)<br>
                ✅ Real-time progress tracking<br>
                ✅ Subcategories only for custody files
            </div>
        </div>
        
        <div class="form-group">
            <label class="form-label">�� Date Override (optional)</label>
            <input type="date" id="date-override" class="form-input" placeholder="Leave empty to auto-extract from filename">
            <small style="color:#666;margin-top:8px;display:block">System will automatically extract date from filename if not specified</small>
        </div>
        
        <div class="form-group">
            <label class="form-label">🗂️ Select Data Category</label>
            <div id="category-grid" class="category-grid"></div>
        </div>
        
        <div class="form-group" id="subcategory-section" style="display:none">
            <label class="form-label">📋 Select Subcategory</label>
            <select id="subcategory-select" class="form-select">
                <option value="">Choose subcategory...</option>
            </select>
        </div>
        
        <div class="form-group" id="upload-section" style="display:none">
            <label class="form-label">📎 Upload File</label>
            <div class="upload-area" id="upload-area">
                <div style="font-size:64px;margin-bottom:20px;opacity:0.6">📁</div>
                <h3 style="margin:0 0 12px 0;color:#333">Drag & drop your file here</h3>
                <p style="margin:0 0 24px 0;color:#666">or click to browse (Excel, CSV files up to 100MB)</p>
                <input type="file" id="file-input" accept=".xlsx,.xls,.csv" style="display:none">
                <button type="button" class="upload-btn" onclick="document.getElementById('file-input').click()">Choose File</button>
            </div>
            
            <div class="loading" id="loading">
                <div class="spinner"></div>
                <span>Processing file...</span>
            </div>
            
            <div class="progress-container" id="progress-container">
                <div class="progress-bar">
                    <div class="progress-fill" id="progress-fill"></div>
                </div>
                <div style="text-align:center;margin-top:8px;font-size:14px;color:#666" id="progress-text">Uploading...</div>
            </div>
            
            <button type="button" id="upload-btn" class="upload-btn" style="width:100%;margin-top:24px;display:none">
                🚀 Upload File
            </button>
        </div>
        
        <div id="message-area"></div>
    </div>
</div>

<script>
let selectedCategory = null;
let selectedFile = null;
let categories = {};

document.addEventListener('DOMContentLoaded', function() {
    loadCategories();
    setupFileUpload();
    setupDragDrop();
});

async function loadCategories() {
    try {
        showMessage('Loading categories...', 'info');
        const response = await fetch('/api/upload/categories');
        const data = await response.json();
        
        if (data.success) {
            categories = data.categories;
            renderCategories();
            clearMessage();
        } else {
            throw new Error(data.error || 'Failed to load categories');
        }
    } catch (error) {
        console.error('Error loading categories:', error);
        showMessage('Failed to load categories: ' + error.message, 'error');
    }
}

function renderCategories() {
    const grid = document.getElementById('category-grid');
    grid.innerHTML = '';
    
    Object.keys(categories).forEach(categoryKey => {
        const category = categories[categoryKey];
        const card = document.createElement('div');
        card.className = 'category-card';
        card.onclick = () => selectCategory(categoryKey);
        card.innerHTML = 
            '<span class="category-icon">' + category.icon + '</span>' +
            '<div class="category-name">' + category.displayName + '</div>' +
            '<div class="category-desc">' + category.description + '</div>';
        grid.appendChild(card);
    });
}

function selectCategory(categoryKey) {
    selectedCategory = categoryKey;
    
    document.querySelectorAll('.category-card').forEach(card => {
        card.classList.remove('selected');
    });
    event.target.closest('.category-card').classList.add('selected');
    
    const category = categories[categoryKey];
    const subcategorySection = document.getElementById('subcategory-section');
    const subcategorySelect = document.getElementById('subcategory-select');
    
    if (category.hasSubcategories) {
        subcategorySelect.innerHTML = '<option value="">Choose subcategory...</option>';
        category.subcategories.forEach(sub => {
            const option = document.createElement('option');
            option.value = sub;
            option.textContent = sub.charAt(0).toUpperCase() + sub.slice(1).replace(/_/g, ' ');
            subcategorySelect.appendChild(option);
        });
        
        subcategorySection.style.display = 'block';
        
        subcategorySelect.onchange = function() {
            if (this.value) {
                document.getElementById('upload-section').style.display = 'block';
            } else {
                document.getElementById('upload-section').style.display = 'none';
            }
        };
    } else {
        subcategorySection.style.display = 'none';
        document.getElementById('upload-section').style.display = 'block';
    }
}

function setupFileUpload() {
    const fileInput = document.getElementById('file-input');
    const uploadBtn = document.getElementById('upload-btn');
    
    fileInput.onchange = function(e) {
        handleFileSelect(e.target.files[0]);
    };
    
    uploadBtn.onclick = uploadFile;
}

function setupDragDrop() {
    const uploadArea = document.getElementById('upload-area');
    
    uploadArea.ondragover = function(e) {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    };
    
    uploadArea.ondragleave = function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
    };
    
    uploadArea.ondrop = function(e) {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            handleFileSelect(files[0]);
        }
    };
}

function handleFileSelect(file) {
    if (!file) return;
    
    const allowedTypes = ['.xlsx', '.xls', '.csv'];
    const fileExt = '.' + file.name.split('.').pop().toLowerCase();
    
    if (!allowedTypes.includes(fileExt)) {
        showMessage('Invalid file type. Please select Excel (.xlsx, .xls) or CSV files only.', 'error');
        return;
    }
    
    if (file.size > 100 * 1024 * 1024) {
        showMessage('File too large. Maximum size is 100MB.', 'error');
        return;
    }
    
    selectedFile = file;
    document.getElementById('upload-btn').style.display = 'block';
    document.querySelector('#upload-area h3').textContent = 'File selected: ' + file.name;
    document.querySelector('#upload-area p').textContent = 'Size: ' + formatFileSize(file.size);
    
    showMessage('File ready for upload: ' + file.name, 'info');
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

async function uploadFile() {
    if (!selectedCategory || !selectedFile) {
        showMessage('Please select a category and file.', 'error');
        return;
    }
    
    const category = categories[selectedCategory];
    let subcategory = '';
    
    if (category.hasSubcategories) {
        subcategory = document.getElementById('subcategory-select').value;
        if (!subcategory) {
            showMessage('Please select a subcategory.', 'error');
            return;
        }
    } else {
        subcategory = category.subcategories[0];
    }
    
    const dateOverride = document.getElementById('date-override').value;
    
    const formData = new FormData();
    formData.append('file', selectedFile);
    if (dateOverride) {
        formData.append('overrideDate', dateOverride);
    }
    
    // Show loading
    const uploadBtn = document.getElementById('upload-btn');
    const loading = document.getElementById('loading');
    const progressContainer = document.getElementById('progress-container');
    
    uploadBtn.disabled = true;
    uploadBtn.textContent = 'Uploading...';
    loading.style.display = 'flex';
    progressContainer.style.display = 'block';
    
    try {
        const url = '/api/upload/' + selectedCategory + (subcategory ? '/' + subcategory : '');
        const response = await fetch(url, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            showMessage(
                '<strong>✅ Upload Successful!</strong><br>' +
                'File: ' + result.details.fileName + '<br>' +
                'Records: ' + result.details.recordCount.toLocaleString() + '<br>' +
                'Collection: ' + result.details.collectionName + '<br>' +
                'Date: ' + result.details.processingDate + ' (' + result.details.dateSource + ')', 
                'success'
            );
            resetForm();
        } else {
            throw new Error(result.details || result.error || 'Upload failed');
        }
    } catch (error) {
        console.error('Upload error:', error);
        showMessage('Upload failed: ' + error.message, 'error');
    } finally {
        uploadBtn.disabled = false;
        uploadBtn.textContent = '🚀 Upload File';
        loading.style.display = 'none';
        progressContainer.style.display = 'none';
    }
}

function showMessage(message, type) {
    const messageArea = document.getElementById('message-area');
    messageArea.innerHTML = '<div class="message ' + type + '">' + message + '</div>';
    
    if (type === 'success') {
        setTimeout(() => clearMessage(), 10000);
    } else if (type === 'info') {
        setTimeout(() => clearMessage(), 5000);
    }
}

function clearMessage() {
    const messageArea = document.getElementById('message-area');
    messageArea.innerHTML = '';
}

function resetForm() {
    selectedCategory = null;
    selectedFile = null;
    document.querySelectorAll('.category-card').forEach(card => card.classList.remove('selected'));
    document.getElementById('subcategory-section').style.display = 'none';
    document.getElementById('upload-section').style.display = 'none';
    document.getElementById('upload-btn').style.display = 'none';
    document.querySelector('#upload-area h3').textContent = 'Drag & drop your file here';
    document.querySelector('#upload-area p').textContent = 'or click to browse (Excel, CSV files up to 100MB)';
    document.getElementById('date-override').value = '';
}
</script></body></html>`);
});

app.listen(PORT, () => {
    console.log(`🚀 Robust Upload System v2.0 running at http://localhost:${PORT}`);
    console.log('📊 Enhanced features:');
    console.log('   • Improved error handling & validation');
    console.log('   • Better file processing (CSV & Excel)');
    console.log('   • Enhanced date extraction (multiple formats)');
    console.log('   • Larger file support (100MB)');
    console.log('   • Real-time progress tracking');
    console.log('   • Connection pooling & timeout handling');
    console.log('📂 Categories:');
    Object.keys(DATA_CATEGORIES).forEach(key => {
        const cat = DATA_CATEGORIES[key];
        console.log(`   ${cat.icon} ${cat.displayName} ${cat.hasSubcategories ? '(with subcategories)' : ''}`);
    });
});
