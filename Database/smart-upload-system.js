const express = require('express');
const multer = require('multer');
const { MongoClient } = require('mongodb');
const xlsx = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const config = require('./config');

const app = express();
const PORT = 3003;

// Configure multer for file uploads
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const uploadDir = './temp_uploads';
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
        cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        cb(null, `${timestamp}_${file.originalname}`);
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
            cb(new Error('Only Excel and CSV files are allowed'), false);
        }
    },
    limits: {
        fileSize: 50 * 1024 * 1024
    }
});

const mongoUri = config.mongodb.uri + config.mongodb.database;

// Define data categories for MongoDB raw data storage
const DATA_CATEGORIES = {
    custody_files: {
        displayName: 'Custody Files',
        description: 'Raw custody holding files from different custodians (HDFC, AXIS, KOTAK, ORBIS)',
        subcategories: ['hdfc', 'axis', 'kotak', 'orbis', 'other_custodian'],
        icon: '🏛️',
        hasSubcategories: true
    },
    stock_capital_flow: {
        displayName: 'Stock Capital Flow',
        description: 'Raw stock capital flow and equity transaction data',
        subcategories: ['stock_capital_flow'],
        icon: '📊',
        hasSubcategories: false
    },
    cash_capital_flow: {
        displayName: 'Cash Capital Flow',
        description: 'Raw cash flow and liquidity management data',
        subcategories: ['cash_capital_flow'],
        icon: '💰',
        hasSubcategories: false
    },
    distributor_master: {
        displayName: 'Distributor Master',
        description: 'Raw distributor information and channel partner data',
        subcategories: ['distributor_master'],
        icon: '🤝',
        hasSubcategories: false
    },
    contract_notes: {
        displayName: 'Contract Notes',
        description: 'Raw trade contract notes and transaction confirmations',
        subcategories: ['contract_notes'],
        icon: '📋',
        hasSubcategories: false
    },
    mf_allocations: {
        displayName: 'MF Allocations',
        description: 'Raw mutual fund allocation and transaction files',
        subcategories: ['mf_allocations'],
        icon: '📈',
        hasSubcategories: false
    },
    strategy_master: {
        displayName: 'Strategy Master',
        description: 'Raw investment strategy and portfolio configuration files',
        subcategories: ['strategy_master'],
        icon: '🎯',
        hasSubcategories: false
    },
    client_info: {
        displayName: 'Client Info',
        description: 'Raw client master data and information files',
        subcategories: ['client_info'],
        icon: '👥',
        hasSubcategories: false
    },
    trades: {
        displayName: 'Trades',
        description: 'Raw trade execution and transaction data',
        subcategories: ['trades'],
        icon: '💹',
        hasSubcategories: false
    }
};

// Smart date extraction from filename
function extractDateFromFilename(filename) {
    console.log(`🔍 Extracting date from filename: ${filename}`);
    
    // Remove file extension
    const nameWithoutExt = filename.replace(/\.(xlsx?|csv)$/i, '');
    
    // Multiple date patterns to match
    const datePatterns = [
        // DD/MM/YYYY or DD-MM-YYYY or DD_MM_YYYY
        /(\d{1,2})[\/\-_](\d{1,2})[\/\-_](\d{4})/,
        // YYYY-MM-DD or YYYY_MM_DD or YYYY/MM/DD
        /(\d{4})[\/\-_](\d{1,2})[\/\-_](\d{1,2})/,
        // YYYYMMDD
        /(\d{4})(\d{2})(\d{2})/,
        // DDMMYYYY
        /(\d{2})(\d{2})(\d{4})/,
        // Date words like 25Jun2025, 25June2025
        /(\d{1,2})(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|January|February|March|April|May|June|July|August|September|October|November|December)(\d{4})/i
    ];
    
    for (const pattern of datePatterns) {
        const match = nameWithoutExt.match(pattern);
        if (match) {
            try {
                let day, month, year;
                
                if (pattern.source.includes('Jan|Feb')) {
                    // Month name pattern
                    day = parseInt(match[1]);
                    const monthMap = {
                        'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
                        'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6,
                        'jul': 7, 'july': 7, 'aug': 8, 'august': 8, 'sep': 9, 'september': 9,
                        'oct': 10, 'october': 10, 'nov': 11, 'november': 11, 'dec': 12, 'december': 12
                    };
                    month = monthMap[match[2].toLowerCase()];
                    year = parseInt(match[3]);
                } else if (match[1].length === 4) {
                    // YYYY-MM-DD format
                    year = parseInt(match[1]);
                    month = parseInt(match[2]);
                    day = parseInt(match[3]);
                } else if (match[3] && match[3].length === 4) {
                    // DD-MM-YYYY format
                    day = parseInt(match[1]);
                    month = parseInt(match[2]);
                    year = parseInt(match[3]);
                } else if (match[0].length === 8) {
                    // YYYYMMDD format
                    const dateStr = match[0];
                    year = parseInt(dateStr.substring(0, 4));
                    month = parseInt(dateStr.substring(4, 6));
                    day = parseInt(dateStr.substring(6, 8));
                }
                
                // Validate date
                if (year >= 2020 && year <= 2030 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
                    const extractedDate = `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
                    console.log(`✅ Extracted date: ${extractedDate} from ${filename}`);
                    return extractedDate;
                }
            } catch (error) {
                console.log(`⚠️ Error parsing date from ${filename}:`, error.message);
                continue;
            }
        }
    }
    
    console.log(`⚠️ No valid date found in filename: ${filename}, using current date`);
    return new Date().toISOString().split('T')[0];
}

app.use(express.json());

// API endpoints
app.get('/api/upload/categories', (req, res) => {
    res.json({
        success: true,
        categories: DATA_CATEGORIES
    });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        if (!DATA_CATEGORIES[category]) {
            return res.status(400).json({
                success: false,
                error: 'Invalid category selected'
            });
        }
        
        if (!req.file) {
            return res.status(400).json({
                success: false,
                error: 'No file uploaded'
            });
        }
        
        console.log(`📤 Processing upload: ${category}/${subcategory || 'default'}`);
        console.log(`📄 File: ${req.file.originalname}`);
        
        // Smart date extraction from filename
        let extractedDate = extractDateFromFilename(req.file.originalname);
        
        // Use extracted date, or manual override, or current date
        const finalDate = processingDate && processingDate !== extractedDate ? processingDate : extractedDate;
        
        console.log(`📅 Using date: ${finalDate} (extracted: ${extractedDate}, manual: ${processingDate || 'none'})`);
        
        // Process file
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        let processedData = [];
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in the uploaded file');
        }
        
        // Determine subcategory
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        // Add metadata
        const enhancedData = processedData.map(record => ({
            ...record,
            month: finalDate.split('-')[1],
            date: finalDate.split('-')[2],
            fullDate: finalDate,
            fileName: req.file.originalname,
            fileType: finalSubcategory,
            category: category,
            uploadedAt: new Date().toISOString(),
            extractedDate: extractedDate,
            __v: 0
        }));
        
        // Store in MongoDB
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const collectionName = `${category}.${finalDate}.${finalSubcategory}`;
        await db.collection(collectionName).insertMany(enhancedData);
        await client.close();
        
        // Clean up
        fs.unlinkSync(req.file.path);
        
        console.log(`✅ Successfully uploaded ${enhancedData.length} records to ${collectionName}`);
        
        res.json({
            success: true,
            message: 'File uploaded and processed successfully',
            details: {
                category: DATA_CATEGORIES[category].displayName,
                subcategory: finalSubcategory,
                fileName: req.file.originalname,
                recordCount: enhancedData.length,
                collectionName: collectionName,
                processingDate: finalDate,
                extractedDate: extractedDate,
                dateSource: processingDate && processingDate !== extractedDate ? 'manual' : 'filename'
            }
        });
        
    } catch (error) {
        console.error('Upload error:', error);
        if (req.file && fs.existsSync(req.file.path)) {
            fs.unlinkSync(req.file.path);
        }
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Helper functions
async function processCSVFile(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (error) => reject(error));
    });
}

async function processExcelFile(filePath) {
    try {
        const workbook = xlsx.readFile(filePath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        return xlsx.utils.sheet_to_json(worksheet);
    } catch (error) {
        throw new Error(`Error processing Excel file: ${error.message}`);
    }
}

// Main interface
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html><head><title>Smart Upload System - MongoDB</title><style>
body{font-family:'Segoe UI',sans-serif;margin:0;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh}
.container{max-width:1200px;margin:0 auto;padding:20px}
.header{text-align:center;color:white;margin-bottom:40px}
.title{font-size:36px;margin-bottom:10px;text-shadow:2px 2px 4px rgba(0,0,0,0.3)}
.upload-section{background:white;border-radius:12px;padding:30px;margin-bottom:30px;box-shadow:0 10px 30px rgba(0,0,0,0.2)}
.category-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:20px;margin-bottom:30px}
.category-card{border:2px solid #e1e5e9;border-radius:12px;padding:20px;cursor:pointer;transition:all 0.3s}
.category-card:hover{border-color:#1890ff;transform:translateY(-2px)}
.category-card.selected{border-color:#1890ff;background:#f0f9ff}
.category-icon{font-size:32px;margin-bottom:10px}
.category-name{font-size:18px;font-weight:600;margin-bottom:8px}
.form-group{margin-bottom:20px}
.form-select{width:100%;padding:12px;border:2px solid #e1e5e9;border-radius:8px;font-size:16px}
.upload-area{border:3px dashed #d1d5db;border-radius:12px;padding:40px;text-align:center;cursor:pointer}
.upload-btn{background:#1890ff;color:white;border:none;padding:15px 30px;border-radius:8px;font-size:16px;cursor:pointer}
.success-message{background:#f6ffed;border:1px solid #b7eb8f;color:#389e0d;padding:15px;border-radius:8px;margin:20px 0}
.error-message{background:#fff2f0;border:1px solid #ffccc7;color:#cf1322;padding:15px;border-radius:8px;margin:20px 0}
.info-box{background:#e6f7ff;border:1px solid #91d5ff;color:#0050b3;padding:15px;border-radius:8px;margin:20px 0}
</style></head><body>
<div class="container">
    <div class="header">
        <h1 class="title">🧠 Smart Upload System</h1>
        <p>Step 1: Upload raw financial data with smart date extraction from filenames</p>
    </div>
    
    <div class="upload-section">
        <h2>📤 Upload Raw Data Files</h2>
        
        <div class="info-box">
            <strong>🧠 Smart Date Detection:</strong> System automatically extracts dates from filenames like:
            <br>• hdfc06/05/2025.xlsx → 2025-05-06
            <br>• axis_25-Dec-2024.csv → 2024-12-25  
            <br>• kotak20241225.xlsx → 2024-12-25
        </div>
        
        <div class="form-group">
            <label>📅 Manual Date Override (optional)</label>
            <input type="date" id="processing-date" style="width:100%;padding:12px;border:2px solid #e1e5e9;border-radius:8px" placeholder="Leave empty to use filename date">
        </div>
        
        <div class="form-group">
            <label>🗂️ Select Data Category</label>
            <div id="category-grid" class="category-grid"></div>
        </div>
        
        <div class="form-group" id="subcategory-section" style="display:none">
            <label>📋 Select Subcategory</label>
            <select id="subcategory-select" class="form-select">
                <option value="">Choose subcategory...</option>
            </select>
        </div>
        
        <div class="form-group" id="upload-section" style="display:none">
            <label>📎 Upload File</label>
            <div class="upload-area" id="upload-area">
                <div style="font-size:48px;margin-bottom:20px">📁</div>
                <p>Drag & drop your file here or click to browse</p>
                <input type="file" id="file-input" accept=".xlsx,.xls,.csv" style="display:none">
                <button type="button" class="upload-btn" onclick="document.getElementById('file-input').click()">Choose File</button>
            </div>
            <button type="button" id="upload-btn" class="upload-btn" style="width:100%;margin-top:20px;display:none">Upload File</button>
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
});

async function loadCategories() {
    try {
        const response = await fetch('/api/upload/categories');
        const data = await response.json();
        categories = data.categories;
        renderCategories();
    } catch (error) {
        console.error('Error loading categories:', error);
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
            '<div class="category-icon">' + category.icon + '</div>' +
            '<div class="category-name">' + category.displayName + '</div>' +
            '<div style="font-size:14px;color:#666">' + category.description + '</div>';
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
            option.textContent = sub.replace(/_/g, ' ').toUpperCase();
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

function handleFileSelect(file) {
    if (!file) return;
    
    selectedFile = file;
    document.getElementById('upload-btn').style.display = 'block';
    document.querySelector('#upload-area p').textContent = 'File selected: ' + file.name;
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
    
    const processingDate = document.getElementById('processing-date').value;
    
    const formData = new FormData();
    formData.append('file', selectedFile);
    if (processingDate) {
        formData.append('processingDate', processingDate);
    }
    
    try {
        const url = '/api/upload/' + selectedCategory + (subcategory ? '/' + subcategory : '');
        const response = await fetch(url, {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            showMessage(
                '✅ File uploaded successfully!<br>' +
                '<strong>Records:</strong> ' + result.details.recordCount + '<br>' +
                '<strong>Collection:</strong> ' + result.details.collectionName + '<br>' +
                '<strong>Date:</strong> ' + result.details.processingDate + ' (' + result.details.dateSource + ')', 
                'success'
            );
            resetForm();
        } else {
            throw new Error(result.error);
        }
    } catch (error) {
        showMessage('Upload failed: ' + error.message, 'error');
    }
}

function showMessage(message, type) {
    const messageArea = document.getElementById('message-area');
    messageArea.innerHTML = '<div class="' + type + '-message">' + message + '</div>';
    setTimeout(() => messageArea.innerHTML = '', 8000);
}

function resetForm() {
    selectedCategory = null;
    selectedFile = null;
    document.querySelectorAll('.category-card').forEach(card => card.classList.remove('selected'));
    document.getElementById('subcategory-section').style.display = 'none';
    document.getElementById('upload-section').style.display = 'none';
    document.getElementById('upload-btn').style.display = 'none';
    document.querySelector('#upload-area p').textContent = 'Drag & drop your file here or click to browse';
    document.getElementById('processing-date').value = '';
}
</script></body></html>`);
});

app.listen(PORT, () => {
    console.log(`🧠 Smart Upload System running at http://localhost:${PORT}`);
    console.log('📊 Supporting categories:');
    Object.keys(DATA_CATEGORIES).forEach(key => {
        console.log(`   ${DATA_CATEGORIES[key].icon} ${DATA_CATEGORIES[key].displayName} ${DATA_CATEGORIES[key].hasSubcategories ? '(with subcategories)' : ''}`);
    });
    console.log('🔍 Features:');
    console.log('   • Smart date extraction from filenames');
    console.log('   • Manual date override option');
    console.log('   • Subcategories only for custody files');
    console.log('   • Support for multiple date formats');
});
