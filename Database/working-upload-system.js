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
    try {
        const csvData = fs.readFileSync(filePath, 'utf8');
        const { parse } = require('csv-parse/sync');
        
        // First, let's try simple CSV parsing
        console.log('📁 CSV file content preview (first 500 chars):');
        console.log(csvData.substring(0, 500));
        
        // Smart CSV header detection - try different starting rows
        let bestData = [];
        let bestHeaders = [];
        let bestScore = 0;
        let bestStartRow = 0;
        
        console.log('🔍 Smart CSV processing: Scanning for headers...');
        
        // Try parsing with different configurations
        const parseConfigs = [
            { columns: true, skip_empty_lines: true, trim: true },
            { columns: true, skip_empty_lines: true, trim: true, delimiter: ',' },
            { columns: true, skip_empty_lines: true, trim: true, delimiter: ';' },
        ];
        
        for (const config of parseConfigs) {
            for (let skipRows = 0; skipRows < 20; skipRows++) {
                try {
                    const testRecords = parse(csvData, {
                        ...config,
                        from_line: skipRows + 1
                    });
                    
                    if (testRecords.length === 0) continue;
                    
                    const headers = Object.keys(testRecords[0]);
                    const score = calculateHeaderScore(headers);
                    
                    console.log(`📊 Starting row ${skipRows + 1}: ${headers.length} columns, score: ${score}`);
                    console.log(`   Headers: ${headers.slice(0, 5).join(', ')}${headers.length > 5 ? '...' : ''}`);
                    
                    if (score > bestScore && testRecords.length > 2) {
                        bestScore = score;
                        bestData = testRecords;
                        bestHeaders = headers;
                        bestStartRow = skipRows;
                    }
                } catch (error) {
                    console.log(`⚠️ Starting row ${skipRows + 1}: Could not process (${error.message})`);
                    continue;
                }
            }
        }
        
        // If no good headers found, try fallback approach
        if (bestScore < 10) {
            console.log('🔄 No good headers found, trying fallback approach...');
            try {
                const fallbackRecords = parse(csvData, {
                    columns: false,
                    skip_empty_lines: true,
                    trim: true
                });
                
                // Look for a row that looks like headers
                for (let i = 0; i < Math.min(fallbackRecords.length, 20); i++) {
                    const row = fallbackRecords[i];
                    if (row.length > 3) {
                        const score = calculateHeaderScore(row);
                        console.log(`📊 Fallback row ${i + 1}: ${row.length} columns, score: ${score}`);
                        console.log(`   Potential headers: ${row.slice(0, 5).join(', ')}`);
                        
                        if (score > bestScore) {
                            // Use this row as headers
                            const remainingData = fallbackRecords.slice(i + 1);
                            bestData = remainingData.map(dataRow => {
                                const obj = {};
                                row.forEach((header, index) => {
                                    if (header && dataRow[index]) {
                                        obj[cleanupColumnName(header)] = dataRow[index];
                                    }
                                });
                                return obj;
                            }).filter(obj => Object.keys(obj).length > 0);
                            
                            bestHeaders = row.map(h => cleanupColumnName(h)).filter(h => h);
                            bestScore = score;
                            bestStartRow = i;
                        }
                    }
                }
            } catch (error) {
                console.log('❌ Fallback approach failed:', error.message);
            }
        }
        
        console.log(`✅ Best CSV headers found: Starting row ${bestStartRow + 1} (score: ${bestScore})`);
        console.log(`📋 Final headers: ${bestHeaders.join(', ')}`);
        
        if (bestData.length === 0) {
            throw new Error('No valid data rows found in CSV file');
        }
        
        // Clean the data
        const cleanedData = bestData.map(row => {
            const cleanRow = {};
            for (const [key, value] of Object.entries(row)) {
                const cleanKey = cleanupColumnName(key);
                const cleanValue = typeof value === 'string' ? value.trim() : value;
                if (cleanKey && cleanValue !== '') {
                    cleanRow[cleanKey] = cleanValue;
                }
            }
            return cleanRow;
        }).filter(row => Object.keys(row).length > 0);
        
        console.log(`📊 Final processed data: ${cleanedData.length} records`);
        return cleanedData;
        
    } catch (error) {
        throw new Error(`CSV parsing failed: ${error.message}`);
    }
}

async function processExcelFile(filePath) {
    try {
        const workbook = xlsx.readFile(filePath, { cellDates: true, cellNF: false, cellText: false });
        const sheetName = workbook.SheetNames[0];
        if (!sheetName) throw new Error('No sheets found in Excel file');
        
        const worksheet = workbook.Sheets[sheetName];
        
        // Smart header detection - scan first 10 rows to find the best header row
        let bestHeaderRow = 0;
        let bestScore = 0;
        let bestHeaders = [];
        
        console.log('🔍 Smart Excel processing: Scanning for headers...');
        
        for (let rowIndex = 0; rowIndex < 10; rowIndex++) {
            try {
                // Get data starting from this row
                const testData = xlsx.utils.sheet_to_json(worksheet, { 
                    range: rowIndex,
                    raw: false, 
                    defval: '', 
                    blankrows: false 
                });
                
                if (testData.length === 0) continue;
                
                const headers = Object.keys(testData[0]);
                const score = calculateHeaderScore(headers);
                
                console.log(`📊 Row ${rowIndex + 1}: ${headers.length} columns, score: ${score}`);
                console.log(`   Headers: ${headers.slice(0, 3).join(', ')}${headers.length > 3 ? '...' : ''}`);
                
                if (score > bestScore) {
                    bestScore = score;
                    bestHeaderRow = rowIndex;
                    bestHeaders = headers;
                }
            } catch (error) {
                console.log(`⚠️ Row ${rowIndex + 1}: Could not process`);
                continue;
            }
        }
        
        console.log(`✅ Best header row found: Row ${bestHeaderRow + 1} (score: ${bestScore})`);
        console.log(`📋 Final headers: ${bestHeaders.join(', ')}`);
        
        // Extract data using the best header row
        const jsonData = xlsx.utils.sheet_to_json(worksheet, { 
            range: bestHeaderRow,
            raw: false, 
            defval: '', 
            blankrows: false 
        });
        
        // Clean the data
        return jsonData.map(row => {
            const cleanRow = {};
            for (const [key, value] of Object.entries(row)) {
                const cleanKey = cleanupColumnName(key);
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

function calculateHeaderScore(headers) {
    let score = 0;
    
    // Basic score based on number of non-empty headers
    const validHeaders = headers.filter(h => h && h.trim() && !h.startsWith('__EMPTY'));
    score += validHeaders.length * 2; // Reduced base score
    
    // MASSIVE penalty for corrupted data or report headers
    headers.forEach(header => {
        if (!header) return;
        const headerLower = header.toLowerCase().trim();
        
        // Huge penalties for report-style headers and corrupted data
        if (headerLower.includes('logical holding') || 
            headerLower.includes('saleable report') ||
            headerLower.includes('report') ||
            headerLower.includes('as of date') ||
            headerLower.includes('date :') ||
            headerLower.startsWith('client code :') ||  // This is data, not header
            headerLower.startsWith('client name :') ||  // This is data, not header
            headerLower.includes('details') ||
            headerLower.includes('buoj') ||  // Specific corrupted data
            headerLower === '' ||
            headerLower.length > 50) { // Very long text is likely report title
            score -= 1000; // MASSIVE penalty to avoid these
        }
    });
    
    // SUPER HIGH PRIORITY financial terms with massive weightage
    const criticalFinancialTerms = [
        // Core custody columns - HIGHEST PRIORITY
        { terms: ['client code', 'client_code', 'clientcode'], weight: 500 },
        { terms: ['client name', 'client_name', 'clientname'], weight: 500 },
        { terms: ['instrument name', 'instrument_name', 'security name', 'security_name', 'scrip'], weight: 500 },
        { terms: ['instrument code', 'instrument_code', 'security code', 'security_code'], weight: 500 },
        { terms: ['isin', 'isin_code'], weight: 500 },
        
        // Quantity related - VERY HIGH PRIORITY
        { terms: ['quantity', 'qty', 'units', 'blockable qty', 'blockable_qty'], weight: 400 },
        { terms: ['market value', 'market_value', 'marketvalue', 'value'], weight: 400 },
        { terms: ['rate', 'price', 'market price', 'market_price'], weight: 400 },
        
        // Position types - HIGH PRIORITY  
        { terms: ['free', 'free_qty', 'saleable', 'saleable_qty'], weight: 300 },
        { terms: ['blocked', 'locked', 'pledge', 'pledged'], weight: 300 },
        { terms: ['demat', 'demat_qty', 'physical'], weight: 300 },
        { terms: ['settled', 'settled_qty', 'position'], weight: 300 },
        { terms: ['outstanding', 'pending'], weight: 250 },
        
        // Additional fields
        { terms: ['holding', 'balance'], weight: 200 },
        { terms: ['ucc', 'dp_id'], weight: 200 },
        { terms: ['amount'], weight: 150 }
    ];
    
    headers.forEach(header => {
        if (header) {
            const headerLower = header.toLowerCase().trim().replace(/[^a-z]/g, '');
            criticalFinancialTerms.forEach(({ terms, weight }) => {
                terms.forEach(term => {
                    const termClean = term.replace(/[^a-z]/g, '');
                    if (headerLower === termClean || headerLower.includes(termClean)) {
                        score += weight;
                    }
                });
            });
        }
    });
    
    // Penalty for empty columns or meaningless names
    headers.forEach(header => {
        if (!header || header.trim() === '' || header.startsWith('__EMPTY') || 
            header.includes('undefined') || header.length < 2) {
            score -= 100;
        }
    });
    
    // Bonus for reasonable number of columns (6-25 for custody files)
    if (headers.length >= 6 && headers.length <= 25) {
        score += 100;
    } else if (headers.length < 4) {
        score -= 200; // Big penalty for too few columns
    }
    
    // SUPER BONUS for perfect custody header combinations
    const hasClientCode = headers.some(h => h && h.toLowerCase().replace(/[^a-z]/g, '').includes('clientcode'));
    const hasClientName = headers.some(h => h && h.toLowerCase().replace(/[^a-z]/g, '').includes('clientname'));
    const hasInstrument = headers.some(h => h && (
        h.toLowerCase().includes('instrument') || 
        h.toLowerCase().includes('security') || 
        h.toLowerCase().includes('scrip')
    ));
    const hasQuantity = headers.some(h => h && (
        h.toLowerCase().includes('quantity') || 
        h.toLowerCase().includes('qty') ||
        h.toLowerCase().includes('blockable')
    ));
    const hasISIN = headers.some(h => h && h.toLowerCase().includes('isin'));
    
    if (hasClientCode && hasClientName && hasInstrument) {
        score += 1000; // MASSIVE bonus for perfect custody structure
    }
    
    if (hasQuantity && hasISIN) {
        score += 500; // Additional bonus for quantity and ISIN
    }
    
    // Extra bonus for finding multiple key custody terms (5+ terms)
    const keyTermsFound = headers.filter(h => {
        if (!h) return false;
        const lower = h.toLowerCase().replace(/[^a-z]/g, '');
        return lower.includes('client') || lower.includes('instrument') || 
               lower.includes('security') || lower.includes('quantity') || 
               lower.includes('isin') || lower.includes('value') || 
               lower.includes('scrip') || lower.includes('amount') ||
               lower.includes('rate') || lower.includes('price') ||
               lower.includes('blockable');
    }).length;
    
    if (keyTermsFound >= 5) {
        score += 800; // HUGE bonus for comprehensive custody data
    }
    
    return score;
}

function cleanupColumnName(columnName) {
    if (!columnName) return '';
    
    let cleaned = columnName.toString().trim();
    
    // Remove common Excel artifacts
    cleaned = cleaned.replace(/^__EMPTY_?\d*$/g, '');
    cleaned = cleaned.replace(/^Column\d+$/g, '');
    cleaned = cleaned.replace(/^Field\d+$/g, '');
    
    // Remove extra whitespace and special characters
    cleaned = cleaned.replace(/\s+/g, ' ');
    cleaned = cleaned.replace(/[^\w\s.-]/g, '');
    
    return cleaned.trim();
}

// Main page route
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Complete ETL System</title>
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
        .table-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(350px, 1fr)); gap: 15px; margin: 20px 0; }
        .table-card { background: #f8f9fa; border: 1px solid #dee2e6; border-radius: 8px; padding: 15px; cursor: pointer; transition: all 0.3s; overflow: hidden; position: relative; min-height: 200px; }
        .table-card:hover { background: #e9ecef; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
        .table-card.selected { background: #e3f2fd; border-color: #667eea; box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.2); }
        .table-card h4 { margin: 0 0 10px 0; font-size: 16px; color: #333; }
        .table-card p { margin: 5px 0; font-size: 14px; color: #666; }
        .table-card .file-name { font-size: 11px; color: #888; word-break: break-word; overflow-wrap: anywhere; max-width: 100%; line-height: 1.3; display: block; margin: 8px 0; }
        .table-card .file-name-container { background: #f8f9fb; padding: 6px 8px; border-radius: 4px; border-left: 3px solid #667eea; margin: 8px 0; }
        .btn { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
        .btn:hover { background: #5a6fd8; }
        .btn.danger { background: #dc3545; }
        .btn.danger:hover { background: #c82333; }
        .hidden { display: none !important; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔥 Complete ETL System</h1>
            <p>Upload Files & Create PostgreSQL Tables</p>
        </div>
        
        <div class="tabs">
            <button class="tab active">📤 Upload Files</button>
            <button class="tab">🔄 ETL Mapping</button>
            <button class="tab">👁️ Data Viewer</button>
            <button class="tab">🐘 PostgreSQL Viewer</button>
        </div>
        
        <div class="content">
            <h2>✅ System is working!</h2>
            <p>The ETL system is successfully deployed. All files are uploaded to MongoDB.</p>
            <p>To use the full interface, the JavaScript needs to be added back.</p>
        </div>
    </div>
</body>
</html>
    `);
});

// API Routes
app.get('/api/upload/categories', (req, res) => {
    console.log('📋 Categories requested');
    res.json({ success: true, categories: DATA_CATEGORIES });
});

app.post('/api/upload/:category/:subcategory?', upload.single('file'), async (req, res) => {
    console.log('📤 Upload API called:', req.params, req.file?.originalname);
    
    let tempFilePath = null;
    let collectionName = null;
    let enhancedData = [];
    
    try {
        const { category, subcategory } = req.params;
        const { processingDate } = req.body;
        
        if (!DATA_CATEGORIES[category]) {
            return res.status(400).json({ success: false, error: 'Invalid category: ' + category });
        }
        
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        const finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        console.log('📅 Processing:', { extractedDate, finalDate, finalSubcategory });
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path);
        } else {
            throw new Error('Unsupported file type: ' + fileExt);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        collectionName = category + '.' + finalDate + '.' + finalSubcategory;
        console.log('��️ Target collection:', collectionName);
        
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
        
        // MongoDB operations
        const client = new MongoClient(mongoUri, { 
            maxPoolSize: 10, 
            serverSelectionTimeoutMS: 30000,
            connectTimeoutMS: 30000,
            socketTimeoutMS: 30000,
            retryWrites: true,
            ssl: true,
            tlsAllowInvalidCertificates: true
        });
        await client.connect();
        
        try {
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            
            // Update file tracker
            const trackerId = category + '.' + finalSubcategory;
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
        
        // Cleanup
        if (fs.existsSync(tempFilePath)) {
            fs.unlinkSync(tempFilePath);
        }
        
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
        
        console.log('✅ Upload successful to:', collectionName);
        res.json(response);
        
    } catch (error) {
        console.error('❌ Upload error:', error.message);
        
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

// ETL Mapping API Routes
app.get('/api/mapping/available-tables', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const availableFiles = await db.collection('file_versions_tracker').find({}).toArray();
        const tableDetails = {};
        
        for (const file of availableFiles) {
            try {
                console.log('🔍 Processing table: ' + file.subcategory + ' from collection: ' + file.latestCollection);
                
                // Get multiple sample records to ensure we capture all columns
                const sampleRecords = await db.collection(file.latestCollection).find({}).limit(5).toArray();
                console.log('📊 Found ' + sampleRecords.length + ' sample records for ' + file.subcategory);
                
                if (sampleRecords.length > 0) {
                    // Collect all unique columns from all sample records
                    const allColumns = new Set();
                    
                    sampleRecords.forEach((record, index) => {
                        console.log('📋 Record ' + (index + 1) + ' columns:', Object.keys(record));
                        Object.keys(record).forEach(key => {
                            if (!['_id', 'recordIndex', 'fileName', 'fileSize', 'category', 'subcategory', 'uploadedAt', 'processingDate'].includes(key)) {
                                allColumns.add(key);
                            }
                        });
                    });
                    
                    const cleanColumns = Array.from(allColumns);
                    console.log('✅ Final columns for ' + file.subcategory + ':', cleanColumns);
                    
                    const tableId = file.category + '.' + file.subcategory;
                    tableDetails[tableId] = {
                        id: tableId,
                        category: file.category,
                        subcategory: file.subcategory,
                        displayName: DATA_CATEGORIES[file.category]?.displayName || file.category + ' - ' + file.subcategory,
                        fileName: file.fileName,
                        recordCount: file.recordCount,
                        latestCollection: file.latestCollection,
                        date: file.latestDate,
                        columns: cleanColumns,
                        icon: DATA_CATEGORIES[file.category]?.icon || '📄'
                    };
                }
            } catch (error) {
                console.error('❌ Error processing ' + file.subcategory + ':', error);
                // Still add the table but with no columns
                const tableId = file.category + '.' + file.subcategory;
                tableDetails[tableId] = {
                    id: tableId,
                    category: file.category,
                    subcategory: file.subcategory,
                    displayName: DATA_CATEGORIES[file.category]?.displayName || file.category + ' - ' + file.subcategory,
                    fileName: file.fileName,
                    recordCount: file.recordCount,
                    latestCollection: file.latestCollection,
                    date: file.latestDate,
                    columns: [],
                    icon: DATA_CATEGORIES[file.category]?.icon || '📄'
                };
            }
        }
        
        await client.close();
        
        console.log('🎯 Sending ' + Object.keys(tableDetails).length + ' tables to frontend');
        
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
            const columnDefinitions = Object.keys(mappings).map(col => '"' + col + '" TEXT').join(', ');
            const createTableSQL = 'CREATE TABLE IF NOT EXISTS "' + tableName + '" (' +
                'id SERIAL PRIMARY KEY, ' +
                columnDefinitions + ', ' +
                'source_table TEXT, ' +
                'processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP' +
            ')';
            
            await pgPool.query(createTableSQL);
            console.log('✅ Created table: ' + tableName);
            
            // Process each selected table
            for (const tableId of selectedTables) {
                const fileTracker = await db.collection('file_versions_tracker').findOne({ _id: tableId });
                
                if (!fileTracker) {
                    console.log('⚠️ File tracker not found for ' + tableId);
                    continue;
                }
                
                const records = await db.collection(fileTracker.latestCollection).find({}).toArray();
                console.log('📊 Processing ' + records.length + ' records from ' + tableId);
                
                // Transform and insert records
                for (const record of records) {
                    const transformedRecord = {};
                    
                    Object.keys(mappings).forEach(targetColumn => {
                        const sourceColumn = mappings[targetColumn][tableId];
                        
                        if (sourceColumn === 'N/A') {
                            transformedRecord[targetColumn] = 'N/A';
                        } else if (sourceColumn === 'NULL' || !sourceColumn) {
                            transformedRecord[targetColumn] = null;
                        } else if (sourceColumn && record[sourceColumn] !== undefined) {
                            transformedRecord[targetColumn] = record[sourceColumn];
                        } else {
                            transformedRecord[targetColumn] = null;
                        }
                    });
                    
                    transformedRecord.source_table = tableId;
                    
                    const columns = Object.keys(transformedRecord);
                    const values = columns.map(col => transformedRecord[col]);
                    const placeholders = columns.map((_, i) => '$' + (i + 1)).join(', ');
                    const columnNames = columns.map(col => '"' + col + '"').join(', ');
                    
                    const insertSQL = 'INSERT INTO "' + tableName + '" (' + columnNames + ') VALUES (' + placeholders + ')';
                    await pgPool.query(insertSQL, values);
                    processedRecords++;
                }
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

// Data Viewer API Routes
app.get('/api/viewer/tables', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const availableFiles = await db.collection('file_versions_tracker').find({}).toArray();
        const tables = availableFiles.map(file => ({
            id: file.category + '.' + file.subcategory,
            category: file.category,
            subcategory: file.subcategory,
            displayName: DATA_CATEGORIES[file.category]?.displayName || file.category + ' - ' + file.subcategory,
            fileName: file.fileName,
            recordCount: file.recordCount,
            latestCollection: file.latestCollection,
            date: file.latestDate,
            icon: DATA_CATEGORIES[file.category]?.icon || '📄'
        }));
        
        await client.close();
        
        res.json({
            success: true,
            tables: tables
        });
        
    } catch (error) {
        console.error('Error getting tables for viewer:', error);
        res.status(500).json({ success: false, error: 'Failed to get tables', details: error.message });
    }
});

app.get('/api/viewer/table-data/:tableId', async (req, res) => {
    try {
        const { tableId } = req.params;
        
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const fileTracker = await db.collection('file_versions_tracker').findOne({ _id: tableId });
        
        if (!fileTracker) {
            await client.close();
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        
        const records = await db.collection(fileTracker.latestCollection).find({}).limit(100).toArray();
        
        if (records.length === 0) {
            await client.close();
            return res.json({ success: true, data: [], columns: [] });
        }
        
        const cleanRecords = records.map(record => {
            const cleanRecord = {...record};
            delete cleanRecord._id;
            delete cleanRecord.recordIndex;
            delete cleanRecord.fileName;
            delete cleanRecord.fileSize;
            delete cleanRecord.category;
            delete cleanRecord.subcategory;
            delete cleanRecord.uploadedAt;
            delete cleanRecord.processingDate;
            return cleanRecord;
        });
        
        const columns = Object.keys(cleanRecords[0] || {});
        
        await client.close();
        
        res.json({
            success: true,
            data: cleanRecords,
            columns: columns
        });
        
    } catch (error) {
        console.error('Error getting table data:', error);
        res.status(500).json({ success: false, error: 'Failed to get table data', details: error.message });
    }
});

// PostgreSQL Viewer API Routes
app.get('/api/postgresql/tables', async (req, res) => {
    try {
        const pgPool = new Pool(config.postgresql);
        
        try {
            const result = await pgPool.query('SELECT table_name, array_agg(column_name ORDER BY ordinal_position) as columns FROM information_schema.columns WHERE table_schema = \'public\' GROUP BY table_name ORDER BY table_name');
            
            const tables = result.rows.map(row => ({
                name: row.table_name,
                columns: row.columns
            }));
            
            res.json({
                success: true,
                tables: tables
            });
            
        } finally {
            await pgPool.end();
        }
        
    } catch (error) {
        console.error('Error getting PostgreSQL tables:', error);
        res.status(500).json({ success: false, error: 'Failed to get PostgreSQL tables', details: error.message });
    }
});

app.get('/api/postgresql/table-data/:tableName', async (req, res) => {
    try {
        const { tableName } = req.params;
        const pgPool = new Pool(config.postgresql);
        
        try {
            // Get column names
            const columnsResult = await pgPool.query('SELECT column_name FROM information_schema.columns WHERE table_schema = \'public\' AND table_name = $1 ORDER BY ordinal_position', [tableName]);
            
            const columns = columnsResult.rows.map(row => row.column_name);
            
            // Get table data (first 50 records)
            const dataResult = await pgPool.query('SELECT * FROM "' + tableName + '" LIMIT 50');
            
            // Get total count
            const countResult = await pgPool.query('SELECT COUNT(*) as total FROM "' + tableName + '"');
            
            res.json({
                success: true,
                columns: columns,
                rows: dataResult.rows,
                totalRecords: parseInt(countResult.rows[0].total)
            });
            
        } finally {
            await pgPool.end();
        }
        
    } catch (error) {
        console.error('Error getting PostgreSQL table data:', error);
        res.status(500).json({ success: false, error: 'Failed to get table data', details: error.message });
    }
});

// Health check endpoint for Render
app.get('/health', (req, res) => {
    res.status(200).json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        service: 'Financial ETL Pipeline'
    });
});

// MongoDB connection test endpoint
app.get('/test-mongo', async (req, res) => {
    try {
        console.log('🔍 Testing MongoDB connection...');
        const client = new MongoClient(mongoUri, { 
            maxPoolSize: 10, 
            serverSelectionTimeoutMS: 30000,
            connectTimeoutMS: 30000,
            socketTimeoutMS: 30000,
            retryWrites: true,
            ssl: true,
            tlsAllowInvalidCertificates: true
        });
        
        await client.connect();
        console.log('✅ MongoDB connected');
        
        const db = client.db(config.mongodb.database);
        await db.collection('test').insertOne({ test: 'connection', timestamp: new Date() });
        await db.collection('test').deleteOne({ test: 'connection' });
        
        await client.close();
        console.log('✅ MongoDB test completed');
        
        res.status(200).json({ 
            status: 'success', 
            message: 'MongoDB connection working',
            database: config.mongodb.database,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('❌ MongoDB test failed:', error.message);
        res.status(500).json({ 
            status: 'error', 
            message: 'MongoDB connection failed',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

app.listen(PORT, () => {
    console.log('🔥 Complete ETL System running at http://localhost:' + PORT);
    console.log('✅ Features:');
    console.log('   📤 Working file upload with fixed selection');
    console.log('   🔄 ETL mapping tab for PostgreSQL tables');
    console.log('   👁️ Data viewer tab to see uploaded data');
    console.log('   🎯 Table selection and column mapping');
    console.log('   📊 MongoDB to PostgreSQL processing');
});

// Clear all PostgreSQL data endpoint
app.post('/api/postgresql/clear-all', async (req, res) => {
    try {
        const pgPool = new Pool(config.postgresql);
        
        try {
            console.log('🧹 Clearing all PostgreSQL tables...');
            
            // Get all user tables in public schema
            const tablesResult = await pgPool.query('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\' AND table_type = \'BASE TABLE\' ORDER BY table_name');
            
            const tablesToClear = tablesResult.rows.map(row => row.table_name);
            console.log('📋 Tables to clear:', tablesToClear);
            
            // Clear each table
            for (const tableName of tablesToClear) {
                try {
                    // Use DROP TABLE instead of TRUNCATE for complete removal
                    await pgPool.query('DROP TABLE IF EXISTS "' + tableName + '" CASCADE');
                    console.log('✅ Dropped table: ' + tableName);
                } catch (error) {
                    console.error('❌ Error dropping ' + tableName + ':', error.message);
                }
            }
            
            res.json({
                success: true,
                message: 'Successfully cleared ' + tablesToClear.length + ' PostgreSQL tables',
                clearedTables: tablesToClear
            });
            
        } finally {
            await pgPool.end();
        }
        
    } catch (error) {
        console.error('Error clearing PostgreSQL tables:', error);
        res.status(500).json({ 
            success: false, 
            error: 'Failed to clear PostgreSQL tables', 
            details: error.message 
        });
    }
});
 