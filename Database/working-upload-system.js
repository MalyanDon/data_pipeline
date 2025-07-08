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
    broker_master: { displayName: 'Broker Master', subcategories: ['broker_master'], icon: '🏢', hasSubcategories: false },
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

// Add this function after cleanupColumnName function
function isVerticalFormatCategory(category, subcategory) {
    const verticalCategories = [
        'client_info',
        'distributor_master',
        'broker_master',
        'strategy_master'
    ];
    return verticalCategories.includes(category);
}

async function processVerticalFormatFile(filePath) {
    try {
        const csvData = fs.readFileSync(filePath, 'utf8');
        const { parse } = require('csv-parse/sync');
        
        // Parse all rows
        const rows = parse(csvData, {
            skip_empty_lines: false, // Don't skip empty lines to preserve structure
            trim: true
        });

        console.log('🔄 Processing vertical format file...');
        console.log(`📊 Total rows found: ${rows.length}`);
        
        // Debug: Show first 10 rows
        console.log('📋 First 10 rows preview:');
        rows.slice(0, 10).forEach((row, index) => {
            console.log(`   Row ${index + 1}: [${row.join(' | ')}] (${row.length} columns)`);
        });
        
        const record = {};
        let fieldsProcessed = 0;
        
        rows.forEach((row, index) => {
            if (row && row.length >= 1) {
                const fieldName = cleanupColumnName(row[0]);
                if (fieldName && fieldName.trim() !== '') {
                    // Take all remaining columns as potential values
                    const values = row.slice(1).filter(val => val !== '' && val !== null && val !== undefined);
                    
                    // Always add the field, even if no values
                    if (values.length === 0) {
                        record[fieldName] = 'N/A';
                        fieldsProcessed++;
                        console.log(`   ✅ Field ${fieldsProcessed}: "${fieldName}" = "N/A" (no value)`);
                    } else {
                        record[fieldName] = values.length === 1 ? values[0] : values.join(' ');
                        fieldsProcessed++;
                        console.log(`   ✅ Field ${fieldsProcessed}: "${fieldName}" = "${record[fieldName]}"`);
                    }
                }
            }
        });
        
        console.log(`📊 Total fields processed: ${fieldsProcessed}`);
        console.log(`📋 Final record keys: ${Object.keys(record).join(', ')}`);
        
        if (Object.keys(record).length === 0) {
            console.log('⚠️ No fields found, falling back to horizontal format processing');
            return await processHorizontalFormatFile(filePath);
        }
        
        return [record]; // Return as array to maintain consistency with horizontal format
    } catch (error) {
        console.error('❌ Error processing vertical format file:', error);
        throw error;
    }
}

async function processHorizontalFormatFile(filePath) {
    try {
        const csvData = fs.readFileSync(filePath, 'utf8');
        const { parse } = require('csv-parse/sync');
        
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
        
        if (bestData.length === 0) {
            throw new Error('No valid data rows found in file');
                                    }
        
        return bestData;
            } catch (error) {
        console.error('❌ Error processing horizontal format file:', error);
        throw error;
            }
        }
        
async function processCSVFile(filePath, category = '', subcategory = '') {
    try {
        const csvData = fs.readFileSync(filePath, 'utf8');
        
        // First, let's try simple CSV parsing
        console.log('📁 CSV file content preview (first 500 chars):');
        console.log(csvData.substring(0, 500));
        
        // Determine format based on category
        const isVertical = isVerticalFormatCategory(category);
        console.log(`📊 Format detection: ${isVertical ? 'Vertical' : 'Horizontal'} format detected for category: ${category}`);
        
        // Process based on format
        const processedData = isVertical 
            ? await processVerticalFormatFile(filePath)
            : await processHorizontalFormatFile(filePath);
        
        console.log(`✅ Processed ${processedData.length} records`);
        return processedData;
        
    } catch (error) {
        console.error('❌ Error processing file:', error);
        throw error;
    }
}

async function processExcelFile(filePath, category = '', subcategory = '') {
    try {
        const workbook = xlsx.readFile(filePath, { cellDates: true, cellNF: false, cellText: false });
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];
        
        // Convert to array of arrays first - don't use defval to preserve empty cells
        const rawData = xlsx.utils.sheet_to_json(worksheet, { header: 1, raw: false });
        
        // Don't filter out rows, just work with what we have
        const rows = rawData;
        
        console.log('🔍 Smart Excel processing: Scanning for headers...');
        console.log(`📊 Total rows found: ${rows.length}`);
        
        // Debug: Show first 10 rows with ALL columns
        console.log('📋 First 10 rows preview (showing all columns):');
        rows.slice(0, 10).forEach((row, index) => {
            console.log(`   Row ${index + 1}: [${row.join(' | ')}] (${row.length} columns)`);
        });
        
        // Determine format based on category
        const isVertical = isVerticalFormatCategory(category);
        console.log(`📊 Format detection: ${isVertical ? 'Vertical' : 'Horizontal'} format detected for category: ${category}`);
        
        if (isVertical) {
            // Process vertical format (column names in first column)
            console.log('🔄 Processing vertical format Excel...');
            const record = {};
            let fieldsProcessed = 0;
            
            rows.forEach((row, index) => {
                if (row && row.length >= 1) {
                    const fieldName = cleanupColumnName(row[0]);
                    if (fieldName && fieldName.trim() !== '') {
                        // Take all remaining columns as potential values
                        const values = row.slice(1).filter(val => val !== '' && val !== null && val !== undefined);
                        
                        // Always add the field, even if no values
                        if (values.length === 0) {
                            record[fieldName] = 'N/A';
                            fieldsProcessed++;
                            console.log(`   ✅ Field ${fieldsProcessed}: "${fieldName}" = "N/A" (no value)`);
                        } else {
                            record[fieldName] = values.length === 1 ? values[0] : values.join(' ');
                            fieldsProcessed++;
                            console.log(`   ✅ Field ${fieldsProcessed}: "${fieldName}" = "${record[fieldName]}"`);
                        }
                    }
                }
            });
            
            console.log(`📊 Total fields processed: ${fieldsProcessed}`);
            console.log(`📋 Final record keys: ${Object.keys(record).join(', ')}`);
            
            if (Object.keys(record).length === 0) {
                console.log('⚠️ No fields found in vertical format, falling back to horizontal format processing');
                // Fall back to horizontal processing
                return await processHorizontalExcelFormat(rows);
            }
            
            return [record]; // Single record with all fields
        } else {
            return await processHorizontalExcelFormat(rows);
        }
    } catch (error) {
        console.error('❌ Error processing Excel file:', error);
        throw error;
    }
}

async function processHorizontalExcelFormat(rows) {
    // Process horizontal format
    console.log('🔄 Processing horizontal format Excel...');
    let bestData = [];
    let bestHeaders = [];
    let bestScore = 0;
    let bestStartRow = 0;
    
    // Try different starting rows
    for (let startRow = 0; startRow < Math.min(20, rows.length); startRow++) {
        try {
            const potentialHeaders = rows[startRow];
            if (!potentialHeaders || potentialHeaders.length < 2) continue;
            
            const cleanHeaders = potentialHeaders.map(h => cleanupColumnName(h.toString()));
            const score = calculateHeaderScore(cleanHeaders);
            
            console.log(`📊 Row ${startRow + 1}: ${cleanHeaders.length} columns, score: ${score}`);
            console.log(`   Headers: ${cleanHeaders.slice(0, 5).join(', ')}${cleanHeaders.length > 5 ? '...' : ''}`);
            
            if (score > bestScore && rows.length > startRow + 2) {
                const data = [];
                // Convert remaining rows to objects
                for (let i = startRow + 1; i < rows.length; i++) {
                    const row = rows[i];
                    if (row.length < 2) continue;
                    
                    const record = {};
                    cleanHeaders.forEach((header, index) => {
                        if (header && row[index] !== undefined && row[index] !== '') {
                            record[header] = row[index];
                        }
                    });
                    
                    if (Object.keys(record).length > 0) {
                        data.push(record);
                    }
                }
                
                if (data.length > 0) {
                    bestScore = score;
                    bestData = data;
                    bestHeaders = cleanHeaders;
                    bestStartRow = startRow;
                }
            }
        } catch (error) {
            console.log(`⚠️ Error processing row ${startRow + 1}:`, error.message);
            continue;
        }
    }
    
    if (bestData.length === 0) {
        throw new Error('No valid data rows found in Excel file');
    }
    
    console.log(`✅ Best Excel headers found: Starting row ${bestStartRow + 1} (score: ${bestScore})`);
    console.log(`📋 Final headers: ${bestHeaders.join(', ')}`);
    
    return bestData;
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

// Add this function after the cleanupColumnName function
function generateSafeCollectionName(filename) {
    // Remove file extension
    let safeName = filename.replace(/\.[^/.]+$/, "");
    // Remove special characters and spaces
    safeName = safeName.replace(/[^a-zA-Z0-9_]/g, "_");
    // Ensure it starts with a letter (MongoDB requirement)
    if (/^[^a-zA-Z]/.test(safeName)) {
        safeName = "file_" + safeName;
    }
    // Limit length to avoid very long collection names
    return safeName.substring(0, 40);
}

// Add this function after the cleanupColumnName function
function extractFileIdentifier(filename) {
    // Remove file extension
    let name = filename.replace(/\.[^/.]+$/, "");
    
    // Extract patterns like DL123, DL_123, etc.
    const matches = name.match(/([A-Za-z]+[_]?\d+)/);
    if (matches) {
        return matches[1].toUpperCase();
    }
    
    // If no specific pattern found, create a clean identifier from the name
    name = name.replace(/[^a-zA-Z0-9]/g, "_");
    name = name.replace(/_+/g, "_"); // Replace multiple underscores with single
    name = name.replace(/^_|_$/g, ""); // Remove leading/trailing underscores
    
    // Ensure it starts with a letter
    if (/^[^a-zA-Z]/.test(name)) {
        name = "FILE_" + name;
    }
    
    // Limit length and return uppercase
    return name.substring(0, 30).toUpperCase();
}

function generateTimestamp() {
    return new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14); // YYYYMMDDHHmmss
}

// Main page route
app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🔥 Complete ETL System</title>
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
            .upload-area { border: 2px dashed #ced4da; border-radius: 10px; padding: 40px; text-align: center; margin: 20px 0; transition: all 0.3s; cursor: pointer; }
            .upload-area:hover { border-color: #667eea; background: #f0f4ff; }
            .upload-area.drag-over { border-color: #667eea; background: #f0f4ff; }
            .btn { background: #667eea; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 16px; transition: all 0.3s; }
            .btn:hover { background: #5a6fd8; }
            .btn:disabled { background: #6c757d; cursor: not-allowed; }
            .btn.danger { background: #dc3545; }
            .btn.danger:hover { background: #c82333; }
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
            .current-selection { background: #e3f2fd; padding: 15px; border-radius: 8px; margin: 15px 0; border: 1px solid #667eea; }
            .file-input-hidden { position: absolute; left: -9999px; opacity: 0; pointer-events: none; }
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
                <button class="tab active" onclick="switchTab('upload')">📤 Upload Files</button>
                <button class="tab" onclick="switchTab('mapping')">🔄 ETL Mapping</button>
                <button class="tab" onclick="switchTab('viewer')">👁️ Data Viewer</button>
                <button class="tab" onclick="switchTab('postgres')">🐘 PostgreSQL Viewer</button>
            </div>
            
            <div class="content">
                <!-- Upload Tab -->
                <div id="uploadTab" class="tab-content">
                    <h2>📤 Upload Files to MongoDB</h2>
                    
                    <div class="current-selection" id="currentSelection">
                        <strong>Current Selection:</strong> <span id="selectionDisplay">Please select category and subcategory</span>
                    </div>
                    
                    <div class="category-grid" id="categoryGrid">
                        <!-- Categories will be loaded here -->
                    </div>
                    
                    <!-- Hidden file input -->
                    <input type="file" id="fileInput" class="file-input-hidden" accept=".xlsx,.xls,.csv" />
                    
                    <div class="upload-area" id="uploadArea" onclick="openFileDialog()">
                        <div class="upload-content">
                            <h3>📁 Click here to select file or drag & drop</h3>
                            <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                            <button type="button" class="btn" onclick="openFileDialog(); event.stopPropagation();">Choose File</button>
                        </div>
                    </div>
                    
                    <div class="form-group">
                        <label for="dateInput">Processing Date (optional - auto-extracted from filename):</label>
                        <input type="date" id="dateInput">
                    </div>
                    
                    <button type="button" class="btn" id="uploadBtn" onclick="uploadFile()" disabled>Upload File</button>
                    
                    <div id="uploadStatus"></div>
                </div>
                
                <!-- Data Viewer Tab -->
                <div id="viewerTab" class="tab-content hidden">
                    <h2>👁️ Data Viewer</h2>
                    
                    <button class="btn" onclick="loadDataViewer()">🔄 Refresh Data</button>
                    
                    <div id="dataViewerStatus"></div>
                    
                    <div id="dataTablesContainer">
                        <!-- Data tables will be loaded here -->
                    </div>
                </div>
                
                <!-- ETL Mapping Tab -->
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
                        <div style="background: #e3f2fd; padding: 15px; border-radius: 8px; margin: 15px 0; border-left: 4px solid #667eea;">
                            <p><strong>💡 Mapping Options:</strong></p>
                            <ul style="margin: 10px 0; padding-left: 20px;">
                                <li><strong>🚫 N/A (Not Available)</strong> - Use when the column doesn't exist in that custodian's file</li>
                                <li><strong>∅ NULL (Empty Value)</strong> - Use to explicitly set empty/null values</li>
                                <li><strong>Column Names</strong> - Select actual column names from the uploaded files</li>
                            </ul>
                            <p style="margin-top: 10px; font-size: 12px; color: #666;"><em>Every dropdown has N/A and NULL options for maximum flexibility!</em></p>
                        </div>
                        <button class="btn" onclick="addColumnMapping()">➕ Add Column Mapping</button>
                        <div id="columnMappings"></div>
                        
                        <button class="btn" onclick="processMapping()" style="margin-top: 20px;">🚀 Process to PostgreSQL</button>
                    </div>
                    
                    <div id="mappingStatus"></div>
                </div>
                
                <!-- PostgreSQL Viewer Tab -->
                <div id="postgresTab" class="tab-content hidden">
                    <h2>🐘 PostgreSQL Viewer</h2>
                    
                    <button class="btn" onclick="loadPostgreSQLTables()">🔄 Refresh PostgreSQL Tables</button>
                    
                    <div id="postgresStatus"></div>
                    
                    <div id="postgresTablesContainer">
                        <!-- PostgreSQL tables will be loaded here -->
                    </div>
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
            
            console.log('🔥 Complete ETL System initialized');
            
            // Initialize
            loadCategories();
            
            function switchTab(tabName) {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(t => t.classList.add('hidden'));
                
                event.target.classList.add('active');
                document.getElementById(tabName + 'Tab').classList.remove('hidden');
                
                if (tabName === 'mapping') {
                    loadAvailableTables();
                } else if (tabName === 'viewer') {
                    loadDataViewer();
                }
            }
            
            // File input handling
            function openFileDialog() {
                console.log('📁 Opening file dialog...');
                const fileInput = document.getElementById('fileInput');
                fileInput.click();
            }
            
            function setupFileHandlers() {
                const fileInput = document.getElementById('fileInput');
                const uploadArea = document.getElementById('uploadArea');
                
                fileInput.addEventListener('change', function(e) {
                    console.log('📁 File input changed:', e.target.files.length);
                    if (e.target.files.length > 0) {
                        handleFileSelect(e.target.files[0]);
                    }
                });
                
                uploadArea.addEventListener('dragover', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    uploadArea.classList.add('drag-over');
                });
                
                uploadArea.addEventListener('dragleave', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    uploadArea.classList.remove('drag-over');
                });
                
                uploadArea.addEventListener('drop', function(e) {
                    e.preventDefault();
                    e.stopPropagation();
                    uploadArea.classList.remove('drag-over');
                    
                    const files = e.dataTransfer.files;
                    if (files.length > 0) {
                        handleFileSelect(files[0]);
                    }
                });
            }
            
            async function loadCategories() {
                try {
                    console.log('📋 Loading categories...');
                    const response = await fetch('/api/upload/categories');
                    const data = await response.json();
                    
                    if (data.success) {
                        console.log('✅ Categories loaded');
                        renderCategories(data.categories);
                        setupFileHandlers();
                    }
                } catch (error) {
                    console.error('❌ Error loading categories:', error);
                }
            }
            
            function renderCategories(categories) {
                const grid = document.getElementById('categoryGrid');
                grid.innerHTML = '';
                
                Object.entries(categories).forEach(([key, category]) => {
                    const card = document.createElement('div');
                    card.className = 'category-card';
                    card.onclick = function() { selectCategory(key, category); };
                    
                    let subcategoryHTML = '';
                    if (category.hasSubcategories) {
                        subcategoryHTML = \`
                        <div class="subcategory-container" id="sub_\${key}">
                            <h4>Choose subcategory:</h4>
                            <div class="subcategory-grid">
                                \${category.subcategories.map(sub => \`
                                    <div class="subcategory-btn" onclick="selectSubcategory(event, '\${sub}')">\${sub.toUpperCase()}</div>
                                \`).join('')}
                            </div>
                        </div>
                        \`;
                    }
                    
                    card.innerHTML = \`
                        <h3>\${category.icon} \${category.displayName}</h3>
                        <p>Click to select this category</p>
                        \${subcategoryHTML}
                    \`;
                    
                    grid.appendChild(card);
                });
            }
            
            function selectCategory(key, category) {
                console.log('🎯 Category selected:', key);
                
                selectedCategory = null;
                selectedSubcategory = null;
                
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                event.currentTarget.classList.add('selected');
                selectedCategory = key;
                
                if (category.hasSubcategories) {
                    const subcategoryContainer = document.getElementById(\`sub_\${key}\`);
                    if (subcategoryContainer) {
                        subcategoryContainer.classList.add('show');
                    }
                    console.log('📂 Showing subcategories for:', key);
                } else {
                    selectedSubcategory = category.subcategories[0];
                    console.log('✅ Auto-selected subcategory:', selectedSubcategory);
                }
                
                updateDisplay();
                updateUploadButton();
            }
            
            function selectSubcategory(event, subcategory) {
                console.log('🎯 Subcategory selected:', subcategory);
                event.stopPropagation();
                
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                event.target.classList.add('selected');
                selectedSubcategory = subcategory;
                
                console.log('✅ Final selection:', { category: selectedCategory, subcategory: selectedSubcategory });
                
                updateDisplay();
                updateUploadButton();
            }
            
            function updateDisplay() {
                const display = document.getElementById('selectionDisplay');
                if (selectedCategory && selectedSubcategory) {
                    display.innerHTML = \`<strong>Category:</strong> \${selectedCategory} | <strong>Subcategory:</strong> \${selectedSubcategory}\`;
                } else if (selectedCategory) {
                    display.innerHTML = \`<strong>Category:</strong> \${selectedCategory} | <strong>Subcategory:</strong> Please select\`;
                } else {
                    display.innerHTML = 'Please select category and subcategory';
                }
            }
            
            function updateUploadButton() {
                const btn = document.getElementById('uploadBtn');
                const canUpload = selectedCategory && selectedSubcategory && selectedFile;
                btn.disabled = !canUpload;
                
                console.log('🔄 Button update:', { 
                    selectedCategory, 
                    selectedSubcategory, 
                    selectedFile: selectedFile?.name, 
                    canUpload 
                });
            }
            
            function handleFileSelect(file) {
                console.log('📁 File selected:', file.name, file.size);
                selectedFile = file;
                
                const uploadArea = document.getElementById('uploadArea');
                uploadArea.innerHTML = \`
                    <div class="upload-content">
                        <h3>✅ File Selected: \${file.name}</h3>
                        <p>Size: \${(file.size / (1024*1024)).toFixed(2)} MB</p>
                        <button type="button" class="btn" onclick="openFileDialog(); event.stopPropagation();">Choose Different File</button>
                    </div>
                \`;
                
                updateUploadButton();
            }
            
            async function uploadFile() {
                console.log('🚀 Upload initiated:', { 
                    category: selectedCategory, 
                    subcategory: selectedSubcategory, 
                    file: selectedFile?.name 
                });
                
                if (!selectedCategory || !selectedSubcategory || !selectedFile) {
                    showStatus('uploadStatus', 'Please select category, subcategory and file', 'error');
                    return;
                }
                
                const formData = new FormData();
                formData.append('file', selectedFile);
                
                const processingDate = document.getElementById('dateInput').value;
                if (processingDate) {
                    formData.append('processingDate', processingDate);
                }
                
                try {
                    showStatus('uploadStatus', \`Uploading \${selectedFile.name} to \${selectedCategory}/\${selectedSubcategory}...\`, 'info');
                    
                    const url = \`/api/upload/\${selectedCategory}/\${selectedSubcategory}\`;
                    console.log('📡 Upload URL:', url);
                    
                    const response = await fetch(url, {
                        method: 'POST',
                        body: formData
                    });
                    
                    const result = await response.json();
                    console.log('📥 Upload result:', result);
                    
                    if (result.success) {
                        showStatus('uploadStatus', \`✅ Success! \${result.details.recordCount} records uploaded to \${result.details.collectionName}\`, 'success');
                        resetUploadForm();
                    } else {
                        showStatus('uploadStatus', \`❌ Upload failed: \${result.error}\`, 'error');
                    }
                } catch (error) {
                    console.error('❌ Upload error:', error);
                    showStatus('uploadStatus', \`❌ Upload error: \${error.message}\`, 'error');
                }
            }
            
            function resetUploadForm() {
                selectedCategory = null;
                selectedSubcategory = null;
                selectedFile = null;
                
                const fileInput = document.getElementById('fileInput');
                fileInput.value = '';
                document.getElementById('dateInput').value = '';
                
                document.querySelectorAll('.category-card').forEach(c => c.classList.remove('selected'));
                document.querySelectorAll('.subcategory-container').forEach(c => c.classList.remove('show'));
                document.querySelectorAll('.subcategory-btn').forEach(b => b.classList.remove('selected'));
                
                const uploadArea = document.getElementById('uploadArea');
                uploadArea.innerHTML = \`
                    <div class="upload-content">
                        <h3>📁 Click here to select file or drag & drop</h3>
                        <p>Supports Excel (.xlsx, .xls) and CSV files up to 100MB</p>
                        <button type="button" class="btn" onclick="openFileDialog(); event.stopPropagation();">Choose File</button>
                    </div>
                \`;
                
                updateDisplay();
                updateUploadButton();
            }
            
            // ETL Mapping functionality
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
                
                if (Object.keys(tables).length === 0) {
                    container.innerHTML = '<p>No tables found. Please upload some files first.</p>';
                    return;
                }
                
                Object.entries(tables).forEach(([id, table]) => {
                    const card = document.createElement('div');
                    card.className = 'table-card';
                    card.onclick = () => toggleTableSelection(id, card);
                    
                    card.innerHTML = \`
                        <h4>\${table.icon} \${table.displayName}</h4>
                        <p><strong>Records:</strong> \${table.recordCount}</p>
                        <p><strong>Date:</strong> \${table.date}</p>
                        <p><strong>File:</strong> \${table.fileName}</p>
                        <p><strong>Columns:</strong> \${table.columns.length}</p>
                        <div style="margin-top: 10px; font-size: 12px; color: #666;">
                            <strong>Available Columns:</strong><br>
                            \${table.columns.slice(0, 5).join(', ')}\${table.columns.length > 5 ? '...' : ''}
                        </div>
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
                if (targetColumn && targetColumn.trim()) {
                    const cleanTargetColumn = targetColumn.trim();
                    if (!columnMappings[cleanTargetColumn]) {
                        columnMappings[cleanTargetColumn] = {};
                        updateColumnMappings();
                    } else {
                        alert('Column already exists!');
                    }
                }
            }
            
            function updateColumnMappings() {
                const container = document.getElementById('columnMappings');
                container.innerHTML = '';
                
                if (Object.keys(columnMappings).length === 0) {
                    container.innerHTML = '<p>No column mappings defined. Click "Add Column Mapping" to start.</p>';
                    return;
                }
                
                // Create table-style mapping
                const tableHTML = \`
                    <div style="overflow-x: auto; margin: 20px 0;">
                        <table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd;">
                            <thead>
                                <tr style="background: #f8f9fa;">
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: left; min-width: 150px;">
                                        Target Column Name
                                    </th>
                                    \${selectedTables.map(tableId => {
                                        const table = availableTablesData[tableId];
                                        return \`<th style="border: 1px solid #ddd; padding: 12px; text-align: left; min-width: 200px;">
                                            \${table.displayName}<br>
                                            <small style="color: #666;">(\${table.columns.length} columns)</small>
                                        </th>\`;
                                    }).join('')}
                                    <th style="border: 1px solid #ddd; padding: 12px; text-align: center; width: 100px;">
                                        Action
                                    </th>
                                </tr>
                            </thead>
                            <tbody>
                                \${Object.keys(columnMappings).map(targetColumn => \`
                                    <tr>
                                        <td style="border: 1px solid #ddd; padding: 12px; font-weight: bold; background: #f8f9fa;">
                                            \${targetColumn}
                                        </td>
                                        \${selectedTables.map(tableId => {
                                            const table = availableTablesData[tableId];
                                            const selectedValue = columnMappings[targetColumn][tableId] || '';
                                            return \`<td style="border: 1px solid #ddd; padding: 8px;">
                                                <select style="width: 100%; padding: 6px;" onchange="updateMapping('\${targetColumn}', '\${tableId}', this.value)">
                                                    <option value="">-- Select Column --</option>
                                                    <option value="N/A" \${selectedValue === 'N/A' ? 'selected' : ''} style="background: #ffe6e6; font-weight: bold; color: #d63384;">🚫 N/A (Not Available)</option>
                                                    <option value="NULL" \${selectedValue === 'NULL' ? 'selected' : ''} style="background: #f0f0f0; font-weight: bold; color: #6c757d;">∅ NULL (Empty Value)</option>
                                                    <option disabled style="background: #e9ecef; font-weight: bold; text-align: center;">━━━ Available Columns ━━━</option>
                                                    \${table.columns && table.columns.length > 0 ? table.columns.map(col => \`
                                                        <option value="\${col}" \${selectedValue === col ? 'selected' : ''}>\${col}</option>
                                                    \`).join('') : '<option disabled style="color: #dc3545;">⚠️ No columns available in this table</option>'}
                                                </select>
                                            </td>\`;
                                        }).join('')}
                                        <td style="border: 1px solid #ddd; padding: 8px; text-align: center;">
                                            <button class="btn danger" onclick="removeColumnMapping('\${targetColumn}')" 
                                                    style="padding: 4px 8px; font-size: 12px;">
                                                ❌ Remove
                                            </button>
                                        </td>
                                    </tr>
                                \`).join('')}
                            </tbody>
                        </table>
                    </div>
                \`;
                
                container.innerHTML = tableHTML;
            }
            
            function updateMapping(targetColumn, tableId, sourceColumn) {
                columnMappings[targetColumn][tableId] = sourceColumn;
                console.log('Updated mapping:', { targetColumn, tableId, sourceColumn });
            }
            
            function removeColumnMapping(targetColumn) {
                if (confirm(\`Are you sure you want to remove column "\${targetColumn}"?\`)) {
                    delete columnMappings[targetColumn];
                    updateColumnMappings();
                }
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
            
            // PostgreSQL Viewer functionality
            async function loadPostgreSQLTables() {
                try {
                    showStatus('postgresStatus', 'Loading PostgreSQL tables...', 'info');
                    
                    const response = await fetch('/api/postgresql/tables');
                    const data = await response.json();
                    
                    if (data.success) {
                        renderPostgreSQLTables(data.tables);
                        showStatus('postgresStatus', \`✅ Found \${data.tables.length} PostgreSQL tables\`, 'success');
                    } else {
                        showStatus('postgresStatus', \`❌ Error loading tables: \${data.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('postgresStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function renderPostgreSQLTables(tables) {
                const container = document.getElementById('postgresTablesContainer');
                
                if (tables.length === 0) {
                    container.innerHTML = '<p>No PostgreSQL tables found. Create some tables using the ETL mapping first.</p>';
                    return;
                }
                
                container.innerHTML = \`
                    <div style="margin: 20px 0;">
                        \${tables.map(table => \`
                            <div class="table-card" style="margin-bottom: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 8px;">
                                <h4>🗂️ \${table.name}</h4>
                                <p><strong>Columns:</strong> \${table.columns.length}</p>
                                <button class="btn" onclick="viewTableData('\${table.name}')">👁️ View Data</button>
                                <div id="tableData_\${table.name}" style="margin-top: 15px; display: none;"></div>
                            </div>
                        \`).join('')}
                    </div>
                \`;
            }
            
            async function viewTableData(tableName) {
                try {
                    const response = await fetch(\`/api/postgresql/table-data/\${tableName}\`);
                    const data = await response.json();
                    
                    const container = document.getElementById(\`tableData_\${tableName}\`);
                    
                    if (data.success) {
                        container.style.display = 'block';
                        container.innerHTML = \`
                            <div style="overflow-x: auto;">
                                <table style="width: 100%; border-collapse: collapse; border: 1px solid #ddd; font-size: 12px;">
                                    <thead>
                                        <tr style="background: #f8f9fa;">
                                            \${data.columns.map(col => \`<th style="border: 1px solid #ddd; padding: 8px; text-align: left;">\${col}</th>\`).join('')}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        \${data.rows.map(row => \`
                                            <tr>
                                                \${data.columns.map(col => \`<td style="border: 1px solid #ddd; padding: 8px;">\${row[col] || ''}</td>\`).join('')}
                                            </tr>
                                        \`).join('')}
                                    </tbody>
                                </table>
                                <p style="margin-top: 10px; color: #666; font-size: 12px;">
                                    Showing first 50 records. Total records: \${data.totalRecords}
                                </p>
                            </div>
                        \`;
                    } else {
                        container.innerHTML = \`<p style="color: red;">Error loading data: \${data.error}</p>\`;
                    }
                } catch (error) {
                    const container = document.getElementById(\`tableData_\${tableName}\`);
                    container.innerHTML = \`<p style="color: red;">Error: \${error.message}</p>\`;
                }
            }
            
            // Data Viewer functionality
            async function loadDataViewer() {
                try {
                    showStatus('dataViewerStatus', 'Loading data tables...', 'info');
                    
                    const response = await fetch('/api/viewer/tables');
                    const data = await response.json();
                    
                    if (data.success) {
                        renderDataTables(data.tables);
                        showStatus('dataViewerStatus', \`✅ Loaded \${data.tables.length} tables\`, 'success');
                    } else {
                        showStatus('dataViewerStatus', \`❌ Error loading data: \${data.error}\`, 'error');
                    }
                } catch (error) {
                    showStatus('dataViewerStatus', \`❌ Error: \${error.message}\`, 'error');
                }
            }
            
            function renderDataTables(tables) {
                const container = document.getElementById('dataTablesContainer');
                container.innerHTML = '';
                
                if (tables.length === 0) {
                    container.innerHTML = '<p>No tables found. Please upload some files first.</p>';
                    return;
                }
                
                tables.forEach(table => {
                    const tableDiv = document.createElement('div');
                    tableDiv.style.marginBottom = '30px';
                    
                    tableDiv.innerHTML = \`
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin-bottom: 10px;">
                            <h3>\${table.icon} \${table.displayName}</h3>
                            <p><strong>File:</strong> \${table.fileName} | <strong>Records:</strong> \${table.recordCount} | <strong>Date:</strong> \${table.date}</p>
                            <button class="btn" onclick="loadTableData('\${table.id}')" style="margin-top: 10px;">
                                👁️ View Data
                            </button>
                        </div>
                        <div id="tableData_\${table.id}" style="display: none;"></div>
                    \`;
                    
                    container.appendChild(tableDiv);
                });
            }
            
            async function loadTableData(tableId) {
                try {
                    const dataContainer = document.getElementById(\`tableData_\${tableId}\`);
                    dataContainer.innerHTML = '<p>Loading data...</p>';
                    dataContainer.style.display = 'block';
                    
                    const response = await fetch(\`/api/viewer/table-data/\${tableId}\`);
                    const data = await response.json();
                    
                    if (data.success) {
                        renderTableData(dataContainer, data.data, data.columns);
                    } else {
                        dataContainer.innerHTML = \`<p>❌ Error loading data: \${data.error}</p>\`;
                    }
                } catch (error) {
                    const dataContainer = document.getElementById(\`tableData_\${tableId}\`);
                    dataContainer.innerHTML = \`<p>❌ Error: \${error.message}</p>\`;
                }
            }
            
            function renderTableData(container, data, columns) {
                if (!data || data.length === 0) {
                    container.innerHTML = '<p>No data found.</p>';
                    return;
                }
                
                const tableHTML = \`
                    <div style="overflow-x: auto; max-height: 400px; overflow-y: auto; border: 1px solid #ddd; border-radius: 5px;">
                        <table style="width: 100%; border-collapse: collapse;">
                            <thead style="position: sticky; top: 0; background: #f8f9fa;">
                                <tr>
                                    \${columns.map(col => \`<th style="border: 1px solid #ddd; padding: 8px; text-align: left; white-space: nowrap;">\${col}</th>\`).join('')}
                                </tr>
                            </thead>
                            <tbody>
                                \${data.slice(0, 50).map(row => \`
                                    <tr>
                                        \${columns.map(col => \`<td style="border: 1px solid #ddd; padding: 8px; white-space: nowrap;">\${row[col] || ''}</td>\`).join('')}
                                    </tr>
                                \`).join('')}
                            </tbody>
                        </table>
                    </div>
                    <p style="margin-top: 10px; color: #666; font-size: 12px;">
                        Showing first 50 records of \${data.length} total records
                    </p>
                \`;
                
                container.innerHTML = tableHTML;
            }
        </script>
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
            return res.status(400).json({ success: false, error: `Invalid category: ${category}` });
        }
        
        if (!req.file) {
            return res.status(400).json({ success: false, error: 'No file uploaded' });
        }
        
        tempFilePath = req.file.path;
        
        const extractedDate = extractDateFromFilename(req.file.originalname);
        const finalDate = processingDate || extractedDate;
        let finalSubcategory = subcategory || DATA_CATEGORIES[category].subcategories[0];
        
        // If category is 'others', extract subcategory from filename
        if (finalSubcategory === 'others') {
            finalSubcategory = extractFileIdentifier(req.file.originalname);
            console.log('📑 Extracted subcategory from filename:', finalSubcategory);
        }
        
        console.log('📅 Processing:', { extractedDate, finalDate, finalSubcategory });
        
        let processedData = [];
        const fileExt = path.extname(req.file.originalname).toLowerCase();
        
        if (fileExt === '.csv') {
            processedData = await processCSVFile(req.file.path, category, finalSubcategory);
        } else if (['.xlsx', '.xls'].includes(fileExt)) {
            processedData = await processExcelFile(req.file.path, category, finalSubcategory);
        } else {
            throw new Error(`Unsupported file type: ${fileExt}`);
        }
        
        if (processedData.length === 0) {
            throw new Error('No data found in file or file is empty');
        }
        
        // Add timestamp to collection name
        const timestamp = generateTimestamp();
        collectionName = `${category}.${finalDate}.${finalSubcategory}.${timestamp}`;
        console.log('🗂️ Target collection:', collectionName);
        
        enhancedData = processedData.map((record, index) => ({
            ...record,
            recordIndex: index + 1,
            fileName: req.file.originalname,
            fileSize: req.file.size,
            category: category,
            subcategory: finalSubcategory,
            uploadedAt: new Date().toISOString(),
            processingDate: finalDate,
            collectionTimestamp: timestamp
        }));
        
        // MongoDB operations
        const client = new MongoClient(mongoUri, { maxPoolSize: 10, serverSelectionTimeoutMS: 5000 });
        await client.connect();
        
        try {
            const db = client.db('financial_data_2025');
            await db.collection(collectionName).insertMany(enhancedData);
            
            // Update file tracker with version history
            const trackerId = `${category}.${finalSubcategory}`;
            const trackerUpdate = {
                $set: {
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
                $push: {
                    versions: {
                        collection: collectionName,
                        date: finalDate,
                        fileName: req.file.originalname,
                        recordCount: enhancedData.length,
                        uploadedAt: new Date().toISOString(),
                        timestamp: timestamp
                    }
                }
            };
            
            await db.collection('file_versions_tracker').updateOne(
                { _id: trackerId },
                trackerUpdate,
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
    console.log('📊 Getting available tables for mapping...');
    const tableDetails = {};
    
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        // Get all file trackers
        const fileTrackers = await db.collection('file_versions_tracker').find({}).toArray();
        console.log(`📋 Found ${fileTrackers.length} file trackers`);
        
        for (const file of fileTrackers) {
            try {
                // Get sample records from the latest collection
                const sampleRecords = await db.collection(file.latestCollection).find({}).limit(5).toArray();
                console.log(`📑 Processing ${file.latestCollection}: ${sampleRecords.length} sample records`);
                
                if (sampleRecords.length > 0) {
                    // Collect all unique columns from all sample records
                    const allColumns = new Set();
                    
                    sampleRecords.forEach((record, index) => {
                        console.log(`📋 Record ${index + 1} columns:`, Object.keys(record));
                        Object.keys(record).forEach(key => {
                            if (!['_id', 'recordIndex', 'fileName', 'fileSize', 'category', 'subcategory', 'uploadedAt', 'processingDate', 'collectionTimestamp'].includes(key)) {
                                allColumns.add(key);
                            }
                        });
                    });
                    
                    const cleanColumns = Array.from(allColumns);
                    console.log(`✅ Final columns for ${file.subcategory}:`, cleanColumns);
                    
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
                        columns: cleanColumns,
                        icon: DATA_CATEGORIES[file.category]?.icon || '📄',
                        versions: file.versions?.length || 1
                    };
                }
            } catch (error) {
                console.error(`❌ Error processing ${file.subcategory}:`, error);
                // Still add the table but with no columns
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
                    columns: [],
                    icon: DATA_CATEGORIES[file.category]?.icon || '📄',
                    versions: file.versions?.length || 1
                };
            }
        }
        
        await client.close();
        
        console.log(`🎯 Sending ${Object.keys(tableDetails).length} tables to frontend`);
        
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
            console.log(`✅ Created table: ${tableName}`);
            
            // Process each selected table
            for (const tableId of selectedTables) {
                const fileTracker = await db.collection('file_versions_tracker').findOne({ _id: tableId });
                
                if (!fileTracker) {
                    console.log(`⚠️ File tracker not found for ${tableId}`);
                    continue;
                }
                
                const records = await db.collection(fileTracker.latestCollection).find({}).toArray();
                console.log(`📊 Processing ${records.length} records from ${tableId}`);
                
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
                    const placeholders = columns.map((_, i) => `$${i + 1}`).join(', ');
                    const columnNames = columns.map(col => `"${col}"`).join(', ');
                    
                    const insertSQL = `INSERT INTO "${tableName}" (${columnNames}) VALUES (${placeholders})`;
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
            id: `${file.category}.${file.subcategory}`,
            category: file.category,
            subcategory: file.subcategory,
            displayName: `${DATA_CATEGORIES[file.category]?.displayName || file.category} - ${file.subcategory}`,
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
            const result = await pgPool.query(`
                SELECT table_name, 
                       array_agg(column_name ORDER BY ordinal_position) as columns
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                GROUP BY table_name
                ORDER BY table_name
            `);
            
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
            const columnsResult = await pgPool.query(`
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            `, [tableName]);
            
            const columns = columnsResult.rows.map(row => row.column_name);
            
            // Get table data (first 50 records)
            const dataResult = await pgPool.query(`SELECT * FROM "${tableName}" LIMIT 50`);
            
            // Get total count
            const countResult = await pgPool.query(`SELECT COUNT(*) as total FROM "${tableName}"`);
            
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

// PostgreSQL Viewer API Routes
app.get('/api/postgresql/tables', async (req, res) => {
    try {
        const pgPool = new Pool(config.postgresql);
        
        try {
            const result = await pgPool.query(`
                SELECT table_name, 
                       array_agg(column_name ORDER BY ordinal_position) as columns
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                GROUP BY table_name
                ORDER BY table_name
            `);
            
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
            const columnsResult = await pgPool.query(`
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            `, [tableName]);
            
            const columns = columnsResult.rows.map(row => row.column_name);
            
            // Get table data (first 50 records)
            const dataResult = await pgPool.query(`SELECT * FROM "${tableName}" LIMIT 50`);
            
            // Get total count
            const countResult = await pgPool.query(`SELECT COUNT(*) as total FROM "${tableName}"`);
            
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

app.listen(PORT, () => {
    console.log(`🔥 Complete ETL System running at http://localhost:${PORT}`);
    console.log(`✅ Features:`);
    console.log(`   📤 Working file upload with fixed selection`);
    console.log(`   🔄 ETL mapping tab for PostgreSQL tables`);
    console.log(`   👁️ Data viewer tab to see uploaded data`);
    console.log(`   🎯 Table selection and column mapping`);
    console.log(`   📊 MongoDB to PostgreSQL processing`);
});

// Add this new endpoint before app.listen
app.get('/api/client/:clientId', async (req, res) => {
    try {
        const clientId = req.params.clientId;
        console.log(`🔍 Searching for client with ID: ${clientId}`);
        
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        // Get the latest client_info collection from file_versions_tracker
        const tracker = await db.collection('file_versions_tracker')
            .findOne({ 
                category: 'client_info',
                subcategory: 'client_info'
            });
            
        if (!tracker || !tracker.latestCollection) {
            throw new Error('No client information collection found');
        }
        
        console.log(`📂 Searching in collection: ${tracker.latestCollection}`);
        
        // Search for client in the latest collection
        const clientInfo = await db.collection(tracker.latestCollection)
            .findOne({
                $or: [
                    { "Client Id": clientId },
                    { "Client_Id": clientId },
                    { "ClientId": clientId },
                    { "Client_ID": clientId },
                    { "client_id": clientId }
                ]
            });
            
        if (!clientInfo) {
            return res.status(404).json({
                success: false,
                error: `Client with ID ${clientId} not found`
            });
        }
        
        // Format the response
        const formattedInfo = {
            clientId: clientInfo["Client Id"] || clientInfo["Client_Id"] || clientInfo["ClientId"] || clientInfo["Client_ID"] || clientInfo["client_id"],
            clientName: clientInfo["Client Name"] || clientInfo["Client_Name"] || clientInfo["ClientName"] || "",
            clientCode: clientInfo["Client Code"] || clientInfo["Client_Code"] || clientInfo["ClientCode"] || "",
            firstName: clientInfo["First Holder First Name"] || clientInfo["FirstName"] || "",
            middleName: clientInfo["First Holder Middle Name"] || clientInfo["MiddleName"] || "",
            lastName: clientInfo["First Holder Last Name"] || clientInfo["LastName"] || "",
            gender: clientInfo["First Holder Gender"] || clientInfo["Gender"] || "",
            // Add any other fields you want to include
            fullData: clientInfo // Include all raw data for reference
        };
        
        res.json({
            success: true,
            data: formattedInfo
        });
        
    } catch (error) {
        console.error('❌ Error fetching client info:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to fetch client information',
            details: error.message
        });
    }
});

// Also add an endpoint to list all clients
app.get('/api/clients', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        // Get the latest client_info collection
        const tracker = await db.collection('file_versions_tracker')
            .findOne({ 
                category: 'client_info',
                subcategory: 'client_info'
            });
            
        if (!tracker || !tracker.latestCollection) {
            throw new Error('No client information collection found');
        }
        
        // Get all clients with basic info
        const clients = await db.collection(tracker.latestCollection)
            .find({})
            .project({
                "Client Id": 1,
                "Client_Id": 1,
                "ClientId": 1,
                "Client Name": 1,
                "Client_Name": 1,
                "ClientName": 1,
                "Client Code": 1,
                "Client_Code": 1,
                "ClientCode": 1
            })
            .toArray();
            
        // Format the response
        const formattedClients = clients.map(client => ({
            clientId: client["Client Id"] || client["Client_Id"] || client["ClientId"] || "",
            clientName: client["Client Name"] || client["Client_Name"] || client["ClientName"] || "",
            clientCode: client["Client Code"] || client["Client_Code"] || client["ClientCode"] || ""
        }));
        
        res.json({
            success: true,
            count: formattedClients.length,
            data: formattedClients
        });
        
    } catch (error) {
        console.error('❌ Error listing clients:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to list clients',
            details: error.message
        });
    }
});
