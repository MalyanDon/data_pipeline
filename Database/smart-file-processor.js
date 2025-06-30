const { MongoClient } = require('mongodb');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const FieldMapper = require('./custody-normalization/extractors/fieldMapper');

class SmartFileProcessor {
    constructor(mongoUri = 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/') {
        this.mongoUri = mongoUri;
        this.client = null;
        this.custodyFieldMapper = new FieldMapper();
    }

    async connect() {
        this.client = new MongoClient(this.mongoUri);
        await this.client.connect();
        console.log('✅ Connected to MongoDB Atlas');
    }

    async disconnect() {
        if (this.client) {
            await this.client.close();
            console.log('✅ Disconnected from MongoDB');
        }
    }

    /**
     * Intelligent file type detection from filename
     */
    detectFileType(fileName) {
        const name = fileName.toLowerCase();
        
        // Custody/Custodian files - use clean custodian name
        if (/custody|eod/i.test(name) || /\.(xlsx|xls|csv)$/i.test(name)) {
            if (/hdfc/i.test(name)) return 'hdfc';
            if (/kotak/i.test(name)) return 'kotak';
            if (/orbis/i.test(name)) return 'orbis';
            if (/deutsche/i.test(name)) return 'deutsche';
            if (/trustpms|trust.*pms/i.test(name)) return 'trustpms';
            if (/icici/i.test(name)) return 'icici';
            if (/axis/i.test(name)) return 'axis';
            if (/sbi/i.test(name)) return 'sbi';
            if (/edelweiss/i.test(name)) return 'edelweiss';
            if (/zerodha/i.test(name)) return 'zerodha';
            if (/nuvama/i.test(name)) return 'nuvama';
        }
        
        // Other file types
        if (/broker.*master/i.test(name)) return 'broker_master';
        if (/cash.*capital.*flow/i.test(name)) return 'cash_capital_flow';
        if (/stock.*capital.*flow/i.test(name)) return 'stock_capital_flow';
        if (/contract.*note/i.test(name)) return 'contract_note';
        if (/distributor.*master/i.test(name)) return 'distributor_master';
        if (/strategy.*master/i.test(name)) return 'strategy_master';
        if (/allocation|alloc/i.test(name)) return 'mf_allocations';
        if (/client.*info/i.test(name)) return 'client_info';
        
        // Default to general data
        return 'general_data';
    }

    /**
     * Extract date from filename if present
     */
    extractDateFromFilename(fileName) {
        const dateMatch = fileName.match(/(\d{4})[_-](\d{2})[_-](\d{2})/);
        if (dateMatch) {
            const [, year, month, day] = dateMatch;
            return `${year}-${month}-${day}`;
        }
        return null;
    }

    /**
     * Read file data based on extension with enhanced Excel support and content detection
     */
    async readFileData(filePath) {
        let ext = path.extname(filePath).toLowerCase();
        
        // If no extension, try to detect by file content
        if (!ext) {
            console.log(`🔍 No extension found for ${filePath}, detecting by content...`);
            try {
                const buffer = fs.readFileSync(filePath);
                
                // Check file signatures (magic numbers)
                if (buffer.length > 8) {
                    const hex = buffer.toString('hex', 0, 8);
                    console.log(`🔬 File signature: ${hex}`);
                    
                    // Excel file signatures
                    if (hex.startsWith('504b0304') || hex.startsWith('504b0506') || hex.startsWith('504b0708')) {
                        ext = '.xlsx'; // ZIP-based format (XLSX)
                        console.log(`✅ Detected as Excel XLSX file`);
                    } else if (hex.startsWith('d0cf11e0')) {
                        ext = '.xls'; // OLE2 format (XLS)
                        console.log(`✅ Detected as Excel XLS file`);
                    } else if (buffer.toString('utf8', 0, 100).includes(',') || buffer.toString('utf8', 0, 100).includes('\t')) {
                        ext = '.csv'; // Likely CSV if contains commas or tabs
                        console.log(`✅ Detected as CSV file`);
                    } else if (hex.startsWith('7b') || buffer.toString('utf8', 0, 10).trim().startsWith('{')) {
                        ext = '.json'; // JSON starts with {
                        console.log(`✅ Detected as JSON file`);
                    } else {
                        ext = '.txt'; // Default to text
                        console.log(`⚠️ Unknown format, treating as text file`);
                    }
                }
            } catch (error) {
                console.log(`⚠️ Error detecting file type: ${error.message}`);
                ext = '.txt'; // Default fallback
            }
        }
        
        console.log(`📄 Processing file as: ${ext} format`);
        
        try {
            if (ext === '.csv') {
                const content = fs.readFileSync(filePath, 'utf8');
                const lines = content.split('\n').filter(line => line.trim());
                if (lines.length <= 1) return [];
                
                const headers = lines[0].split(',');
                return lines.slice(1).map(line => {
                    const values = line.split(',');
                    const obj = {};
                    headers.forEach((h, i) => obj[h.trim()] = values[i]?.trim() || '');
                    return obj;
                });
            } else if (['.xlsx', '.xls'].includes(ext)) {
                console.log(`🔍 Reading Excel file: ${filePath}`);
                const workbook = XLSX.readFile(filePath);
                console.log(`📋 Excel file has ${workbook.SheetNames.length} sheets: ${workbook.SheetNames.join(', ')}`);
                
                let allData = [];
                
                // Try to read from all sheets to find data
                for (const sheetName of workbook.SheetNames) {
                    console.log(`\n🔍 Processing sheet: "${sheetName}"`);
                    const worksheet = workbook.Sheets[sheetName];
                    
                    // Debug: Check worksheet range and properties
                    console.log(`📐 Worksheet range: ${worksheet['!ref']}`);
                    console.log(`🔧 Worksheet properties:`, Object.keys(worksheet).filter(k => k.startsWith('!')));
                    
                    // Try different parsing options
                    let sheetData = [];
                    
                    // Method 1: Standard JSON conversion
                    try {
                        sheetData = XLSX.utils.sheet_to_json(worksheet, { defval: '' });
                        console.log(`📊 Sheet "${sheetName}": ${sheetData.length} rows (standard parsing)`);
                        if (sheetData.length > 0) {
                            console.log(`🔍 Sample data from "${sheetName}":`, JSON.stringify(sheetData[0], null, 2));
                        }
                    } catch (error) {
                        console.log(`⚠️ Standard parsing failed for sheet "${sheetName}": ${error.message}`);
                    }
                    
                    // Method 2: If standard parsing gives no data, try with header row detection
                    if (sheetData.length === 0) {
                        console.log(`🔄 Trying header detection for sheet "${sheetName}"`);
                        try {
                            const rawData = XLSX.utils.sheet_to_json(worksheet, { 
                                header: 1, // Use first row as headers
                                defval: '',
                                blankrows: false 
                            });
                            
                            console.log(`📊 Raw data from header detection: ${rawData.length} rows`);
                            if (rawData.length > 0) {
                                console.log(`🔍 First few raw rows:`, rawData.slice(0, 3));
                            }
                            
                            if (rawData.length > 1) {
                                const headers = rawData[0];
                                console.log(`📋 Detected headers:`, headers);
                                sheetData = rawData.slice(1).map(row => {
                                    const obj = {};
                                    headers.forEach((h, i) => {
                                        obj[h || `Column_${i + 1}`] = row[i] || '';
                                    });
                                    return obj;
                                });
                                console.log(`📊 Sheet "${sheetName}": ${sheetData.length} rows (header detection)`);
                                if (sheetData.length > 0) {
                                    console.log(`🔍 Sample processed data:`, JSON.stringify(sheetData[0], null, 2));
                                }
                            }
                        } catch (error) {
                            console.log(`⚠️ Header detection failed for sheet "${sheetName}": ${error.message}`);
                        }
                    }
                    
                    // Method 3: If still no data, try raw cell reading
                    if (sheetData.length === 0) {
                        try {
                            const range = XLSX.utils.decode_range(worksheet['!ref'] || 'A1:Z100');
                            console.log(`📐 Sheet "${sheetName}" range: ${worksheet['!ref']}`);
                            
                            if (range.e.r > 0) { // Has more than one row
                                sheetData = XLSX.utils.sheet_to_json(worksheet, {
                                    range: 0,
                                    defval: '',
                                    raw: false
                                });
                                console.log(`📊 Sheet "${sheetName}": ${sheetData.length} rows (raw parsing)`);
                            }
                        } catch (error) {
                            console.log(`⚠️ Raw parsing failed for sheet "${sheetName}": ${error.message}`);
                        }
                    }
                    
                    // Add sheet data if found
                    if (sheetData.length > 0) {
                        // Add sheet name to each record for reference
                        const sheetDataWithSource = sheetData.map(row => ({
                            ...row,
                            _sheetName: sheetName
                        }));
                        allData.push(...sheetDataWithSource);
                        console.log(`✅ Added ${sheetData.length} records from sheet "${sheetName}"`);
                    } else {
                        console.log(`⚠️ No data found in sheet "${sheetName}"`);
                    }
                }
                
                console.log(`📊 Total records from all sheets: ${allData.length}`);
                return allData;
                
            } else if (ext === '.json') {
                const content = fs.readFileSync(filePath, 'utf8');
                const jsonData = JSON.parse(content);
                return Array.isArray(jsonData) ? jsonData : [jsonData];
            } else if (ext === '.txt' || ext === '.tsv') {
                const content = fs.readFileSync(filePath, 'utf8');
                const separator = ext === '.tsv' ? '\t' : ',';
                const lines = content.split('\n').filter(line => line.trim());
                if (lines.length <= 1) return [];
                
                const headers = lines[0].split(separator);
                return lines.slice(1).map(line => {
                    const values = line.split(separator);
                    const obj = {};
                    headers.forEach((h, i) => obj[h.trim()] = values[i]?.trim() || '');
                    return obj;
                });
            } else if (ext === '.xml') {
                // Basic XML parsing - would need xml2js for complex XML
                const content = fs.readFileSync(filePath, 'utf8');
                console.log('📄 XML file detected - basic parsing (implement xml2js for complex XML)');
                return [{ xmlContent: content, _fileType: 'xml' }];
            }
            
        } catch (error) {
            console.error(`❌ Error reading file ${filePath}: ${error.message}`);
            return [];
        }
        
        return [];
    }

    /**
     * Smart file processing with intelligent versioning
     */
    async processFiles(files, options = {}) {
        const { 
            versioningMode = 'timestamp', // 'timestamp' = keep all versions with timestamps, 'replace' = old behavior
            recordDate = null 
        } = options;

        if (!this.client) {
            await this.connect();
        }

        const results = [];
        const processedSources = new Set();

        for (const file of files) {
            try {
                console.log(`\n🔄 Processing: ${file.originalname || file.name}`);
                
                const filePath = file.path || file;
                const fileName = file.originalname || path.basename(filePath);
                
                // Detect file type (source)
                const sourceType = this.detectFileType(fileName);
                console.log(`📂 Detected source: ${sourceType}`);

                // Extract or use provided date
                const fileDate = this.extractDateFromFilename(fileName) || recordDate || new Date().toISOString().split('T')[0];
                console.log(`📅 Processing date: ${fileDate}`);

                // Read file data
                const fileData = await this.readFileData(filePath);
                console.log(`📊 Records found: ${fileData.length}`);

                if (fileData.length === 0) {
                    console.log('⚠️ No data found in file, skipping...');
                    results.push({
                        fileName,
                        sourceType,
                        success: false,
                        message: 'No data found',
                        recordsProcessed: 0
                    });
                    continue;
                }

                // Get database and collection with date-based naming
                const yearDB = fileDate.split('-')[0];
                const db = this.client.db(`financial_data_${yearDB}`);
                
                // Create date-based collection name: sourceType_YYYY_MM_DD
                const formattedDate = fileDate.replace(/-/g, '_'); // Convert 2024-10-15 to 2024_10_15
                const collectionName = `${sourceType}_${formattedDate}`;
                const collection = db.collection(collectionName);
                
                console.log(`📂 Collection: ${collectionName}`);

                let recordsProcessed = 0;
                const currentTimestamp = new Date();
                const versionId = currentTimestamp.getTime(); // Unique version ID

                if (versioningMode === 'timestamp') {
                    // INTELLIGENT VERSIONING: Keep all versions, mark latest as active
                    console.log(`📝 Using intelligent versioning for ${sourceType}...`);
                    
                    // First, mark all existing records as inactive (historical) for this date
                    await collection.updateMany(
                        { 
                            sourceType: sourceType,
                            recordDate: fileDate
                        },
                        { 
                            $set: { 
                                isActive: false,
                                deactivatedAt: currentTimestamp
                            }
                        }
                    );
                    console.log(`📦 Previous versions archived as historical data`);
                    
                    // Insert new data with active status and enhanced metadata
                    const documentsToInsert = fileData.map(row => ({
                        ...row,
                        recordDate: fileDate,
                        fileName: fileName,
                        sourceType: sourceType,
                        collectionName: collectionName, // Store collection name for reference
                        uploadedAt: currentTimestamp,
                        lastUpdated: currentTimestamp,
                        versionId: versionId,
                        isActive: true, // Mark as latest/active version
                        uploadTimestamp: currentTimestamp.toISOString(),
                        uploadVersion: `v${versionId}`, // Human readable version
                        dataDate: fileDate, // Explicit data date field
                        yearMonth: fileDate.substring(0, 7), // YYYY-MM for easy filtering
                        processingDate: currentTimestamp.toISOString().split('T')[0] // When it was processed
                    }));
                    
                    const insertResult = await collection.insertMany(documentsToInsert);
                    recordsProcessed = insertResult.insertedCount;
                    console.log(`✅ Inserted ${recordsProcessed} records as ACTIVE version`);
                    console.log(`🕒 Version: v${versionId} (${currentTimestamp.toLocaleString()})`);
                    
                    // Get count of historical versions
                    const historicalCount = await collection.countDocuments({ 
                        sourceType: sourceType, 
                        isActive: false 
                    });
                    console.log(`📚 Historical versions preserved: ${historicalCount} records`);
                    
                } else {
                    // OLD BEHAVIOR: Complete replacement for this specific date
                    console.log(`🗑️ Replacing ${sourceType} data for ${fileDate} (old behavior)...`);
                    const deleteResult = await collection.deleteMany({ 
                        sourceType: sourceType,
                        recordDate: fileDate 
                    });
                    console.log(`✅ Removed ${deleteResult.deletedCount} old records for ${fileDate}`);
                    
                    // Insert new data with enhanced metadata
                    const documentsToInsert = fileData.map(row => ({
                        ...row,
                        recordDate: fileDate,
                        fileName: fileName,
                        sourceType: sourceType,
                        collectionName: collectionName,
                        uploadedAt: currentTimestamp,
                        lastUpdated: currentTimestamp,
                        versionId: versionId,
                        isActive: true,
                        dataDate: fileDate,
                        yearMonth: fileDate.substring(0, 7),
                        processingDate: currentTimestamp.toISOString().split('T')[0]
                    }));
                    
                    const insertResult = await collection.insertMany(documentsToInsert);
                    recordsProcessed = insertResult.insertedCount;
                    console.log(`✅ Inserted ${recordsProcessed} new records`);
                }

                processedSources.add(sourceType);
                
                results.push({
                    fileName,
                    sourceType,
                    collectionName,
                    success: true,
                    message: `Successfully processed ${recordsProcessed} records (${versioningMode} mode)`,
                    recordsProcessed,
                    recordDate: fileDate,
                    versioningMode,
                    versionId: `v${versionId}`,
                    uploadTimestamp: currentTimestamp.toISOString(),
                    isActive: true,
                    hasHistoricalVersions: versioningMode === 'timestamp',
                    dataDate: fileDate,
                    processingDate: currentTimestamp.toISOString().split('T')[0]
                });

                console.log(`✅ ${fileName} processed successfully with versioning`);

            } catch (error) {
                console.error(`❌ Error processing file: ${error.message}`);
                results.push({
                    fileName: file.originalname || file.name,
                    success: false,
                    message: error.message,
                    recordsProcessed: 0
                });
            }
        }

        // Summary
        const successCount = results.filter(r => r.success).length;
        const totalRecords = results.reduce((sum, r) => sum + r.recordsProcessed, 0);
        const uniqueSources = Array.from(processedSources);
        const hasVersioning = versioningMode === 'timestamp';

        console.log(`\n📋 Processing Summary:`);
        console.log(`✅ Files processed: ${successCount}/${files.length}`);
        console.log(`📊 Total records: ${totalRecords.toLocaleString()}`);
        console.log(`📂 Sources updated: ${uniqueSources.join(', ')}`);
        console.log(`🕒 Versioning mode: ${versioningMode}`);
        if (hasVersioning) {
            console.log(`📚 Historical data preserved - only latest version is active`);
        } else {
            console.log(`🗑️ Previous data replaced completely`);
        }

        return {
            success: successCount === files.length,
            filesProcessed: successCount,
            totalFiles: files.length,
            totalRecords,
            uniqueSources,
            versioningMode,
            results,
            summary: {
                message: `${successCount}/${files.length} files processed, ${totalRecords} records with ${versioningMode} versioning`,
                versioningEnabled: hasVersioning,
                sourcesUpdated: uniqueSources.length,
                historicalDataPreserved: hasVersioning
            }
        };
    }

    /**
     * Get all collections info (for dashboard display) - Shows date-based collections
     */
    async getCollectionsInfo() {
        if (!this.client) {
            await this.connect();
        }

        const databases = ['financial_data_2024', 'financial_data_2025'];
        const collections = [];

        for (const dbName of databases) {
            try {
                const db = this.client.db(dbName);
                const collectionNames = await db.listCollections().toArray();
                
                for (const collectionInfo of collectionNames) {
                    const collection = db.collection(collectionInfo.name);
                    
                    // Count active records (latest version)
                    const activeCount = await collection.countDocuments({ isActive: true });
                    // Count total records (including historical)
                    const totalCount = await collection.countDocuments();
                    const historicalCount = totalCount - activeCount;
                    
                    if (totalCount > 0) {
                        // Get latest active record for info
                        const latestRecord = await collection.findOne(
                            { isActive: true }, 
                            { sort: { uploadedAt: -1 } }
                        );
                        
                        // Parse collection name to extract source type and date
                        const nameParts = collectionInfo.name.split('_');
                        let sourceType = collectionInfo.name;
                        let dataDate = null;
                        
                        if (nameParts.length >= 4) {
                            // Format: sourceType_YYYY_MM_DD
                            sourceType = nameParts.slice(0, -3).join('_'); // Everything before date
                            dataDate = `${nameParts[nameParts.length-3]}-${nameParts[nameParts.length-2]}-${nameParts[nameParts.length-1]}`;
                        }
                        
                        // Get oldest historical record to show version span
                        const oldestRecord = await collection.findOne(
                            { isActive: false },
                            { sort: { uploadedAt: 1 } }
                        );
                        
                        collections.push({
                            name: collectionInfo.name,
                            collectionName: collectionInfo.name, // Full collection name with date
                            database: dbName,
                            count: activeCount, // Show active count prominently
                            totalCount: totalCount, // Include total for reference
                            historicalCount: historicalCount,
                            sourceType: sourceType, // Extracted source type
                            dataDate: dataDate || latestRecord?.dataDate || latestRecord?.recordDate, // Date from name or record
                            lastUpdated: latestRecord?.uploadedAt || latestRecord?.lastUpdated,
                            processingDate: latestRecord?.processingDate,
                            currentVersion: latestRecord?.uploadVersion || 'v1',
                            versioningEnabled: activeCount < totalCount, // Has historical versions
                            versionsAvailable: historicalCount > 0 ? Math.floor(historicalCount / (activeCount || 1)) + 1 : 1,
                            dateRange: oldestRecord ? {
                                from: oldestRecord.uploadedAt,
                                to: latestRecord?.uploadedAt
                            } : null,
                            fileName: latestRecord?.fileName,
                            status: activeCount > 0 ? `✅ ${sourceType.toUpperCase()} - ${dataDate || 'Active'}` : '⚠️ No Active Data'
                        });
                    }
                }
            } catch (error) {
                console.error(`Error accessing database ${dbName}:`, error);
            }
        }

        // Sort by source type and date
        return collections.sort((a, b) => {
            if (a.sourceType !== b.sourceType) {
                return a.sourceType.localeCompare(b.sourceType);
            }
            return (b.dataDate || '').localeCompare(a.dataDate || '');
        });
    }

    /**
     * Get version history for a specific source
     */
    async getVersionHistory(sourceType, limit = 10) {
        if (!this.client) {
            await this.connect();
        }

        const databases = ['financial_data_2024', 'financial_data_2025'];
        const versions = [];

        for (const dbName of databases) {
            try {
                const db = this.client.db(dbName);
                const collection = db.collection(sourceType);
                
                // Get distinct version info
                const versionInfo = await collection.aggregate([
                    {
                        $group: {
                            _id: '$versionId',
                            uploadTimestamp: { $first: '$uploadedAt' },
                            recordCount: { $sum: 1 },
                            isActive: { $first: '$isActive' },
                            fileName: { $first: '$fileName' },
                            uploadVersion: { $first: '$uploadVersion' },
                            recordDate: { $first: '$recordDate' }
                        }
                    },
                    { $sort: { uploadTimestamp: -1 } },
                    { $limit: limit }
                ]).toArray();

                versions.push(...versionInfo.map(v => ({
                    ...v,
                    database: dbName,
                    sourceType
                })));

            } catch (error) {
                console.error(`Error getting version history for ${sourceType}:`, error);
            }
        }

        return versions.sort((a, b) => new Date(b.uploadTimestamp) - new Date(a.uploadTimestamp));
    }

    /**
     * Activate a specific version (rollback functionality)
     */
    async activateVersion(sourceType, versionId) {
        if (!this.client) {
            await this.connect();
        }

        const databases = ['financial_data_2024', 'financial_data_2025'];
        let updatedCount = 0;

        for (const dbName of databases) {
            try {
                const db = this.client.db(dbName);
                const collection = db.collection(sourceType);
                
                // Deactivate all versions
                await collection.updateMany(
                    { sourceType: sourceType },
                    { 
                        $set: { 
                            isActive: false,
                            deactivatedAt: new Date()
                        }
                    }
                );

                // Activate the specified version
                const result = await collection.updateMany(
                    { sourceType: sourceType, versionId: versionId },
                    { 
                        $set: { 
                            isActive: true,
                            reactivatedAt: new Date()
                        },
                        $unset: { deactivatedAt: 1 }
                    }
                );

                updatedCount += result.modifiedCount;

            } catch (error) {
                console.error(`Error activating version ${versionId} for ${sourceType}:`, error);
            }
        }

        return {
            success: updatedCount > 0,
            sourceType,
            versionId,
            recordsActivated: updatedCount,
            message: `Activated version ${versionId} for ${sourceType} (${updatedCount} records)`
        };
    }

    /**
     * Clean up old versions (keep only N latest)
     */
    async cleanupOldVersions(sourceType, keepVersions = 5) {
        if (!this.client) {
            await this.connect();
        }

        const databases = ['financial_data_2024', 'financial_data_2025'];
        let deletedCount = 0;

        for (const dbName of databases) {
            try {
                const db = this.client.db(dbName);
                const collection = db.collection(sourceType);
                
                // Get version IDs sorted by upload time (newest first)
                const versions = await collection.distinct('versionId', { sourceType: sourceType });
                const versionDetails = await collection.aggregate([
                    { $match: { sourceType: sourceType } },
                    {
                        $group: {
                            _id: '$versionId',
                            uploadTimestamp: { $first: '$uploadedAt' }
                        }
                    },
                    { $sort: { uploadTimestamp: -1 } }
                ]).toArray();

                // Keep only the latest N versions
                const versionsToDelete = versionDetails.slice(keepVersions).map(v => v._id);

                if (versionsToDelete.length > 0) {
                    const result = await collection.deleteMany({
                        sourceType: sourceType,
                        versionId: { $in: versionsToDelete }
                    });
                    deletedCount += result.deletedCount;
                }

            } catch (error) {
                console.error(`Error cleaning up versions for ${sourceType}:`, error);
            }
        }

        return {
            success: true,
            sourceType,
            versionsKept: keepVersions,
            recordsDeleted: deletedCount,
            message: `Cleaned up old versions for ${sourceType}, kept ${keepVersions} latest versions`
        };
    }

    /**
     * Clear specific source data (for cleanup)
     */
    async clearSourceData(sourceType, year = null) {
        if (!this.client) {
            await this.connect();
        }

        const databases = year ? [`financial_data_${year}`] : ['financial_data_2024', 'financial_data_2025'];
        let totalDeleted = 0;

        for (const dbName of databases) {
            try {
                const db = this.client.db(dbName);
                const collection = db.collection(sourceType);
                const deleteResult = await collection.deleteMany({});
                totalDeleted += deleteResult.deletedCount;
                console.log(`🗑️ Cleared ${deleteResult.deletedCount} records from ${dbName}.${sourceType}`);
            } catch (error) {
                console.error(`Error clearing ${dbName}.${sourceType}:`, error);
            }
        }

        return {
            success: true,
            sourceType,
            recordsDeleted: totalDeleted,
            message: `Cleared ${totalDeleted} records from ${sourceType}`
        };
    }

    // Intelligent file type detection based on collection names
    detectFileType(collectionName) {
        const name = collectionName.toLowerCase();
        
        // Master Data Types - Match both old and new patterns
        if (name.includes('broker_master') || name.includes('broker_master_data')) return 'broker_master';
        if (name.includes('client_info') || name.includes('client_info_data')) return 'client_master';
        if (name.includes('distributor_master') || name.includes('distributor_master_data')) return 'distributor_master';
        if (name.includes('strategy_master') || name.includes('strategy_master_data')) return 'strategy_master';
        
        // Transaction Data Types - Match both old and new patterns
        if (name.includes('contract_note') || name.includes('contract_notes')) return 'contract_notes';
        if (name.includes('cash_capital_flow') || name.includes('cash_flow')) return 'cash_flow';
        if (name.includes('stock_capital_flow') || name.includes('stock_flow')) return 'stock_flow';
        if (name.includes('mf_allocation') || name.includes('mf_alloc')) return 'mf_allocations';
        
        // Custody Data Types
        if (name.includes('axis')) return 'custody';
        if (name.includes('hdfc')) return 'custody';
        if (name.includes('kotak')) return 'custody';
        if (name.includes('deutsche') || name.includes('164_ec0000720')) return 'custody';
        if (name.includes('orbis')) return 'custody';
        if (name.includes('trust') || name.includes('end_client_holding')) return 'custody';
        
        // Default fallback
        return 'unknown';
    }

    // Get target table for file type
    getTargetTable(fileType) {
        const tableMap = {
            // Master Data
            'broker_master': 'brokers',
            'client_master': 'clients',
            'distributor_master': 'distributors',
            'strategy_master': 'strategies',
            
            // Transaction Data (Simplified - no ENSO prefixes)
            'contract_notes': 'contract_notes',
            'cash_flow': 'cash_capital_flow',
            'stock_flow': 'stock_capital_flow',
            'mf_allocations': 'mf_allocations',
            
            // Custody Data - use unified table
            'custody': 'unified_custody_master'
        };
        
        return tableMap[fileType] || 'raw_uploads';
    }

    // Process records based on file type
    processRecords(records, fileType, metadata) {
        switch (fileType) {
            case 'broker_master':
                return this.processBrokerMaster(records, metadata);
            case 'client_master':
                return this.processClientMaster(records, metadata);
            case 'distributor_master':
                return this.processDistributorMaster(records, metadata);
            case 'strategy_master':
                return this.processStrategyMaster(records, metadata);
            case 'contract_notes':
                return this.processContractNotes(records, metadata);
            case 'cash_flow':
                return this.processCashFlow(records, metadata);
            case 'stock_flow':
                return this.processStockFlow(records, metadata);
            case 'mf_allocations':
                return this.processMFAllocations(records, metadata);
            default:
                // Handle custody types with existing logic
                if (fileType.includes('custody')) {
                    const custodyType = fileType.replace('_custody', '');
                    return this.custodyFieldMapper.mapRecords(records, custodyType, metadata);
                }
                return this.processUnknownType(records, metadata);
        }
    }

    // Master Data Processors
    processBrokerMaster(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Handle actual column names found in uploaded files
                // broker_id is auto-generated by database (serial)
                const mapped = {
                    broker_code: this.cleanCodeValue(
                        record['Broker Code'] || record['BrokerCode'] || record['broker_code'] || 
                        record['Broker Master Templete'] || `BROKER_${index + 1}`
                    ),
                    broker_name: this.cleanValue(
                        record['Broker Name'] || record['BrokerName'] || record['broker_name'] ||
                        record['Broker Master Templete'] || `Broker ${index + 1}`
                    ),
                    broker_type: this.cleanValue(record['Broker Type'] || record['BrokerType'] || record['broker_type'] || 'Unknown'),
                    registration_number: this.cleanValue(record['Registration Number'] || record['RegNumber'] || record['registration_number']),
                    contact_person: this.cleanValue(record['Contact Person'] || record['ContactPerson'] || record['contact_person']),
                    email: this.cleanValue(record['Email'] || record['email']),
                    phone: this.cleanValue(record['Phone'] || record['phone']),
                    address: this.cleanValue(record['Address'] || record['address']),
                    city: this.cleanValue(record['City'] || record['city']),
                    state: this.cleanValue(record['State'] || record['state']),
                    country: this.cleanValue(record['Country'] || record['country'] || 'India'),
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.broker_code || mapped.broker_code === '') {
                    mapped.broker_code = this.cleanCodeValue(`AUTO_BROKER_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated broker_code: ${mapped.broker_code}`);
                }
                if (!mapped.broker_name || mapped.broker_name === '') {
                    mapped.broker_name = `Auto Broker ${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated broker_name: ${mapped.broker_name}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processClientMaster(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Handle various column name formats and auto-generate missing data
                // client_id is auto-generated by database (serial)
                const mapped = {
                    client_code: this.cleanCodeValue(
                        record['Client Code'] || record['ClientCode'] || record['client_code'] ||
                        Object.keys(record)[0] || `CLIENT_${index + 1}`  // Use first non-empty column or auto-generate
                    ),
                    client_name: this.cleanValue(
                        record['Client Name'] || record['ClientName'] || record['client_name'] ||
                        Object.values(record).find(val => val && typeof val === 'string' && val.length > 2) ||
                        `Client ${index + 1}`
                    ),
                    client_type: this.cleanValue(record['Client Type'] || record['ClientType'] || record['client_type'] || 'Individual'),
                    pan_number: this.cleanValue(record['PAN'] || record['PAN Number'] || record['pan_number']),
                    email: this.cleanValue(record['Email'] || record['email']),
                    phone: this.cleanValue(record['Phone'] || record['Mobile'] || record['phone']),
                    address: this.cleanValue(record['Address'] || record['address']),
                    city: this.cleanValue(record['City'] || record['city']),
                    state: this.cleanValue(record['State'] || record['state']),
                    country: this.cleanValue(record['Country'] || record['country'] || 'India'),
                    risk_category: this.cleanValue(record['Risk Category'] || record['RiskCategory'] || record['risk_category'] || 'Medium'),
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.client_code || mapped.client_code === '') {
                    mapped.client_code = this.cleanCodeValue(`AUTO_CLIENT_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
                }
                if (!mapped.client_name || mapped.client_name === '') {
                    mapped.client_name = `Auto Client ${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated client_name: ${mapped.client_name}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processDistributorMaster(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Handle actual column names like "email" and auto-generate missing data
                // distributor_id is auto-generated by database (serial)
                const mapped = {
                    distributor_arn_number: this.cleanCodeValue(
                        record['distributor arn number'] || record['Distributor ARN Number'] || 
                        record['ARN Number'] || record['ARN'] || record['arn_number'] ||
                        record['arn'] || record['AMFI Registration Number'] ||
                        `AUTO_ARN_${Date.now()}_${index + 1}`
                    ),
                    distributor_code: this.cleanCodeValue(
                        record['Distributor Code'] || record['DistributorCode'] || record['distributor_code'] ||
                        record['email'] || `DIST_${index + 1}`  // Use email as fallback or auto-generate
                    ),
                    distributor_name: this.cleanValue(
                        record['Distributor Name'] || record['DistributorName'] || record['distributor_name'] ||
                        Object.values(record).find(val => val && typeof val === 'string' && val.length > 2) ||
                        `Distributor ${index + 1}`
                    ),
                    distributor_type: this.cleanValue(record['Distributor Type'] || record['DistributorType'] || record['distributor_type'] || 'External'),
                    commission_rate: this.parseNumeric(record['Commission Rate'] || record['CommissionRate'] || record['commission_rate']) || 0,
                    contact_person: this.cleanValue(record['Contact Person'] || record['ContactPerson'] || record['contact_person']),
                    email: this.cleanValue(
                        record['Email'] || record['email']  // Actual column name in uploaded file
                    ),
                    phone: this.cleanValue(record['Phone'] || record['phone']),
                    address: this.cleanValue(record['Address'] || record['address']),
                    city: this.cleanValue(record['City'] || record['city']),
                    state: this.cleanValue(record['State'] || record['state']),
                    country: this.cleanValue(record['Country'] || record['country'] || 'India'),
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.distributor_arn_number || mapped.distributor_arn_number === '' || mapped.distributor_arn_number.startsWith('AUTO_ARN_')) {
                    mapped.distributor_arn_number = this.cleanCodeValue(`AUTO_ARN_${Date.now()}_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated distributor_arn_number: ${mapped.distributor_arn_number}`);
                }
                if (!mapped.distributor_code || mapped.distributor_code === '') {
                    mapped.distributor_code = `AUTO_DIST_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated distributor_code: ${mapped.distributor_code}`);
                }
                if (!mapped.distributor_name || mapped.distributor_name === '') {
                    mapped.distributor_name = `Auto Distributor ${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated distributor_name: ${mapped.distributor_name}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processStrategyMaster(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Handle actual column names: "Filed Name", "Data"
                // strategy_id is auto-generated by database (serial)
                const mapped = {
                    strategy_code: this.cleanCodeValue(
                        record['Strategy Code'] || record['StrategyCode'] || record['strategy_code'] ||
                        record['Filed Name'] || `STRATEGY_${index + 1}`
                    ),
                    strategy_name: this.cleanValue(
                        record['Strategy Name'] || record['StrategyName'] || record['strategy_name'] ||
                        record['Data'] || record['Filed Name'] || `Strategy ${index + 1}`
                    ),
                    strategy_type: this.cleanValue(record['Strategy Type'] || record['StrategyType'] || record['strategy_type'] || 'Equity'),
                    description: this.cleanValue(
                        record['Description'] || record['description'] ||
                        record['Data']  // Actual column name in uploaded file
                    ),
                    benchmark: this.cleanValue(record['Benchmark'] || record['benchmark']),
                    risk_level: this.cleanValue(record['Risk Level'] || record['RiskLevel'] || record['risk_level'] || 'Medium'),
                    min_investment: this.parseNumeric(record['Min Investment'] || record['MinInvestment'] || record['min_investment']) || 0,
                    max_investment: this.parseNumeric(record['Max Investment'] || record['MaxInvestment'] || record['max_investment']) || 0,
                    management_fee: this.parseNumeric(record['Management Fee'] || record['ManagementFee'] || record['management_fee']) || 0,
                    performance_fee: this.parseNumeric(record['Performance Fee'] || record['PerformanceFee'] || record['performance_fee']) || 0,
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.strategy_code || mapped.strategy_code === '') {
                    mapped.strategy_code = `AUTO_STRATEGY_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated strategy_code: ${mapped.strategy_code}`);
                }
                if (!mapped.strategy_name || mapped.strategy_name === '') {
                    mapped.strategy_name = `Auto Strategy ${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated strategy_name: ${mapped.strategy_name}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    // Transaction Data Processors
    processContractNotes(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Use EXACT field mappings from Contract Note format.xlsx
                // Primary Key: ECN No → ecn_number
                const mapped = {
                    // Primary Key
                    ecn_number: this.cleanValue(
                        record['ECN No'] || record['ecn_number'] || 
                        record['ECN Number'] || record['Contract Note Number'] ||
                        `AUTO_ECN_${index + 1}`
                    ),
                    ecn_status: this.cleanValue(
                        record['ECN Status'] || record['ecn_status'] || 'ACTIVE'
                    ),
                    ecn_date: this.parseDate(
                        record['ECN Date'] || record['ecn_date'] ||
                        record['Trade Date'] || record['TradeDate'] || new Date()
                    ),
                    client_code: this.cleanCodeValue(
                        record['Client Exchange Code/UCC'] || record['client_code'] ||
                        record['Client Code'] || record['ClientCode'] ||
                        `CLIENT_${index + 1}`
                    ),
                    broker_name: this.cleanValue(
                        record['Broker Name'] || record['broker_name'] ||
                        record['Broker Code'] || record['BrokerCode'] || 'AUTO_BROKER'
                    ),
                    instrument_isin: this.cleanValue(
                        record['ISIN Code'] || record['instrument_isin'] ||
                        record['ISIN'] || record['isin']
                    ),
                    instrument_name: this.cleanValue(
                        record['Security Name'] || record['instrument_name'] ||
                        record['Instrument Name'] || record['InstrumentName'] ||
                        `Security ${index + 1}`
                    ),
                    transaction_type: this.cleanValue(
                        record['Transaction Type'] || record['transaction_type'] ||
                        record['TransactionType'] || 'BUY'
                    ),
                    delivery_type: this.cleanValue(
                        record['Delivery Type'] || record['delivery_type'] || 'DELIVERY'
                    ),
                    exchange: this.cleanValue(
                        record['Exchange'] || record['exchange'] || 'NSE'
                    ),
                    settlement_date: this.parseDate(
                        record['Sett. Date'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    quantity: this.parseNumeric(
                        record['Qty'] || record['quantity'] ||
                        record['Quantity']
                    ) || 0,
                    net_amount: this.parseNumeric(
                        record['Net Amount'] || record['net_amount'] ||
                        record['NetAmount']
                    ) || 0,
                    net_rate: this.parseNumeric(
                        record['Net Rate'] || record['net_rate'] ||
                        record['Price'] || record['price']
                    ) || 0,
                    brokerage_amount: this.parseNumeric(
                        record['Brokerage Amount'] || record['brokerage_amount'] ||
                        record['Brokerage'] || record['brokerage']
                    ) || 0,
                    service_tax: this.parseNumeric(
                        record['Service Tax'] || record['service_tax'] ||
                        record['Taxes'] || record['taxes']
                    ) || 0,
                    stt_amount: this.parseNumeric(
                        record['STT Amount'] || record['stt_amount'] ||
                        record['STT'] || record['stt']
                    ) || 0,
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.ecn_number || mapped.ecn_number === '' || mapped.ecn_number.startsWith('AUTO_ECN_')) {
                    mapped.ecn_number = `AUTO_ECN_${Date.now()}_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated ecn_number: ${mapped.ecn_number}`);
                }
                if (!mapped.client_code || mapped.client_code === '') {
                    mapped.client_code = this.cleanCodeValue(`AUTO_CLIENT_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processCashFlow(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Use EXACT field mappings from Cash Capital Flow for Hdfc Custodian file
                // Primary Key: TRANSREF → transaction_ref
                const mapped = {
                    // Primary Key
                    transaction_ref: this.cleanValue(
                        record['TRANSREF'] || record['transaction_ref'] ||
                        record['Transaction ID'] || record['TransactionID'] ||
                        `AUTO_CASH_${Date.now()}_${index + 1}`
                    ),
                    broker_code: this.cleanCodeValue(
                        record['BROKER CODE'] || record['broker_code'] ||
                        record['Broker Code'] || record['BrokerCode'] ||
                        'AUTO_BROKER'
                    ),
                    client_code: this.cleanCodeValue(
                        record['CLIENT CODE'] || record['client_code'] ||
                        record['Client Code'] || record['ClientCode'] ||
                        `CLIENT_${index + 1}`
                    ),
                    instrument_isin: this.cleanValue(
                        record['ISIN'] || record['instrument_isin'] ||
                        record['isin'] || 'CASH'
                    ),
                    exchange: this.cleanValue(
                        record['EXCHANGE'] || record['exchange'] ||
                        record['Exchange'] || 'NSE'
                    ),
                    transaction_type: this.cleanValue(
                        record['TRANSACTION TYPE'] || record['transaction_type'] ||
                        record['Transaction Type'] || record['TransactionType'] ||
                        'CASH_IN'
                    ),
                    transaction_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    settlement_date: this.parseDate(
                        record['SETTLEMENT DATE'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    amount: this.parseNumeric(
                        record['AMOUNT'] || record['amount'] ||
                        record['Amount']
                    ) || 0,
                    charges: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage'] || record['CHARGES'] || record['charges']
                    ) || 0,
                    tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax'] || record['TAX'] || record['tax']
                    ) || 0,
                    net_amount: this.parseNumeric(
                        record['NET AMOUNT'] || record['net_amount'] ||
                        record['Net Amount'] || record['PRICE'] || record['price']
                    ) || 0,
                    payment_mode: this.cleanValue(
                        record['PAYMENT MODE'] || record['payment_mode'] ||
                        record['Payment Mode'] || 'ONLINE'
                    ),
                    bank_reference: this.cleanValue(
                        record['BANK REFERENCE'] || record['bank_reference'] ||
                        record['Bank Reference'] || record['Reference']
                    ),
                    remarks: this.cleanValue(
                        record['REMARKS'] || record['remarks'] ||
                        record['Remarks'] || record['Description']
                    ),
                    created_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.transaction_ref || mapped.transaction_ref === '' || mapped.transaction_ref.startsWith('AUTO_CASH_')) {
                    mapped.transaction_ref = `AUTO_CASH_${Date.now()}_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated transaction_ref: ${mapped.transaction_ref}`);
                }
                if (!mapped.client_code || mapped.client_code === '') {
                    mapped.client_code = this.cleanCodeValue(`AUTO_CLIENT_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
                }
                if (!mapped.broker_code || mapped.broker_code === '') {
                    mapped.broker_code = this.cleanCodeValue('AUTO_BROKER');
                    warnings.push(`Row ${index + 1}: Auto-generated broker_code: ${mapped.broker_code}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processStockFlow(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Use EXACT field mappings from Stock Capital Flow for Hdfc Custodian file
                const mapped = {
                    // Primary Key
                    transaction_ref: this.cleanValue(
                        record['TRANSREF'] || record['transaction_ref'] ||
                        record['Transaction ID'] || record['TransactionID'] ||
                        `STOCK_${index + 1}`
                    ),
                    broker_code: this.cleanCodeValue(
                        record['BROKER CODE'] || record['broker_code'] ||
                        record['Broker Code'] || record['BrokerCode'] ||
                        'AUTO_BROKER'
                    ),
                    client_code: this.cleanCodeValue(
                        record['CLIENT CODE'] || record['client_code'] ||
                        record['Client Code'] || record['ClientCode'] ||
                        `CLIENT_${index + 1}`
                    ),
                    instrument_isin: this.cleanValue(
                        record['ISIN'] || record['instrument_isin'] ||
                        record['isin']
                    ),
                    exchange: this.cleanValue(
                        record['EXCHANGE'] || record['exchange'] ||
                        record['Exchange'] || 'NSE'
                    ),
                    transaction_type: this.cleanValue(
                        record['TRANSACTION TYPE'] || record['transaction_type'] ||
                        record['Transaction Type'] || record['TransactionType'] ||
                        'BUY'
                    ),
                    acquisition_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    security_in_date: this.parseDate(
                        record['SECURITY IN DATE'] || record['security_in_date'] ||
                        record['Security In Date']
                    ),
                    quantity: this.parseNumeric(
                        record['QUANTITY'] || record['quantity'] ||
                        record['Quantity']
                    ) || 0,
                    original_price: this.parseNumeric(
                        record['ORIGINAL PRICE'] || record['original_price'] ||
                        record['Original Price'] || record['Price']
                    ) || 0,
                    brokerage: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage']
                    ) || 0,
                    service_tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax']
                    ) || 0,
                    settlement_date_flag: this.cleanValue(
                        record['SETTLEMENT DATE FLAG'] || record['settlement_date_flag'] ||
                        record['Settlement Date Flag']
                    ),
                    market_rate: this.parseNumeric(
                        record['MARKET RATE AS ON SECURITY IN DATE'] || record['market_rate'] ||
                        record['Market Rate']
                    ) || 0,
                    cash_symbol: this.cleanValue(
                        record['CASH SYMBOL'] || record['cash_symbol'] ||
                        record['Cash Symbol']
                    ),
                    stt_amount: this.parseNumeric(
                        record['STT AMOUNT'] || record['stt_amount'] ||
                        record['STT Amount'] || record['STT']
                    ) || 0,
                    accrued_interest: this.parseNumeric(
                        record['ACCRUED INTEREST'] || record['accrued_interest'] ||
                        record['Accrued Interest']
                    ) || 0,
                    block_ref: this.cleanValue(
                        record['BLOCK REF.'] || record['block_ref'] ||
                        record['Block Ref'] || record['Reference Number']
                    ),
                    remarks: this.cleanValue(
                        record['REMARKS'] || record['remarks'] ||
                        record['Remarks'] || record['Description']
                    ),
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.transaction_ref || mapped.transaction_ref === '' || mapped.transaction_ref.startsWith('STOCK_')) {
                    mapped.transaction_ref = `AUTO_STOCK_${Date.now()}_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated transaction_ref: ${mapped.transaction_ref}`);
                }
                if (!mapped.client_code || mapped.client_code === '' || mapped.client_code.startsWith('CLIENT_')) {
                    mapped.client_code = this.cleanCodeValue(`AUTO_CLIENT_${Date.now()}_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
                }
                if (!mapped.broker_code || mapped.broker_code === '' || mapped.broker_code === 'AUTO_BROKER') {
                    mapped.broker_code = this.cleanCodeValue(`AUTO_BROKER_${Date.now()}`);
                    warnings.push(`Row ${index + 1}: Auto-generated broker_code: ${mapped.broker_code}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processMFAllocations(records, metadata) {
        const mappedRecords = [];
        const errors = [];
        const warnings = [];

        records.forEach((record, index) => {
            try {
                // Use EXACT field mappings from MF Buy Allocation file format.xlsx
                const mapped = {
                    // allocation_id is auto-generated (serial)
                    allocation_date: this.parseDate(
                        record['Date'] || record['allocation_date'] ||
                        record['Allocation Date'] || new Date()
                    ),
                    client_name: this.cleanValue(
                        record['Client Name'] || record['client_name'] ||
                        record['ClientName']
                    ),
                    custody_code: this.cleanCodeValue(
                        record['Custody Code'] || record['custody_code'] ||
                        record['CustodyCode']
                    ),
                    pan: this.cleanValue(
                        record['PAN'] || record['pan'] ||
                        record['Pan']
                    ),
                    debit_account_number: this.cleanValue(
                        record['Debit Bank account Number'] || record['debit_account_number'] ||
                        record['Debit Account Number'] || record['DebitAccountNumber']
                    ),
                    folio_number: this.cleanValue(
                        record['Folio No'] || record['folio_number'] ||
                        record['Folio Number'] || record['FolioNumber']
                    ),
                    amc_name: this.cleanValue(
                        record['AMC Name'] || record['amc_name'] ||
                        record['AMC'] || record['AmcName']
                    ),
                    scheme_name: this.cleanValue(
                        record['Scheme Name - Plan - Option'] || record['scheme_name'] ||
                        record['Scheme Name'] || record['SchemeName']
                    ),
                    instrument_isin: this.cleanValue(
                        record['ISIN No'] || record['instrument_isin'] ||
                        record['ISIN'] || record['isin']
                    ),
                    purchase_amount: this.parseNumeric(
                        record['Purchase Amount'] || record['purchase_amount'] ||
                        record['Amount'] || record['PurchaseAmount']
                    ) || 0,
                    beneficiary_account_name: this.cleanValue(
                        record['Beneficiary Account Name'] || record['beneficiary_account_name'] ||
                        record['BeneficiaryAccountName']
                    ),
                    beneficiary_account_number: this.cleanValue(
                        record['Benecificiary Account Number'] || record['Beneficiary Account Number'] ||
                        record['beneficiary_account_number'] || record['BeneficiaryAccountNumber']
                    ),
                    beneficiary_bank_name: this.cleanValue(
                        record['Beneficiary Bank Name'] || record['beneficiary_bank_name'] ||
                        record['BeneficiaryBankName']
                    ),
                    ifsc_code: this.cleanValue(
                        record['IFSC Code'] || record['ifsc_code'] ||
                        record['IFSC'] || record['IfscCode']
                    ),
                    euin: this.cleanValue(
                        record['EUIN'] || record['euin'] ||
                        record['Euin']
                    ),
                    arn_code: this.cleanValue(
                        record['ARN Code'] || record['arn_code'] ||
                        record['ARN'] || record['ArnCode']
                    ),
                    created_at: new Date(),
                    updated_at: new Date()
                };

                // Auto-generate missing required fields
                if (!mapped.allocation_date) {
                    mapped.allocation_date = new Date();
                    warnings.push(`Row ${index + 1}: Auto-generated allocation_date: ${mapped.allocation_date.toDateString()}`);
                }
                if (!mapped.client_name || mapped.client_name === '') {
                    mapped.client_name = mapped.custody_code || `Client_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated client_name: ${mapped.client_name}`);
                }
                if (!mapped.custody_code || mapped.custody_code === '') {
                    mapped.custody_code = this.cleanCodeValue(`AUTO_CUSTODY_${index + 1}`);
                    warnings.push(`Row ${index + 1}: Auto-generated custody_code: ${mapped.custody_code}`);
                }
                if (!mapped.scheme_name || mapped.scheme_name === '') {
                    mapped.scheme_name = mapped.instrument_isin || `Scheme_${index + 1}`;
                    warnings.push(`Row ${index + 1}: Auto-generated scheme_name: ${mapped.scheme_name}`);
                }

                mappedRecords.push(mapped);
            } catch (error) {
                errors.push(`Row ${index + 1}: ${error.message}`);
            }
        });

        return {
            mappedRecords,
            mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
        };
    }

    processUnknownType(records, metadata) {
        // For unknown types, store as-is in raw_uploads table
        const mappedRecords = records.map((record, index) => ({
            ...record,
            _file_name: metadata.fileName,
            _upload_timestamp: new Date(),
            _processing_status: 'unknown_type'
        }));

        return {
            mappedRecords,
            mappingResults: { 
                errors: [], 
                warnings: [`Unknown file type - storing ${records.length} records as raw data`], 
                totalRecords: records.length, 
                mappedRecords: mappedRecords.length 
            }
        };
    }

    // Utility methods
    cleanValue(value) {
        if (value == null) return '';
        return String(value).trim();
    }

    cleanCodeValue(value) {
        if (value == null) return '';
        // Clean and ensure alphanumeric format for CODE fields
        const cleaned = String(value).trim().toUpperCase();
        // Remove special characters, keep only letters, numbers, and common separators
        const alphanumeric = cleaned.replace(/[^A-Z0-9_-]/g, '');
        return alphanumeric;
    }

    parseNumeric(value) {
        if (value == null || value === '') return null;
        const cleanValue = String(value).replace(/[,\s]/g, '');
        const numericValue = parseFloat(cleanValue);
        return isNaN(numericValue) ? null : numericValue;
    }

    parseDate(value) {
        if (!value) return null;
        const date = new Date(value);
        return isNaN(date.getTime()) ? null : date;
    }
}

module.exports = SmartFileProcessor; 