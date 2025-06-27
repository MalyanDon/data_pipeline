const { MongoClient } = require('mongodb');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

class SmartFileProcessor {
    constructor(mongoUri = 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/') {
        this.mongoUri = mongoUri;
        this.client = null;
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
}

module.exports = SmartFileProcessor; 