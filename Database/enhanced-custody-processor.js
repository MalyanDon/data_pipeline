const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

// PostgreSQL connection
const pgPool = new Pool({
    user: 'abhishekmalyan',
    host: 'localhost',
    database: 'financial_data',
    password: '',
    port: 5432,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

class EnhancedCustodyProcessor {
    constructor() {
        this.mongoClient = null;
        this.processedColumns = new Set();
    }

    async initialize() {
        try {
            // Connect to MongoDB
            const mongoUri = config.mongodb.uri.replace('/?', 'financial_data_2025?');
            this.mongoClient = new MongoClient(mongoUri);
            await this.mongoClient.connect();
            console.log('✅ Connected to MongoDB Atlas');

            // Test PostgreSQL
            await pgPool.query('SELECT NOW()');
            console.log('✅ Connected to PostgreSQL');

            return true;
        } catch (error) {
            console.error('❌ Connection failed:', error.message);
            return false;
        }
    }

    // Automatically detect and add new columns
    async ensureColumnExists(tableName, columnName, columnType) {
        const client = await pgPool.connect();
        try {
            // Check if column exists
            const checkQuery = `
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = $2
            `;
            const result = await client.query(checkQuery, [tableName, columnName]);
            
            if (result.rows.length === 0) {
                // Column doesn't exist, add it
                const alterQuery = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`;
                await client.query(alterQuery);
                console.log(`✅ Added new column: ${tableName}.${columnName} (${columnType})`);
            }
        } catch (error) {
            console.error(`❌ Error adding column ${columnName}:`, error.message);
        } finally {
            client.release();
        }
    }

    // Intelligent column type detection
    detectColumnType(value) {
        if (value === null || value === undefined) return 'TEXT';
        
        const str = String(value).trim();
        
        // Number detection
        if (!isNaN(str) && !isNaN(parseFloat(str))) {
            if (str.includes('.')) return 'DECIMAL(15,4)';
            return 'BIGINT';
        }
        
        // Date detection
        if (str.match(/^\d{4}-\d{2}-\d{2}$/) || str.match(/^\d{2}\/\d{2}\/\d{4}$/)) {
            return 'DATE';
        }
        
        // Boolean detection
        if (str.toLowerCase() === 'true' || str.toLowerCase() === 'false') {
            return 'BOOLEAN';
        }
        
        // Text length based typing
        if (str.length > 500) return 'TEXT';
        if (str.length > 100) return 'VARCHAR(500)';
        return 'VARCHAR(100)';
    }

    // Enhanced dynamic processing
    async processCustodyCollection(collectionName, sourceSystem) {
        console.log(`\n🔍 Processing ${collectionName} (${sourceSystem})`);
        
        const db = this.mongoClient.db('financial_data_2025');
        const collection = db.collection(collectionName);
        
        // Get sample documents to analyze schema
        const sampleDocs = await collection.find({}).limit(10).toArray();
        if (sampleDocs.length === 0) {
            console.log('❌ No documents found');
            return;
        }

        console.log(`📊 Analyzing ${sampleDocs.length} sample documents for schema`);

        // Collect all unique columns across all documents
        const allColumns = new Set();
        const columnExamples = {};
        
        sampleDocs.forEach(doc => {
            Object.keys(doc).forEach(key => {
                if (!['_id', 'month', 'date', 'fileName', 'fileType', 'uploadedAt'].includes(key)) {
                    allColumns.add(key);
                    if (!columnExamples[key]) {
                        columnExamples[key] = doc[key];
                    }
                }
            });
        });

        console.log(`🔍 Found ${allColumns.size} unique columns:`, Array.from(allColumns));

        // Ensure all columns exist in PostgreSQL
        for (const columnName of allColumns) {
            const cleanColumnName = columnName.toLowerCase()
                .replace(/[^a-z0-9_]/g, '_')
                .replace(/_{2,}/g, '_')
                .replace(/^_|_$/g, '');
            
            const columnType = this.detectColumnType(columnExamples[columnName]);
            await this.ensureColumnExists('unified_custody_master', cleanColumnName, columnType);
        }

        // Process all documents
        const allDocs = await collection.find({}).toArray();
        console.log(`📋 Processing ${allDocs.length} documents`);

        let processedCount = 0;
        const batchSize = 100;

        for (let i = 0; i < allDocs.length; i += batchSize) {
            const batch = allDocs.slice(i, i + batchSize);
            const batchResult = await this.processCustodyBatch(batch, sourceSystem, allColumns);
            processedCount += batchResult.processed;
            
            if (i % 500 === 0) {
                console.log(`📈 Progress: ${processedCount}/${allDocs.length} (${Math.round(processedCount/allDocs.length*100)}%)`);
            }
        }

        console.log(`✅ Completed: ${processedCount}/${allDocs.length} records processed`);
        return { processed: processedCount, total: allDocs.length };
    }

    async processCustodyBatch(documents, sourceSystem, allColumns) {
        const client = await pgPool.connect();
        let processed = 0;

        try {
            for (const doc of documents) {
                try {
                    // Build dynamic INSERT statement
                    const columns = ['source_system', 'file_name', 'record_date'];
                    const values = [sourceSystem, doc.fileName || 'mongodb_import', new Date().toISOString().split('T')[0]];
                    const placeholders = ['$1', '$2', '$3'];
                    let paramIndex = 4;

                    // Add all detected columns
                    for (const columnName of allColumns) {
                        const cleanColumnName = columnName.toLowerCase()
                            .replace(/[^a-z0-9_]/g, '_')
                            .replace(/_{2,}/g, '_')
                            .replace(/^_|_$/g, '');

                        columns.push(cleanColumnName);
                        placeholders.push(`$${paramIndex}`);
                        
                        let value = doc[columnName];
                        
                        // Type conversion
                        if (typeof value === 'number') {
                            values.push(value);
                        } else if (typeof value === 'boolean') {
                            values.push(value);
                        } else if (value) {
                            values.push(String(value));
                        } else {
                            values.push(null);
                        }
                        
                        paramIndex++;
                    }

                    const insertSQL = `
                        INSERT INTO unified_custody_master (${columns.join(', ')}) 
                        VALUES (${placeholders.join(', ')})
                        ON CONFLICT DO NOTHING
                    `;

                    await client.query(insertSQL, values);
                    processed++;

                } catch (error) {
                    console.error(`❌ Row error: ${error.message.substring(0, 100)}`);
                }
            }
        } finally {
            client.release();
        }

        return { processed };
    }

    async processAllCustodySystems() {
        console.log('🚀 Starting Enhanced Custody Processing with Dynamic Schema');
        
        const custodyCollections = [
            { name: 'hdfc_06_25', system: 'HDFC' },
            { name: 'axis_06_25', system: 'AXIS' },
            { name: 'kotak_06_25', system: 'KOTAK' },
            { name: 'orbis_06_28', system: 'ORBIS' },
            { name: 'end_client_holding_trustpms_20250625004802_270225530_new_06_28', system: 'TRUSTPMS' }
        ];

        const results = {};
        
        for (const { name, system } of custodyCollections) {
            try {
                const result = await this.processCustodyCollection(name, system);
                results[system] = result;
            } catch (error) {
                console.error(`❌ Failed to process ${system}:`, error.message);
                results[system] = { error: error.message };
            }
        }

        return results;
    }

    async getCurrentCustodyStatus() {
        const client = await pgPool.connect();
        try {
            // Get current counts by source system
            const query = `
                SELECT source_system, COUNT(*) as count
                FROM unified_custody_master 
                GROUP BY source_system 
                ORDER BY count DESC
            `;
            const result = await client.query(query);
            
            console.log('\n📊 Current PostgreSQL Custody Status:');
            result.rows.forEach(row => {
                console.log(`  📁 ${row.source_system}: ${parseInt(row.count).toLocaleString()} records`);
            });
            
            return result.rows;
        } finally {
            client.release();
        }
    }

    async showTableSchema() {
        const client = await pgPool.connect();
        try {
            const query = `
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'unified_custody_master'
                ORDER BY ordinal_position
            `;
            const result = await client.query(query);
            
            console.log('\n🏗️ Current unified_custody_master Schema:');
            result.rows.forEach(row => {
                const length = row.character_maximum_length ? `(${row.character_maximum_length})` : '';
                console.log(`  📋 ${row.column_name}: ${row.data_type}${length}`);
            });
            
            return result.rows;
        } finally {
            client.release();
        }
    }

    async cleanup() {
        if (this.mongoClient) {
            await this.mongoClient.close();
        }
    }
}

// Main execution
async function main() {
    const processor = new EnhancedCustodyProcessor();
    
    try {
        const connected = await processor.initialize();
        if (!connected) {
            console.error('❌ Failed to connect to databases');
            return;
        }

        // Show current status
        await processor.getCurrentCustodyStatus();
        await processor.showTableSchema();

        // Process missing HDFC data
        console.log('\n🎯 Processing HDFC data (53 records missing from PostgreSQL)');
        const hdfc_result = await processor.processCustodyCollection('hdfc_06_25', 'HDFC');
        
        // Show final status
        console.log('\n✅ Final Status:');
        await processor.getCurrentCustodyStatus();
        
    } catch (error) {
        console.error('❌ Processing failed:', error);
    } finally {
        await processor.cleanup();
        process.exit(0);
    }
}

if (require.main === module) {
    main();
}

module.exports = EnhancedCustodyProcessor; 
const { MongoClient } = require('mongodb');
const config = require('./config');

// PostgreSQL connection
const pgPool = new Pool({
    user: 'abhishekmalyan',
    host: 'localhost',
    database: 'financial_data',
    password: '',
    port: 5432,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

class EnhancedCustodyProcessor {
    constructor() {
        this.mongoClient = null;
        this.processedColumns = new Set();
    }

    async initialize() {
        try {
            // Connect to MongoDB
            const mongoUri = config.mongodb.uri.replace('/?', 'financial_data_2025?');
            this.mongoClient = new MongoClient(mongoUri);
            await this.mongoClient.connect();
            console.log('✅ Connected to MongoDB Atlas');

            // Test PostgreSQL
            await pgPool.query('SELECT NOW()');
            console.log('✅ Connected to PostgreSQL');

            return true;
        } catch (error) {
            console.error('❌ Connection failed:', error.message);
            return false;
        }
    }

    // Automatically detect and add new columns
    async ensureColumnExists(tableName, columnName, columnType) {
        const client = await pgPool.connect();
        try {
            // Check if column exists
            const checkQuery = `
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = $1 AND column_name = $2
            `;
            const result = await client.query(checkQuery, [tableName, columnName]);
            
            if (result.rows.length === 0) {
                // Column doesn't exist, add it
                const alterQuery = `ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`;
                await client.query(alterQuery);
                console.log(`✅ Added new column: ${tableName}.${columnName} (${columnType})`);
            }
        } catch (error) {
            console.error(`❌ Error adding column ${columnName}:`, error.message);
        } finally {
            client.release();
        }
    }

    // Intelligent column type detection
    detectColumnType(value) {
        if (value === null || value === undefined) return 'TEXT';
        
        const str = String(value).trim();
        
        // Number detection
        if (!isNaN(str) && !isNaN(parseFloat(str))) {
            if (str.includes('.')) return 'DECIMAL(15,4)';
            return 'BIGINT';
        }
        
        // Date detection
        if (str.match(/^\d{4}-\d{2}-\d{2}$/) || str.match(/^\d{2}\/\d{2}\/\d{4}$/)) {
            return 'DATE';
        }
        
        // Boolean detection
        if (str.toLowerCase() === 'true' || str.toLowerCase() === 'false') {
            return 'BOOLEAN';
        }
        
        // Text length based typing
        if (str.length > 500) return 'TEXT';
        if (str.length > 100) return 'VARCHAR(500)';
        return 'VARCHAR(100)';
    }

    // Enhanced dynamic processing
    async processCustodyCollection(collectionName, sourceSystem) {
        console.log(`\n🔍 Processing ${collectionName} (${sourceSystem})`);
        
        const db = this.mongoClient.db('financial_data_2025');
        const collection = db.collection(collectionName);
        
        // Get sample documents to analyze schema
        const sampleDocs = await collection.find({}).limit(10).toArray();
        if (sampleDocs.length === 0) {
            console.log('❌ No documents found');
            return;
        }

        console.log(`📊 Analyzing ${sampleDocs.length} sample documents for schema`);

        // Collect all unique columns across all documents
        const allColumns = new Set();
        const columnExamples = {};
        
        sampleDocs.forEach(doc => {
            Object.keys(doc).forEach(key => {
                if (!['_id', 'month', 'date', 'fileName', 'fileType', 'uploadedAt'].includes(key)) {
                    allColumns.add(key);
                    if (!columnExamples[key]) {
                        columnExamples[key] = doc[key];
                    }
                }
            });
        });

        console.log(`🔍 Found ${allColumns.size} unique columns:`, Array.from(allColumns));

        // Ensure all columns exist in PostgreSQL
        for (const columnName of allColumns) {
            const cleanColumnName = columnName.toLowerCase()
                .replace(/[^a-z0-9_]/g, '_')
                .replace(/_{2,}/g, '_')
                .replace(/^_|_$/g, '');
            
            const columnType = this.detectColumnType(columnExamples[columnName]);
            await this.ensureColumnExists('unified_custody_master', cleanColumnName, columnType);
        }

        // Process all documents
        const allDocs = await collection.find({}).toArray();
        console.log(`📋 Processing ${allDocs.length} documents`);

        let processedCount = 0;
        const batchSize = 100;

        for (let i = 0; i < allDocs.length; i += batchSize) {
            const batch = allDocs.slice(i, i + batchSize);
            const batchResult = await this.processCustodyBatch(batch, sourceSystem, allColumns);
            processedCount += batchResult.processed;
            
            if (i % 500 === 0) {
                console.log(`📈 Progress: ${processedCount}/${allDocs.length} (${Math.round(processedCount/allDocs.length*100)}%)`);
            }
        }

        console.log(`✅ Completed: ${processedCount}/${allDocs.length} records processed`);
        return { processed: processedCount, total: allDocs.length };
    }

    async processCustodyBatch(documents, sourceSystem, allColumns) {
        const client = await pgPool.connect();
        let processed = 0;

        try {
            for (const doc of documents) {
                try {
                    // Build dynamic INSERT statement
                    const columns = ['source_system', 'file_name', 'record_date'];
                    const values = [sourceSystem, doc.fileName || 'mongodb_import', new Date().toISOString().split('T')[0]];
                    const placeholders = ['$1', '$2', '$3'];
                    let paramIndex = 4;

                    // Add all detected columns
                    for (const columnName of allColumns) {
                        const cleanColumnName = columnName.toLowerCase()
                            .replace(/[^a-z0-9_]/g, '_')
                            .replace(/_{2,}/g, '_')
                            .replace(/^_|_$/g, '');

                        columns.push(cleanColumnName);
                        placeholders.push(`$${paramIndex}`);
                        
                        let value = doc[columnName];
                        
                        // Type conversion
                        if (typeof value === 'number') {
                            values.push(value);
                        } else if (typeof value === 'boolean') {
                            values.push(value);
                        } else if (value) {
                            values.push(String(value));
                        } else {
                            values.push(null);
                        }
                        
                        paramIndex++;
                    }

                    const insertSQL = `
                        INSERT INTO unified_custody_master (${columns.join(', ')}) 
                        VALUES (${placeholders.join(', ')})
                        ON CONFLICT DO NOTHING
                    `;

                    await client.query(insertSQL, values);
                    processed++;

                } catch (error) {
                    console.error(`❌ Row error: ${error.message.substring(0, 100)}`);
                }
            }
        } finally {
            client.release();
        }

        return { processed };
    }

    async processAllCustodySystems() {
        console.log('🚀 Starting Enhanced Custody Processing with Dynamic Schema');
        
        const custodyCollections = [
            { name: 'hdfc_06_25', system: 'HDFC' },
            { name: 'axis_06_25', system: 'AXIS' },
            { name: 'kotak_06_25', system: 'KOTAK' },
            { name: 'orbis_06_28', system: 'ORBIS' },
            { name: 'end_client_holding_trustpms_20250625004802_270225530_new_06_28', system: 'TRUSTPMS' }
        ];

        const results = {};
        
        for (const { name, system } of custodyCollections) {
            try {
                const result = await this.processCustodyCollection(name, system);
                results[system] = result;
            } catch (error) {
                console.error(`❌ Failed to process ${system}:`, error.message);
                results[system] = { error: error.message };
            }
        }

        return results;
    }

    async getCurrentCustodyStatus() {
        const client = await pgPool.connect();
        try {
            // Get current counts by source system
            const query = `
                SELECT source_system, COUNT(*) as count
                FROM unified_custody_master 
                GROUP BY source_system 
                ORDER BY count DESC
            `;
            const result = await client.query(query);
            
            console.log('\n📊 Current PostgreSQL Custody Status:');
            result.rows.forEach(row => {
                console.log(`  📁 ${row.source_system}: ${parseInt(row.count).toLocaleString()} records`);
            });
            
            return result.rows;
        } finally {
            client.release();
        }
    }

    async showTableSchema() {
        const client = await pgPool.connect();
        try {
            const query = `
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'unified_custody_master'
                ORDER BY ordinal_position
            `;
            const result = await client.query(query);
            
            console.log('\n🏗️ Current unified_custody_master Schema:');
            result.rows.forEach(row => {
                const length = row.character_maximum_length ? `(${row.character_maximum_length})` : '';
                console.log(`  📋 ${row.column_name}: ${row.data_type}${length}`);
            });
            
            return result.rows;
        } finally {
            client.release();
        }
    }

    async cleanup() {
        if (this.mongoClient) {
            await this.mongoClient.close();
        }
    }
}

// Main execution
async function main() {
    const processor = new EnhancedCustodyProcessor();
    
    try {
        const connected = await processor.initialize();
        if (!connected) {
            console.error('❌ Failed to connect to databases');
            return;
        }

        // Show current status
        await processor.getCurrentCustodyStatus();
        await processor.showTableSchema();

        // Process missing HDFC data
        console.log('\n🎯 Processing HDFC data (53 records missing from PostgreSQL)');
        const hdfc_result = await processor.processCustodyCollection('hdfc_06_25', 'HDFC');
        
        // Show final status
        console.log('\n✅ Final Status:');
        await processor.getCurrentCustodyStatus();
        
    } catch (error) {
        console.error('❌ Processing failed:', error);
    } finally {
        await processor.cleanup();
        process.exit(0);
    }
}

if (require.main === module) {
    main();
}

module.exports = EnhancedCustodyProcessor; 