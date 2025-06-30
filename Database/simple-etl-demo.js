const express = require('express');
const { MongoClient } = require('mongodb');
const config = require('./config');

const app = express();
const PORT = 3001;
const mongoUri = config.mongodb.uri + config.mongodb.database;

app.use(express.json());

// API to get custody file schemas (now properly organized)
app.get('/api/custody/file-schemas', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        // All 4 custody holdings from different custodians
        const custodyFileTypes = ['hdfc', 'axis', 'kotak', 'orbis'];
        const fileSchemas = [];
        const date = '2025-06-30';
        
        for (const fileType of custodyFileTypes) {
            try {
                // Use new hierarchical structure: custody_files.date.fileType
                const collectionName = `custody_files.${date}.${fileType}`;
                const collection = db.collection(collectionName);
                const sampleDoc = await collection.findOne({});
                const count = await collection.countDocuments({});
                
                if (sampleDoc) {
                    const columns = Object.keys(sampleDoc).filter(key => 
                        !['_id', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt', '__v'].includes(key)
                    );
                    
                    fileSchemas.push({
                        name: fileType,
                        displayName: `${fileType.toUpperCase()} Custody Holdings`,
                        collectionName: collectionName,
                        columns: columns,
                        columnCount: columns.length,
                        recordCount: count,
                        lastUpdated: sampleDoc.uploadedAt || new Date().toISOString(),
                        fileName: sampleDoc.fileName || `${fileType}_custody_file`,
                        custodian: fileType.toUpperCase()
                    });
                }
            } catch (error) {
                console.log(`Error processing ${fileType}:`, error.message);
            }
        }
        
        await client.close();
        
        console.log(`✅ Found ${fileSchemas.length} custody holdings:`, 
                   fileSchemas.map(f => `${f.custodian} (${f.recordCount} records)`));
        
        res.json({
            success: true,
            date: date,
            custodianCount: fileSchemas.length,
            files: fileSchemas,
            totalRecords: fileSchemas.reduce((sum, f) => sum + f.recordCount, 0)
        });
        
    } catch (error) {
        console.error('Error fetching custody file schemas:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// API to get available dates for custody files
app.get('/api/custody/dates', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const collections = await db.listCollections().toArray();
        const custodyDates = new Set();
        
        collections.forEach(col => {
            if (col.name.startsWith('custody_files.')) {
                const parts = col.name.split('.');
                if (parts.length >= 2) {
                    custodyDates.add(parts[1]); // Extract date part
                }
            }
        });
        
        await client.close();
        
        res.json({
            success: true,
            dates: Array.from(custodyDates).sort()
        });
        
    } catch (error) {
        console.error('Error fetching custody dates:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Main interface
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html><head><title>ETL Visual Grid Mapping - All Custody Holdings</title><style>
body{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}
.container{max-width:1400px;margin:0 auto;background:white;padding:20px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}
.header{text-align:center;margin-bottom:30px}
.title{color:#1890ff;font-size:32px;margin-bottom:10px}
.subtitle{color:#666;font-size:18px;margin-bottom:10px}
.files-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:20px;margin-top:20px}
.file-card{border:1px solid #ddd;border-radius:8px;padding:20px;background:#fafafa;position:relative}
.file-title{font-size:20px;font-weight:bold;color:#1890ff;margin-bottom:15px}
.file-info{color:#666;margin-bottom:15px;line-height:1.6}
.columns-list{max-height:200px;overflow-y:auto;border:1px solid #eee;padding:10px;border-radius:4px;background:white}
.column-item{padding:5px;border-bottom:1px solid #f0f0f0;font-family:monospace;font-size:12px}
.loading{text-align:center;padding:50px;color:#666}
.custody-badge{background:#52c41a;color:white;padding:4px 12px;border-radius:12px;font-size:12px;margin-left:10px}
.custodian-badge{background:#722ed1;color:white;padding:2px 8px;border-radius:8px;font-size:11px;position:absolute;top:15px;right:15px}
.date-info{background:#f0f9ff;padding:15px;border-radius:6px;margin:20px 0;border-left:4px solid #1890ff}
.summary-card{background:#f6ffed;border:1px solid #b7eb8f;border-radius:6px;padding:20px;margin-top:25px}
.stats-row{display:flex;justify-content:space-between;margin:10px 0;font-weight:500}
</style></head><body>
<div class="container">
    <div class="header">
        <h1 class="title">🏦 ETL Visual Grid Mapping System</h1>
        <p class="subtitle">Map columns from multiple custodian holdings to unified schema</p>
        <div class="date-info">
            <strong>📅 Processing Date:</strong> <span id="processing-date">Loading...</span> | 
            <strong>🏛️ Custodians:</strong> <span class="custody-badge">ALL CUSTODY HOLDINGS</span>
        </div>
    </div>
    <div id="loading" class="loading">Loading custody holdings from all custodians...</div>
    <div id="files-container" style="display:none">
        <h2>🏛️ Available Custody Holdings <span class="custody-badge">4 Custodians</span></h2>
        <div id="files-grid" class="files-grid"></div>
        <div style="margin-top:40px;padding:25px;background:#f0f9ff;border-radius:8px;">
            <h3>📝 Multi-Custodian ETL Mapping:</h3>
            <p>✅ <strong>4 Custodians:</strong> HDFC, AXIS, KOTAK, ORBIS - all properly organized</p>
            <p>🔄 <strong>Ready to map:</strong> Create unified schema from different custodian formats</p>
            <p>🎯 <strong>Structure:</strong> <code>custody_files.date.custodian</code></p>
            <p>💡 <strong>Next:</strong> Define mappings between different custodian column formats</p>
        </div>
    </div>
</div>
<script>
async function loadFileSchemas() {
    try {
        const response = await fetch('/api/custody/file-schemas');
        const data = await response.json();
        
        if (data.success) {
            document.getElementById('processing-date').textContent = data.date;
            document.getElementById('loading').style.display = 'none';
            document.getElementById('files-container').style.display = 'block';
            
            const grid = document.getElementById('files-grid');
            grid.innerHTML = '';
            
            data.files.forEach(file => {
                const card = document.createElement('div');
                card.className = 'file-card';
                card.innerHTML = 
                    '<div class="custodian-badge">' + file.custodian + '</div>' +
                    '<div class="file-title">🏛️ ' + file.displayName + '</div>' +
                    '<div class="file-info">' +
                        '📋 Collection: <code>' + file.collectionName + '</code><br>' +
                        '📄 File: ' + file.fileName + '<br>' +
                        '📊 ' + file.columnCount + ' columns<br>' +
                        '📈 <strong>' + file.recordCount.toLocaleString() + '</strong> records<br>' +
                        '🕒 Updated: ' + new Date(file.lastUpdated).toLocaleDateString() +
                    '</div>' +
                    '<div class="columns-list">' + 
                        file.columns.map(col => '<div class="column-item">📄 ' + col + '</div>').join('') + 
                    '</div>';
                grid.appendChild(card);
            });
            
            // Show detailed summary
            const summary = document.createElement('div');
            summary.className = 'summary-card';
            summary.innerHTML = 
                '<h3>📊 Multi-Custodian Holdings Summary</h3>' +
                '<div class="stats-row">' +
                    '<span>🏛️ Total Custodians:</span>' +
                    '<span><strong>' + data.custodianCount + '</strong></span>' +
                '</div>' +
                '<div class="stats-row">' +
                    '<span>📈 Total Records:</span>' +
                    '<span><strong>' + data.totalRecords.toLocaleString() + '</strong></span>' +
                '</div>' +
                data.files.map(f => 
                    '<div class="stats-row">' +
                        '<span>🏛️ ' + f.custodian + ':</span>' +
                        '<span>' + f.recordCount.toLocaleString() + ' records (' + f.columnCount + ' cols)</span>' +
                    '</div>'
                ).join('') +
                '<hr style="margin:15px 0;">' +
                '<p><strong>🎯 Ready for ETL mapping across all custodians!</strong></p>';
            grid.appendChild(summary);
        } else {
            throw new Error(data.error || 'Failed to load data');
        }
    } catch (error) {
        console.error('Error:', error);
        document.getElementById('loading').innerHTML = '❌ Error: ' + error.message;
    }
}
window.addEventListener('DOMContentLoaded', loadFileSchemas);
</script></body></html>`);
});

app.listen(PORT, () => {
    console.log("🏛️ ETL Multi-Custodian System running at http://localhost:" + PORT);
    console.log("✅ Supporting 4 custodians: HDFC, AXIS, KOTAK, ORBIS");
    console.log("📊 Total custody records: 18,471+");
});

const { MongoClient } = require('mongodb');
const config = require('./config');

const app = express();
const PORT = 3001;
const mongoUri = config.mongodb.uri + config.mongodb.database;

app.use(express.json());

// API to get custody file schemas (now properly organized)
app.get('/api/custody/file-schemas', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        // All 4 custody holdings from different custodians
        const custodyFileTypes = ['hdfc', 'axis', 'kotak', 'orbis'];
        const fileSchemas = [];
        const date = '2025-06-30';
        
        for (const fileType of custodyFileTypes) {
            try {
                // Use new hierarchical structure: custody_files.date.fileType
                const collectionName = `custody_files.${date}.${fileType}`;
                const collection = db.collection(collectionName);
                const sampleDoc = await collection.findOne({});
                const count = await collection.countDocuments({});
                
                if (sampleDoc) {
                    const columns = Object.keys(sampleDoc).filter(key => 
                        !['_id', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt', '__v'].includes(key)
                    );
                    
                    fileSchemas.push({
                        name: fileType,
                        displayName: `${fileType.toUpperCase()} Custody Holdings`,
                        collectionName: collectionName,
                        columns: columns,
                        columnCount: columns.length,
                        recordCount: count,
                        lastUpdated: sampleDoc.uploadedAt || new Date().toISOString(),
                        fileName: sampleDoc.fileName || `${fileType}_custody_file`,
                        custodian: fileType.toUpperCase()
                    });
                }
            } catch (error) {
                console.log(`Error processing ${fileType}:`, error.message);
            }
        }
        
        await client.close();
        
        console.log(`✅ Found ${fileSchemas.length} custody holdings:`, 
                   fileSchemas.map(f => `${f.custodian} (${f.recordCount} records)`));
        
        res.json({
            success: true,
            date: date,
            custodianCount: fileSchemas.length,
            files: fileSchemas,
            totalRecords: fileSchemas.reduce((sum, f) => sum + f.recordCount, 0)
        });
        
    } catch (error) {
        console.error('Error fetching custody file schemas:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// API to get available dates for custody files
app.get('/api/custody/dates', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const collections = await db.listCollections().toArray();
        const custodyDates = new Set();
        
        collections.forEach(col => {
            if (col.name.startsWith('custody_files.')) {
                const parts = col.name.split('.');
                if (parts.length >= 2) {
                    custodyDates.add(parts[1]); // Extract date part
                }
            }
        });
        
        await client.close();
        
        res.json({
            success: true,
            dates: Array.from(custodyDates).sort()
        });
        
    } catch (error) {
        console.error('Error fetching custody dates:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Main interface
app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html>
<html><head><title>ETL Visual Grid Mapping - All Custody Holdings</title><style>
body{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}
.container{max-width:1400px;margin:0 auto;background:white;padding:20px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}
.header{text-align:center;margin-bottom:30px}
.title{color:#1890ff;font-size:32px;margin-bottom:10px}
.subtitle{color:#666;font-size:18px;margin-bottom:10px}
.files-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(350px,1fr));gap:20px;margin-top:20px}
.file-card{border:1px solid #ddd;border-radius:8px;padding:20px;background:#fafafa;position:relative}
.file-title{font-size:20px;font-weight:bold;color:#1890ff;margin-bottom:15px}
.file-info{color:#666;margin-bottom:15px;line-height:1.6}
.columns-list{max-height:200px;overflow-y:auto;border:1px solid #eee;padding:10px;border-radius:4px;background:white}
.column-item{padding:5px;border-bottom:1px solid #f0f0f0;font-family:monospace;font-size:12px}
.loading{text-align:center;padding:50px;color:#666}
.custody-badge{background:#52c41a;color:white;padding:4px 12px;border-radius:12px;font-size:12px;margin-left:10px}
.custodian-badge{background:#722ed1;color:white;padding:2px 8px;border-radius:8px;font-size:11px;position:absolute;top:15px;right:15px}
.date-info{background:#f0f9ff;padding:15px;border-radius:6px;margin:20px 0;border-left:4px solid #1890ff}
.summary-card{background:#f6ffed;border:1px solid #b7eb8f;border-radius:6px;padding:20px;margin-top:25px}
.stats-row{display:flex;justify-content:space-between;margin:10px 0;font-weight:500}
</style></head><body>
<div class="container">
    <div class="header">
        <h1 class="title">🏦 ETL Visual Grid Mapping System</h1>
        <p class="subtitle">Map columns from multiple custodian holdings to unified schema</p>
        <div class="date-info">
            <strong>📅 Processing Date:</strong> <span id="processing-date">Loading...</span> | 
            <strong>🏛️ Custodians:</strong> <span class="custody-badge">ALL CUSTODY HOLDINGS</span>
        </div>
    </div>
    <div id="loading" class="loading">Loading custody holdings from all custodians...</div>
    <div id="files-container" style="display:none">
        <h2>🏛️ Available Custody Holdings <span class="custody-badge">4 Custodians</span></h2>
        <div id="files-grid" class="files-grid"></div>
        <div style="margin-top:40px;padding:25px;background:#f0f9ff;border-radius:8px;">
            <h3>📝 Multi-Custodian ETL Mapping:</h3>
            <p>✅ <strong>4 Custodians:</strong> HDFC, AXIS, KOTAK, ORBIS - all properly organized</p>
            <p>🔄 <strong>Ready to map:</strong> Create unified schema from different custodian formats</p>
            <p>🎯 <strong>Structure:</strong> <code>custody_files.date.custodian</code></p>
            <p>💡 <strong>Next:</strong> Define mappings between different custodian column formats</p>
        </div>
    </div>
</div>
<script>
async function loadFileSchemas() {
    try {
        const response = await fetch('/api/custody/file-schemas');
        const data = await response.json();
        
        if (data.success) {
            document.getElementById('processing-date').textContent = data.date;
            document.getElementById('loading').style.display = 'none';
            document.getElementById('files-container').style.display = 'block';
            
            const grid = document.getElementById('files-grid');
            grid.innerHTML = '';
            
            data.files.forEach(file => {
                const card = document.createElement('div');
                card.className = 'file-card';
                card.innerHTML = 
                    '<div class="custodian-badge">' + file.custodian + '</div>' +
                    '<div class="file-title">🏛️ ' + file.displayName + '</div>' +
                    '<div class="file-info">' +
                        '📋 Collection: <code>' + file.collectionName + '</code><br>' +
                        '📄 File: ' + file.fileName + '<br>' +
                        '📊 ' + file.columnCount + ' columns<br>' +
                        '📈 <strong>' + file.recordCount.toLocaleString() + '</strong> records<br>' +
                        '🕒 Updated: ' + new Date(file.lastUpdated).toLocaleDateString() +
                    '</div>' +
                    '<div class="columns-list">' + 
                        file.columns.map(col => '<div class="column-item">📄 ' + col + '</div>').join('') + 
                    '</div>';
                grid.appendChild(card);
            });
            
            // Show detailed summary
            const summary = document.createElement('div');
            summary.className = 'summary-card';
            summary.innerHTML = 
                '<h3>📊 Multi-Custodian Holdings Summary</h3>' +
                '<div class="stats-row">' +
                    '<span>🏛️ Total Custodians:</span>' +
                    '<span><strong>' + data.custodianCount + '</strong></span>' +
                '</div>' +
                '<div class="stats-row">' +
                    '<span>📈 Total Records:</span>' +
                    '<span><strong>' + data.totalRecords.toLocaleString() + '</strong></span>' +
                '</div>' +
                data.files.map(f => 
                    '<div class="stats-row">' +
                        '<span>🏛️ ' + f.custodian + ':</span>' +
                        '<span>' + f.recordCount.toLocaleString() + ' records (' + f.columnCount + ' cols)</span>' +
                    '</div>'
                ).join('') +
                '<hr style="margin:15px 0;">' +
                '<p><strong>🎯 Ready for ETL mapping across all custodians!</strong></p>';
            grid.appendChild(summary);
        } else {
            throw new Error(data.error || 'Failed to load data');
        }
    } catch (error) {
        console.error('Error:', error);
        document.getElementById('loading').innerHTML = '❌ Error: ' + error.message;
    }
}
window.addEventListener('DOMContentLoaded', loadFileSchemas);
</script></body></html>`);
});

app.listen(PORT, () => {
    console.log("🏛️ ETL Multi-Custodian System running at http://localhost:" + PORT);
    console.log("✅ Supporting 4 custodians: HDFC, AXIS, KOTAK, ORBIS");
    console.log("📊 Total custody records: 18,471+");
});
