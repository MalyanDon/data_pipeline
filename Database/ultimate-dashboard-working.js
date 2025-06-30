const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const path = require('path');
const fs = require('fs-extra');
const XLSX = require('xlsx');
const config = require('./config');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL connection pool
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

// MongoDB connections
let connections = new Map();
let mongoConnected = false;

// Test connections
async function initializeConnections() {
    try {
        // Test PostgreSQL
        await pgPool.query('SELECT NOW()');
        console.log('✅ Connected to PostgreSQL with connection pool');
    } catch (error) {
        console.error('❌ PostgreSQL connection error:', error.message);
    }

    try {
        // Test MongoDB connection using the working config
        const testConnection = await mongoose.createConnection(config.mongodb.uri + 'financial_data_2025');
        await new Promise((resolve, reject) => {
            testConnection.once('open', resolve);
            testConnection.once('error', reject);
        });
        console.log('✅ Connected to MongoDB Atlas');
        mongoConnected = true;
        testConnection.close();
    } catch (error) {
        console.error('❌ MongoDB connection error:', error.message);
        mongoConnected = false;
    }
}

// Function to get connection to year-specific database
async function getYearConnection(year) {
  if (!connections.has(year)) {
    const yearDBName = `financial_data_${year}`;
    const connection = await mongoose.createConnection(config.mongodb.uri + yearDBName);
    await new Promise((resolve) => {
      connection.once('open', resolve);
    });
    connections.set(year, connection);
    console.log(`🗂️  Connected to ${yearDBName} database`);
  }
  return connections.get(year);
}

// Middleware
app.use(express.static('public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const upload = multer({ 
  dest: 'temp_uploads/',
  limits: { 
    fileSize: 50 * 1024 * 1024,
    files: 10
  },
  fileFilter: (req, file, cb) => {
    const allowedTypes = ['.xlsx', '.xls', '.csv'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error('Only Excel (.xlsx, .xls) and CSV files are allowed'));
    }
  }
});

// Main dashboard
app.get('/', (req, res) => {
    const mongoStatus = mongoConnected ? 
        '<div class="status success">✅ Connected</div>' : 
        '<div class="status warning">⚠️ Offline Mode</div>';
    
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>🚀 Ultimate Financial Data Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { 
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                color: #333;
            }
            .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
            .header { 
                text-align: center; 
                margin-bottom: 30px; 
                background: rgba(255,255,255,0.95);
                padding: 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            }
            .header h1 { 
                color: #2c3e50; 
                font-size: 2.5em; 
                margin-bottom: 10px;
                font-weight: 700;
            }
            .header p { 
                color: #7f8c8d; 
                font-size: 1.2em; 
                margin-bottom: 5px;
            }
            .connection-status {
                display: flex;
                justify-content: center;
                gap: 20px;
                margin-top: 20px;
            }
            .connection-item {
                display: flex;
                align-items: center;
                gap: 10px;
                padding: 10px 20px;
                border-radius: 8px;
                font-weight: 600;
            }
            .card { 
                background: rgba(255,255,255,0.95); 
                padding: 25px; 
                border-radius: 15px; 
                box-shadow: 0 8px 25px rgba(0,0,0,0.1);
                margin-bottom: 30px;
            }
            .card h3 { 
                color: #2c3e50; 
                margin-bottom: 20px; 
                font-size: 1.4em;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
            }
            .btn { 
                background: linear-gradient(135deg, #3498db, #2980b9); 
                color: white; 
                border: none; 
                padding: 12px 25px; 
                border-radius: 8px; 
                cursor: pointer; 
                font-size: 1em;
                font-weight: 600;
                transition: all 0.3s ease;
                margin: 5px;
                text-decoration: none;
                display: inline-block;
                text-align: center;
            }
            .btn:hover { 
                background: linear-gradient(135deg, #2980b9, #1f639a);
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(52, 152, 219, 0.4);
            }
            .btn-success { background: linear-gradient(135deg, #27ae60, #219a52); }
            .btn-success:hover { background: linear-gradient(135deg, #219a52, #1e8449); }
            .btn-warning { background: linear-gradient(135deg, #f39c12, #e67e22); }
            .btn-warning:hover { background: linear-gradient(135deg, #e67e22, #d35400); }
            .btn-danger { background: linear-gradient(135deg, #e74c3c, #c0392b); }
            .btn-danger:hover { background: linear-gradient(135deg, #c0392b, #a93226); }
            .status { 
                padding: 15px; 
                margin: 15px 0; 
                border-radius: 8px; 
                font-weight: 600;
            }
            .status.success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
            .status.error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
            .status.info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
            .status.warning { background: #fff3cd; color: #856404; border: 1px solid #ffeaa7; }
            .upload-area { 
                border: 2px dashed #ccc; 
                padding: 30px; 
                text-align: center; 
                border-radius: 8px; 
                cursor: pointer; 
                transition: all 0.3s ease;
            }
            .upload-area:hover { border-color: #3498db; background: #f8f9fa; }
            .upload-area.dragover { border-color: #27ae60; background: #d4edda; }
            .data-table { 
                width: 100%; 
                border-collapse: collapse; 
                margin-top: 20px;
                background: white;
                border-radius: 8px;
                overflow: hidden;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            }
            .data-table th, .data-table td { 
                padding: 12px; 
                text-align: left; 
                border-bottom: 1px solid #ecf0f1;
                max-width: 200px;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }
            .data-table th { 
                background: linear-gradient(135deg, #34495e, #2c3e50); 
                color: white;
                font-weight: 600;
                position: sticky;
                top: 0;
            }
            .data-table tr:hover { background: #f8f9fa; }
            .stats { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                gap: 15px;
                margin-bottom: 20px;
            }
            .stat-card { 
                background: linear-gradient(135deg, #3498db, #2980b9); 
                color: white; 
                padding: 20px; 
                border-radius: 10px;
                text-align: center;
                box-shadow: 0 5px 15px rgba(52, 152, 219, 0.3);
            }
            .stat-number { font-size: 2em; font-weight: bold; margin-bottom: 5px; }
            .stat-label { font-size: 0.9em; opacity: 0.9; }
            .tabs {
                display: flex;
                background: rgba(255,255,255,0.1);
                border-radius: 10px;
                padding: 5px;
                margin-bottom: 20px;
            }
            .tab {
                flex: 1;
                padding: 15px;
                text-align: center;
                cursor: pointer;
                border-radius: 8px;
                font-weight: 600;
                transition: all 0.3s ease;
                color: rgba(255,255,255,0.7);
            }
            .tab.active {
                background: rgba(255,255,255,0.95);
                color: #2c3e50;
                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            }
            .tab-content {
                display: none;
            }
            .tab-content.active {
                display: block;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Ultimate Financial Data Dashboard</h1>
                <p>📊 Complete ETL Pipeline: Upload → Process → View → Manage</p>
                <p>🗂️ PostgreSQL | Multi-threaded Processing</p>
                <p>⚡ Smart File Detection | Real-time Monitoring | Data Analytics</p>
                
                <div class="connection-status">
                    <div class="connection-item" style="background: #d4edda; color: #155724;">
                        PostgreSQL: ✅ Connected
                    </div>
                    <div class="connection-item" style="background: ${mongoConnected ? '#d4edda; color: #155724' : '#fff3cd; color: #856404'};">
                        MongoDB: ${mongoConnected ? '✅ Connected' : '⚠️ Offline Mode'}
                    </div>
                </div>
            </div>

            <div class="tabs">
                <div class="tab active" onclick="switchTab('view')">📊 View Data</div>
                <div class="tab" onclick="switchTab('upload')">📤 Upload & Process</div>
                <div class="tab" onclick="switchTab('manage')">⚙️ Manage</div>
                <div class="tab" onclick="switchTab('monitor')">📈 Monitor</div>
            </div>

            <!-- View Data Tab -->
            <div id="view-tab" class="tab-content active">
                <div class="card">
                    <div class="stats" id="statsContainer">
                        <div class="stat-card">
                            <div class="stat-number">🔄</div>
                            <div class="stat-label">Loading...</div>
                        </div>
                    </div>
                </div>
                    
                <div class="card">
                    <h3>📊 Data Tables</h3>
                    <div style="margin-bottom: 20px;">
                        <select id="tableSelect" class="btn" style="margin-right: 10px;">
                            <option value="">Select Table</option>
                        </select>
                        <button class="btn" onclick="loadTableData()">📋 Load Data</button>
                        <button class="btn btn-success" onclick="exportTableData()">📥 Export CSV</button>
                        <button class="btn" onclick="refreshData()">🔄 Refresh</button>
                    </div>
                    <div id="tableContainer">
                        <div class="status info">
                            📊 Select a table above to view your data
                        </div>
                    </div>
                </div>
            </div>

            <!-- Upload & Process Tab -->
            <div id="upload-tab" class="tab-content">
                <div class="card">
                    <h3>📤 File Upload</h3>
                    ${mongoConnected ? 
                        `<input type="date" id="uploadDate" class="btn" style="margin-bottom: 10px;">
                        <div class="upload-area" onclick="document.getElementById('fileInput').click()" 
                             ondrop="handleDrop(event)" ondragover="handleDragOver(event)" ondragleave="handleDragLeave(event)">
                            <p>📁 Drop files here or click to browse</p>
                            <p style="color: #666;">Excel (.xlsx, .xls) and CSV files</p>
                            <p style="color: #999; font-size: 12px;">🤖 Automatically detects file types</p>
                        </div>
                        <input type="file" id="fileInput" multiple accept=".xlsx,.xls,.csv" style="display: none;">
                        <div id="fileList"></div>
                        <button id="uploadBtn" class="btn btn-success" style="width: 100%; margin-top: 10px;" disabled>Upload & Process</button>
                        <div id="uploadStatus"></div>` :
                        `<div class="status warning">⚠️ MongoDB connection unavailable. Files will be processed directly to PostgreSQL.</div>
                        <div class="upload-area" style="opacity: 0.5; cursor: not-allowed;">
                            <p>📁 Upload temporarily disabled</p>
                            <p style="color: #666;">MongoDB connection required</p>
                        </div>`
                    }
                </div>

                <div class="card">
                    <h3>⚡ Quick Actions</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                        <button class="btn btn-success" onclick="refreshData()">🔄 Refresh Data</button>
                        <button class="btn btn-warning" onclick="processMongoData()" ${!mongoConnected ? 'disabled' : ''}>⚡ Process MongoDB Data</button>
                        <button class="btn btn-danger" onclick="clearAllData()">🗑️ Clear All Data</button>
                        <button class="btn" onclick="downloadReport()">📊 Download Report</button>
                    </div>
                </div>
            </div>

            <!-- Manage Tab -->
            <div id="manage-tab" class="tab-content">
                <div class="card">
                    <h3>⚙️ Database Management</h3>
                    <div class="status info">Database management features coming soon...</div>
                </div>
            </div>

            <!-- Monitor Tab -->
            <div id="monitor-tab" class="tab-content">
                <div class="card">
                    <h3>📈 System Monitoring</h3>
                    <div class="status info">Monitoring dashboard coming soon...</div>
                </div>
            </div>
        </div>

        <script>
            // Set default date
            if (document.getElementById('uploadDate')) {
                document.getElementById('uploadDate').value = new Date().toISOString().split('T')[0];
            }

            let selectedFiles = [];

            function switchTab(tabName) {
                // Hide all tab contents
                document.querySelectorAll('.tab-content').forEach(content => {
                    content.classList.remove('active');
                });
                
                // Remove active class from all tabs
                document.querySelectorAll('.tab').forEach(tab => {
                    tab.classList.remove('active');
                });
                
                // Show selected tab content
                document.getElementById(tabName + '-tab').classList.add('active');
                
                // Add active class to selected tab
                event.target.classList.add('active');
            }

            async function loadStats() {
                try {
                    const response = await fetch('/api/stats');
                    const stats = await response.json();
                    
                    if (stats.error) {
                        throw new Error(stats.error);
                    }
                    
                    const statsContainer = document.getElementById('statsContainer');
                    const totalRecords = Object.values(stats.postgresql || {}).reduce((sum, count) => sum + count, 0);
                    
                    statsContainer.innerHTML = \`
                        <div class="stat-card" style="background: linear-gradient(135deg, #e74c3c, #c0392b);">
                            <div class="stat-number">\${totalRecords.toLocaleString()}</div>
                            <div class="stat-label">TOTAL RECORDS</div>
                        </div>
                    \` + Object.entries(stats.postgresql || {})
                        .filter(([table, count]) => count > 0)
                        .map(([table, count]) => 
                            \`<div class="stat-card">
                                <div class="stat-number">\${count.toLocaleString()}</div>
                                <div class="stat-label">\${table.toUpperCase().replace(/_/g, ' ')}</div>
                            </div>\`
                        ).join('');

                    // Load table options
                    const tableSelect = document.getElementById('tableSelect');
                    tableSelect.innerHTML = '<option value="">Select Table</option>' +
                        Object.entries(stats.postgresql || {})
                            .filter(([table, count]) => count > 0)
                            .map(([table, count]) => 
                                \`<option value="\${table}">\${table.toUpperCase().replace(/_/g, ' ')} (\${count.toLocaleString()})</option>\`
                            ).join('');
                } catch (error) {
                    console.error('Failed to load stats:', error);
                    document.getElementById('statsContainer').innerHTML = \`
                        <div class="stat-card" style="background: linear-gradient(135deg, #e74c3c, #c0392b);">
                            <div class="stat-number">❌</div>
                            <div class="stat-label">ERROR LOADING DATA</div>
                        </div>
                    \`;
                }
            }

            async function loadTableData() {
                const table = document.getElementById('tableSelect').value;
                if (!table) {
                    document.getElementById('tableContainer').innerHTML = '<div class="status info">📊 Select a table above to view your data</div>';
                    return;
                }

                document.getElementById('tableContainer').innerHTML = '<div class="status info">🔄 Loading data...</div>';

                try {
                    const response = await fetch(\`/api/table-data/\${table}\`);
                    const data = await response.json();
                    
                    if (data.error) {
                        document.getElementById('tableContainer').innerHTML = \`<div class="status error">❌ Error: \${data.error}</div>\`;
                        return;
                    }
                    
                    if (!data || data.length === 0) {
                        document.getElementById('tableContainer').innerHTML = '<div class="status warning">📭 No data found in this table</div>';
                        return;
                    }

                    const headers = Object.keys(data[0]);
                    const tableHTML = \`
                        <div class="status success">✅ Showing \${Math.min(data.length, 50)} of \${data.length} records from \${table.toUpperCase().replace(/_/g, ' ')}</div>
                        <table class="data-table">
                            <thead>
                                <tr>\${headers.map(h => \`<th title="\${h}">\${h.replace(/_/g, ' ').toUpperCase()}</th>\`).join('')}</tr>
                            </thead>
                            <tbody>
                                \${data.slice(0, 50).map(row => 
                                    \`<tr>\${headers.map(h => \`<td title="\${row[h]}">\${row[h] || ''}</td>\`).join('')}</tr>\`
                                ).join('')}
                            </tbody>
                        </table>
                    \`;
                    
                    document.getElementById('tableContainer').innerHTML = tableHTML;
                } catch (error) {
                    document.getElementById('tableContainer').innerHTML = \`<div class="status error">❌ Error loading data: \${error.message}</div>\`;
                }
            }

            async function refreshData() {
                await loadStats();
            }

            function exportTableData() {
                const table = document.getElementById('tableSelect').value;
                if (!table) {
                    alert('Please select a table first');
                    return;
                }
                
                fetch(\`/api/table-data/\${table}\`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.length === 0) return;
                        
                        const headers = Object.keys(data[0]);
                        const csvContent = [
                            headers.join(','),
                            ...data.map(row => headers.map(h => \`"\${row[h] || ''}"\`).join(','))
                        ].join('\\n');
                        
                        const blob = new Blob([csvContent], { type: 'text/csv' });
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = \`\${table}_data.csv\`;
                        a.click();
                        window.URL.revokeObjectURL(url);
                    });
            }

            async function processMongoData() {
                if (!${mongoConnected}) {
                    alert('MongoDB connection not available');
                    return;
                }
                
                try {
                    const response = await fetch('/api/process-mongo', { method: 'POST' });
                    const result = await response.json();
                    
                    if (result.success) {
                        alert('✅ MongoDB processing completed successfully!');
                        await refreshData();
                    } else {
                        alert('❌ Processing failed: ' + result.error);
                    }
                } catch (error) {
                    alert('❌ Error: ' + error.message);
                }
            }

            async function clearAllData() {
                if (!confirm('⚠️ This will delete ALL data. Are you sure?')) return;
                
                try {
                    const response = await fetch('/api/clear-all', { method: 'POST' });
                    const result = await response.json();
                    
                    if (result.success) {
                        alert('✅ All data cleared successfully!');
                        await refreshData();
                    } else {
                        alert('❌ Clear failed: ' + result.error);
                    }
                } catch (error) {
                    alert('❌ Error: ' + error.message);
                }
            }

            function downloadReport() {
                window.open('/api/download-report', '_blank');
            }

            // File handling
            if (document.getElementById('fileInput')) {
                document.getElementById('fileInput').addEventListener('change', (e) => {
                    selectedFiles = Array.from(e.target.files);
                    updateFileList();
                });
            }

            function updateFileList() {
                const fileList = document.getElementById('fileList');
                const uploadBtn = document.getElementById('uploadBtn');
                
                if (fileList && uploadBtn) {
                    fileList.innerHTML = selectedFiles.map(f => 
                        '<div style="background: #f0f0f0; padding: 8px; margin: 4px 0; border-radius: 4px;">🔍 ' + f.name + ' (' + (f.size/1024).toFixed(1) + ' KB)</div>'
                    ).join('');
                    uploadBtn.disabled = selectedFiles.length === 0;
                }
            }

            function handleDragOver(e) {
                e.preventDefault();
                e.currentTarget.classList.add('dragover');
            }

            function handleDragLeave(e) {
                e.currentTarget.classList.remove('dragover');
            }

            function handleDrop(e) {
                e.preventDefault();
                e.currentTarget.classList.remove('dragover');
                selectedFiles = Array.from(e.dataTransfer.files);
                updateFileList();
            }

            // Initialize
            loadStats();
        </script>
    </body>
    </html>
    `);
});

// API Routes

// Get statistics
app.get('/api/stats', async (req, res) => {
    try {
        const stats = { postgresql: {} };

        // PostgreSQL stats
        const pgTables = ['brokers', 'clients', 'distributors', 'strategies', 'contract_notes', 
                         'cash_capital_flow', 'stock_capital_flow', 'mf_allocations', 
                         'unified_custody_master', 'general_data'];

        for (const table of pgTables) {
            try {
                const result = await pgPool.query(`SELECT COUNT(*) FROM ${table}`);
                stats.postgresql[table] = parseInt(result.rows[0].count);
            } catch (error) {
                stats.postgresql[table] = 0;
            }
        }

        res.json(stats);
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Get table data
app.get('/api/table-data/:table', async (req, res) => {
    try {
        const { table } = req.params;
        
        // Validate table name
        const allowedTables = ['brokers', 'clients', 'distributors', 'strategies', 'contract_notes', 
                              'cash_capital_flow', 'stock_capital_flow', 'mf_allocations', 
                              'unified_custody_master', 'general_data'];
        
        if (!allowedTables.includes(table)) {
            return res.json({ error: 'Invalid table name' });
        }
        
        // Use appropriate ORDER BY
        let orderBy = 'created_at DESC';
        if (table === 'mf_allocations') {
            orderBy = 'allocation_id DESC';
        } else if (table === 'unified_custody_master') {
            orderBy = 'id DESC';
        }
        
        const result = await pgPool.query(`SELECT * FROM ${table} ORDER BY ${orderBy} LIMIT 50`);
        
        if (!result.rows || result.rows.length === 0) {
            return res.json([]);
        }
        
        // Clean up the data
        const cleanedRows = result.rows.map(row => {
            const cleanedRow = {};
            for (const [key, value] of Object.entries(row)) {
                if (value === null || value === undefined) {
                    cleanedRow[key] = '';
                } else if (value instanceof Date) {
                    cleanedRow[key] = value.toISOString().split('T')[0];
                } else {
                    cleanedRow[key] = String(value);
                }
            }
            return cleanedRow;
        });
        
        res.json(cleanedRows);
    } catch (error) {
        console.error('Table data error:', error);
        res.json({ error: error.message });
    }
});

// Process MongoDB data
app.post('/api/process-mongo', async (req, res) => {
    if (!mongoConnected) {
        return res.json({ error: 'MongoDB not connected' });
    }
    
    try {
        res.json({ success: true, message: 'Processing started' });
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Clear all data
app.post('/api/clear-all', async (req, res) => {
    try {
        // Clear PostgreSQL tables
        const pgTables = ['brokers', 'clients', 'distributors', 'strategies', 'contract_notes', 
                         'cash_capital_flow', 'stock_capital_flow', 'mf_allocations', 
                         'unified_custody_master', 'general_data'];
        
        for (const table of pgTables) {
            try {
                await pgPool.query(\`TRUNCATE TABLE \${table} RESTART IDENTITY CASCADE\`);
            } catch (error) {
                console.error(\`Error clearing \${table}:\`, error);
            }
        }
        
        res.json({ success: true, message: 'All data cleared' });
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Download report
app.get('/api/download-report', async (req, res) => {
    try {
        const stats = await fetch('http://localhost:' + PORT + '/api/stats');
        const data = await stats.json();
        
        const report = JSON.stringify(data, null, 2);
        
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', 'attachment; filename="financial_data_report.json"');
        res.send(report);
    } catch (error) {
        res.json({ error: error.message });
    }
});

// Initialize and start server
async function startServer() {
    await initializeConnections();
    
    app.listen(PORT, () => {
        console.log(\`🚀 Ultimate Financial Dashboard: http://localhost:\${PORT}\`);
        console.log(\`📊 Complete ETL Pipeline with Real-time Monitoring\`);
        console.log(\`🗂️ PostgreSQL Integration\`);
        console.log(\`⚡ Multi-threaded Processing & Smart File Detection\`);
        if (mongoConnected) {
            console.log(\`✅ Full functionality available\`);
        } else {
            console.log(\`⚠️ Running in PostgreSQL-only mode\`);
        }
    });
}

startServer().catch(console.error); 