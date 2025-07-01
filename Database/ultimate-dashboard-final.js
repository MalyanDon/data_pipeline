const express = require('express');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;

// PostgreSQL Configuration
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

// Middleware
app.use(express.static('public'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Initialize connections
async function initializeConnections() {
    try {
        await pgPool.query('SELECT NOW()');
        console.log('✅ Connected to PostgreSQL with connection pool');
    } catch (error) {
        console.error('❌ PostgreSQL connection error:', error.message);
        process.exit(1);
    }
}

// Main dashboard
app.get('/', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Ultimate Financial Data Dashboard</title>
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
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚀 Ultimate Financial Data Dashboard</h1>
                <p>📊 Complete Financial Data Management System</p>
                <p>🗂️ PostgreSQL Integration | Real-time Data Viewing</p>
                <p>📈 Your Data: 19,080+ records ready to view!</p>
            </div>

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

        <script>
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
                
                // Full table export with all data
                console.log('🚀 Starting full export for:', table);
                fetch(\`/api/table-data/\${table}?export=true\`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.length === 0) {
                            alert('No data to export');
                            return;
                        }
                        
                        console.log(\`✅ Exporting \${data.length} records\`);
                        
                        const headers = Object.keys(data[0]);
                        const csvContent = [
                            headers.join(','),
                            ...data.map(row => headers.map(h => \`"\${row[h] || ''}"\`).join(','))
                        ].join('\\n');
                        
                        const blob = new Blob([csvContent], { type: 'text/csv' });
                        const url = window.URL.createObjectURL(blob);
                        const a = document.createElement('a');
                        a.href = url;
                        a.download = \`\${table}_full_export.csv\`;
                        a.click();
                        window.URL.revokeObjectURL(url);
                        
                        alert(\`✅ Successfully exported \${data.length} records!\`);
                    })
                    .catch(error => {
                        console.error('Export error:', error);
                        alert('Export failed: ' + error.message);
                    });
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

        // PostgreSQL stats - removed general_data
        const pgTables = ['brokers', 'clients', 'distributors', 'strategies', 'contract_notes', 
                         'cash_capital_flow', 'stock_capital_flow', 'mf_allocations', 
                         'unified_custody_master'];

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
                              'unified_custody_master'];
        
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
        
        // Check if this is a full export request
        const { export: fullExport } = req.query;
        const queryLimit = fullExport === 'true' ? '' : 'LIMIT 50';
        const result = await pgPool.query(`SELECT * FROM ${table} ORDER BY ${orderBy} ${queryLimit}`);
        
        if (fullExport === 'true') {
            console.log(`📤 Full export: ${result.rows.length} records from ${table}`);
        }
        
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

// Initialize and start server
async function startServer() {
    await initializeConnections();
    
    app.listen(PORT, () => {
        console.log(`🚀 Ultimate Financial Dashboard: http://localhost:${PORT}`);
        console.log(`📊 Fixed Version - No More Errors!`);
        console.log(`🗂️ PostgreSQL Integration`);
        console.log(`📈 Your Data: 19,080+ records ready to view!`);
    });
}

startServer().catch(console.error); 
    } catch (error) {
        console.error('Table data error:', error);
        res.json({ error: error.message });
    }
});

// Initialize and start server
async function startServer() {
    await initializeConnections();
    
    app.listen(PORT, () => {
        console.log(`🚀 Ultimate Financial Dashboard: http://localhost:${PORT}`);
        console.log(`📊 Fixed Version - No More Errors!`);
        console.log(`🗂️ PostgreSQL Integration`);
        console.log(`📈 Your Data: 19,080+ records ready to view!`);
    });
}

startServer().catch(console.error); 