const express = require('express');
const { Client } = require('pg');

const app = express();
const PORT = 3005;

// Simple PostgreSQL connection
async function getPostgreSQLData() {
  const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: '',
    port: 5432,
  });

  await client.connect();
  
  const tables = [
    'unified_custody_master',
    'general_data', 
    'clients',
    'brokers',
    'distributors',
    'strategies',
    'contract_notes',
    'cash_capital_flow',
    'stock_capital_flow',
    'mf_allocations'
  ];

  const result = {};
  
  for (const tableName of tables) {
    try {
      const countResult = await client.query(`SELECT COUNT(*) as count FROM ${tableName}`);
      const recordCount = parseInt(countResult.rows[0].count);
      
      if (recordCount > 0) {
        let sampleQuery = `SELECT * FROM ${tableName} LIMIT 20`;
        if (tableName === 'general_data') {
          sampleQuery = `SELECT id, data_type, source_file, created_at FROM ${tableName} LIMIT 20`;
        }
        
        const sampleResult = await client.query(sampleQuery);
        
        result[tableName] = {
          record_count: recordCount,
          sample_data: sampleResult.rows,
          columns: Object.keys(sampleResult.rows[0] || {})
        };
      }
    } catch (error) {
      console.log(`Error with ${tableName}:`, error.message);
    }
  }
  
  await client.end();
  return result;
}

// API endpoint
app.get('/api/data', async (req, res) => {
  try {
    console.log('🔍 Fetching PostgreSQL data...');
    const data = await getPostgreSQLData();
    console.log(`✅ Found data in ${Object.keys(data).length} tables`);
    res.json({ success: true, data });
  } catch (error) {
    console.log('❌ Error:', error.message);
    res.json({ success: false, error: error.message });
  }
});

// Main page
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>PostgreSQL Data Viewer</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #4CAF50; color: white; padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 20px; }
        .table-card { background: white; margin: 20px 0; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .table-header { background: #e8f5e8; padding: 15px; border-radius: 5px; margin-bottom: 15px; }
        .sample-data { background: #f9f9f9; padding: 10px; border-radius: 5px; margin: 10px 0; }
        .btn { background: #4CAF50; color: white; border: none; padding: 15px 30px; border-radius: 5px; cursor: pointer; font-size: 16px; }
        .btn:hover { background: #45a049; }
        .loading { color: #ff9800; font-size: 18px; text-align: center; padding: 20px; }
        .error { background: #f8d7da; color: #721c24; padding: 15px; border-radius: 5px; margin: 10px 0; }
        .success { background: #d4edda; color: #155724; padding: 15px; border-radius: 5px; margin: 10px 0; }
        table { width: 100%; border-collapse: collapse; margin: 10px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; font-size: 12px; }
        th { background: #f0f0f0; }
        .record-count { font-size: 18px; font-weight: bold; color: #4CAF50; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🗄️ PostgreSQL Data Viewer</h1>
            <p>View all your financial data tables</p>
        </div>
        
        <div style="text-align: center; margin: 30px 0;">
            <button class="btn" onclick="loadData()">🔍 Load PostgreSQL Data</button>
        </div>
        
        <div id="dataContainer">
            <div class="loading">Click the button above to load your data</div>
        </div>
    </div>

    <script>
        async function loadData() {
            const container = document.getElementById('dataContainer');
            container.innerHTML = '<div class="loading">🔍 Loading PostgreSQL data...</div>';
            
            try {
                const response = await fetch('/api/data');
                const result = await response.json();
                
                if (result.success && result.data) {
                    displayData(result.data);
                } else {
                    container.innerHTML = '<div class="error">❌ Error: ' + (result.error || 'No data found') + '</div>';
                }
            } catch (error) {
                container.innerHTML = '<div class="error">❌ Network Error: ' + error.message + '</div>';
            }
        }
        
        function displayData(data) {
            const container = document.getElementById('dataContainer');
            
            if (Object.keys(data).length === 0) {
                container.innerHTML = '<div class="error">❌ No tables found with data</div>';
                return;
            }
            
            let html = '<div class="success">✅ Successfully loaded data from ' + Object.keys(data).length + ' tables!</div>';
            
            let totalRecords = 0;
            Object.values(data).forEach(table => totalRecords += table.record_count);
            
            html += '<div style="background: #e3f2fd; padding: 20px; border-radius: 8px; margin: 20px 0; text-align: center;">';
            html += '<h2>📊 Database Overview</h2>';
            html += '<div class="record-count">Total Records: ' + totalRecords.toLocaleString() + '</div>';
            html += '</div>';
            
            Object.keys(data).sort().forEach(tableName => {
                const tableData = data[tableName];
                const displayName = tableName.replace(/_/g, ' ').replace(/\\b\\w/g, l => l.toUpperCase());
                
                html += '<div class="table-card">';
                html += '<div class="table-header">';
                html += '<h3>🗄️ ' + displayName + '</h3>';
                html += '<div class="record-count">' + tableData.record_count.toLocaleString() + ' records</div>';
                html += '<p><strong>Columns:</strong> ' + tableData.columns.join(', ') + '</p>';
                html += '</div>';
                
                if (tableData.sample_data && tableData.sample_data.length > 0) {
                    html += '<div class="sample-data">';
                    html += '<h4>📋 Sample Data:</h4>';
                    html += '<table>';
                    
                    // Headers (show more columns)
                    html += '<tr>';
                    tableData.columns.slice(0, 8).forEach(col => {
                        html += '<th>' + col.replace(/_/g, ' ') + '</th>';
                    });
                    if (tableData.columns.length > 8) html += '<th>...</th>';
                    html += '</tr>';
                    
                    // Show more rows (up to 15)
                    tableData.sample_data.slice(0, 15).forEach(row => {
                        html += '<tr>';
                        tableData.columns.slice(0, 8).forEach(col => {
                            let value = row[col] || '';
                            if (typeof value === 'string' && value.length > 40) {
                                value = value.substring(0, 40) + '...';
                            }
                            html += '<td>' + value + '</td>';
                        });
                        if (tableData.columns.length > 8) html += '<td>...</td>';
                        html += '</tr>';
                    });
                    
                    if (tableData.sample_data.length > 15) {
                        html += '<tr><td colspan="' + (tableData.columns.length > 8 ? 9 : tableData.columns.length) + '" style="text-align: center; font-style: italic; color: #666;">... and ' + (tableData.sample_data.length - 15) + ' more rows (showing first 15)</td></tr>';
                    }
                    
                    html += '</table>';
                    html += '</div>';
                }
                
                html += '</div>';
            });
            
            container.innerHTML = html;
        }
    </script>
</body>
</html>
  `);
});

app.listen(PORT, () => {
  console.log(`🚀 Simple Data Viewer: http://localhost:${PORT}`);
  console.log('📊 This WILL show your PostgreSQL data!');
});
