const express = require('express');
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3001;

// Use the correct MongoDB URI from config
const config = require('./config');
const mongoUri = config.mongodb.uri + config.mongodb.database;

const pgConfig = {
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: '',
    port: 5432,
};

app.use(express.json());
app.use(express.static('public'));

// API to get custody file schemas for grid headers
app.get('/api/custody/file-schemas', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const fileTypes = ['hdfc', 'axis', 'broker_master', 'contract_note'];
        const fileSchemas = [];
        
        // Use the actual date format from clean-dashboard: fileType_MM_DD
        const month = '06';
        const day = '30';
        
        for (const fileType of fileTypes) {
            try {
                const collectionName = `${fileType}_${month}_${day}`;
                const collection = db.collection(collectionName);
                
                const sampleDoc = await collection.findOne({});
                const count = await collection.countDocuments({});
                
                if (sampleDoc) {
                    const columns = Object.keys(sampleDoc).filter(key => 
                        !['_id', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt', '__v'].includes(key)
                    );
                    
                    fileSchemas.push({
                        name: collectionName,
                        displayName: `${fileType.toUpperCase()} File (${month}/${day})`,
                        columns: columns,
                        columnCount: columns.length,
                        recordCount: count,
                        lastUpdated: new Date().toISOString(),
                        fileType: fileType
                    });
                }
            } catch (error) {
                console.log(`Warning: Could not process ${fileType}:`, error.message);
            }
        }
        
        await client.close();
        res.json({ files: fileSchemas });
    } catch (error) {
        console.error('Error fetching file schemas:', error);
        res.status(500).json({ error: 'Failed to fetch file schemas' });
    }
});

// API to save grid mapping configuration
app.post('/api/mappings/save-grid', async (req, res) => {
    try {
        const { templateName, unifiedTableName, columnMappings } = req.body;
        
        // Save to a JSON file for now (in production, save to database)
        const mappingConfig = {
            templateName,
            unifiedTableName,
            columnMappings,
            createdAt: new Date().toISOString()
        };
        
        fs.writeFileSync('mapping-template.json', JSON.stringify(mappingConfig, null, 2));
        
        res.json({ 
            success: true, 
            message: 'Grid mapping configuration saved successfully',
            templateId: Date.now()
        });
    } catch (error) {
        console.error('Error saving grid mapping:', error);
        res.status(500).json({ error: 'Failed to save grid mapping' });
    }
});

// API to execute grid-based ETL pipeline
app.post('/api/pipeline/execute-grid', async (req, res) => {
    try {
        const { mappingConfig, targetTable, processingDate } = req.body;
        
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const pgPool = new Pool(pgConfig);
        
        // Build unified table schema
        const createTableQuery = buildCreateTableQuery(targetTable, mappingConfig);
        await pgPool.query(createTableQuery);
        
        let totalProcessed = 0;
        let totalErrors = 0;
        
        // Process each file according to mapping configuration
        for (const mapping of mappingConfig) {
            for (const [fileName, fileMapping] of Object.entries(mapping.fileMappings)) {
                if (fileMapping.sourceColumn || fileMapping.customValue) {
                    try {
                        const collection = db.collection(fileName);
                        const documents = await collection.find({}).toArray();
                        
                        for (const record of documents) {
                            try {
                                const insertQuery = buildInsertQuery(targetTable, mapping, record, fileMapping);
                                await pgPool.query(insertQuery.text, insertQuery.values);
                                totalProcessed++;
                            } catch (error) {
                                console.error('Error inserting record:', error);
                                totalErrors++;
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing ${fileName}:`, error);
                        totalErrors++;
                    }
                }
            }
        }
        
        await client.close();
        await pgPool.end();
        
        res.json({
            success: true,
            message: 'ETL pipeline executed successfully',
            statistics: {
                totalProcessed,
                totalErrors,
                successRate: totalProcessed > 0 ? (totalProcessed / (totalProcessed + totalErrors) * 100) : 0
            }
        });
    } catch (error) {
        console.error('Error executing ETL pipeline:', error);
        res.status(500).json({ error: 'Failed to execute ETL pipeline' });
    }
});

// Helper function to build CREATE TABLE query
function buildCreateTableQuery(tableName, mappingConfig) {
    const columns = mappingConfig.map(mapping => {
        const dataType = getPostgreSQLDataType(mapping.unifiedColumnType);
        return `${mapping.unifiedColumnName} ${dataType}`;
    });
    
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        source_file VARCHAR(255),
        processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ${columns.join(',\n        ')}
    )`;
}

// Helper function to build INSERT query
function buildInsertQuery(tableName, mapping, document, fileMapping) {
    let value;
    
    if (fileMapping.sourceColumn && document[fileMapping.sourceColumn] !== undefined) {
        value = document[fileMapping.sourceColumn];
    } else if (fileMapping.customValue) {
        value = fileMapping.customValue;
    } else {
        value = null;
    }
    
    return {
        text: `INSERT INTO ${tableName} (${mapping.unifiedColumnName}, source_file) VALUES ($1, $2)`,
        values: [value, document.fileName || 'unknown']
    };
}

// Helper function to map data types
function getPostgreSQLDataType(type) {
    const typeMap = {
        'VARCHAR': 'VARCHAR(255)',
        'DECIMAL': 'DECIMAL(15,2)',
        'DATE': 'DATE',
        'INTEGER': 'INTEGER',
        'BOOLEAN': 'BOOLEAN'
    };
    return typeMap[type] || 'VARCHAR(255)';
}

// Serve the main React application
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Visual Grid Mapping System</title>
    <script src="https://unpkg.com/react@18/umd/react.development.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/antd@5.12.1/dist/reset.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/antd@5.12.1/dist/antd.min.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        
        .mapping-container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 24px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .page-header {
            margin-bottom: 24px;
            text-align: center;
        }
        
        .page-title {
            font-size: 28px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
        }
        
        .page-subtitle {
            font-size: 16px;
            color: #666;
        }
        
        .mapping-grid {
            display: grid;
            grid-template-columns: 300px repeat(6, 1fr);
            gap: 1px;
            background-color: #e6f7ff;
            border: 2px solid #1890ff;
            border-radius: 8px;
            overflow: hidden;
            margin: 24px 0;
        }
        
        .grid-header {
            display: contents;
        }
        
        .header-cell {
            background: linear-gradient(135deg, #1890ff, #096dd9);
            color: white;
            padding: 16px 12px;
            font-weight: 600;
            text-align: center;
            font-size: 14px;
        }
        
        .unified-column-header {
            background: linear-gradient(135deg, #52c41a, #389e0d);
        }
        
        .grid-row {
            display: contents;
        }
        
        .unified-column-cell {
            background-color: #f6ffed;
            padding: 12px;
            border-right: 1px solid #d9d9d9;
            display: flex;
            flex-direction: column;
            gap: 8px;
            min-height: 120px;
        }
        
        .file-mapping-cell {
            background-color: #ffffff;
            padding: 12px;
            border-right: 1px solid #d9d9d9;
            min-height: 120px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .file-header {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 4px;
        }
        
        .file-title {
            font-weight: 600;
            color: #1890ff;
        }
        
        .file-metadata {
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
            justify-content: center;
        }
        
        .add-column-section {
            grid-column: 1 / -1;
            background-color: #f0f9ff;
            padding: 20px;
            text-align: center;
            border-top: 2px dashed #1890ff;
        }
        
        .control-panel {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 24px;
            padding: 20px;
            background: linear-gradient(135deg, #f0f9ff, #e6f7ff);
            border-radius: 8px;
            border: 1px solid #1890ff;
        }
        
        .status-panel {
            margin-bottom: 24px;
        }
        
        .mapping-status-tag {
            margin-top: 4px;
        }
        
        .dropdown-container {
            position: relative;
        }
        
        .custom-dropdown {
            width: 100%;
            padding: 8px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            background: white;
            cursor: pointer;
        }
        
        .custom-dropdown:hover {
            border-color: #40a9ff;
        }
        
        .custom-dropdown:focus {
            border-color: #1890ff;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
            outline: none;
        }
        
        .dropdown-option {
            padding: 8px 12px;
            cursor: pointer;
            border-bottom: 1px solid #f0f0f0;
        }
        
        .dropdown-option:hover {
            background-color: #f5f5f5;
        }
        
        .dropdown-option.selected {
            background-color: #e6f7ff;
            color: #1890ff;
            font-weight: 500;
        }
        
        .ant-tag {
            margin: 2px;
        }
        
        .success-tag {
            background-color: #f6ffed;
            color: #52c41a;
            border-color: #52c41a;
        }
        
        .warning-tag {
            background-color: #fff7e6;
            color: #fa8c16;
            border-color: #fa8c16;
        }
        
        .error-tag {
            background-color: #fff2f0;
            color: #ff4d4f;
            border-color: #ff4d4f;
        }
        
        .info-tag {
            background-color: #f0f9ff;
            color: #1890ff;
            border-color: #1890ff;
        }
        
        @media (max-width: 1200px) {
            .mapping-grid {
                grid-template-columns: 250px repeat(6, minmax(150px, 1fr));
                overflow-x: auto;
            }
        }
    </style>
</head>
<body>
    <div id="root"></div>

    <script type="text/babel">
        const { useState, useEffect } = React;
        const { Button, Input, Select, Tag, Card, Row, Col, Statistic, Progress, Spin, message, Modal } = antd;
        const { Option, OptGroup } = Select;

        // Main Grid Mapping Component
        const ETLVisualGridMapping = () => {
            const [custodyFiles, setCustodyFiles] = useState([]);
            const [columnMappings, setColumnMappings] = useState([]);
            const [loading, setLoading] = useState(true);
            const [templateName, setTemplateName] = useState('');
            const [executing, setExecuting] = useState(false);

            useEffect(() => {
                fetchFileSchemas();
            }, []);

            const fetchFileSchemas = async () => {
                try {
                    const response = await fetch('/api/custody/file-schemas');
                    const data = await response.json();
                    setCustodyFiles(data.files || []);
                    setLoading(false);
                } catch (error) {
                    console.error('Error fetching file schemas:', error);
                    message.error('Failed to load file schemas');
                    setLoading(false);
                }
            };

            const addNewColumn = () => {
                const newMapping = {
                    id: Date.now().toString(),
                    unifiedColumnName: '',
                    unifiedColumnType: 'VARCHAR',
                    fileMappings: {}
                };
                
                // Initialize file mappings
                custodyFiles.forEach(file => {
                    newMapping.fileMappings[file.name] = {
                        sourceColumn: null,
                        nullHandling: 'NULL',
                        customValue: ''
                    };
                });
                
                setColumnMappings([...columnMappings, newMapping]);
            };

            const updateUnifiedColumnName = (mappingId, name) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? { ...mapping, unifiedColumnName: name }
                            : mapping
                    )
                );
            };

            const updateColumnType = (mappingId, type) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? { ...mapping, unifiedColumnType: type }
                            : mapping
                    )
                );
            };

            const updateFileMapping = (mappingId, fileName, config) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? {
                                ...mapping,
                                fileMappings: {
                                    ...mapping.fileMappings,
                                    [fileName]: config
                                }
                            }
                            : mapping
                    )
                );
            };

            const removeMapping = (mappingId) => {
                setColumnMappings(mappings =>
                    mappings.filter(mapping => mapping.id !== mappingId)
                );
            };

            const saveTemplate = async () => {
                if (!templateName.trim()) {
                    message.error('Please enter a template name');
                    return;
                }

                if (columnMappings.length === 0) {
                    message.error('Please add at least one column mapping');
                    return;
                }

                try {
                    const response = await fetch('/api/mappings/save-grid', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            templateName,
                            unifiedTableName: \`unified_custody_\${new Date().toISOString().split('T')[0].replace(/-/g, '_')}\`,
                            columnMappings
                        })
                    });

                    if (response.ok) {
                        message.success('Template saved successfully!');
                    } else {
                        message.error('Failed to save template');
                    }
                } catch (error) {
                    console.error('Error saving template:', error);
                    message.error('Failed to save template');
                }
            };

            const executeETL = async () => {
                if (columnMappings.length === 0) {
                    message.error('Please add at least one column mapping');
                    return;
                }

                setExecuting(true);
                try {
                    const response = await fetch('/api/pipeline/execute-grid', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            mappingConfig: columnMappings,
                            targetTable: \`unified_custody_\${new Date().toISOString().split('T')[0].replace(/-/g, '_')}\`,
                            processingDate: new Date().toISOString()
                        })
                    });

                    const result = await response.json();
                    if (result.success) {
                        message.success(\`ETL completed! Processed \${result.statistics.totalProcessed} records\");
                    } else {
                        message.error('ETL execution failed');
                    }
                } catch (error) {
                    console.error('Error executing ETL:', error);
                    message.error('Failed to execute ETL pipeline');
                } finally {
                    setExecuting(false);
                }
            };

            if (loading) {
                return (
                    <div style={{ textAlign: 'center', padding: '50px' }}>
                        <Spin size="large" />
                        <p>Loading custody file schemas...</p>
                    </div>
                );
            }

            const stats = calculateMappingStats(columnMappings, custodyFiles);

            return (
                <div className="mapping-container">
                    <div className="page-header">
                        <h1 className="page-title">🎯 ETL Visual Grid Mapping System</h1>
                        <p className="page-subtitle">
                            Map different source columns from each custody file to unified column names
                        </p>
                    </div>

                    <MappingStatusPanel stats={stats} files={custodyFiles} />

                    <div className="mapping-grid">
                        {/* Grid Header */}
                        <div className="grid-header">
                            <div className="header-cell unified-column-header">
                                <div>📊 Unified Column</div>
                                <div style={{ fontSize: '12px', opacity: 0.9 }}>Name & Type</div>
                            </div>
                            {custodyFiles.map(file => (
                                <div key={file.name} className="header-cell">
                                    <FileColumnHeader file={file} />
                                </div>
                            ))}
                        </div>

                        {/* Mapping Rows */}
                        {columnMappings.map(mapping => (
                            <MappingRow
                                key={mapping.id}
                                mapping={mapping}
                                files={custodyFiles}
                                onUpdateUnifiedColumnName={updateUnifiedColumnName}
                                onUpdateColumnType={updateColumnType}
                                onUpdateFileMapping={updateFileMapping}
                                onRemoveMapping={removeMapping}
                            />
                        ))}

                        {/* Add New Column Section */}
                        <div className="add-column-section">
                            <Button
                                type="dashed"
                                size="large"
                                onClick={addNewColumn}
                                style={{ padding: '12px 24px', height: 'auto' }}
                            >
                                ➕ Add New Unified Column
                            </Button>
                        </div>
                    </div>

                    <div className="control-panel">
                        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                            <Input
                                placeholder="Enter template name (e.g., Daily Custody Mapping v1)"
                                value={templateName}
                                onChange={(e) => setTemplateName(e.target.value)}
                                style={{ width: '300px' }}
                            />
                            <Button type="default" onClick={saveTemplate}>
                                💾 Save Template
                            </Button>
                        </div>
                        
                        <Button
                            type="primary"
                            size="large"
                            onClick={executeETL}
                            loading={executing}
                            disabled={columnMappings.length === 0}
                        >
                            🚀 Execute ETL Pipeline
                        </Button>
                    </div>
                </div>
            );
        };

        // File Column Header Component
        const FileColumnHeader = ({ file }) => (
            <div className="file-header">
                <div className="file-title">{file.displayName}</div>
                <div className="file-metadata">
                    <Tag color="blue" style={{ margin: '1px' }}>{file.columnCount} cols</Tag>
                    <Tag color="green" style={{ margin: '1px' }}>{file.recordCount} rows</Tag>
                </div>
            </div>
        );

        // Mapping Row Component
        const MappingRow = ({
            mapping,
            files,
            onUpdateUnifiedColumnName,
            onUpdateColumnType,
            onUpdateFileMapping,
            onRemoveMapping
        }) => (
            <div className="grid-row">
                {/* Unified Column Cell */}
                <div className="unified-column-cell">
                    <Input
                        placeholder="Enter column name"
                        value={mapping.unifiedColumnName}
                        onChange={(e) => onUpdateUnifiedColumnName(mapping.id, e.target.value)}
                        style={{ marginBottom: '8px' }}
                    />
                    <Select
                        value={mapping.unifiedColumnType}
                        onChange={(type) => onUpdateColumnType(mapping.id, type)}
                        style={{ width: '100%', marginBottom: '8px' }}
                        size="small"
                    >
                        <Option value="VARCHAR">📝 Text</Option>
                        <Option value="DECIMAL">🔢 Number</Option>
                        <Option value="DATE">📅 Date</Option>
                        <Option value="INTEGER">🔢 Integer</Option>
                        <Option value="BOOLEAN">✅ Boolean</Option>
                    </Select>
                    <Button
                        type="text"
                        danger
                        size="small"
                        onClick={() => onRemoveMapping(mapping.id)}
                        style={{ alignSelf: 'flex-start' }}
                    >
                        🗑️ Remove
                    </Button>
                </div>

                {/* File Mapping Cells */}
                {files.map(file => (
                    <div key={file.name} className="file-mapping-cell">
                        <GridCellDropdown
                            file={file}
                            mapping={mapping.fileMappings[file.name] || {}}
                            onChange={(config) => onUpdateFileMapping(mapping.id, file.name, config)}
                        />
                    </div>
                ))}
            </div>
        );

        // Grid Cell Dropdown Component
        const GridCellDropdown = ({ file, mapping, onChange }) => {
            const [showCustomInput, setShowCustomInput] = useState(mapping.nullHandling === 'CUSTOM_VALUE');

            const handleSourceColumnChange = (value) => {
                if (value === 'NULL') {
                    onChange({
                        sourceColumn: null,
                        nullHandling: 'NULL',
                        customValue: ''
                    });
                    setShowCustomInput(false);
                } else if (value === 'CUSTOM_VALUE') {
                    onChange({
                        sourceColumn: null,
                        nullHandling: 'CUSTOM_VALUE',
                        customValue: mapping.customValue || ''
                    });
                    setShowCustomInput(true);
                } else {
                    onChange({
                        sourceColumn: value,
                        nullHandling: 'COLUMN',
                        customValue: ''
                    });
                    setShowCustomInput(false);
                }
            };

            const handleCustomValueChange = (value) => {
                onChange({
                    ...mapping,
                    customValue: value
                });
            };

            return (
                <div>
                    <Select
                        placeholder="Select mapping..."
                        value={mapping.sourceColumn || mapping.nullHandling || undefined}
                        onChange={handleSourceColumnChange}
                        style={{ width: '100%', marginBottom: '8px' }}
                        size="small"
                    >
                        <OptGroup label="Available Columns">
                            {file.columns.map(column => (
                                <Option key={column} value={column}>
                                    📄 {column}
                                </Option>
                            ))}
                        </OptGroup>
                        <OptGroup label="Null Handling">
                            <Option value="NULL">⚪ NULL (not available)</Option>
                            <Option value="CUSTOM_VALUE">✏️ Custom Default Value</Option>
                        </OptGroup>
                    </Select>

                    {showCustomInput && (
                        <Input
                            placeholder="Enter default value"
                            value={mapping.customValue || ''}
                            onChange={(e) => handleCustomValueChange(e.target.value)}
                            size="small"
                            style={{ marginBottom: '8px' }}
                        />
                    )}

                    <MappingStatusTag mapping={mapping} />
                </div>
            );
        };

        // Mapping Status Tag Component
        const MappingStatusTag = ({ mapping }) => {
            if (mapping.sourceColumn) {
                return <Tag className="success-tag">✅ {mapping.sourceColumn}</Tag>;
            } else if (mapping.nullHandling === 'CUSTOM_VALUE' && mapping.customValue) {
                return <Tag className="info-tag">💬 {mapping.customValue}</Tag>;
            } else if (mapping.nullHandling === 'NULL') {
                return <Tag className="warning-tag">⚪ NULL</Tag>;
            } else {
                return <Tag className="error-tag">❌ Not Mapped</Tag>;
            }
        };

        // Mapping Status Panel Component
        const MappingStatusPanel = ({ stats, files }) => (
            <Card title="📊 Mapping Overview" className="status-panel">
                <Row gutter={16}>
                    <Col span={6}>
                        <Statistic
                            title="Total Columns"
                            value={stats.totalMappings}
                            prefix="📊"
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Complete Mappings"
                            value={stats.completeMappings}
                            prefix="✅"
                            valueStyle={{ color: '#52c41a' }}
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Null Mappings"
                            value={stats.nullMappings}
                            prefix="⚪"
                            valueStyle={{ color: '#fa8c16' }}
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Custom Values"
                            value={stats.customValues}
                            prefix="💬"
                            valueStyle={{ color: '#1890ff' }}
                        />
                    </Col>
                </Row>

                {stats.totalMappings > 0 && (
                    <div style={{ marginTop: '16px' }}>
                        <div style={{ marginBottom: '8px' }}>
                            <strong>Mapping Progress: {Math.round(stats.progressPercent)}%</strong>
                        </div>
                        <Progress
                            percent={stats.progressPercent}
                            status="active"
                            strokeColor={{
                                from: '#108ee9',
                                to: '#87d068',
                            }}
                        />
                    </div>
                )}
            </Card>
        );

        // Helper function to calculate mapping statistics
        const calculateMappingStats = (mappings, files) => {
            const totalMappings = mappings.length;
            let completeMappings = 0;
            let nullMappings = 0;
            let customValues = 0;
            let totalCells = 0;
            let mappedCells = 0;

            mappings.forEach(mapping => {
                if (mapping.unifiedColumnName) {
                    completeMappings++;
                }

                Object.values(mapping.fileMappings || {}).forEach(fileMapping => {
                    totalCells++;
                    if (fileMapping.sourceColumn) {
                        mappedCells++;
                    } else if (fileMapping.nullHandling === 'NULL') {
                        nullMappings++;
                        mappedCells++;
                    } else if (fileMapping.nullHandling === 'CUSTOM_VALUE' && fileMapping.customValue) {
                        customValues++;
                        mappedCells++;
                    }
                });
            });

            return {
                totalMappings,
                completeMappings,
                nullMappings,
                customValues,
                progressPercent: totalCells > 0 ? (mappedCells / totalCells) * 100 : 0
            };
        };

        // Render the main application
        ReactDOM.render(<ETLVisualGridMapping />, document.getElementById('root'));
    </script>
</body>
</html>
    ");
});

app.listen(PORT, () => {
    console.log("🎯 ETL Visual Grid Mapping System running at http://localhost:" + PORT);
    console.log("📊 Features:");
    console.log("   • Visual grid-based column mapping");
    console.log("   • Multi-file source to unified columns");
    console.log("   • NULL and custom value handling");
    console.log("   • Template save/load functionality");
    console.log("   • Real-time ETL pipeline execution");
}); 
const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3001;

// Use the correct MongoDB URI from config
const config = require('./config');
const mongoUri = config.mongodb.uri + config.mongodb.database;

const pgConfig = {
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: '',
    port: 5432,
};

app.use(express.json());
app.use(express.static('public'));

// API to get custody file schemas for grid headers
app.get('/api/custody/file-schemas', async (req, res) => {
    try {
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const fileTypes = ['hdfc', 'axis', 'broker_master', 'contract_note'];
        const fileSchemas = [];
        
        // Use the actual date format from clean-dashboard: fileType_MM_DD
        const month = '06';
        const day = '30';
        
        for (const fileType of fileTypes) {
            try {
                const collectionName = `${fileType}_${month}_${day}`;
                const collection = db.collection(collectionName);
                
                const sampleDoc = await collection.findOne({});
                const count = await collection.countDocuments({});
                
                if (sampleDoc) {
                    const columns = Object.keys(sampleDoc).filter(key => 
                        !['_id', 'month', 'date', 'fullDate', 'fileName', 'fileType', 'uploadedAt', '__v'].includes(key)
                    );
                    
                    fileSchemas.push({
                        name: collectionName,
                        displayName: `${fileType.toUpperCase()} File (${month}/${day})`,
                        columns: columns,
                        columnCount: columns.length,
                        recordCount: count,
                        lastUpdated: new Date().toISOString(),
                        fileType: fileType
                    });
                }
            } catch (error) {
                console.log(`Warning: Could not process ${fileType}:`, error.message);
            }
        }
        
        await client.close();
        res.json({ files: fileSchemas });
    } catch (error) {
        console.error('Error fetching file schemas:', error);
        res.status(500).json({ error: 'Failed to fetch file schemas' });
    }
});

// API to save grid mapping configuration
app.post('/api/mappings/save-grid', async (req, res) => {
    try {
        const { templateName, unifiedTableName, columnMappings } = req.body;
        
        // Save to a JSON file for now (in production, save to database)
        const mappingConfig = {
            templateName,
            unifiedTableName,
            columnMappings,
            createdAt: new Date().toISOString()
        };
        
        fs.writeFileSync('mapping-template.json', JSON.stringify(mappingConfig, null, 2));
        
        res.json({ 
            success: true, 
            message: 'Grid mapping configuration saved successfully',
            templateId: Date.now()
        });
    } catch (error) {
        console.error('Error saving grid mapping:', error);
        res.status(500).json({ error: 'Failed to save grid mapping' });
    }
});

// API to execute grid-based ETL pipeline
app.post('/api/pipeline/execute-grid', async (req, res) => {
    try {
        const { mappingConfig, targetTable, processingDate } = req.body;
        
        const client = new MongoClient(mongoUri);
        await client.connect();
        const db = client.db('financial_data_2025');
        
        const pgPool = new Pool(pgConfig);
        
        // Build unified table schema
        const createTableQuery = buildCreateTableQuery(targetTable, mappingConfig);
        await pgPool.query(createTableQuery);
        
        let totalProcessed = 0;
        let totalErrors = 0;
        
        // Process each file according to mapping configuration
        for (const mapping of mappingConfig) {
            for (const [fileName, fileMapping] of Object.entries(mapping.fileMappings)) {
                if (fileMapping.sourceColumn || fileMapping.customValue) {
                    try {
                        const collection = db.collection(fileName);
                        const documents = await collection.find({}).toArray();
                        
                        for (const record of documents) {
                            try {
                                const insertQuery = buildInsertQuery(targetTable, mapping, record, fileMapping);
                                await pgPool.query(insertQuery.text, insertQuery.values);
                                totalProcessed++;
                            } catch (error) {
                                console.error('Error inserting record:', error);
                                totalErrors++;
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing ${fileName}:`, error);
                        totalErrors++;
                    }
                }
            }
        }
        
        await client.close();
        await pgPool.end();
        
        res.json({
            success: true,
            message: 'ETL pipeline executed successfully',
            statistics: {
                totalProcessed,
                totalErrors,
                successRate: totalProcessed > 0 ? (totalProcessed / (totalProcessed + totalErrors) * 100) : 0
            }
        });
    } catch (error) {
        console.error('Error executing ETL pipeline:', error);
        res.status(500).json({ error: 'Failed to execute ETL pipeline' });
    }
});

// Helper function to build CREATE TABLE query
function buildCreateTableQuery(tableName, mappingConfig) {
    const columns = mappingConfig.map(mapping => {
        const dataType = getPostgreSQLDataType(mapping.unifiedColumnType);
        return `${mapping.unifiedColumnName} ${dataType}`;
    });
    
    return `CREATE TABLE IF NOT EXISTS ${tableName} (
        id SERIAL PRIMARY KEY,
        source_file VARCHAR(255),
        processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ${columns.join(',\n        ')}
    )`;
}

// Helper function to build INSERT query
function buildInsertQuery(tableName, mapping, document, fileMapping) {
    let value;
    
    if (fileMapping.sourceColumn && document[fileMapping.sourceColumn] !== undefined) {
        value = document[fileMapping.sourceColumn];
    } else if (fileMapping.customValue) {
        value = fileMapping.customValue;
    } else {
        value = null;
    }
    
    return {
        text: `INSERT INTO ${tableName} (${mapping.unifiedColumnName}, source_file) VALUES ($1, $2)`,
        values: [value, document.fileName || 'unknown']
    };
}

// Helper function to map data types
function getPostgreSQLDataType(type) {
    const typeMap = {
        'VARCHAR': 'VARCHAR(255)',
        'DECIMAL': 'DECIMAL(15,2)',
        'DATE': 'DATE',
        'INTEGER': 'INTEGER',
        'BOOLEAN': 'BOOLEAN'
    };
    return typeMap[type] || 'VARCHAR(255)';
}

// Serve the main React application
app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ETL Visual Grid Mapping System</title>
    <script src="https://unpkg.com/react@18/umd/react.development.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
    <script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
    <link href="https://cdn.jsdelivr.net/npm/antd@5.12.1/dist/reset.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/antd@5.12.1/dist/antd.min.js"></script>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        
        .mapping-container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 24px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        .page-header {
            margin-bottom: 24px;
            text-align: center;
        }
        
        .page-title {
            font-size: 28px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 8px;
        }
        
        .page-subtitle {
            font-size: 16px;
            color: #666;
        }
        
        .mapping-grid {
            display: grid;
            grid-template-columns: 300px repeat(6, 1fr);
            gap: 1px;
            background-color: #e6f7ff;
            border: 2px solid #1890ff;
            border-radius: 8px;
            overflow: hidden;
            margin: 24px 0;
        }
        
        .grid-header {
            display: contents;
        }
        
        .header-cell {
            background: linear-gradient(135deg, #1890ff, #096dd9);
            color: white;
            padding: 16px 12px;
            font-weight: 600;
            text-align: center;
            font-size: 14px;
        }
        
        .unified-column-header {
            background: linear-gradient(135deg, #52c41a, #389e0d);
        }
        
        .grid-row {
            display: contents;
        }
        
        .unified-column-cell {
            background-color: #f6ffed;
            padding: 12px;
            border-right: 1px solid #d9d9d9;
            display: flex;
            flex-direction: column;
            gap: 8px;
            min-height: 120px;
        }
        
        .file-mapping-cell {
            background-color: #ffffff;
            padding: 12px;
            border-right: 1px solid #d9d9d9;
            min-height: 120px;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .file-header {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 4px;
        }
        
        .file-title {
            font-weight: 600;
            color: #1890ff;
        }
        
        .file-metadata {
            display: flex;
            gap: 4px;
            flex-wrap: wrap;
            justify-content: center;
        }
        
        .add-column-section {
            grid-column: 1 / -1;
            background-color: #f0f9ff;
            padding: 20px;
            text-align: center;
            border-top: 2px dashed #1890ff;
        }
        
        .control-panel {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 24px;
            padding: 20px;
            background: linear-gradient(135deg, #f0f9ff, #e6f7ff);
            border-radius: 8px;
            border: 1px solid #1890ff;
        }
        
        .status-panel {
            margin-bottom: 24px;
        }
        
        .mapping-status-tag {
            margin-top: 4px;
        }
        
        .dropdown-container {
            position: relative;
        }
        
        .custom-dropdown {
            width: 100%;
            padding: 8px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            background: white;
            cursor: pointer;
        }
        
        .custom-dropdown:hover {
            border-color: #40a9ff;
        }
        
        .custom-dropdown:focus {
            border-color: #1890ff;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
            outline: none;
        }
        
        .dropdown-option {
            padding: 8px 12px;
            cursor: pointer;
            border-bottom: 1px solid #f0f0f0;
        }
        
        .dropdown-option:hover {
            background-color: #f5f5f5;
        }
        
        .dropdown-option.selected {
            background-color: #e6f7ff;
            color: #1890ff;
            font-weight: 500;
        }
        
        .ant-tag {
            margin: 2px;
        }
        
        .success-tag {
            background-color: #f6ffed;
            color: #52c41a;
            border-color: #52c41a;
        }
        
        .warning-tag {
            background-color: #fff7e6;
            color: #fa8c16;
            border-color: #fa8c16;
        }
        
        .error-tag {
            background-color: #fff2f0;
            color: #ff4d4f;
            border-color: #ff4d4f;
        }
        
        .info-tag {
            background-color: #f0f9ff;
            color: #1890ff;
            border-color: #1890ff;
        }
        
        @media (max-width: 1200px) {
            .mapping-grid {
                grid-template-columns: 250px repeat(6, minmax(150px, 1fr));
                overflow-x: auto;
            }
        }
    </style>
</head>
<body>
    <div id="root"></div>

    <script type="text/babel">
        const { useState, useEffect } = React;
        const { Button, Input, Select, Tag, Card, Row, Col, Statistic, Progress, Spin, message, Modal } = antd;
        const { Option, OptGroup } = Select;

        // Main Grid Mapping Component
        const ETLVisualGridMapping = () => {
            const [custodyFiles, setCustodyFiles] = useState([]);
            const [columnMappings, setColumnMappings] = useState([]);
            const [loading, setLoading] = useState(true);
            const [templateName, setTemplateName] = useState('');
            const [executing, setExecuting] = useState(false);

            useEffect(() => {
                fetchFileSchemas();
            }, []);

            const fetchFileSchemas = async () => {
                try {
                    const response = await fetch('/api/custody/file-schemas');
                    const data = await response.json();
                    setCustodyFiles(data.files || []);
                    setLoading(false);
                } catch (error) {
                    console.error('Error fetching file schemas:', error);
                    message.error('Failed to load file schemas');
                    setLoading(false);
                }
            };

            const addNewColumn = () => {
                const newMapping = {
                    id: Date.now().toString(),
                    unifiedColumnName: '',
                    unifiedColumnType: 'VARCHAR',
                    fileMappings: {}
                };
                
                // Initialize file mappings
                custodyFiles.forEach(file => {
                    newMapping.fileMappings[file.name] = {
                        sourceColumn: null,
                        nullHandling: 'NULL',
                        customValue: ''
                    };
                });
                
                setColumnMappings([...columnMappings, newMapping]);
            };

            const updateUnifiedColumnName = (mappingId, name) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? { ...mapping, unifiedColumnName: name }
                            : mapping
                    )
                );
            };

            const updateColumnType = (mappingId, type) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? { ...mapping, unifiedColumnType: type }
                            : mapping
                    )
                );
            };

            const updateFileMapping = (mappingId, fileName, config) => {
                setColumnMappings(mappings =>
                    mappings.map(mapping =>
                        mapping.id === mappingId
                            ? {
                                ...mapping,
                                fileMappings: {
                                    ...mapping.fileMappings,
                                    [fileName]: config
                                }
                            }
                            : mapping
                    )
                );
            };

            const removeMapping = (mappingId) => {
                setColumnMappings(mappings =>
                    mappings.filter(mapping => mapping.id !== mappingId)
                );
            };

            const saveTemplate = async () => {
                if (!templateName.trim()) {
                    message.error('Please enter a template name');
                    return;
                }

                if (columnMappings.length === 0) {
                    message.error('Please add at least one column mapping');
                    return;
                }

                try {
                    const response = await fetch('/api/mappings/save-grid', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            templateName,
                            unifiedTableName: \`unified_custody_\${new Date().toISOString().split('T')[0].replace(/-/g, '_')}\`,
                            columnMappings
                        })
                    });

                    if (response.ok) {
                        message.success('Template saved successfully!');
                    } else {
                        message.error('Failed to save template');
                    }
                } catch (error) {
                    console.error('Error saving template:', error);
                    message.error('Failed to save template');
                }
            };

            const executeETL = async () => {
                if (columnMappings.length === 0) {
                    message.error('Please add at least one column mapping');
                    return;
                }

                setExecuting(true);
                try {
                    const response = await fetch('/api/pipeline/execute-grid', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            mappingConfig: columnMappings,
                            targetTable: \`unified_custody_\${new Date().toISOString().split('T')[0].replace(/-/g, '_')}\`,
                            processingDate: new Date().toISOString()
                        })
                    });

                    const result = await response.json();
                    if (result.success) {
                        message.success(\`ETL completed! Processed \${result.statistics.totalProcessed} records\");
                    } else {
                        message.error('ETL execution failed');
                    }
                } catch (error) {
                    console.error('Error executing ETL:', error);
                    message.error('Failed to execute ETL pipeline');
                } finally {
                    setExecuting(false);
                }
            };

            if (loading) {
                return (
                    <div style={{ textAlign: 'center', padding: '50px' }}>
                        <Spin size="large" />
                        <p>Loading custody file schemas...</p>
                    </div>
                );
            }

            const stats = calculateMappingStats(columnMappings, custodyFiles);

            return (
                <div className="mapping-container">
                    <div className="page-header">
                        <h1 className="page-title">🎯 ETL Visual Grid Mapping System</h1>
                        <p className="page-subtitle">
                            Map different source columns from each custody file to unified column names
                        </p>
                    </div>

                    <MappingStatusPanel stats={stats} files={custodyFiles} />

                    <div className="mapping-grid">
                        {/* Grid Header */}
                        <div className="grid-header">
                            <div className="header-cell unified-column-header">
                                <div>📊 Unified Column</div>
                                <div style={{ fontSize: '12px', opacity: 0.9 }}>Name & Type</div>
                            </div>
                            {custodyFiles.map(file => (
                                <div key={file.name} className="header-cell">
                                    <FileColumnHeader file={file} />
                                </div>
                            ))}
                        </div>

                        {/* Mapping Rows */}
                        {columnMappings.map(mapping => (
                            <MappingRow
                                key={mapping.id}
                                mapping={mapping}
                                files={custodyFiles}
                                onUpdateUnifiedColumnName={updateUnifiedColumnName}
                                onUpdateColumnType={updateColumnType}
                                onUpdateFileMapping={updateFileMapping}
                                onRemoveMapping={removeMapping}
                            />
                        ))}

                        {/* Add New Column Section */}
                        <div className="add-column-section">
                            <Button
                                type="dashed"
                                size="large"
                                onClick={addNewColumn}
                                style={{ padding: '12px 24px', height: 'auto' }}
                            >
                                ➕ Add New Unified Column
                            </Button>
                        </div>
                    </div>

                    <div className="control-panel">
                        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                            <Input
                                placeholder="Enter template name (e.g., Daily Custody Mapping v1)"
                                value={templateName}
                                onChange={(e) => setTemplateName(e.target.value)}
                                style={{ width: '300px' }}
                            />
                            <Button type="default" onClick={saveTemplate}>
                                💾 Save Template
                            </Button>
                        </div>
                        
                        <Button
                            type="primary"
                            size="large"
                            onClick={executeETL}
                            loading={executing}
                            disabled={columnMappings.length === 0}
                        >
                            🚀 Execute ETL Pipeline
                        </Button>
                    </div>
                </div>
            );
        };

        // File Column Header Component
        const FileColumnHeader = ({ file }) => (
            <div className="file-header">
                <div className="file-title">{file.displayName}</div>
                <div className="file-metadata">
                    <Tag color="blue" style={{ margin: '1px' }}>{file.columnCount} cols</Tag>
                    <Tag color="green" style={{ margin: '1px' }}>{file.recordCount} rows</Tag>
                </div>
            </div>
        );

        // Mapping Row Component
        const MappingRow = ({
            mapping,
            files,
            onUpdateUnifiedColumnName,
            onUpdateColumnType,
            onUpdateFileMapping,
            onRemoveMapping
        }) => (
            <div className="grid-row">
                {/* Unified Column Cell */}
                <div className="unified-column-cell">
                    <Input
                        placeholder="Enter column name"
                        value={mapping.unifiedColumnName}
                        onChange={(e) => onUpdateUnifiedColumnName(mapping.id, e.target.value)}
                        style={{ marginBottom: '8px' }}
                    />
                    <Select
                        value={mapping.unifiedColumnType}
                        onChange={(type) => onUpdateColumnType(mapping.id, type)}
                        style={{ width: '100%', marginBottom: '8px' }}
                        size="small"
                    >
                        <Option value="VARCHAR">📝 Text</Option>
                        <Option value="DECIMAL">🔢 Number</Option>
                        <Option value="DATE">📅 Date</Option>
                        <Option value="INTEGER">🔢 Integer</Option>
                        <Option value="BOOLEAN">✅ Boolean</Option>
                    </Select>
                    <Button
                        type="text"
                        danger
                        size="small"
                        onClick={() => onRemoveMapping(mapping.id)}
                        style={{ alignSelf: 'flex-start' }}
                    >
                        🗑️ Remove
                    </Button>
                </div>

                {/* File Mapping Cells */}
                {files.map(file => (
                    <div key={file.name} className="file-mapping-cell">
                        <GridCellDropdown
                            file={file}
                            mapping={mapping.fileMappings[file.name] || {}}
                            onChange={(config) => onUpdateFileMapping(mapping.id, file.name, config)}
                        />
                    </div>
                ))}
            </div>
        );

        // Grid Cell Dropdown Component
        const GridCellDropdown = ({ file, mapping, onChange }) => {
            const [showCustomInput, setShowCustomInput] = useState(mapping.nullHandling === 'CUSTOM_VALUE');

            const handleSourceColumnChange = (value) => {
                if (value === 'NULL') {
                    onChange({
                        sourceColumn: null,
                        nullHandling: 'NULL',
                        customValue: ''
                    });
                    setShowCustomInput(false);
                } else if (value === 'CUSTOM_VALUE') {
                    onChange({
                        sourceColumn: null,
                        nullHandling: 'CUSTOM_VALUE',
                        customValue: mapping.customValue || ''
                    });
                    setShowCustomInput(true);
                } else {
                    onChange({
                        sourceColumn: value,
                        nullHandling: 'COLUMN',
                        customValue: ''
                    });
                    setShowCustomInput(false);
                }
            };

            const handleCustomValueChange = (value) => {
                onChange({
                    ...mapping,
                    customValue: value
                });
            };

            return (
                <div>
                    <Select
                        placeholder="Select mapping..."
                        value={mapping.sourceColumn || mapping.nullHandling || undefined}
                        onChange={handleSourceColumnChange}
                        style={{ width: '100%', marginBottom: '8px' }}
                        size="small"
                    >
                        <OptGroup label="Available Columns">
                            {file.columns.map(column => (
                                <Option key={column} value={column}>
                                    📄 {column}
                                </Option>
                            ))}
                        </OptGroup>
                        <OptGroup label="Null Handling">
                            <Option value="NULL">⚪ NULL (not available)</Option>
                            <Option value="CUSTOM_VALUE">✏️ Custom Default Value</Option>
                        </OptGroup>
                    </Select>

                    {showCustomInput && (
                        <Input
                            placeholder="Enter default value"
                            value={mapping.customValue || ''}
                            onChange={(e) => handleCustomValueChange(e.target.value)}
                            size="small"
                            style={{ marginBottom: '8px' }}
                        />
                    )}

                    <MappingStatusTag mapping={mapping} />
                </div>
            );
        };

        // Mapping Status Tag Component
        const MappingStatusTag = ({ mapping }) => {
            if (mapping.sourceColumn) {
                return <Tag className="success-tag">✅ {mapping.sourceColumn}</Tag>;
            } else if (mapping.nullHandling === 'CUSTOM_VALUE' && mapping.customValue) {
                return <Tag className="info-tag">💬 {mapping.customValue}</Tag>;
            } else if (mapping.nullHandling === 'NULL') {
                return <Tag className="warning-tag">⚪ NULL</Tag>;
            } else {
                return <Tag className="error-tag">❌ Not Mapped</Tag>;
            }
        };

        // Mapping Status Panel Component
        const MappingStatusPanel = ({ stats, files }) => (
            <Card title="📊 Mapping Overview" className="status-panel">
                <Row gutter={16}>
                    <Col span={6}>
                        <Statistic
                            title="Total Columns"
                            value={stats.totalMappings}
                            prefix="📊"
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Complete Mappings"
                            value={stats.completeMappings}
                            prefix="✅"
                            valueStyle={{ color: '#52c41a' }}
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Null Mappings"
                            value={stats.nullMappings}
                            prefix="⚪"
                            valueStyle={{ color: '#fa8c16' }}
                        />
                    </Col>
                    <Col span={6}>
                        <Statistic
                            title="Custom Values"
                            value={stats.customValues}
                            prefix="💬"
                            valueStyle={{ color: '#1890ff' }}
                        />
                    </Col>
                </Row>

                {stats.totalMappings > 0 && (
                    <div style={{ marginTop: '16px' }}>
                        <div style={{ marginBottom: '8px' }}>
                            <strong>Mapping Progress: {Math.round(stats.progressPercent)}%</strong>
                        </div>
                        <Progress
                            percent={stats.progressPercent}
                            status="active"
                            strokeColor={{
                                from: '#108ee9',
                                to: '#87d068',
                            }}
                        />
                    </div>
                )}
            </Card>
        );

        // Helper function to calculate mapping statistics
        const calculateMappingStats = (mappings, files) => {
            const totalMappings = mappings.length;
            let completeMappings = 0;
            let nullMappings = 0;
            let customValues = 0;
            let totalCells = 0;
            let mappedCells = 0;

            mappings.forEach(mapping => {
                if (mapping.unifiedColumnName) {
                    completeMappings++;
                }

                Object.values(mapping.fileMappings || {}).forEach(fileMapping => {
                    totalCells++;
                    if (fileMapping.sourceColumn) {
                        mappedCells++;
                    } else if (fileMapping.nullHandling === 'NULL') {
                        nullMappings++;
                        mappedCells++;
                    } else if (fileMapping.nullHandling === 'CUSTOM_VALUE' && fileMapping.customValue) {
                        customValues++;
                        mappedCells++;
                    }
                });
            });

            return {
                totalMappings,
                completeMappings,
                nullMappings,
                customValues,
                progressPercent: totalCells > 0 ? (mappedCells / totalCells) * 100 : 0
            };
        };

        // Render the main application
        ReactDOM.render(<ETLVisualGridMapping />, document.getElementById('root'));
    </script>
</body>
</html>
    ");
});

app.listen(PORT, () => {
    console.log("🎯 ETL Visual Grid Mapping System running at http://localhost:" + PORT);
    console.log("📊 Features:");
    console.log("   • Visual grid-based column mapping");
    console.log("   • Multi-file source to unified columns");
    console.log("   • NULL and custom value handling");
    console.log("   • Template save/load functionality");
    console.log("   • Real-time ETL pipeline execution");
}); 