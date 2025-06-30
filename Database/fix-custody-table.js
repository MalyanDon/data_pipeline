const { Pool } = require('pg');

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: 'password',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

async function createUnifiedCustodyTable() {
    try {
        console.log('🔧 Creating unified_custody_master table with correct schema...');
        
        // Drop table if exists to start fresh
        await pool.query(`DROP TABLE IF EXISTS unified_custody_master CASCADE`);
        
        // Create table with exact schema expected by processing code
        const createTableQuery = `
            CREATE TABLE unified_custody_master (
                id SERIAL PRIMARY KEY,
                client_reference VARCHAR(100),
                client_name VARCHAR(255),
                instrument_isin VARCHAR(50),
                instrument_name VARCHAR(255),
                instrument_code VARCHAR(50),
                blocked_quantity DECIMAL(15,4) DEFAULT 0,
                pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
                pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
                total_position DECIMAL(15,4) DEFAULT 0,
                saleable_quantity DECIMAL(15,4) DEFAULT 0,
                source_system VARCHAR(50),
                file_name VARCHAR(255),
                record_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `;
        
        await pool.query(createTableQuery);
        
        // Create indexes for better performance
        await pool.query(`CREATE INDEX idx_custody_client_ref ON unified_custody_master(client_reference)`);
        await pool.query(`CREATE INDEX idx_custody_isin ON unified_custody_master(instrument_isin)`);
        await pool.query(`CREATE INDEX idx_custody_source ON unified_custody_master(source_system)`);
        await pool.query(`CREATE INDEX idx_custody_date ON unified_custody_master(record_date)`);
        
        console.log('✅ unified_custody_master table created successfully!');
        console.log('📋 Columns: client_reference, client_name, instrument_isin, instrument_name, instrument_code');
        console.log('📋 Financial: blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity');
        console.log('📋 Metadata: source_system, file_name, record_date, created_at, updated_at');
        
        // Verify table creation
        const result = await pool.query(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'unified_custody_master' 
            ORDER BY ordinal_position
        `);
        
        console.log('\n🔍 Table schema verification:');
        result.rows.forEach(row => {
            console.log(`   ${row.column_name}: ${row.data_type}`);
        });
        
    } catch (error) {
        console.error('❌ Error creating table:', error.message);
    } finally {
        await pool.end();
    }
}

createUnifiedCustodyTable(); 