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

async function createContractNotesTable() {
    try {
        console.log('🔧 Creating contract_notes table with correct schema...');
        
        // Drop table if exists to start fresh
        await pool.query(`DROP TABLE IF EXISTS contract_notes CASCADE`);
        
        // Create table with exact schema expected by processing code
        const createTableQuery = `
            CREATE TABLE contract_notes (
                id SERIAL PRIMARY KEY,
                ecn_number VARCHAR(50) NOT NULL,
                ecn_status VARCHAR(20),
                ecn_date DATE,
                client_code VARCHAR(50),
                broker_name VARCHAR(255),
                instrument_isin VARCHAR(50),
                instrument_name VARCHAR(255),
                transaction_type VARCHAR(20),
                delivery_type VARCHAR(20),
                exchange VARCHAR(50),
                settlement_date DATE,
                quantity DECIMAL(15,4),
                net_amount DECIMAL(15,2),
                net_rate DECIMAL(15,4),
                brokerage_amount DECIMAL(15,2),
                service_tax DECIMAL(15,2),
                stt_amount DECIMAL(15,2),
                market_type VARCHAR(20),
                settlement_number VARCHAR(50),
                brokerage_rate DECIMAL(10,6),
                stamp_duty DECIMAL(15,2),
                sebi_registration VARCHAR(100),
                scheme_name VARCHAR(255),
                custodian_name VARCHAR(255),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `;
        
        await pool.query(createTableQuery);
        
        // Create indexes for better performance
        await pool.query(`CREATE INDEX idx_contract_notes_ecn ON contract_notes(ecn_number)`);
        await pool.query(`CREATE INDEX idx_contract_notes_client ON contract_notes(client_code)`);
        await pool.query(`CREATE INDEX idx_contract_notes_date ON contract_notes(ecn_date)`);
        await pool.query(`CREATE INDEX idx_contract_notes_isin ON contract_notes(instrument_isin)`);
        
        console.log('✅ contract_notes table created successfully!');
        console.log('📋 Key columns: ecn_number, market_type, settlement_number, brokerage_rate');
        console.log('📋 Financial: stamp_duty, sebi_registration, scheme_name, custodian_name');
        console.log('📋 Core: client_code, broker_name, instrument_isin, transaction_type');
        
        // Verify table creation
        const result = await pool.query(`
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'contract_notes' 
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

createContractNotesTable(); 