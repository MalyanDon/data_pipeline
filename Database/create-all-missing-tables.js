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

async function createAllMissingTables() {
    try {
        console.log('🔧 Creating all missing PostgreSQL tables...');
        
        // 1. Stock Capital Flow Table
        console.log('📋 Creating stock_capital_flow table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS stock_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                acquisition_date DATE,
                security_in_date DATE,
                quantity DECIMAL(15,4),
                original_price DECIMAL(15,4),
                brokerage DECIMAL(15,2),
                service_tax DECIMAL(15,2),
                settlement_date_flag VARCHAR(10),
                market_rate DECIMAL(15,4),
                cash_symbol VARCHAR(10),
                stt_amount DECIMAL(15,2),
                accrued_interest DECIMAL(15,2),
                block_ref VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 2. Cash Capital Flow Table
        console.log('📋 Creating cash_capital_flow table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS cash_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                transaction_date DATE,
                settlement_date DATE,
                amount DECIMAL(15,2),
                charges DECIMAL(15,2),
                tax DECIMAL(15,2),
                net_amount DECIMAL(15,2),
                payment_mode VARCHAR(50),
                bank_reference VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 3. MF Allocations Table
        console.log('📋 Creating mf_allocations table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS mf_allocations (
                allocation_id SERIAL PRIMARY KEY,
                client_code VARCHAR(50),
                scheme_code VARCHAR(50),
                scheme_name VARCHAR(255),
                allocation_date DATE,
                allocation_type VARCHAR(20),
                amount DECIMAL(15,2),
                units DECIMAL(15,4),
                nav DECIMAL(15,4),
                broker_code VARCHAR(50),
                distributor_code VARCHAR(50),
                commission_rate DECIMAL(8,4),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 4. Brokers Table
        console.log('📋 Creating brokers table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS brokers (
                broker_id SERIAL PRIMARY KEY,
                broker_code VARCHAR(50) UNIQUE NOT NULL,
                broker_name VARCHAR(200),
                broker_type VARCHAR(50),
                registration_number VARCHAR(100),
                contact_person VARCHAR(100),
                email VARCHAR(100),
                phone VARCHAR(20),
                address TEXT,
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 5. Distributors Table
        console.log('📋 Creating distributors table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS distributors (
                distributor_id SERIAL PRIMARY KEY,
                distributor_arn_number VARCHAR(50),
                distributor_code VARCHAR(50) UNIQUE NOT NULL,
                distributor_name VARCHAR(200),
                distributor_type VARCHAR(50),
                commission_rate DECIMAL(8,4),
                contact_person VARCHAR(100),
                email VARCHAR(100),
                phone VARCHAR(20),
                address TEXT,
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 6. Strategies Table
        console.log('📋 Creating strategies table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS strategies (
                strategy_id SERIAL PRIMARY KEY,
                strategy_code VARCHAR(50) UNIQUE NOT NULL,
                strategy_name VARCHAR(200),
                strategy_type VARCHAR(50),
                description TEXT,
                risk_level VARCHAR(20),
                benchmark VARCHAR(100),
                inception_date DATE,
                manager_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 7. Clients Table
        console.log('📋 Creating clients table...');
        await pool.query(`
            CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                client_code VARCHAR(50) UNIQUE NOT NULL,
                client_name VARCHAR(200),
                client_type VARCHAR(50),
                pan_number VARCHAR(20),
                email VARCHAR(100),
                phone VARCHAR(20),
                address TEXT,
                city VARCHAR(100),
                state VARCHAR(100),
                country VARCHAR(100),
                risk_category VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create indexes for better performance
        console.log('📋 Creating indexes...');
        
        // Stock capital flow indexes
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_stock_flow_client ON stock_capital_flow(client_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_stock_flow_isin ON stock_capital_flow(instrument_isin)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_stock_flow_date ON stock_capital_flow(acquisition_date)`);
        
        // Cash capital flow indexes
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_cash_flow_client ON cash_capital_flow(client_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_cash_flow_date ON cash_capital_flow(transaction_date)`);
        
        // MF allocations indexes
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_mf_client ON mf_allocations(client_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_mf_scheme ON mf_allocations(scheme_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_mf_date ON mf_allocations(allocation_date)`);
        
        // Master data indexes
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_brokers_code ON brokers(broker_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_distributors_code ON distributors(distributor_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_strategies_code ON strategies(strategy_code)`);
        await pool.query(`CREATE INDEX IF NOT EXISTS idx_clients_code ON clients(client_code)`);

        console.log('✅ All tables and indexes created successfully!');
        
        // Verify tables exist
        const result = await pool.query(`
            SELECT tablename, pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename
        `);
        
        console.log('\n🔍 Created tables:');
        result.rows.forEach(row => {
            console.log(`   📋 ${row.tablename}: ${row.size}`);
        });
        
    } catch (error) {
        console.error('❌ Error creating tables:', error.message);
    } finally {
        await pool.end();
    }
}

createAllMissingTables(); 