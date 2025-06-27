const { Client } = require('pg');
const config = require('./config');

// PostgreSQL Schema for Financial Data
const createTablesSQL = `
-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS mf_allocations CASCADE;
DROP TABLE IF EXISTS trades CASCADE;
DROP TABLE IF EXISTS capital_flows CASCADE;
DROP TABLE IF EXISTS custody_holdings CASCADE;
DROP TABLE IF EXISTS clients CASCADE;
DROP TABLE IF EXISTS securities CASCADE;
DROP TABLE IF EXISTS strategies CASCADE;
DROP TABLE IF EXISTS distributors CASCADE;
DROP TABLE IF EXISTS brokers CASCADE;

-- 1. Master Tables (Reference Data)

-- Brokers/Custodians
CREATE TABLE brokers (
    broker_id SERIAL PRIMARY KEY,
    broker_name VARCHAR(100) UNIQUE NOT NULL,
    broker_code VARCHAR(20),
    contact_info TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Securities/Instruments
CREATE TABLE securities (
    security_id SERIAL PRIMARY KEY,
    isin_code VARCHAR(12) UNIQUE,
    symbol VARCHAR(50) NOT NULL,
    security_name VARCHAR(200) NOT NULL,
    security_type VARCHAR(50), -- Equity/Debt/MF
    exchange VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Clients
CREATE TABLE clients (
    client_id SERIAL PRIMARY KEY,
    client_code VARCHAR(50) NOT NULL,
    client_name VARCHAR(200) NOT NULL,
    client_type VARCHAR(50), -- Individual/Corporate
    pan_number VARCHAR(10),
    broker_id INTEGER REFERENCES brokers(broker_id),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(client_code, broker_id)
);

-- Strategies
CREATE TABLE strategies (
    strategy_id SERIAL PRIMARY KEY,
    strategy_code VARCHAR(50) UNIQUE NOT NULL,
    strategy_name VARCHAR(200) NOT NULL,
    strategy_type VARCHAR(50),
    aum DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Distributors
CREATE TABLE distributors (
    distributor_id SERIAL PRIMARY KEY,
    distributor_code VARCHAR(50) UNIQUE NOT NULL,
    distributor_name VARCHAR(200) NOT NULL,
    contact_person VARCHAR(100),
    contact_info TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. Transactional Tables (Daily Data)

-- Custody Holdings (EOD Positions)
CREATE TABLE custody_holdings (
    holding_id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    broker_id INTEGER REFERENCES brokers(broker_id),
    client_id INTEGER REFERENCES clients(client_id),
    security_id INTEGER REFERENCES securities(security_id),
    quantity DECIMAL(15,4) NOT NULL,
    market_value DECIMAL(15,2),
    avg_cost DECIMAL(10,4),
    book_value DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(trade_date, broker_id, client_id, security_id)
);

-- Capital Flows
CREATE TABLE capital_flows (
    flow_id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    broker_id INTEGER REFERENCES brokers(broker_id),
    client_id INTEGER REFERENCES clients(client_id),
    flow_type VARCHAR(20) NOT NULL, -- CASH_IN/CASH_OUT/STOCK_IN/STOCK_OUT
    amount DECIMAL(15,2),
    security_id INTEGER REFERENCES securities(security_id), -- For stock flows
    quantity DECIMAL(15,4), -- For stock flows
    narration TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Contract Notes/Trades
CREATE TABLE trades (
    trade_id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    broker_id INTEGER REFERENCES brokers(broker_id),
    client_id INTEGER REFERENCES clients(client_id),
    security_id INTEGER REFERENCES securities(security_id),
    trade_type VARCHAR(10) NOT NULL, -- BUY/SELL
    quantity DECIMAL(15,4) NOT NULL,
    price DECIMAL(10,4) NOT NULL,
    gross_amount DECIMAL(15,2) NOT NULL,
    charges DECIMAL(10,2),
    net_amount DECIMAL(15,2),
    settlement_date DATE,
    order_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- MF Allocations
CREATE TABLE mf_allocations (
    allocation_id SERIAL PRIMARY KEY,
    allocation_date DATE NOT NULL,
    strategy_id INTEGER REFERENCES strategies(strategy_id),
    security_id INTEGER REFERENCES securities(security_id),
    allocation_percentage DECIMAL(5,2),
    target_amount DECIMAL(15,2),
    actual_amount DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- 3. Insert Master Data

-- Insert known custodians
INSERT INTO brokers (broker_name, broker_code) VALUES 
('HDFC Securities', 'HDFC'),
('Kotak Securities', 'KOTAK'),
('Orbis Financial', 'ORBIS'),
('ICICI Securities', 'ICICI'),
('Axis Securities', 'AXIS'),
('SBI Securities', 'SBI'),
('Edelweiss Securities', 'EDELWEISS'),
('Zerodha', 'ZERODHA'),
('Nuvama', 'NUVAMA');

-- 4. Create Indexes for Performance
CREATE INDEX idx_custody_holdings_date_broker ON custody_holdings(trade_date, broker_id);
CREATE INDEX idx_capital_flows_date_broker ON capital_flows(trade_date, broker_id);
CREATE INDEX idx_trades_date_broker ON trades(trade_date, broker_id);
CREATE INDEX idx_securities_symbol ON securities(symbol);
CREATE INDEX idx_securities_isin ON securities(isin_code);
CREATE INDEX idx_clients_code ON clients(client_code);

-- 5. Create Views for Easy Querying
CREATE OR REPLACE VIEW v_custody_summary AS
SELECT 
    h.trade_date,
    b.broker_name,
    c.client_name,
    s.symbol,
    s.security_name,
    h.quantity,
    h.market_value,
    h.avg_cost
FROM custody_holdings h
JOIN brokers b ON h.broker_id = b.broker_id
JOIN clients c ON h.client_id = c.client_id
JOIN securities s ON h.security_id = s.security_id;

CREATE OR REPLACE VIEW v_daily_flows AS
SELECT 
    cf.trade_date,
    b.broker_name,
    c.client_name,
    cf.flow_type,
    cf.amount,
    s.symbol,
    cf.quantity
FROM capital_flows cf
JOIN brokers b ON cf.broker_id = b.broker_id
JOIN clients c ON cf.client_id = c.client_id
LEFT JOIN securities s ON cf.security_id = s.security_id;

CREATE OR REPLACE VIEW v_trade_summary AS
SELECT 
    t.trade_date,
    b.broker_name,
    c.client_name,
    s.symbol,
    t.trade_type,
    t.quantity,
    t.price,
    t.gross_amount,
    t.net_amount
FROM trades t
JOIN brokers b ON t.broker_id = b.broker_id
JOIN clients c ON t.client_id = c.client_id
JOIN securities s ON t.security_id = s.security_id;
`;

async function setupPostgreSQL() {
  const client = new Client({
    connectionString: config.postgresql.connectionString,
  });

  try {
    console.log('🔗 Connecting to PostgreSQL (Neon)...');
    await client.connect();
    console.log('✅ Connected to PostgreSQL successfully!');

    console.log('📋 Creating tables and schema...');
    await client.query(createTablesSQL);
    console.log('✅ All tables created successfully!');

    // Verify tables creation
    const result = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      ORDER BY table_name;
    `);

    console.log('📊 Created tables:');
    result.rows.forEach(row => {
      console.log(`  📄 ${row.table_name}`);
    });

    // Check brokers data
    const brokers = await client.query('SELECT * FROM brokers;');
    console.log(`\n🏦 Inserted ${brokers.rows.length} brokers:`);
    brokers.rows.forEach(broker => {
      console.log(`  🏢 ${broker.broker_name} (${broker.broker_code})`);
    });

    console.log('\n🎉 PostgreSQL setup completed successfully!');
    console.log('🔗 Database URL: neon.tech dashboard');
    console.log('📊 Ready for ETL pipeline!');

  } catch (error) {
    console.error('❌ Error setting up PostgreSQL:', error);
    console.error('💡 Check your connection string and try again');
  } finally {
    await client.end();
  }
}

// Run the setup
if (require.main === module) {
  setupPostgreSQL();
}

module.exports = { setupPostgreSQL }; 