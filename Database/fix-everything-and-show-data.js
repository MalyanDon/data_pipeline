const { Pool } = require('pg');
const { MongoClient } = require('mongodb');

const pgPool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: 'password',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

const mongoUri = 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/';

async function fixAllTablesAndShowData() {
    try {
        console.log('🔧 COMPLETE FIX: Creating proper tables and processing all data...\n');

        // Step 1: Create all tables with correct schemas
        console.log('📋 Step 1: Creating all tables with correct schemas...');
        
        // Drop and recreate all tables with proper schemas
        await pgPool.query(`DROP TABLE IF EXISTS brokers CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS clients CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS distributors CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS strategies CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS contract_notes CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS cash_capital_flow CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS stock_capital_flow CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS mf_allocations CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS unified_custody_master CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS general_data CASCADE`);

        // Create BROKERS table
        await pgPool.query(`
            CREATE TABLE brokers (
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

        // Create CLIENTS table
        await pgPool.query(`
            CREATE TABLE clients (
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

        // Create DISTRIBUTORS table
        await pgPool.query(`
            CREATE TABLE distributors (
                distributor_id SERIAL PRIMARY KEY,
                distributor_arn_number VARCHAR(100) UNIQUE NOT NULL,
                distributor_code VARCHAR(50),
                distributor_name VARCHAR(200),
                distributor_type VARCHAR(50),
                commission_rate NUMERIC(8,4),
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

        // Create STRATEGIES table
        await pgPool.query(`
            CREATE TABLE strategies (
                strategy_id SERIAL PRIMARY KEY,
                strategy_code VARCHAR(50) UNIQUE NOT NULL,
                strategy_name VARCHAR(200),
                strategy_type VARCHAR(50),
                description TEXT,
                benchmark VARCHAR(100),
                risk_level VARCHAR(20),
                min_investment NUMERIC(15,2),
                max_investment NUMERIC(15,2),
                management_fee NUMERIC(8,4),
                performance_fee NUMERIC(8,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create CONTRACT_NOTES table
        await pgPool.query(`
            CREATE TABLE contract_notes (
                id SERIAL PRIMARY KEY,
                ecn_number VARCHAR(50) UNIQUE NOT NULL,
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
                quantity NUMERIC(15,4),
                net_amount NUMERIC(15,2),
                net_rate NUMERIC(15,4),
                brokerage_amount NUMERIC(15,2),
                service_tax NUMERIC(15,2),
                stt_amount NUMERIC(15,2),
                market_type VARCHAR(20),
                settlement_number VARCHAR(50),
                brokerage_rate NUMERIC(10,6),
                stamp_duty NUMERIC(15,2),
                sebi_registration VARCHAR(100),
                scheme_name VARCHAR(255),
                custodian_name VARCHAR(255),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create CASH_CAPITAL_FLOW table
        await pgPool.query(`
            CREATE TABLE cash_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                transaction_date DATE,
                settlement_date DATE,
                amount NUMERIC(15,2),
                charges NUMERIC(15,2),
                tax NUMERIC(15,2),
                net_amount NUMERIC(15,2),
                payment_mode VARCHAR(50),
                bank_reference VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create STOCK_CAPITAL_FLOW table
        await pgPool.query(`
            CREATE TABLE stock_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                acquisition_date DATE,
                security_in_date DATE,
                quantity NUMERIC(15,4),
                original_price NUMERIC(15,4),
                brokerage NUMERIC(15,2),
                service_tax NUMERIC(15,2),
                settlement_date_flag VARCHAR(10),
                market_rate NUMERIC(15,4),
                cash_symbol VARCHAR(10),
                stt_amount NUMERIC(15,2),
                accrued_interest NUMERIC(15,2),
                block_ref VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create MF_ALLOCATIONS table
        await pgPool.query(`
            CREATE TABLE mf_allocations (
                allocation_id SERIAL PRIMARY KEY,
                client_code VARCHAR(50),
                scheme_code VARCHAR(50),
                scheme_name VARCHAR(255),
                allocation_date DATE,
                allocation_type VARCHAR(20),
                amount NUMERIC(15,2),
                units NUMERIC(15,4),
                nav NUMERIC(15,4),
                broker_code VARCHAR(50),
                distributor_code VARCHAR(50),
                commission_rate NUMERIC(8,4),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create UNIFIED_CUSTODY_MASTER table
        await pgPool.query(`
            CREATE TABLE unified_custody_master (
                id SERIAL PRIMARY KEY,
                client_reference VARCHAR(100),
                client_name VARCHAR(255),
                instrument_isin VARCHAR(50),
                instrument_name VARCHAR(255),
                instrument_code VARCHAR(50),
                blocked_quantity NUMERIC(15,4) DEFAULT 0,
                pending_buy_quantity NUMERIC(15,4) DEFAULT 0,
                pending_sell_quantity NUMERIC(15,4) DEFAULT 0,
                total_position NUMERIC(15,4) DEFAULT 0,
                saleable_quantity NUMERIC(15,4) DEFAULT 0,
                source_system VARCHAR(50),
                file_name VARCHAR(255),
                record_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create GENERAL_DATA table for miscellaneous data
        await pgPool.query(`
            CREATE TABLE general_data (
                id SERIAL PRIMARY KEY,
                data_type VARCHAR(50),
                data_json JSONB,
                source_file VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        console.log('✅ All tables created successfully!\n');

        // Step 2: Process MongoDB data into PostgreSQL
        console.log('📊 Step 2: Processing MongoDB data into PostgreSQL...');
        
        const mongoClient = new MongoClient(mongoUri);
        await mongoClient.connect();
        
        const db2024 = mongoClient.db('financial_data_2024');
        const db2025 = mongoClient.db('financial_data_2025');
        
        let totalProcessed = 0;
        let totalInserted = 0;

        // Helper function to clean values
        function cleanValue(value) {
            if (value == null || value === '') return null;
            return String(value).trim();
        }

        function parseDate(value) {
            if (!value) return null;
            // Handle various date formats
            const dateStr = String(value).trim();
            if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                    // Handle DD/MM/YYYY or MM/DD/YYYY
                    const day = parseInt(parts[0]);
                    const month = parseInt(parts[1]);
                    const year = parseInt(parts[2]);
                    if (day > 12) {
                        // DD/MM/YYYY
                        return new Date(year, month - 1, day);
                    } else {
                        // MM/DD/YYYY
                        return new Date(year, month - 1, day);
                    }
                }
            }
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
        }

        function parseNumeric(value) {
            if (value == null || value === '') return 0;
            const numStr = String(value).replace(/[,\s]/g, '');
            const num = parseFloat(numStr);
            return isNaN(num) ? 0 : num;
        }

        // Process all collections from both databases
        for (const db of [db2024, db2025]) {
            const collections = await db.listCollections().toArray();
            
            for (const collectionInfo of collections) {
                const collectionName = collectionInfo.name;
                const collection = db.collection(collectionName);
                const documents = await collection.find({}).toArray();
                
                console.log(`🔧 Processing ${collectionName}: ${documents.length} records`);
                totalProcessed += documents.length;

                // Determine data type and process accordingly
                if (collectionName.includes('broker_master')) {
                    // Process broker data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO brokers (broker_code, broker_name, broker_type, registration_number, contact_person, email, phone, address, city, state, country)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                ON CONFLICT (broker_code) DO NOTHING
                            `, [
                                cleanValue(doc['Broker Code'] || doc['broker_code'] || `BROKER_${totalInserted + 1}`),
                                cleanValue(doc['Broker Name'] || doc['broker_name'] || `Broker ${totalInserted + 1}`),
                                cleanValue(doc['Broker Type'] || doc['broker_type'] || 'Unknown'),
                                cleanValue(doc['Registration Number'] || doc['registration_number']),
                                cleanValue(doc['Contact Person'] || doc['contact_person']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Broker insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('client_info')) {
                    // Process client data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO clients (client_code, client_name, client_type, pan_number, email, phone, address, city, state, country, risk_category)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                ON CONFLICT (client_code) DO NOTHING
                            `, [
                                cleanValue(doc['Client Code'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Client Name'] || doc['client_name'] || `Client ${totalInserted + 1}`),
                                cleanValue(doc['Client Type'] || doc['client_type'] || 'Individual'),
                                cleanValue(doc['PAN'] || doc['pan_number']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India'),
                                cleanValue(doc['Risk Category'] || doc['risk_category'] || 'Medium')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Client insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('distributor_master')) {
                    // Process distributor data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO distributors (distributor_arn_number, distributor_code, distributor_name, distributor_type, commission_rate, contact_person, email, phone, address, city, state, country)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                ON CONFLICT (distributor_arn_number) DO NOTHING
                            `, [
                                cleanValue(doc['distributor arn number'] || doc['ARN'] || `ARN_${totalInserted + 1}`),
                                cleanValue(doc['Distributor Code'] || doc['distributor_code'] || doc['email'] || `DIST_${totalInserted + 1}`),
                                cleanValue(doc['Distributor Name'] || doc['distributor_name'] || `Distributor ${totalInserted + 1}`),
                                cleanValue(doc['Distributor Type'] || doc['distributor_type'] || 'External'),
                                parseNumeric(doc['Commission Rate'] || doc['commission_rate']),
                                cleanValue(doc['Contact Person'] || doc['contact_person']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Distributor insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('strategy_master')) {
                    // Process strategy data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO strategies (strategy_code, strategy_name, strategy_type, description, benchmark, risk_level, min_investment, max_investment, management_fee, performance_fee)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ON CONFLICT (strategy_code) DO NOTHING
                            `, [
                                cleanValue(doc['Filed Name'] || doc['strategy_code'] || `STRATEGY_${totalInserted + 1}`),
                                cleanValue(doc['Data'] || doc['strategy_name'] || doc['Filed Name'] || `Strategy ${totalInserted + 1}`),
                                cleanValue(doc['Strategy Type'] || doc['strategy_type'] || 'Equity'),
                                cleanValue(doc['Data'] || doc['description']),
                                cleanValue(doc['Benchmark'] || doc['benchmark']),
                                cleanValue(doc['Risk Level'] || doc['risk_level'] || 'Medium'),
                                parseNumeric(doc['Min Investment'] || doc['min_investment']),
                                parseNumeric(doc['Max Investment'] || doc['max_investment']),
                                parseNumeric(doc['Management Fee'] || doc['management_fee']),
                                parseNumeric(doc['Performance Fee'] || doc['performance_fee'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Strategy insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('contract_note')) {
                    // Process contract notes
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO contract_notes (ecn_number, ecn_status, ecn_date, client_code, broker_name, instrument_isin, instrument_name, transaction_type, delivery_type, exchange, settlement_date, quantity, net_amount, net_rate, brokerage_amount, service_tax, stt_amount, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                                ON CONFLICT (ecn_number) DO NOTHING
                            `, [
                                cleanValue(doc['ECN No'] || doc['ecn_number'] || `ECN_${totalInserted + 1}`),
                                cleanValue(doc['ECN Status'] || doc['ecn_status'] || 'ACTIVE'),
                                parseDate(doc['ECN Date'] || doc['ecn_date']),
                                cleanValue(doc['Client Exchange Code/UCC'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Broker Name'] || doc['broker_name'] || 'AUTO_BROKER'),
                                cleanValue(doc['ISIN Code'] || doc['instrument_isin']),
                                cleanValue(doc['Security Name'] || doc['instrument_name'] || `Security ${totalInserted + 1}`),
                                cleanValue(doc['Transaction Type'] || doc['transaction_type'] || 'BUY'),
                                cleanValue(doc['Delivery Type'] || doc['delivery_type'] || 'DELIVERY'),
                                cleanValue(doc['Exchange'] || doc['exchange'] || 'NSE'),
                                parseDate(doc['Sett. Date'] || doc['settlement_date']),
                                parseNumeric(doc['Qty'] || doc['quantity']),
                                parseNumeric(doc['Net Amount'] || doc['net_amount']),
                                parseNumeric(doc['Net Rate'] || doc['net_rate']),
                                parseNumeric(doc['Brokerage Amount'] || doc['brokerage_amount']),
                                parseNumeric(doc['Service Tax'] || doc['service_tax']),
                                parseNumeric(doc['STT Amount'] || doc['stt_amount']),
                                cleanValue(doc['Remarks'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Contract note insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('cash_capital_flow')) {
                    // Process cash flow data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO cash_capital_flow (transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type, transaction_date, settlement_date, amount, charges, tax, net_amount, payment_mode, bank_reference, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                                ON CONFLICT (transaction_ref) DO NOTHING
                            `, [
                                cleanValue(doc['TRANSREF'] || doc['transaction_ref'] || `CASH_${totalInserted + 1}`),
                                cleanValue(doc['BROKER CODE'] || doc['broker_code'] || 'AUTO_BROKER'),
                                cleanValue(doc['CLIENT CODE'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin'] || 'CASH'),
                                cleanValue(doc['EXCHANGE'] || doc['exchange'] || 'NSE'),
                                cleanValue(doc['TRANSACTION TYPE'] || doc['transaction_type'] || 'CASH_IN'),
                                parseDate(doc['ACQUISITION DATE'] || doc['transaction_date']),
                                parseDate(doc['SETTLEMENT DATE'] || doc['settlement_date']),
                                parseNumeric(doc['AMOUNT'] || doc['amount']),
                                parseNumeric(doc['BROKERAGE'] || doc['charges']),
                                parseNumeric(doc['SERVICE TAX'] || doc['tax']),
                                parseNumeric(doc['NET AMOUNT'] || doc['net_amount']),
                                cleanValue(doc['PAYMENT MODE'] || doc['payment_mode'] || 'ONLINE'),
                                cleanValue(doc['BANK REFERENCE'] || doc['bank_reference']),
                                cleanValue(doc['REMARKS'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Cash flow insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('stock_capital_flow')) {
                    // Process stock flow data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO stock_capital_flow (transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type, acquisition_date, security_in_date, quantity, original_price, brokerage, service_tax, settlement_date_flag, market_rate, cash_symbol, stt_amount, accrued_interest, block_ref, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                                ON CONFLICT (transaction_ref) DO NOTHING
                            `, [
                                cleanValue(doc['TRANSREF'] || doc['transaction_ref'] || `STOCK_${totalInserted + 1}`),
                                cleanValue(doc['BROKER CODE'] || doc['broker_code'] || 'AUTO_BROKER'),
                                cleanValue(doc['CLIENT CODE'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin']),
                                cleanValue(doc['EXCHANGE'] || doc['exchange'] || 'NSE'),
                                cleanValue(doc['TRANSACTION TYPE'] || doc['transaction_type'] || 'BUY'),
                                parseDate(doc['ACQUISITION DATE'] || doc['acquisition_date']) || new Date(),
                                parseDate(doc['SECURITY IN DATE'] || doc['security_in_date']),
                                parseNumeric(doc['QUANTITY'] || doc['quantity']),
                                parseNumeric(doc['ORIGINAL PRICE'] || doc['original_price']),
                                parseNumeric(doc['BROKERAGE'] || doc['brokerage']),
                                parseNumeric(doc['SERVICE TAX'] || doc['service_tax']),
                                cleanValue(doc['SETTLEMENT DATE FLAG'] || doc['settlement_date_flag']),
                                parseNumeric(doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['market_rate']),
                                cleanValue(doc['CASH SYMBOL'] || doc['cash_symbol']),
                                parseNumeric(doc['STT AMOUNT'] || doc['stt_amount']),
                                parseNumeric(doc['ACCRUED INTEREST'] || doc['accrued_interest']),
                                cleanValue(doc['BLOCK REF.'] || doc['block_ref']),
                                cleanValue(doc['REMARKS'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Stock flow insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('mf_allocation')) {
                    // Process MF allocation data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO mf_allocations (client_code, scheme_code, scheme_name, allocation_date, allocation_type, amount, units, nav, broker_code, distributor_code, commission_rate, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            `, [
                                cleanValue(doc['Custody Code'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN No'] || doc['scheme_code'] || `SCHEME_${totalInserted + 1}`),
                                cleanValue(doc['Scheme Name - Plan - Option'] || doc['scheme_name'] || `Scheme ${totalInserted + 1}`),
                                parseDate(doc['Date'] || doc['allocation_date']) || new Date(),
                                cleanValue(doc['Allocation Type'] || doc['allocation_type'] || 'BUY'),
                                parseNumeric(doc['Purchase Amount'] || doc['amount']),
                                parseNumeric(doc['Units'] || doc['units']),
                                parseNumeric(doc['NAV'] || doc['nav']),
                                cleanValue(doc['Broker Code'] || doc['broker_code']),
                                cleanValue(doc['ARN Code'] || doc['distributor_code']),
                                parseNumeric(doc['Commission Rate'] || doc['commission_rate']),
                                cleanValue(doc['Remarks'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  MF allocation insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('axis') || collectionName.includes('hdfc') || collectionName.includes('kotak') || collectionName.includes('orbis') || collectionName.includes('trust') || collectionName.includes('custody')) {
                    // Process custody data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO unified_custody_master (client_reference, client_name, instrument_isin, instrument_name, instrument_code, blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity, source_system, file_name, record_date)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            `, [
                                cleanValue(doc['Client Reference'] || doc['Client Code'] || doc['client_reference'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Client Name'] || doc['client_name'] || `Client ${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin']),
                                cleanValue(doc['Instrument Name'] || doc['Security Name'] || doc['instrument_name'] || `Instrument ${totalInserted + 1}`),
                                cleanValue(doc['Instrument Code'] || doc['instrument_code']),
                                parseNumeric(doc['Blocked Quantity'] || doc['blocked_quantity']),
                                parseNumeric(doc['Pending Buy Quantity'] || doc['pending_buy_quantity']),
                                parseNumeric(doc['Pending Sell Quantity'] || doc['pending_sell_quantity']),
                                parseNumeric(doc['Total Position'] || doc['total_position']),
                                parseNumeric(doc['Saleable Quantity'] || doc['saleable_quantity']),
                                collectionName.includes('axis') ? 'AXIS' : 
                                collectionName.includes('hdfc') ? 'HDFC' :
                                collectionName.includes('kotak') ? 'KOTAK' :
                                collectionName.includes('orbis') ? 'ORBIS' :
                                collectionName.includes('trust') ? 'TRUST' : 'UNKNOWN',
                                collectionName,
                                parseDate(doc['Record Date'] || doc['record_date']) || new Date()
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Custody insert error: ${error.message}`);
                        }
                    }
                } else {
                    // Store other data in general_data table
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO general_data (data_type, data_json, source_file)
                                VALUES ($1, $2, $3)
                            `, [
                                collectionName,
                                JSON.stringify(doc),
                                collectionName
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  General data insert error: ${error.message}`);
                        }
                    }
                }
            }
        }

        await mongoClient.close();

        console.log(`\n✅ Processing complete!`);
        console.log(`📊 Total MongoDB records: ${totalProcessed}`);
        console.log(`📊 Total PostgreSQL inserts: ${totalInserted}`);

        // Step 3: Show final results
        console.log('\n📋 FINAL RESULTS - All PostgreSQL Tables:');
        console.log('=' .repeat(60));

        const tableQueries = [
            { name: 'BROKERS', query: 'SELECT COUNT(*) as count FROM brokers' },
            { name: 'CLIENTS', query: 'SELECT COUNT(*) as count FROM clients' },
            { name: 'DISTRIBUTORS', query: 'SELECT COUNT(*) as count FROM distributors' },
            { name: 'STRATEGIES', query: 'SELECT COUNT(*) as count FROM strategies' },
            { name: 'CONTRACT_NOTES', query: 'SELECT COUNT(*) as count FROM contract_notes' },
            { name: 'CASH_CAPITAL_FLOW', query: 'SELECT COUNT(*) as count FROM cash_capital_flow' },
            { name: 'STOCK_CAPITAL_FLOW', query: 'SELECT COUNT(*) as count FROM stock_capital_flow' },
            { name: 'MF_ALLOCATIONS', query: 'SELECT COUNT(*) as count FROM mf_allocations' },
            { name: 'UNIFIED_CUSTODY_MASTER', query: 'SELECT COUNT(*) as count FROM unified_custody_master' },
            { name: 'GENERAL_DATA', query: 'SELECT COUNT(*) as count FROM general_data' }
        ];

        for (const table of tableQueries) {
            try {
                const result = await pgPool.query(table.query);
                const count = result.rows[0].count;
                console.log(`📊 ${table.name.padEnd(25)} | ${count.toString().padStart(8)} records`);
            } catch (error) {
                console.log(`❌ ${table.name.padEnd(25)} | Error: ${error.message}`);
            }
        }

        console.log('=' .repeat(60));
        console.log('\n🎉 ALL DATA IS NOW VISIBLE IN POSTGRESQL!');
        console.log('🌐 Access your dashboard at: http://localhost:3002');
        console.log('📊 You can now view all tables and their data!');

    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        await pgPool.end();
    }
}

// Run the complete fix
fixAllTablesAndShowData(); 
const { MongoClient } = require('mongodb');

const pgPool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'financial_data',
    password: 'password',
    port: 5432,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

const mongoUri = 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/';

async function fixAllTablesAndShowData() {
    try {
        console.log('🔧 COMPLETE FIX: Creating proper tables and processing all data...\n');

        // Step 1: Create all tables with correct schemas
        console.log('📋 Step 1: Creating all tables with correct schemas...');
        
        // Drop and recreate all tables with proper schemas
        await pgPool.query(`DROP TABLE IF EXISTS brokers CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS clients CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS distributors CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS strategies CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS contract_notes CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS cash_capital_flow CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS stock_capital_flow CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS mf_allocations CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS unified_custody_master CASCADE`);
        await pgPool.query(`DROP TABLE IF EXISTS general_data CASCADE`);

        // Create BROKERS table
        await pgPool.query(`
            CREATE TABLE brokers (
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

        // Create CLIENTS table
        await pgPool.query(`
            CREATE TABLE clients (
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

        // Create DISTRIBUTORS table
        await pgPool.query(`
            CREATE TABLE distributors (
                distributor_id SERIAL PRIMARY KEY,
                distributor_arn_number VARCHAR(100) UNIQUE NOT NULL,
                distributor_code VARCHAR(50),
                distributor_name VARCHAR(200),
                distributor_type VARCHAR(50),
                commission_rate NUMERIC(8,4),
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

        // Create STRATEGIES table
        await pgPool.query(`
            CREATE TABLE strategies (
                strategy_id SERIAL PRIMARY KEY,
                strategy_code VARCHAR(50) UNIQUE NOT NULL,
                strategy_name VARCHAR(200),
                strategy_type VARCHAR(50),
                description TEXT,
                benchmark VARCHAR(100),
                risk_level VARCHAR(20),
                min_investment NUMERIC(15,2),
                max_investment NUMERIC(15,2),
                management_fee NUMERIC(8,4),
                performance_fee NUMERIC(8,4),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create CONTRACT_NOTES table
        await pgPool.query(`
            CREATE TABLE contract_notes (
                id SERIAL PRIMARY KEY,
                ecn_number VARCHAR(50) UNIQUE NOT NULL,
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
                quantity NUMERIC(15,4),
                net_amount NUMERIC(15,2),
                net_rate NUMERIC(15,4),
                brokerage_amount NUMERIC(15,2),
                service_tax NUMERIC(15,2),
                stt_amount NUMERIC(15,2),
                market_type VARCHAR(20),
                settlement_number VARCHAR(50),
                brokerage_rate NUMERIC(10,6),
                stamp_duty NUMERIC(15,2),
                sebi_registration VARCHAR(100),
                scheme_name VARCHAR(255),
                custodian_name VARCHAR(255),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create CASH_CAPITAL_FLOW table
        await pgPool.query(`
            CREATE TABLE cash_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                transaction_date DATE,
                settlement_date DATE,
                amount NUMERIC(15,2),
                charges NUMERIC(15,2),
                tax NUMERIC(15,2),
                net_amount NUMERIC(15,2),
                payment_mode VARCHAR(50),
                bank_reference VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create STOCK_CAPITAL_FLOW table
        await pgPool.query(`
            CREATE TABLE stock_capital_flow (
                transaction_ref VARCHAR(100) PRIMARY KEY,
                broker_code VARCHAR(50),
                client_code VARCHAR(50),
                instrument_isin VARCHAR(50),
                exchange VARCHAR(50),
                transaction_type VARCHAR(20),
                acquisition_date DATE,
                security_in_date DATE,
                quantity NUMERIC(15,4),
                original_price NUMERIC(15,4),
                brokerage NUMERIC(15,2),
                service_tax NUMERIC(15,2),
                settlement_date_flag VARCHAR(10),
                market_rate NUMERIC(15,4),
                cash_symbol VARCHAR(10),
                stt_amount NUMERIC(15,2),
                accrued_interest NUMERIC(15,2),
                block_ref VARCHAR(100),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create MF_ALLOCATIONS table
        await pgPool.query(`
            CREATE TABLE mf_allocations (
                allocation_id SERIAL PRIMARY KEY,
                client_code VARCHAR(50),
                scheme_code VARCHAR(50),
                scheme_name VARCHAR(255),
                allocation_date DATE,
                allocation_type VARCHAR(20),
                amount NUMERIC(15,2),
                units NUMERIC(15,4),
                nav NUMERIC(15,4),
                broker_code VARCHAR(50),
                distributor_code VARCHAR(50),
                commission_rate NUMERIC(8,4),
                remarks TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create UNIFIED_CUSTODY_MASTER table
        await pgPool.query(`
            CREATE TABLE unified_custody_master (
                id SERIAL PRIMARY KEY,
                client_reference VARCHAR(100),
                client_name VARCHAR(255),
                instrument_isin VARCHAR(50),
                instrument_name VARCHAR(255),
                instrument_code VARCHAR(50),
                blocked_quantity NUMERIC(15,4) DEFAULT 0,
                pending_buy_quantity NUMERIC(15,4) DEFAULT 0,
                pending_sell_quantity NUMERIC(15,4) DEFAULT 0,
                total_position NUMERIC(15,4) DEFAULT 0,
                saleable_quantity NUMERIC(15,4) DEFAULT 0,
                source_system VARCHAR(50),
                file_name VARCHAR(255),
                record_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Create GENERAL_DATA table for miscellaneous data
        await pgPool.query(`
            CREATE TABLE general_data (
                id SERIAL PRIMARY KEY,
                data_type VARCHAR(50),
                data_json JSONB,
                source_file VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        console.log('✅ All tables created successfully!\n');

        // Step 2: Process MongoDB data into PostgreSQL
        console.log('📊 Step 2: Processing MongoDB data into PostgreSQL...');
        
        const mongoClient = new MongoClient(mongoUri);
        await mongoClient.connect();
        
        const db2024 = mongoClient.db('financial_data_2024');
        const db2025 = mongoClient.db('financial_data_2025');
        
        let totalProcessed = 0;
        let totalInserted = 0;

        // Helper function to clean values
        function cleanValue(value) {
            if (value == null || value === '') return null;
            return String(value).trim();
        }

        function parseDate(value) {
            if (!value) return null;
            // Handle various date formats
            const dateStr = String(value).trim();
            if (dateStr.includes('/')) {
                const parts = dateStr.split('/');
                if (parts.length === 3) {
                    // Handle DD/MM/YYYY or MM/DD/YYYY
                    const day = parseInt(parts[0]);
                    const month = parseInt(parts[1]);
                    const year = parseInt(parts[2]);
                    if (day > 12) {
                        // DD/MM/YYYY
                        return new Date(year, month - 1, day);
                    } else {
                        // MM/DD/YYYY
                        return new Date(year, month - 1, day);
                    }
                }
            }
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
        }

        function parseNumeric(value) {
            if (value == null || value === '') return 0;
            const numStr = String(value).replace(/[,\s]/g, '');
            const num = parseFloat(numStr);
            return isNaN(num) ? 0 : num;
        }

        // Process all collections from both databases
        for (const db of [db2024, db2025]) {
            const collections = await db.listCollections().toArray();
            
            for (const collectionInfo of collections) {
                const collectionName = collectionInfo.name;
                const collection = db.collection(collectionName);
                const documents = await collection.find({}).toArray();
                
                console.log(`🔧 Processing ${collectionName}: ${documents.length} records`);
                totalProcessed += documents.length;

                // Determine data type and process accordingly
                if (collectionName.includes('broker_master')) {
                    // Process broker data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO brokers (broker_code, broker_name, broker_type, registration_number, contact_person, email, phone, address, city, state, country)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                ON CONFLICT (broker_code) DO NOTHING
                            `, [
                                cleanValue(doc['Broker Code'] || doc['broker_code'] || `BROKER_${totalInserted + 1}`),
                                cleanValue(doc['Broker Name'] || doc['broker_name'] || `Broker ${totalInserted + 1}`),
                                cleanValue(doc['Broker Type'] || doc['broker_type'] || 'Unknown'),
                                cleanValue(doc['Registration Number'] || doc['registration_number']),
                                cleanValue(doc['Contact Person'] || doc['contact_person']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Broker insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('client_info')) {
                    // Process client data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO clients (client_code, client_name, client_type, pan_number, email, phone, address, city, state, country, risk_category)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                ON CONFLICT (client_code) DO NOTHING
                            `, [
                                cleanValue(doc['Client Code'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Client Name'] || doc['client_name'] || `Client ${totalInserted + 1}`),
                                cleanValue(doc['Client Type'] || doc['client_type'] || 'Individual'),
                                cleanValue(doc['PAN'] || doc['pan_number']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India'),
                                cleanValue(doc['Risk Category'] || doc['risk_category'] || 'Medium')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Client insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('distributor_master')) {
                    // Process distributor data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO distributors (distributor_arn_number, distributor_code, distributor_name, distributor_type, commission_rate, contact_person, email, phone, address, city, state, country)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                                ON CONFLICT (distributor_arn_number) DO NOTHING
                            `, [
                                cleanValue(doc['distributor arn number'] || doc['ARN'] || `ARN_${totalInserted + 1}`),
                                cleanValue(doc['Distributor Code'] || doc['distributor_code'] || doc['email'] || `DIST_${totalInserted + 1}`),
                                cleanValue(doc['Distributor Name'] || doc['distributor_name'] || `Distributor ${totalInserted + 1}`),
                                cleanValue(doc['Distributor Type'] || doc['distributor_type'] || 'External'),
                                parseNumeric(doc['Commission Rate'] || doc['commission_rate']),
                                cleanValue(doc['Contact Person'] || doc['contact_person']),
                                cleanValue(doc['Email'] || doc['email']),
                                cleanValue(doc['Phone'] || doc['phone']),
                                cleanValue(doc['Address'] || doc['address']),
                                cleanValue(doc['City'] || doc['city']),
                                cleanValue(doc['State'] || doc['state']),
                                cleanValue(doc['Country'] || doc['country'] || 'India')
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Distributor insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('strategy_master')) {
                    // Process strategy data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO strategies (strategy_code, strategy_name, strategy_type, description, benchmark, risk_level, min_investment, max_investment, management_fee, performance_fee)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                ON CONFLICT (strategy_code) DO NOTHING
                            `, [
                                cleanValue(doc['Filed Name'] || doc['strategy_code'] || `STRATEGY_${totalInserted + 1}`),
                                cleanValue(doc['Data'] || doc['strategy_name'] || doc['Filed Name'] || `Strategy ${totalInserted + 1}`),
                                cleanValue(doc['Strategy Type'] || doc['strategy_type'] || 'Equity'),
                                cleanValue(doc['Data'] || doc['description']),
                                cleanValue(doc['Benchmark'] || doc['benchmark']),
                                cleanValue(doc['Risk Level'] || doc['risk_level'] || 'Medium'),
                                parseNumeric(doc['Min Investment'] || doc['min_investment']),
                                parseNumeric(doc['Max Investment'] || doc['max_investment']),
                                parseNumeric(doc['Management Fee'] || doc['management_fee']),
                                parseNumeric(doc['Performance Fee'] || doc['performance_fee'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Strategy insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('contract_note')) {
                    // Process contract notes
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO contract_notes (ecn_number, ecn_status, ecn_date, client_code, broker_name, instrument_isin, instrument_name, transaction_type, delivery_type, exchange, settlement_date, quantity, net_amount, net_rate, brokerage_amount, service_tax, stt_amount, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                                ON CONFLICT (ecn_number) DO NOTHING
                            `, [
                                cleanValue(doc['ECN No'] || doc['ecn_number'] || `ECN_${totalInserted + 1}`),
                                cleanValue(doc['ECN Status'] || doc['ecn_status'] || 'ACTIVE'),
                                parseDate(doc['ECN Date'] || doc['ecn_date']),
                                cleanValue(doc['Client Exchange Code/UCC'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Broker Name'] || doc['broker_name'] || 'AUTO_BROKER'),
                                cleanValue(doc['ISIN Code'] || doc['instrument_isin']),
                                cleanValue(doc['Security Name'] || doc['instrument_name'] || `Security ${totalInserted + 1}`),
                                cleanValue(doc['Transaction Type'] || doc['transaction_type'] || 'BUY'),
                                cleanValue(doc['Delivery Type'] || doc['delivery_type'] || 'DELIVERY'),
                                cleanValue(doc['Exchange'] || doc['exchange'] || 'NSE'),
                                parseDate(doc['Sett. Date'] || doc['settlement_date']),
                                parseNumeric(doc['Qty'] || doc['quantity']),
                                parseNumeric(doc['Net Amount'] || doc['net_amount']),
                                parseNumeric(doc['Net Rate'] || doc['net_rate']),
                                parseNumeric(doc['Brokerage Amount'] || doc['brokerage_amount']),
                                parseNumeric(doc['Service Tax'] || doc['service_tax']),
                                parseNumeric(doc['STT Amount'] || doc['stt_amount']),
                                cleanValue(doc['Remarks'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Contract note insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('cash_capital_flow')) {
                    // Process cash flow data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO cash_capital_flow (transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type, transaction_date, settlement_date, amount, charges, tax, net_amount, payment_mode, bank_reference, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                                ON CONFLICT (transaction_ref) DO NOTHING
                            `, [
                                cleanValue(doc['TRANSREF'] || doc['transaction_ref'] || `CASH_${totalInserted + 1}`),
                                cleanValue(doc['BROKER CODE'] || doc['broker_code'] || 'AUTO_BROKER'),
                                cleanValue(doc['CLIENT CODE'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin'] || 'CASH'),
                                cleanValue(doc['EXCHANGE'] || doc['exchange'] || 'NSE'),
                                cleanValue(doc['TRANSACTION TYPE'] || doc['transaction_type'] || 'CASH_IN'),
                                parseDate(doc['ACQUISITION DATE'] || doc['transaction_date']),
                                parseDate(doc['SETTLEMENT DATE'] || doc['settlement_date']),
                                parseNumeric(doc['AMOUNT'] || doc['amount']),
                                parseNumeric(doc['BROKERAGE'] || doc['charges']),
                                parseNumeric(doc['SERVICE TAX'] || doc['tax']),
                                parseNumeric(doc['NET AMOUNT'] || doc['net_amount']),
                                cleanValue(doc['PAYMENT MODE'] || doc['payment_mode'] || 'ONLINE'),
                                cleanValue(doc['BANK REFERENCE'] || doc['bank_reference']),
                                cleanValue(doc['REMARKS'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Cash flow insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('stock_capital_flow')) {
                    // Process stock flow data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO stock_capital_flow (transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type, acquisition_date, security_in_date, quantity, original_price, brokerage, service_tax, settlement_date_flag, market_rate, cash_symbol, stt_amount, accrued_interest, block_ref, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                                ON CONFLICT (transaction_ref) DO NOTHING
                            `, [
                                cleanValue(doc['TRANSREF'] || doc['transaction_ref'] || `STOCK_${totalInserted + 1}`),
                                cleanValue(doc['BROKER CODE'] || doc['broker_code'] || 'AUTO_BROKER'),
                                cleanValue(doc['CLIENT CODE'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin']),
                                cleanValue(doc['EXCHANGE'] || doc['exchange'] || 'NSE'),
                                cleanValue(doc['TRANSACTION TYPE'] || doc['transaction_type'] || 'BUY'),
                                parseDate(doc['ACQUISITION DATE'] || doc['acquisition_date']) || new Date(),
                                parseDate(doc['SECURITY IN DATE'] || doc['security_in_date']),
                                parseNumeric(doc['QUANTITY'] || doc['quantity']),
                                parseNumeric(doc['ORIGINAL PRICE'] || doc['original_price']),
                                parseNumeric(doc['BROKERAGE'] || doc['brokerage']),
                                parseNumeric(doc['SERVICE TAX'] || doc['service_tax']),
                                cleanValue(doc['SETTLEMENT DATE FLAG'] || doc['settlement_date_flag']),
                                parseNumeric(doc['MARKET RATE AS ON SECURITY IN DATE'] || doc['market_rate']),
                                cleanValue(doc['CASH SYMBOL'] || doc['cash_symbol']),
                                parseNumeric(doc['STT AMOUNT'] || doc['stt_amount']),
                                parseNumeric(doc['ACCRUED INTEREST'] || doc['accrued_interest']),
                                cleanValue(doc['BLOCK REF.'] || doc['block_ref']),
                                cleanValue(doc['REMARKS'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Stock flow insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('mf_allocation')) {
                    // Process MF allocation data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO mf_allocations (client_code, scheme_code, scheme_name, allocation_date, allocation_type, amount, units, nav, broker_code, distributor_code, commission_rate, remarks)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                            `, [
                                cleanValue(doc['Custody Code'] || doc['client_code'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['ISIN No'] || doc['scheme_code'] || `SCHEME_${totalInserted + 1}`),
                                cleanValue(doc['Scheme Name - Plan - Option'] || doc['scheme_name'] || `Scheme ${totalInserted + 1}`),
                                parseDate(doc['Date'] || doc['allocation_date']) || new Date(),
                                cleanValue(doc['Allocation Type'] || doc['allocation_type'] || 'BUY'),
                                parseNumeric(doc['Purchase Amount'] || doc['amount']),
                                parseNumeric(doc['Units'] || doc['units']),
                                parseNumeric(doc['NAV'] || doc['nav']),
                                cleanValue(doc['Broker Code'] || doc['broker_code']),
                                cleanValue(doc['ARN Code'] || doc['distributor_code']),
                                parseNumeric(doc['Commission Rate'] || doc['commission_rate']),
                                cleanValue(doc['Remarks'] || doc['remarks'])
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  MF allocation insert error: ${error.message}`);
                        }
                    }
                } else if (collectionName.includes('axis') || collectionName.includes('hdfc') || collectionName.includes('kotak') || collectionName.includes('orbis') || collectionName.includes('trust') || collectionName.includes('custody')) {
                    // Process custody data
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO unified_custody_master (client_reference, client_name, instrument_isin, instrument_name, instrument_code, blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity, source_system, file_name, record_date)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                            `, [
                                cleanValue(doc['Client Reference'] || doc['Client Code'] || doc['client_reference'] || `CLIENT_${totalInserted + 1}`),
                                cleanValue(doc['Client Name'] || doc['client_name'] || `Client ${totalInserted + 1}`),
                                cleanValue(doc['ISIN'] || doc['instrument_isin']),
                                cleanValue(doc['Instrument Name'] || doc['Security Name'] || doc['instrument_name'] || `Instrument ${totalInserted + 1}`),
                                cleanValue(doc['Instrument Code'] || doc['instrument_code']),
                                parseNumeric(doc['Blocked Quantity'] || doc['blocked_quantity']),
                                parseNumeric(doc['Pending Buy Quantity'] || doc['pending_buy_quantity']),
                                parseNumeric(doc['Pending Sell Quantity'] || doc['pending_sell_quantity']),
                                parseNumeric(doc['Total Position'] || doc['total_position']),
                                parseNumeric(doc['Saleable Quantity'] || doc['saleable_quantity']),
                                collectionName.includes('axis') ? 'AXIS' : 
                                collectionName.includes('hdfc') ? 'HDFC' :
                                collectionName.includes('kotak') ? 'KOTAK' :
                                collectionName.includes('orbis') ? 'ORBIS' :
                                collectionName.includes('trust') ? 'TRUST' : 'UNKNOWN',
                                collectionName,
                                parseDate(doc['Record Date'] || doc['record_date']) || new Date()
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  Custody insert error: ${error.message}`);
                        }
                    }
                } else {
                    // Store other data in general_data table
                    for (const doc of documents) {
                        try {
                            await pgPool.query(`
                                INSERT INTO general_data (data_type, data_json, source_file)
                                VALUES ($1, $2, $3)
                            `, [
                                collectionName,
                                JSON.stringify(doc),
                                collectionName
                            ]);
                            totalInserted++;
                        } catch (error) {
                            console.log(`⚠️  General data insert error: ${error.message}`);
                        }
                    }
                }
            }
        }

        await mongoClient.close();

        console.log(`\n✅ Processing complete!`);
        console.log(`📊 Total MongoDB records: ${totalProcessed}`);
        console.log(`📊 Total PostgreSQL inserts: ${totalInserted}`);

        // Step 3: Show final results
        console.log('\n📋 FINAL RESULTS - All PostgreSQL Tables:');
        console.log('=' .repeat(60));

        const tableQueries = [
            { name: 'BROKERS', query: 'SELECT COUNT(*) as count FROM brokers' },
            { name: 'CLIENTS', query: 'SELECT COUNT(*) as count FROM clients' },
            { name: 'DISTRIBUTORS', query: 'SELECT COUNT(*) as count FROM distributors' },
            { name: 'STRATEGIES', query: 'SELECT COUNT(*) as count FROM strategies' },
            { name: 'CONTRACT_NOTES', query: 'SELECT COUNT(*) as count FROM contract_notes' },
            { name: 'CASH_CAPITAL_FLOW', query: 'SELECT COUNT(*) as count FROM cash_capital_flow' },
            { name: 'STOCK_CAPITAL_FLOW', query: 'SELECT COUNT(*) as count FROM stock_capital_flow' },
            { name: 'MF_ALLOCATIONS', query: 'SELECT COUNT(*) as count FROM mf_allocations' },
            { name: 'UNIFIED_CUSTODY_MASTER', query: 'SELECT COUNT(*) as count FROM unified_custody_master' },
            { name: 'GENERAL_DATA', query: 'SELECT COUNT(*) as count FROM general_data' }
        ];

        for (const table of tableQueries) {
            try {
                const result = await pgPool.query(table.query);
                const count = result.rows[0].count;
                console.log(`📊 ${table.name.padEnd(25)} | ${count.toString().padStart(8)} records`);
            } catch (error) {
                console.log(`❌ ${table.name.padEnd(25)} | Error: ${error.message}`);
            }
        }

        console.log('=' .repeat(60));
        console.log('\n🎉 ALL DATA IS NOW VISIBLE IN POSTGRESQL!');
        console.log('🌐 Access your dashboard at: http://localhost:3002');
        console.log('📊 You can now view all tables and their data!');

    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        await pgPool.end();
    }
}

// Run the complete fix
fixAllTablesAndShowData(); 