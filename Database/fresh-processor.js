const { MongoClient } = require('mongodb');
const { Pool } = require('pg');
const config = require('./config');

// Database connections
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

async function processAllData() {
  console.log('🚀 FRESH PROCESSING: MongoDB → PostgreSQL');
  console.log('═══════════════════════════════════════════════');
  
  const mongoClient = new MongoClient(config.mongodb.uri);
  
  try {
    // Connect to MongoDB
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB');
    
    // Connect to PostgreSQL
    const pgClient = await pgPool.connect();
    console.log('✅ Connected to PostgreSQL');
    
    // Get all databases and collections
    const databases = ['financial_data_2024', 'financial_data_2025', 'financial_data_2026'];
    let totalProcessed = 0;
    let totalValid = 0;
    
    for (const dbName of databases) {
      try {
        const db = mongoClient.db(dbName);
        const collections = await db.listCollections().toArray();
        
        if (collections.length === 0) continue;
        
        console.log(`\n📂 Processing database: ${dbName}`);
        console.log(`📋 Found ${collections.length} collections`);
        
        for (const collection of collections) {
          const collectionName = collection.name;
          console.log(`\n🔧 Processing: ${collectionName}`);
          
          try {
            const coll = db.collection(collectionName);
            const documents = await coll.find({}).toArray();
            
            if (documents.length === 0) {
              console.log(`⚪ ${collectionName}: No documents found`);
              continue;
            }
            
            totalProcessed += documents.length;
            
            // Determine table and schema based on collection name
            const { tableName, schema, insertQuery } = getTableConfig(collectionName, documents[0]);
            
            // Create table if not exists
            await pgClient.query(schema);
            console.log(`📅 Table ready: ${tableName}`);
            
            // Insert data in batches
            const batchSize = 100;
            let validCount = 0;
            
            for (let i = 0; i < documents.length; i += batchSize) {
              const batch = documents.slice(i, i + batchSize);
              
              for (const doc of batch) {
                try {
                  const values = extractValues(tableName, doc);
                  if (values && values.length > 0) {
                    await pgClient.query(insertQuery, values);
                    validCount++;
                  }
                } catch (error) {
                  console.log(`❌ Row error: ${error.message}`);
                }
              }
            }
            
            console.log(`✅ ${collectionName}: ${validCount}/${documents.length} records processed`);
            totalValid += validCount;
            
          } catch (error) {
            console.log(`❌ Collection error: ${error.message}`);
          }
        }
      } catch (error) {
        console.log(`❌ Database error for ${dbName}: ${error.message}`);
      }
    }
    
    pgClient.release();
    
    console.log('\n🎉 PROCESSING COMPLETE!');
    console.log('═══════════════════════════════════════════════');
    console.log(`📊 Total Processed: ${totalProcessed.toLocaleString()}`);
    console.log(`✅ Total Valid: ${totalValid.toLocaleString()}`);
    console.log(`🎯 Success Rate: ${totalProcessed > 0 ? Math.round((totalValid/totalProcessed)*100) : 0}%`);
    
  } catch (error) {
    console.error('💥 Processing failed:', error.message);
    throw error;
  } finally {
    await mongoClient.close();
    await pgPool.end();
  }
}

function getTableConfig(collectionName, sampleDoc) {
  const name = collectionName.toLowerCase();
  
  // Custody data
  if (name.includes('axis') || name.includes('hdfc') || name.includes('kotak') || 
      name.includes('orbis') || name.includes('deutsche') || name.includes('trustpms') || 
      name.includes('custody') || name.includes('holding')) {
    return {
      tableName: 'unified_custody_master',
      schema: `
        CREATE TABLE IF NOT EXISTS unified_custody_master (
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
        )`,
      insertQuery: `
        INSERT INTO unified_custody_master (
          client_reference, client_name, instrument_isin, instrument_name, instrument_code,
          blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
          source_system, file_name, record_date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
    };
  }
  
  // Contract notes
  if (name.includes('contract')) {
    return {
      tableName: 'contract_notes',
      schema: `
        CREATE TABLE IF NOT EXISTS contract_notes (
          ecn_number VARCHAR(50) PRIMARY KEY,
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
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO contract_notes (
          ecn_number, ecn_status, ecn_date, client_code, broker_name, instrument_isin, instrument_name,
          transaction_type, delivery_type, exchange, settlement_date, quantity, net_amount, net_rate,
          brokerage_amount, service_tax, stt_amount
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (ecn_number) DO NOTHING`
    };
  }
  
  // Cash capital flow
  if (name.includes('cash')) {
    return {
      tableName: 'cash_capital_flow',
      schema: `
        CREATE TABLE IF NOT EXISTS cash_capital_flow (
          transaction_ref VARCHAR(100) PRIMARY KEY,
          broker_code VARCHAR(50),
          client_code VARCHAR(50),
          transaction_type VARCHAR(50),
          transaction_date DATE,
          settlement_date DATE,
          amount DECIMAL(15,2),
          balance DECIMAL(15,2),
          narration TEXT,
          voucher_number VARCHAR(100),
          cheque_number VARCHAR(100),
          bank_name VARCHAR(255),
          branch_name VARCHAR(255),
          utr_number VARCHAR(100),
          payment_mode VARCHAR(50),
          currency VARCHAR(10),
          exchange_rate DECIMAL(10,4),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO cash_capital_flow (
          transaction_ref, broker_code, client_code, transaction_type, transaction_date, settlement_date,
          amount, balance, narration, voucher_number, cheque_number, bank_name, branch_name,
          utr_number, payment_mode, currency, exchange_rate, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT (transaction_ref) DO NOTHING`
    };
  }
  
  // Stock capital flow
  if (name.includes('stock')) {
    return {
      tableName: 'stock_capital_flow',
      schema: `
        CREATE TABLE IF NOT EXISTS stock_capital_flow (
          transaction_ref VARCHAR(100) PRIMARY KEY,
          broker_code VARCHAR(50),
          client_code VARCHAR(50),
          instrument_isin VARCHAR(50),
          exchange VARCHAR(50),
          transaction_type VARCHAR(50),
          acquisition_date DATE,
          security_in_date DATE,
          quantity DECIMAL(15,4),
          original_price DECIMAL(15,4),
          brokerage DECIMAL(15,2),
          service_tax DECIMAL(15,2),
          settlement_date_flag VARCHAR(10),
          market_rate DECIMAL(15,4),
          cash_symbol VARCHAR(20),
          stt_amount DECIMAL(15,2),
          accrued_interest DECIMAL(15,2),
          block_ref VARCHAR(100),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO stock_capital_flow (
          transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type,
          acquisition_date, security_in_date, quantity, original_price, brokerage, service_tax,
          settlement_date_flag, market_rate, cash_symbol, stt_amount, accrued_interest, block_ref, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ON CONFLICT (transaction_ref) DO NOTHING`
    };
  }
  
  // MF allocations
  if (name.includes('mf') || name.includes('allocation')) {
    return {
      tableName: 'mf_allocations',
      schema: `
        CREATE TABLE IF NOT EXISTS mf_allocations (
          allocation_id SERIAL PRIMARY KEY,
          client_code VARCHAR(50),
          scheme_code VARCHAR(50),
          scheme_name VARCHAR(255),
          folio_number VARCHAR(100),
          transaction_type VARCHAR(50),
          transaction_date DATE,
          nav_date DATE,
          nav_price DECIMAL(15,4),
          units DECIMAL(15,4),
          amount DECIMAL(15,2),
          load_amount DECIMAL(15,2),
          broker_code VARCHAR(50),
          sub_broker_code VARCHAR(50),
          distributor_code VARCHAR(50),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO mf_allocations (
          client_code, scheme_code, scheme_name, folio_number, transaction_type, transaction_date,
          nav_date, nav_price, units, amount, load_amount, broker_code, sub_broker_code,
          distributor_code, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
    };
  }
  
  // Broker master
  if (name.includes('broker')) {
    return {
      tableName: 'brokers',
      schema: `
        CREATE TABLE IF NOT EXISTS brokers (
          broker_code VARCHAR(50) PRIMARY KEY,
          broker_name VARCHAR(255),
          broker_type VARCHAR(50),
          registration_number VARCHAR(100),
          contact_person VARCHAR(255),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO brokers (
          broker_code, broker_name, broker_type, registration_number, contact_person,
          email, phone, address, city, state, country
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (broker_code) DO NOTHING`
    };
  }
  
  // Client master
  if (name.includes('client')) {
    return {
      tableName: 'clients',
      schema: `
        CREATE TABLE IF NOT EXISTS clients (
          client_code VARCHAR(50) PRIMARY KEY,
          client_name VARCHAR(255),
          client_type VARCHAR(50),
          pan_number VARCHAR(20),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          risk_category VARCHAR(50),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO clients (
          client_code, client_name, client_type, pan_number, email, phone,
          address, city, state, country, risk_category
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (client_code) DO NOTHING`
    };
  }
  
  // Distributor master
  if (name.includes('distributor')) {
    return {
      tableName: 'distributors',
      schema: `
        CREATE TABLE IF NOT EXISTS distributors (
          distributor_code VARCHAR(50) PRIMARY KEY,
          distributor_name VARCHAR(255),
          distributor_type VARCHAR(50),
          distributor_arn_number VARCHAR(100),
          commission_rate DECIMAL(5,4),
          contact_person VARCHAR(255),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO distributors (
          distributor_code, distributor_name, distributor_type, distributor_arn_number,
          commission_rate, contact_person, email, phone, address, city, state, country
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (distributor_code) DO NOTHING`
    };
  }
  
  // Strategy master
  if (name.includes('strategy')) {
    return {
      tableName: 'strategies',
      schema: `
        CREATE TABLE IF NOT EXISTS strategies (
          strategy_code VARCHAR(50) PRIMARY KEY,
          strategy_name VARCHAR(255),
          strategy_type VARCHAR(50),
          description TEXT,
          risk_level VARCHAR(50),
          benchmark VARCHAR(100),
          inception_date DATE,
          fund_manager VARCHAR(255),
          aum DECIMAL(15,2),
          expense_ratio DECIMAL(5,4),
          minimum_investment DECIMAL(15,2),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO strategies (
          strategy_code, strategy_name, strategy_type, description, risk_level,
          benchmark, inception_date, fund_manager, aum, expense_ratio, minimum_investment
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (strategy_code) DO NOTHING`
    };
  }
  
  // Default: general data
  return {
    tableName: 'general_data',
    schema: `
      CREATE TABLE IF NOT EXISTS general_data (
        id SERIAL PRIMARY KEY,
        collection_name VARCHAR(255),
        data_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
    insertQuery: `
      INSERT INTO general_data (collection_name, data_json) 
      VALUES ($1, $2)`
  };
}

function extractValues(tableName, doc) {
  const safeValue = (val) => val === undefined || val === null || val === '' ? null : val;
  const safeNumber = (val) => {
    if (val === undefined || val === null || val === '') return null;
    const num = parseFloat(val);
    return isNaN(num) ? null : num;
  };
  const safeDate = (val) => {
    if (!val) return null;
    const date = new Date(val);
    return isNaN(date.getTime()) ? null : date.toISOString().split('T')[0];
  };
  
  try {
    switch (tableName) {
      case 'unified_custody_master':
        return [
          safeValue(doc.client_reference || doc.Client_Reference || doc['Client Reference']),
          safeValue(doc.client_name || doc.Client_Name || doc['Client Name']),
          safeValue(doc.instrument_isin || doc.ISIN || doc.isin),
          safeValue(doc.instrument_name || doc.Instrument_Name || doc['Instrument Name']),
          safeValue(doc.instrument_code || doc.Symbol || doc.symbol),
          safeNumber(doc.blocked_quantity || doc.Blocked_Quantity || doc['Blocked Quantity'] || 0),
          safeNumber(doc.pending_buy_quantity || doc.Pending_Buy || doc['Pending Buy'] || 0),
          safeNumber(doc.pending_sell_quantity || doc.Pending_Sell || doc['Pending Sell'] || 0),
          safeNumber(doc.total_position || doc.Total_Position || doc['Total Position']),
          safeNumber(doc.saleable_quantity || doc.Saleable_Quantity || doc['Saleable Quantity']),
          safeValue(doc.source_system || 'Unknown'),
          safeValue(doc.file_name || 'Unknown'),
          safeDate(doc.record_date || new Date())
        ];
        
      case 'contract_notes':
        return [
          safeValue(doc.ecn_number || doc.ECN_Number || doc['ECN Number'] || `ECN_${Date.now()}_${Math.random()}`),
          safeValue(doc.ecn_status || doc.Status || 'Active'),
          safeDate(doc.ecn_date || doc.Date),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.broker_name || doc.Broker_Name),
          safeValue(doc.instrument_isin || doc.ISIN),
          safeValue(doc.instrument_name || doc.Instrument_Name),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeValue(doc.delivery_type || doc.Delivery_Type),
          safeValue(doc.exchange || doc.Exchange),
          safeDate(doc.settlement_date || doc.Settlement_Date),
          safeNumber(doc.quantity || doc.Quantity),
          safeNumber(doc.net_amount || doc.Net_Amount),
          safeNumber(doc.net_rate || doc.Net_Rate),
          safeNumber(doc.brokerage_amount || doc.Brokerage || 0),
          safeNumber(doc.service_tax || doc.Service_Tax || 0),
          safeNumber(doc.stt_amount || doc.STT || 0)
        ];
        
      case 'cash_capital_flow':
        return [
          safeValue(doc.transaction_ref || doc.Transaction_Ref || `CASH_${Date.now()}_${Math.random()}`),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.transaction_date || doc.Date),
          safeDate(doc.settlement_date || doc.Settlement_Date),
          safeNumber(doc.amount || doc.Amount),
          safeNumber(doc.balance || doc.Balance),
          safeValue(doc.narration || doc.Narration),
          safeValue(doc.voucher_number || doc.Voucher_Number),
          safeValue(doc.cheque_number || doc.Cheque_Number),
          safeValue(doc.bank_name || doc.Bank_Name),
          safeValue(doc.branch_name || doc.Branch_Name),
          safeValue(doc.utr_number || doc.UTR_Number),
          safeValue(doc.payment_mode || doc.Payment_Mode),
          safeValue(doc.currency || 'INR'),
          safeNumber(doc.exchange_rate || 1),
          safeValue(doc.remarks || '')
        ];
        
      case 'stock_capital_flow':
        return [
          safeValue(doc.transaction_ref || doc.Transaction_Ref || `STOCK_${Date.now()}_${Math.random()}`),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.instrument_isin || doc.ISIN),
          safeValue(doc.exchange || doc.Exchange),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.acquisition_date || doc.Acquisition_Date || doc.transaction_date),
          safeDate(doc.security_in_date || doc.Security_In_Date),
          safeNumber(doc.quantity || doc.Quantity),
          safeNumber(doc.original_price || doc.Price),
          safeNumber(doc.brokerage || doc.Brokerage || 0),
          safeNumber(doc.service_tax || doc.Service_Tax || 0),
          safeValue(doc.settlement_date_flag || 'N'),
          safeNumber(doc.market_rate || doc.Market_Rate),
          safeValue(doc.cash_symbol || doc.Symbol),
          safeNumber(doc.stt_amount || doc.STT || 0),
          safeNumber(doc.accrued_interest || 0),
          safeValue(doc.block_ref || doc.Block_Ref),
          safeValue(doc.remarks || '')
        ];
        
      case 'mf_allocations':
        return [
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.scheme_code || doc.Scheme_Code),
          safeValue(doc.scheme_name || doc.Scheme_Name),
          safeValue(doc.folio_number || doc.Folio_Number),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.transaction_date || doc.Date),
          safeDate(doc.nav_date || doc.NAV_Date),
          safeNumber(doc.nav_price || doc.NAV),
          safeNumber(doc.units || doc.Units),
          safeNumber(doc.amount || doc.Amount),
          safeNumber(doc.load_amount || doc.Load || 0),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.sub_broker_code || doc.Sub_Broker_Code),
          safeValue(doc.distributor_code || doc.Distributor_Code),
          safeValue(doc.remarks || '')
        ];
        
      case 'brokers':
        return [
          safeValue(doc.broker_code || doc.Broker_Code || `BR_${Date.now()}`),
          safeValue(doc.broker_name || doc.Broker_Name || doc.Name),
          safeValue(doc.broker_type || doc.Type || 'Regular'),
          safeValue(doc.registration_number || doc.Registration_Number),
          safeValue(doc.contact_person || doc.Contact_Person),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India')
        ];
        
      case 'clients':
        return [
          safeValue(doc.client_code || doc.Client_Code || `CL_${Date.now()}`),
          safeValue(doc.client_name || doc.Client_Name || doc.Name),
          safeValue(doc.client_type || doc.Type || 'Individual'),
          safeValue(doc.pan_number || doc.PAN),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India'),
          safeValue(doc.risk_category || doc.Risk_Category || 'Medium')
        ];
        
      case 'distributors':
        return [
          safeValue(doc.distributor_code || doc.Distributor_Code || `DT_${Date.now()}`),
          safeValue(doc.distributor_name || doc.Distributor_Name || doc.Name),
          safeValue(doc.distributor_type || doc.Type || 'Regular'),
          safeValue(doc.distributor_arn_number || doc.ARN_Number),
          safeNumber(doc.commission_rate || doc.Commission_Rate || 0),
          safeValue(doc.contact_person || doc.Contact_Person),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India')
        ];
        
      case 'strategies':
        return [
          safeValue(doc.strategy_code || doc.Strategy_Code || `ST_${Date.now()}`),
          safeValue(doc.strategy_name || doc.Strategy_Name || doc.Name),
          safeValue(doc.strategy_type || doc.Type || 'Equity'),
          safeValue(doc.description || doc.Description),
          safeValue(doc.risk_level || doc.Risk_Level || 'Medium'),
          safeValue(doc.benchmark || doc.Benchmark),
          safeDate(doc.inception_date || doc.Inception_Date),
          safeValue(doc.fund_manager || doc.Fund_Manager),
          safeNumber(doc.aum || doc.AUM),
          safeNumber(doc.expense_ratio || doc.Expense_Ratio),
          safeNumber(doc.minimum_investment || doc.Minimum_Investment)
        ];
        
      case 'general_data':
        return [
          safeValue(doc.collection_name || 'unknown'),
          JSON.stringify(doc)
        ];
        
      default:
        return [JSON.stringify(doc)];
    }
  } catch (error) {
    console.log(`❌ Value extraction error: ${error.message}`);
    return null;
  }
}

// Run the processor
if (require.main === module) {
  processAllData()
    .then(() => {
      console.log('\n🎉 Fresh processing completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Processing failed:', error.message);
      process.exit(1);
    });
}

module.exports = { processAllData }; 
const { Pool } = require('pg');
const config = require('./config');

// Database connections
const pgPool = new Pool({
  connectionString: config.postgresql.connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

async function processAllData() {
  console.log('🚀 FRESH PROCESSING: MongoDB → PostgreSQL');
  console.log('═══════════════════════════════════════════════');
  
  const mongoClient = new MongoClient(config.mongodb.uri);
  
  try {
    // Connect to MongoDB
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB');
    
    // Connect to PostgreSQL
    const pgClient = await pgPool.connect();
    console.log('✅ Connected to PostgreSQL');
    
    // Get all databases and collections
    const databases = ['financial_data_2024', 'financial_data_2025', 'financial_data_2026'];
    let totalProcessed = 0;
    let totalValid = 0;
    
    for (const dbName of databases) {
      try {
        const db = mongoClient.db(dbName);
        const collections = await db.listCollections().toArray();
        
        if (collections.length === 0) continue;
        
        console.log(`\n📂 Processing database: ${dbName}`);
        console.log(`📋 Found ${collections.length} collections`);
        
        for (const collection of collections) {
          const collectionName = collection.name;
          console.log(`\n🔧 Processing: ${collectionName}`);
          
          try {
            const coll = db.collection(collectionName);
            const documents = await coll.find({}).toArray();
            
            if (documents.length === 0) {
              console.log(`⚪ ${collectionName}: No documents found`);
              continue;
            }
            
            totalProcessed += documents.length;
            
            // Determine table and schema based on collection name
            const { tableName, schema, insertQuery } = getTableConfig(collectionName, documents[0]);
            
            // Create table if not exists
            await pgClient.query(schema);
            console.log(`📅 Table ready: ${tableName}`);
            
            // Insert data in batches
            const batchSize = 100;
            let validCount = 0;
            
            for (let i = 0; i < documents.length; i += batchSize) {
              const batch = documents.slice(i, i + batchSize);
              
              for (const doc of batch) {
                try {
                  const values = extractValues(tableName, doc);
                  if (values && values.length > 0) {
                    await pgClient.query(insertQuery, values);
                    validCount++;
                  }
                } catch (error) {
                  console.log(`❌ Row error: ${error.message}`);
                }
              }
            }
            
            console.log(`✅ ${collectionName}: ${validCount}/${documents.length} records processed`);
            totalValid += validCount;
            
          } catch (error) {
            console.log(`❌ Collection error: ${error.message}`);
          }
        }
      } catch (error) {
        console.log(`❌ Database error for ${dbName}: ${error.message}`);
      }
    }
    
    pgClient.release();
    
    console.log('\n🎉 PROCESSING COMPLETE!');
    console.log('═══════════════════════════════════════════════');
    console.log(`📊 Total Processed: ${totalProcessed.toLocaleString()}`);
    console.log(`✅ Total Valid: ${totalValid.toLocaleString()}`);
    console.log(`🎯 Success Rate: ${totalProcessed > 0 ? Math.round((totalValid/totalProcessed)*100) : 0}%`);
    
  } catch (error) {
    console.error('💥 Processing failed:', error.message);
    throw error;
  } finally {
    await mongoClient.close();
    await pgPool.end();
  }
}

function getTableConfig(collectionName, sampleDoc) {
  const name = collectionName.toLowerCase();
  
  // Custody data
  if (name.includes('axis') || name.includes('hdfc') || name.includes('kotak') || 
      name.includes('orbis') || name.includes('deutsche') || name.includes('trustpms') || 
      name.includes('custody') || name.includes('holding')) {
    return {
      tableName: 'unified_custody_master',
      schema: `
        CREATE TABLE IF NOT EXISTS unified_custody_master (
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
        )`,
      insertQuery: `
        INSERT INTO unified_custody_master (
          client_reference, client_name, instrument_isin, instrument_name, instrument_code,
          blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity,
          source_system, file_name, record_date
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
    };
  }
  
  // Contract notes
  if (name.includes('contract')) {
    return {
      tableName: 'contract_notes',
      schema: `
        CREATE TABLE IF NOT EXISTS contract_notes (
          ecn_number VARCHAR(50) PRIMARY KEY,
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
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO contract_notes (
          ecn_number, ecn_status, ecn_date, client_code, broker_name, instrument_isin, instrument_name,
          transaction_type, delivery_type, exchange, settlement_date, quantity, net_amount, net_rate,
          brokerage_amount, service_tax, stt_amount
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (ecn_number) DO NOTHING`
    };
  }
  
  // Cash capital flow
  if (name.includes('cash')) {
    return {
      tableName: 'cash_capital_flow',
      schema: `
        CREATE TABLE IF NOT EXISTS cash_capital_flow (
          transaction_ref VARCHAR(100) PRIMARY KEY,
          broker_code VARCHAR(50),
          client_code VARCHAR(50),
          transaction_type VARCHAR(50),
          transaction_date DATE,
          settlement_date DATE,
          amount DECIMAL(15,2),
          balance DECIMAL(15,2),
          narration TEXT,
          voucher_number VARCHAR(100),
          cheque_number VARCHAR(100),
          bank_name VARCHAR(255),
          branch_name VARCHAR(255),
          utr_number VARCHAR(100),
          payment_mode VARCHAR(50),
          currency VARCHAR(10),
          exchange_rate DECIMAL(10,4),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO cash_capital_flow (
          transaction_ref, broker_code, client_code, transaction_type, transaction_date, settlement_date,
          amount, balance, narration, voucher_number, cheque_number, bank_name, branch_name,
          utr_number, payment_mode, currency, exchange_rate, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT (transaction_ref) DO NOTHING`
    };
  }
  
  // Stock capital flow
  if (name.includes('stock')) {
    return {
      tableName: 'stock_capital_flow',
      schema: `
        CREATE TABLE IF NOT EXISTS stock_capital_flow (
          transaction_ref VARCHAR(100) PRIMARY KEY,
          broker_code VARCHAR(50),
          client_code VARCHAR(50),
          instrument_isin VARCHAR(50),
          exchange VARCHAR(50),
          transaction_type VARCHAR(50),
          acquisition_date DATE,
          security_in_date DATE,
          quantity DECIMAL(15,4),
          original_price DECIMAL(15,4),
          brokerage DECIMAL(15,2),
          service_tax DECIMAL(15,2),
          settlement_date_flag VARCHAR(10),
          market_rate DECIMAL(15,4),
          cash_symbol VARCHAR(20),
          stt_amount DECIMAL(15,2),
          accrued_interest DECIMAL(15,2),
          block_ref VARCHAR(100),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO stock_capital_flow (
          transaction_ref, broker_code, client_code, instrument_isin, exchange, transaction_type,
          acquisition_date, security_in_date, quantity, original_price, brokerage, service_tax,
          settlement_date_flag, market_rate, cash_symbol, stt_amount, accrued_interest, block_ref, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ON CONFLICT (transaction_ref) DO NOTHING`
    };
  }
  
  // MF allocations
  if (name.includes('mf') || name.includes('allocation')) {
    return {
      tableName: 'mf_allocations',
      schema: `
        CREATE TABLE IF NOT EXISTS mf_allocations (
          allocation_id SERIAL PRIMARY KEY,
          client_code VARCHAR(50),
          scheme_code VARCHAR(50),
          scheme_name VARCHAR(255),
          folio_number VARCHAR(100),
          transaction_type VARCHAR(50),
          transaction_date DATE,
          nav_date DATE,
          nav_price DECIMAL(15,4),
          units DECIMAL(15,4),
          amount DECIMAL(15,2),
          load_amount DECIMAL(15,2),
          broker_code VARCHAR(50),
          sub_broker_code VARCHAR(50),
          distributor_code VARCHAR(50),
          remarks TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO mf_allocations (
          client_code, scheme_code, scheme_name, folio_number, transaction_type, transaction_date,
          nav_date, nav_price, units, amount, load_amount, broker_code, sub_broker_code,
          distributor_code, remarks
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`
    };
  }
  
  // Broker master
  if (name.includes('broker')) {
    return {
      tableName: 'brokers',
      schema: `
        CREATE TABLE IF NOT EXISTS brokers (
          broker_code VARCHAR(50) PRIMARY KEY,
          broker_name VARCHAR(255),
          broker_type VARCHAR(50),
          registration_number VARCHAR(100),
          contact_person VARCHAR(255),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO brokers (
          broker_code, broker_name, broker_type, registration_number, contact_person,
          email, phone, address, city, state, country
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (broker_code) DO NOTHING`
    };
  }
  
  // Client master
  if (name.includes('client')) {
    return {
      tableName: 'clients',
      schema: `
        CREATE TABLE IF NOT EXISTS clients (
          client_code VARCHAR(50) PRIMARY KEY,
          client_name VARCHAR(255),
          client_type VARCHAR(50),
          pan_number VARCHAR(20),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          risk_category VARCHAR(50),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO clients (
          client_code, client_name, client_type, pan_number, email, phone,
          address, city, state, country, risk_category
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (client_code) DO NOTHING`
    };
  }
  
  // Distributor master
  if (name.includes('distributor')) {
    return {
      tableName: 'distributors',
      schema: `
        CREATE TABLE IF NOT EXISTS distributors (
          distributor_code VARCHAR(50) PRIMARY KEY,
          distributor_name VARCHAR(255),
          distributor_type VARCHAR(50),
          distributor_arn_number VARCHAR(100),
          commission_rate DECIMAL(5,4),
          contact_person VARCHAR(255),
          email VARCHAR(255),
          phone VARCHAR(20),
          address TEXT,
          city VARCHAR(100),
          state VARCHAR(100),
          country VARCHAR(100),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO distributors (
          distributor_code, distributor_name, distributor_type, distributor_arn_number,
          commission_rate, contact_person, email, phone, address, city, state, country
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (distributor_code) DO NOTHING`
    };
  }
  
  // Strategy master
  if (name.includes('strategy')) {
    return {
      tableName: 'strategies',
      schema: `
        CREATE TABLE IF NOT EXISTS strategies (
          strategy_code VARCHAR(50) PRIMARY KEY,
          strategy_name VARCHAR(255),
          strategy_type VARCHAR(50),
          description TEXT,
          risk_level VARCHAR(50),
          benchmark VARCHAR(100),
          inception_date DATE,
          fund_manager VARCHAR(255),
          aum DECIMAL(15,2),
          expense_ratio DECIMAL(5,4),
          minimum_investment DECIMAL(15,2),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
      insertQuery: `
        INSERT INTO strategies (
          strategy_code, strategy_name, strategy_type, description, risk_level,
          benchmark, inception_date, fund_manager, aum, expense_ratio, minimum_investment
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (strategy_code) DO NOTHING`
    };
  }
  
  // Default: general data
  return {
    tableName: 'general_data',
    schema: `
      CREATE TABLE IF NOT EXISTS general_data (
        id SERIAL PRIMARY KEY,
        collection_name VARCHAR(255),
        data_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
    insertQuery: `
      INSERT INTO general_data (collection_name, data_json) 
      VALUES ($1, $2)`
  };
}

function extractValues(tableName, doc) {
  const safeValue = (val) => val === undefined || val === null || val === '' ? null : val;
  const safeNumber = (val) => {
    if (val === undefined || val === null || val === '') return null;
    const num = parseFloat(val);
    return isNaN(num) ? null : num;
  };
  const safeDate = (val) => {
    if (!val) return null;
    const date = new Date(val);
    return isNaN(date.getTime()) ? null : date.toISOString().split('T')[0];
  };
  
  try {
    switch (tableName) {
      case 'unified_custody_master':
        return [
          safeValue(doc.client_reference || doc.Client_Reference || doc['Client Reference']),
          safeValue(doc.client_name || doc.Client_Name || doc['Client Name']),
          safeValue(doc.instrument_isin || doc.ISIN || doc.isin),
          safeValue(doc.instrument_name || doc.Instrument_Name || doc['Instrument Name']),
          safeValue(doc.instrument_code || doc.Symbol || doc.symbol),
          safeNumber(doc.blocked_quantity || doc.Blocked_Quantity || doc['Blocked Quantity'] || 0),
          safeNumber(doc.pending_buy_quantity || doc.Pending_Buy || doc['Pending Buy'] || 0),
          safeNumber(doc.pending_sell_quantity || doc.Pending_Sell || doc['Pending Sell'] || 0),
          safeNumber(doc.total_position || doc.Total_Position || doc['Total Position']),
          safeNumber(doc.saleable_quantity || doc.Saleable_Quantity || doc['Saleable Quantity']),
          safeValue(doc.source_system || 'Unknown'),
          safeValue(doc.file_name || 'Unknown'),
          safeDate(doc.record_date || new Date())
        ];
        
      case 'contract_notes':
        return [
          safeValue(doc.ecn_number || doc.ECN_Number || doc['ECN Number'] || `ECN_${Date.now()}_${Math.random()}`),
          safeValue(doc.ecn_status || doc.Status || 'Active'),
          safeDate(doc.ecn_date || doc.Date),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.broker_name || doc.Broker_Name),
          safeValue(doc.instrument_isin || doc.ISIN),
          safeValue(doc.instrument_name || doc.Instrument_Name),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeValue(doc.delivery_type || doc.Delivery_Type),
          safeValue(doc.exchange || doc.Exchange),
          safeDate(doc.settlement_date || doc.Settlement_Date),
          safeNumber(doc.quantity || doc.Quantity),
          safeNumber(doc.net_amount || doc.Net_Amount),
          safeNumber(doc.net_rate || doc.Net_Rate),
          safeNumber(doc.brokerage_amount || doc.Brokerage || 0),
          safeNumber(doc.service_tax || doc.Service_Tax || 0),
          safeNumber(doc.stt_amount || doc.STT || 0)
        ];
        
      case 'cash_capital_flow':
        return [
          safeValue(doc.transaction_ref || doc.Transaction_Ref || `CASH_${Date.now()}_${Math.random()}`),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.transaction_date || doc.Date),
          safeDate(doc.settlement_date || doc.Settlement_Date),
          safeNumber(doc.amount || doc.Amount),
          safeNumber(doc.balance || doc.Balance),
          safeValue(doc.narration || doc.Narration),
          safeValue(doc.voucher_number || doc.Voucher_Number),
          safeValue(doc.cheque_number || doc.Cheque_Number),
          safeValue(doc.bank_name || doc.Bank_Name),
          safeValue(doc.branch_name || doc.Branch_Name),
          safeValue(doc.utr_number || doc.UTR_Number),
          safeValue(doc.payment_mode || doc.Payment_Mode),
          safeValue(doc.currency || 'INR'),
          safeNumber(doc.exchange_rate || 1),
          safeValue(doc.remarks || '')
        ];
        
      case 'stock_capital_flow':
        return [
          safeValue(doc.transaction_ref || doc.Transaction_Ref || `STOCK_${Date.now()}_${Math.random()}`),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.instrument_isin || doc.ISIN),
          safeValue(doc.exchange || doc.Exchange),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.acquisition_date || doc.Acquisition_Date || doc.transaction_date),
          safeDate(doc.security_in_date || doc.Security_In_Date),
          safeNumber(doc.quantity || doc.Quantity),
          safeNumber(doc.original_price || doc.Price),
          safeNumber(doc.brokerage || doc.Brokerage || 0),
          safeNumber(doc.service_tax || doc.Service_Tax || 0),
          safeValue(doc.settlement_date_flag || 'N'),
          safeNumber(doc.market_rate || doc.Market_Rate),
          safeValue(doc.cash_symbol || doc.Symbol),
          safeNumber(doc.stt_amount || doc.STT || 0),
          safeNumber(doc.accrued_interest || 0),
          safeValue(doc.block_ref || doc.Block_Ref),
          safeValue(doc.remarks || '')
        ];
        
      case 'mf_allocations':
        return [
          safeValue(doc.client_code || doc.Client_Code),
          safeValue(doc.scheme_code || doc.Scheme_Code),
          safeValue(doc.scheme_name || doc.Scheme_Name),
          safeValue(doc.folio_number || doc.Folio_Number),
          safeValue(doc.transaction_type || doc.Transaction_Type),
          safeDate(doc.transaction_date || doc.Date),
          safeDate(doc.nav_date || doc.NAV_Date),
          safeNumber(doc.nav_price || doc.NAV),
          safeNumber(doc.units || doc.Units),
          safeNumber(doc.amount || doc.Amount),
          safeNumber(doc.load_amount || doc.Load || 0),
          safeValue(doc.broker_code || doc.Broker_Code),
          safeValue(doc.sub_broker_code || doc.Sub_Broker_Code),
          safeValue(doc.distributor_code || doc.Distributor_Code),
          safeValue(doc.remarks || '')
        ];
        
      case 'brokers':
        return [
          safeValue(doc.broker_code || doc.Broker_Code || `BR_${Date.now()}`),
          safeValue(doc.broker_name || doc.Broker_Name || doc.Name),
          safeValue(doc.broker_type || doc.Type || 'Regular'),
          safeValue(doc.registration_number || doc.Registration_Number),
          safeValue(doc.contact_person || doc.Contact_Person),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India')
        ];
        
      case 'clients':
        return [
          safeValue(doc.client_code || doc.Client_Code || `CL_${Date.now()}`),
          safeValue(doc.client_name || doc.Client_Name || doc.Name),
          safeValue(doc.client_type || doc.Type || 'Individual'),
          safeValue(doc.pan_number || doc.PAN),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India'),
          safeValue(doc.risk_category || doc.Risk_Category || 'Medium')
        ];
        
      case 'distributors':
        return [
          safeValue(doc.distributor_code || doc.Distributor_Code || `DT_${Date.now()}`),
          safeValue(doc.distributor_name || doc.Distributor_Name || doc.Name),
          safeValue(doc.distributor_type || doc.Type || 'Regular'),
          safeValue(doc.distributor_arn_number || doc.ARN_Number),
          safeNumber(doc.commission_rate || doc.Commission_Rate || 0),
          safeValue(doc.contact_person || doc.Contact_Person),
          safeValue(doc.email || doc.Email),
          safeValue(doc.phone || doc.Phone),
          safeValue(doc.address || doc.Address),
          safeValue(doc.city || doc.City),
          safeValue(doc.state || doc.State),
          safeValue(doc.country || doc.Country || 'India')
        ];
        
      case 'strategies':
        return [
          safeValue(doc.strategy_code || doc.Strategy_Code || `ST_${Date.now()}`),
          safeValue(doc.strategy_name || doc.Strategy_Name || doc.Name),
          safeValue(doc.strategy_type || doc.Type || 'Equity'),
          safeValue(doc.description || doc.Description),
          safeValue(doc.risk_level || doc.Risk_Level || 'Medium'),
          safeValue(doc.benchmark || doc.Benchmark),
          safeDate(doc.inception_date || doc.Inception_Date),
          safeValue(doc.fund_manager || doc.Fund_Manager),
          safeNumber(doc.aum || doc.AUM),
          safeNumber(doc.expense_ratio || doc.Expense_Ratio),
          safeNumber(doc.minimum_investment || doc.Minimum_Investment)
        ];
        
      case 'general_data':
        return [
          safeValue(doc.collection_name || 'unknown'),
          JSON.stringify(doc)
        ];
        
      default:
        return [JSON.stringify(doc)];
    }
  } catch (error) {
    console.log(`❌ Value extraction error: ${error.message}`);
    return null;
  }
}

// Run the processor
if (require.main === module) {
  processAllData()
    .then(() => {
      console.log('\n🎉 Fresh processing completed successfully!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Processing failed:', error.message);
      process.exit(1);
    });
}

module.exports = { processAllData }; 