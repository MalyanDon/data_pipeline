const { Client } = require('pg');
const config = require('./config');

async function testPostgreSQL() {
  const client = new Client({
    connectionString: config.postgresql.connectionString,
  });

  try {
    console.log('🔗 Testing PostgreSQL connection...');
    await client.connect();
    console.log('✅ Connected successfully!\n');

    // Test 1: List all tables
    console.log('📊 Available Tables:');
    const tables = await client.query(`
      SELECT table_name, table_type 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      ORDER BY table_name;
    `);
    
    tables.rows.forEach(row => {
      const icon = row.table_type === 'VIEW' ? '👁️' : '📄';
      console.log(`  ${icon} ${row.table_name} (${row.table_type})`);
    });

    // Test 2: Check brokers
    console.log('\n🏦 Brokers:');
    const brokers = await client.query('SELECT broker_id, broker_name, broker_code FROM brokers ORDER BY broker_name;');
    brokers.rows.forEach(broker => {
      console.log(`  🏢 ${broker.broker_name} (ID: ${broker.broker_id}, Code: ${broker.broker_code})`);
    });

    // Test 3: Check table constraints
    console.log('\n🔗 Foreign Key Relationships:');
    const constraints = await client.query(`
      SELECT 
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
      WHERE tc.constraint_type = 'FOREIGN KEY'
      ORDER BY tc.table_name, kcu.column_name;
    `);

    constraints.rows.forEach(constraint => {
      console.log(`  🔗 ${constraint.table_name}.${constraint.column_name} → ${constraint.foreign_table_name}.${constraint.foreign_column_name}`);
    });

    // Test 4: Check indexes
    console.log('\n⚡ Indexes:');
    const indexes = await client.query(`
      SELECT indexname, tablename, indexdef 
      FROM pg_indexes 
      WHERE schemaname = 'public' 
      AND indexname NOT LIKE '%_pkey'
      ORDER BY tablename, indexname;
    `);

    indexes.rows.forEach(index => {
      console.log(`  ⚡ ${index.tablename}.${index.indexname}`);
    });

    // Test 5: Sample data insertion test
    console.log('\n🧪 Testing Sample Data Insertion:');
    
    // Insert a test security
    await client.query(`
      INSERT INTO securities (symbol, security_name, security_type, exchange)
      VALUES ('RELIANCE', 'Reliance Industries Ltd', 'Equity', 'NSE')
      ON CONFLICT (isin_code) DO NOTHING;
    `);
    
    // Insert a test client
    await client.query(`
      INSERT INTO clients (client_code, client_name, client_type, broker_id)
      VALUES ('TEST001', 'Test Client 1', 'Individual', 1)
      ON CONFLICT (client_code, broker_id) DO NOTHING;
    `);
    
    console.log('  ✅ Sample data inserted successfully');

    // Test 6: Query the view
    console.log('\n👁️ Testing Views:');
    const viewTest = await client.query('SELECT COUNT(*) as total_records FROM v_custody_summary;');
    console.log(`  👁️ v_custody_summary: ${viewTest.rows[0].total_records} records`);

    console.log('\n🎉 All tests passed! PostgreSQL is ready for ETL pipeline.');

  } catch (error) {
    console.error('❌ Error testing PostgreSQL:', error.message);
  } finally {
    await client.end();
  }
}

// Run the test
if (require.main === module) {
  testPostgreSQL();
}

module.exports = { testPostgreSQL }; 