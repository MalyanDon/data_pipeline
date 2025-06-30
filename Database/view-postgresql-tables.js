const { Pool } = require('pg');
const config = require('./config');

async function viewPostgreSQLTables() {
    console.log('🐘 Connecting to PostgreSQL...');
    
    const pool = new Pool({
        host: config.postgresql.host,
        port: config.postgresql.port,
        user: config.postgresql.user,
        password: config.postgresql.password,
        database: config.postgresql.database
    });

    try {
        // Get all tables
        console.log('\n📊 Fetching all tables...');
        const tablesResult = await pool.query(`
            SELECT tablename, schemaname 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename
        `);

        if (tablesResult.rows.length === 0) {
            console.log('❌ No tables found in PostgreSQL database');
            return;
        }

        console.log(`\n✅ Found ${tablesResult.rows.length} tables:\n`);

        for (const table of tablesResult.rows) {
            const tableName = table.tablename;
            console.log(`\n🔹 Table: ${tableName}`);
            console.log('━'.repeat(50));

            try {
                // Get table info
                const countResult = await pool.query(`SELECT COUNT(*) FROM "${tableName}"`);
                const recordCount = countResult.rows[0].count;

                const columnsResult = await pool.query(`
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = $1 AND table_schema = 'public'
                    ORDER BY ordinal_position
                `, [tableName]);

                console.log(`📊 Records: ${recordCount}`);
                console.log(`📋 Columns (${columnsResult.rows.length}):`);
                columnsResult.rows.forEach(col => {
                    console.log(`   • ${col.column_name} (${col.data_type})`);
                });

                // Show sample data
                if (recordCount > 0) {
                    console.log(`\n📄 Sample data (first 3 records):`);
                    const sampleResult = await pool.query(`SELECT * FROM "${tableName}" LIMIT 3`);
                    
                    if (sampleResult.rows.length > 0) {
                        sampleResult.rows.forEach((row, index) => {
                            console.log(`\n   Record ${index + 1}:`);
                            Object.entries(row).forEach(([key, value]) => {
                                if (!['id', 'processed_at'].includes(key)) {
                                    console.log(`     ${key}: ${value}`);
                                }
                            });
                        });
                    }
                }

            } catch (error) {
                console.log(`   ❌ Error reading table: ${error.message}`);
            }
        }

    } catch (error) {
        console.error('❌ PostgreSQL error:', error.message);
    } finally {
        await pool.end();
        console.log('\n🔐 PostgreSQL connection closed');
    }
}

// Run if called directly
if (require.main === module) {
    viewPostgreSQLTables().catch(console.error);
}

module.exports = { viewPostgreSQLTables }; 
const config = require('./config');

async function viewPostgreSQLTables() {
    console.log('🐘 Connecting to PostgreSQL...');
    
    const pool = new Pool({
        host: config.postgresql.host,
        port: config.postgresql.port,
        user: config.postgresql.user,
        password: config.postgresql.password,
        database: config.postgresql.database
    });

    try {
        // Get all tables
        console.log('\n📊 Fetching all tables...');
        const tablesResult = await pool.query(`
            SELECT tablename, schemaname 
            FROM pg_tables 
            WHERE schemaname = 'public' 
            ORDER BY tablename
        `);

        if (tablesResult.rows.length === 0) {
            console.log('❌ No tables found in PostgreSQL database');
            return;
        }

        console.log(`\n✅ Found ${tablesResult.rows.length} tables:\n`);

        for (const table of tablesResult.rows) {
            const tableName = table.tablename;
            console.log(`\n🔹 Table: ${tableName}`);
            console.log('━'.repeat(50));

            try {
                // Get table info
                const countResult = await pool.query(`SELECT COUNT(*) FROM "${tableName}"`);
                const recordCount = countResult.rows[0].count;

                const columnsResult = await pool.query(`
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = $1 AND table_schema = 'public'
                    ORDER BY ordinal_position
                `, [tableName]);

                console.log(`📊 Records: ${recordCount}`);
                console.log(`📋 Columns (${columnsResult.rows.length}):`);
                columnsResult.rows.forEach(col => {
                    console.log(`   • ${col.column_name} (${col.data_type})`);
                });

                // Show sample data
                if (recordCount > 0) {
                    console.log(`\n📄 Sample data (first 3 records):`);
                    const sampleResult = await pool.query(`SELECT * FROM "${tableName}" LIMIT 3`);
                    
                    if (sampleResult.rows.length > 0) {
                        sampleResult.rows.forEach((row, index) => {
                            console.log(`\n   Record ${index + 1}:`);
                            Object.entries(row).forEach(([key, value]) => {
                                if (!['id', 'processed_at'].includes(key)) {
                                    console.log(`     ${key}: ${value}`);
                                }
                            });
                        });
                    }
                }

            } catch (error) {
                console.log(`   ❌ Error reading table: ${error.message}`);
            }
        }

    } catch (error) {
        console.error('❌ PostgreSQL error:', error.message);
    } finally {
        await pool.end();
        console.log('\n🔐 PostgreSQL connection closed');
    }
}

// Run if called directly
if (require.main === module) {
    viewPostgreSQLTables().catch(console.error);
}

module.exports = { viewPostgreSQLTables }; 