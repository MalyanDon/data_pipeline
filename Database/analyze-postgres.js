const { Client } = require('pg');
const config = require('./config');

async function analyzePostgreSQL() {
  const client = new Client({
    connectionString: config.postgresql.connectionString,
  });

  try {
    console.log('🔗 Connecting to PostgreSQL (Neon)...');
    await client.connect();
    console.log('✅ Connected successfully!\n');

    // Get all tables
    const tablesQuery = `
      SELECT table_name, table_type 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE'
      ORDER BY table_name;
    `;
    const tables = await client.query(tablesQuery);

    console.log('📊 DATABASE STRUCTURE ANALYSIS');
    console.log('='.repeat(50));

    for (const table of tables.rows) {
      const tableName = table.table_name;
      console.log(`\n📄 TABLE: ${tableName.toUpperCase()}`);
      console.log('-'.repeat(40));

      // Get column information
      const columnsQuery = `
        SELECT 
          column_name,
          data_type,
          is_nullable,
          column_default,
          character_maximum_length,
          numeric_precision,
          numeric_scale
        FROM information_schema.columns 
        WHERE table_name = $1 
        AND table_schema = 'public'
        ORDER BY ordinal_position;
      `;
      const columns = await client.query(columnsQuery, [tableName]);

      console.log('🏗️  COLUMNS:');
      columns.rows.forEach(col => {
        let typeInfo = col.data_type;
        if (col.character_maximum_length) {
          typeInfo += `(${col.character_maximum_length})`;
        } else if (col.numeric_precision) {
          typeInfo += `(${col.numeric_precision}${col.numeric_scale ? ',' + col.numeric_scale : ''})`;
        }
        
        const nullable = col.is_nullable === 'YES' ? 'NULL' : 'NOT NULL';
        const defaultVal = col.column_default ? ` DEFAULT ${col.column_default}` : '';
        
        console.log(`  📋 ${col.column_name.padEnd(20)} | ${typeInfo.padEnd(15)} | ${nullable}${defaultVal}`);
      });

      // Get row count
      const countQuery = `SELECT COUNT(*) as count FROM ${tableName};`;
      const countResult = await client.query(countQuery);
      console.log(`\n📊 RECORD COUNT: ${countResult.rows[0].count}`);

      // Show sample data if table has records
      if (parseInt(countResult.rows[0].count) > 0) {
        const sampleQuery = `SELECT * FROM ${tableName} LIMIT 3;`;
        const sampleResult = await client.query(sampleQuery);
        
        console.log('\n🔍 SAMPLE DATA:');
        if (sampleResult.rows.length > 0) {
          // Show first row as example
          const firstRow = sampleResult.rows[0];
          Object.keys(firstRow).forEach(key => {
            const value = firstRow[key];
            const displayValue = value !== null ? 
              (typeof value === 'string' && value.length > 30 ? value.substring(0, 30) + '...' : value) 
              : 'NULL';
            console.log(`  📌 ${key.padEnd(20)}: ${displayValue}`);
          });
          
          if (sampleResult.rows.length > 1) {
            console.log(`  ... and ${sampleResult.rows.length - 1} more sample record(s)`);
          }
        }
      } else {
        console.log('\n📭 NO DATA (Empty table)');
      }

      // Get foreign key constraints
      const fkQuery = `
        SELECT 
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
        AND tc.table_name = $1;
      `;
      const fkResult = await client.query(fkQuery, [tableName]);
      
      if (fkResult.rows.length > 0) {
        console.log('\n🔗 FOREIGN KEY RELATIONSHIPS:');
        fkResult.rows.forEach(fk => {
          console.log(`  🔗 ${fk.column_name} → ${fk.foreign_table_name}.${fk.foreign_column_name}`);
        });
      }
    }

    // Show views
    console.log('\n\n👁️  DATABASE VIEWS');
    console.log('='.repeat(30));
    
    const viewsQuery = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'VIEW'
      ORDER BY table_name;
    `;
    const views = await client.query(viewsQuery);
    
    for (const view of views.rows) {
      console.log(`\n👁️  VIEW: ${view.table_name.toUpperCase()}`);
      
      // Get view definition
      const viewDefQuery = `
        SELECT view_definition 
        FROM information_schema.views 
        WHERE table_name = $1 
        AND table_schema = 'public';
      `;
      const viewDef = await client.query(viewDefQuery, [view.table_name]);
      
      // Get row count from view
      const viewCountQuery = `SELECT COUNT(*) as count FROM ${view.table_name};`;
      const viewCount = await client.query(viewCountQuery);
      console.log(`📊 Records: ${viewCount.rows[0].count}`);
      
      // Show sample from view if it has data
      if (parseInt(viewCount.rows[0].count) > 0) {
        const viewSampleQuery = `SELECT * FROM ${view.table_name} LIMIT 2;`;
        const viewSample = await client.query(viewSampleQuery);
        
        console.log('🔍 Sample:');
        if (viewSample.rows.length > 0) {
          Object.keys(viewSample.rows[0]).forEach(key => {
            const value = viewSample.rows[0][key];
            const displayValue = value !== null ? 
              (typeof value === 'string' && value.length > 25 ? value.substring(0, 25) + '...' : value) 
              : 'NULL';
            console.log(`  📌 ${key}: ${displayValue}`);
          });
        }
      }
    }

    // Database summary
    console.log('\n\n📋 DATABASE SUMMARY');
    console.log('='.repeat(30));
    
    const totalTablesQuery = `
      SELECT COUNT(*) as count 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'BASE TABLE';
    `;
    const totalTables = await client.query(totalTablesQuery);
    
    const totalViewsQuery = `
      SELECT COUNT(*) as count 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_type = 'VIEW';
    `;
    const totalViews = await client.query(totalViewsQuery);
    
    console.log(`📊 Total Tables: ${totalTables.rows[0].count}`);
    console.log(`👁️  Total Views: ${totalViews.rows[0].count}`);
    
    // Total records across all tables
    let totalRecords = 0;
    for (const table of tables.rows) {
      const countQuery = `SELECT COUNT(*) as count FROM ${table.table_name};`;
      const countResult = await client.query(countQuery);
      totalRecords += parseInt(countResult.rows[0].count);
    }
    
    console.log(`📈 Total Records: ${totalRecords}`);
    console.log(`🗄️  Database: neondb`);
    console.log(`🔗 Host: ep-falling-union-a15mokzs-pooler.ap-southeast-1.aws.neon.tech`);

    console.log('\n🎉 Analysis complete!');

  } catch (error) {
    console.error('❌ Error analyzing PostgreSQL:', error.message);
  } finally {
    await client.end();
  }
}

// Run the analysis
if (require.main === module) {
  analyzePostgreSQL();
}

module.exports = { analyzePostgreSQL }; 