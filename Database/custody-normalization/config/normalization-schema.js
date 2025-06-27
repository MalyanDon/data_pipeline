const { Pool } = require('pg');
const config = require('../../config');

class NormalizationSchema {
  constructor() {
    this.pool = new Pool(config.postgresql);
  }

  // Generate table name based on date
  getTableName(recordDate) {
    const date = new Date(recordDate);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `unified_custody_master_${year}_${month}_${day}`;
  }

  // Create daily table for specific date
  async createDailyTable(recordDate) {
    const client = await this.pool.connect();
    const tableName = this.getTableName(recordDate);
    
    try {
      console.log(`🔧 Creating daily table: ${tableName}`);
      
      // Create the daily table with enhanced financial data fields
      await client.query(`
        CREATE TABLE IF NOT EXISTS ${tableName} (
          id SERIAL PRIMARY KEY,
          client_reference VARCHAR(100) NOT NULL,
          client_name VARCHAR(500) NOT NULL,
          instrument_isin VARCHAR(20) NOT NULL,
          instrument_name VARCHAR(500), -- Allow NULL for Orbis (they don't have instrument names)
          instrument_code VARCHAR(50),
          blocked_quantity DECIMAL(15,4) DEFAULT 0,
          pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
          pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
          total_position DECIMAL(15,4) DEFAULT 0,
          saleable_quantity DECIMAL(15,4) DEFAULT 0,
          source_system VARCHAR(50) NOT NULL,
          file_name VARCHAR(500) NOT NULL,
          record_date DATE NOT NULL DEFAULT '${recordDate}',
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT unique_custody_record_${tableName} UNIQUE (
            client_reference, 
            instrument_isin, 
            source_system
          )
        )
      `);

      // Create indexes for the daily table including new financial fields
      const indexes = [
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_client ON ${tableName}(client_reference)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_isin ON ${tableName}(instrument_isin)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_source ON ${tableName}(source_system)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_lookup ON ${tableName}(client_reference, instrument_isin)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_blocked ON ${tableName}(blocked_quantity) WHERE blocked_quantity > 0`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_pending ON ${tableName}(pending_buy_quantity, pending_sell_quantity)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_total_position ON ${tableName}(total_position) WHERE total_position > 0`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_saleable ON ${tableName}(saleable_quantity) WHERE saleable_quantity > 0`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_financial ON ${tableName}(client_reference, blocked_quantity, pending_buy_quantity, pending_sell_quantity, total_position, saleable_quantity)`,
        `CREATE INDEX IF NOT EXISTS idx_${tableName}_formula_validation ON ${tableName}(total_position, blocked_quantity, saleable_quantity) WHERE total_position > 0`
      ];

      for (const indexQuery of indexes) {
        await client.query(indexQuery);
      }

      // Create or replace the updated_at trigger for this table
      await client.query(`
        CREATE OR REPLACE FUNCTION update_${tableName}_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
          NEW.updated_at = CURRENT_TIMESTAMP;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
      `);

      await client.query(`
        DROP TRIGGER IF EXISTS update_${tableName}_updated_at ON ${tableName};
        CREATE TRIGGER update_${tableName}_updated_at
          BEFORE UPDATE ON ${tableName}
          FOR EACH ROW
          EXECUTE FUNCTION update_${tableName}_updated_at();
      `);

      console.log(`✅ Daily table ${tableName} created successfully`);
      return { success: true, tableName };
      
    } catch (error) {
      console.error(`❌ Failed to create daily table ${tableName}:`, error.message);
      throw error;
    } finally {
      client.release();
    }
  }

  // Check if daily table exists
  async dailyTableExists(recordDate) {
    const client = await this.pool.connect();
    const tableName = this.getTableName(recordDate);
    
    try {
      const result = await client.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = $1
        )
      `, [tableName]);
      
      return result.rows[0].exists;
      
    } finally {
      client.release();
    }
  }

  // Get all daily tables
  async getAllDailyTables() {
    const client = await this.pool.connect();
    
    try {
      const result = await client.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE 'unified_custody_master_%'
        ORDER BY table_name
      `);
      
      return result.rows.map(row => row.table_name);
      
    } finally {
      client.release();
    }
  }

  // Initialize database (legacy single table - keeping for migration)
  async initializeDatabase() {
    const client = await this.pool.connect();
    
    try {
      console.log('🔧 Initializing legacy unified custody master table...');
      
      // Create the legacy table for migration purposes with enhanced financial fields
      await client.query(`
        CREATE TABLE IF NOT EXISTS unified_custody_master (
          id SERIAL PRIMARY KEY,
          client_reference VARCHAR(100) NOT NULL,
          client_name VARCHAR(500) NOT NULL,
          instrument_isin VARCHAR(20) NOT NULL,
          instrument_name VARCHAR(500), -- Allow NULL for Orbis (they don't have instrument names)
          instrument_code VARCHAR(50),
          blocked_quantity DECIMAL(15,4) DEFAULT 0,
          pending_buy_quantity DECIMAL(15,4) DEFAULT 0,
          pending_sell_quantity DECIMAL(15,4) DEFAULT 0,
          total_position DECIMAL(15,4) DEFAULT 0,
          saleable_quantity DECIMAL(15,4) DEFAULT 0,
          source_system VARCHAR(50) NOT NULL,
          file_name VARCHAR(500) NOT NULL,
          record_date DATE NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT unique_custody_record UNIQUE (
            client_reference, 
            instrument_isin, 
            source_system, 
            record_date
          )
        )
      `);

      console.log('✅ Legacy table initialized (for migration only)');
      return { success: true };
      
    } catch (error) {
      console.error('❌ Legacy table initialization failed:', error.message);
      throw error;
    } finally {
      client.release();
    }
  }

  // Migrate data from legacy table to daily tables
  async migrateToDateBasedTables() {
    const client = await this.pool.connect();
    
    try {
      console.log('🔄 Migrating data from legacy table to date-based tables...');
      
      // Check if legacy table exists and has data
      const legacyCheck = await client.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'unified_custody_master'
        )
      `);
      
      if (!legacyCheck.rows[0].exists) {
        console.log('📝 No legacy table found - starting fresh with daily tables');
        return { success: true, migrated: 0 };
      }
      
      // Get distinct dates from legacy table
      const datesResult = await client.query(`
        SELECT DISTINCT record_date 
        FROM unified_custody_master 
        ORDER BY record_date
      `);
      
      if (datesResult.rows.length === 0) {
        console.log('📝 No data in legacy table to migrate');
        return { success: true, migrated: 0 };
      }
      
      let totalMigrated = 0;
      
      for (const dateRow of datesResult.rows) {
        const recordDate = dateRow.record_date;
        const dateStr = recordDate.toISOString().split('T')[0];
        
        console.log(`📅 Migrating data for date: ${dateStr}`);
        
        // Create daily table for this date
        await this.createDailyTable(dateStr);
        const tableName = this.getTableName(dateStr);
        
        // Copy data from legacy table to daily table (with enhanced financial fields)
        const insertResult = await client.query(`
          INSERT INTO ${tableName} (
            client_reference, client_name, instrument_isin, instrument_name, 
            instrument_code, blocked_quantity, pending_buy_quantity, pending_sell_quantity,
            total_position, saleable_quantity,
            source_system, file_name, record_date, created_at, updated_at
          )
          SELECT 
            client_reference, client_name, instrument_isin, instrument_name,
            instrument_code, blocked_quantity, pending_buy_quantity, pending_sell_quantity,
            COALESCE(total_position, 0), COALESCE(saleable_quantity, 0),
            source_system, file_name, record_date, created_at, updated_at
          FROM unified_custody_master 
          WHERE record_date = $1
          ON CONFLICT (client_reference, instrument_isin, source_system) DO NOTHING
        `, [recordDate]);
        
        const migratedCount = insertResult.rowCount;
        totalMigrated += migratedCount;
        console.log(`   ✅ Migrated ${migratedCount} records to ${tableName}`);
      }
      
      console.log(`🎉 Migration completed: ${totalMigrated} total records migrated to ${datesResult.rows.length} daily tables`);
      console.log(`💡 You can now drop the legacy table if desired: DROP TABLE unified_custody_master;`);
      
      return { success: true, migrated: totalMigrated, dailyTables: datesResult.rows.length };
      
    } catch (error) {
      console.error('❌ Migration failed:', error.message);
      throw error;
    } finally {
      client.release();
    }
  }

  getValidationRules() {
    return {
      requiredFields: [
        'client_reference',
        'client_name', 
        'instrument_isin',
        'instrument_name',
        'source_system',
        'file_name',
        'record_date'
      ],
      optionalFields: [
        'instrument_code',
        'blocked_quantity',
        'pending_buy_quantity', 
        'pending_sell_quantity',
        'total_position',
        'saleable_quantity'
      ],
      validation: {
        client_reference: {
          maxLength: 100,
          required: true
        },
        client_name: {
          maxLength: 500,
          required: true
        },
        instrument_isin: {
          maxLength: 20,
          required: true,
          pattern: /^[A-Z]{2}[A-Z0-9]{9}[0-9]$/
        },
        instrument_name: {
          maxLength: 500,
          required: true, // Required for most custody types, but Orbis uses NULL
          allowNullForOrbis: true
        },
        instrument_code: {
          maxLength: 50,
          required: false
        },
        blocked_quantity: {
          type: 'decimal',
          min: 0,
          max: 999999999999.9999,
          required: false
        },
        pending_buy_quantity: {
          type: 'decimal',
          min: 0,
          max: 999999999999.9999,
          required: false
        },
        pending_sell_quantity: {
          type: 'decimal',
          min: 0,
          max: 999999999999.9999,
          required: false
        },
        total_position: {
          type: 'decimal',
          min: 0,
          max: 999999999999.9999,
          required: false,
          allowNullForTrustPMS: true
        },
        saleable_quantity: {
          type: 'decimal',
          min: 0,
          max: 999999999999.9999,
          required: false
        },
        source_system: {
          maxLength: 50,
          required: true,
          enum: ['AXIS', 'DEUTSCHE', 'TRUSTPMS', 'HDFC', 'KOTAK', 'ORBIS']
        },
        file_name: {
          maxLength: 500,
          required: true
        },
        record_date: {
          type: 'date',
          required: true
        }
      }
    };
  }

  // Add new financial fields to existing tables
  async addNewFinancialFields() {
    const client = await this.pool.connect();
    
    try {
      console.log('🔄 Adding new financial fields to existing tables...');
      
      // Add fields to legacy table if it exists
      const legacyExists = await client.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE table_name = 'unified_custody_master'
        )
      `);
      
      if (legacyExists.rows[0].exists) {
        console.log('📝 Adding fields to legacy unified_custody_master table...');
        
        // Check if columns already exist
        const columnsCheck = await client.query(`
          SELECT column_name FROM information_schema.columns 
          WHERE table_name = 'unified_custody_master' 
          AND column_name IN ('total_position', 'saleable_quantity')
        `);
        
        if (columnsCheck.rows.length === 0) {
          await client.query(`
            ALTER TABLE unified_custody_master 
            ADD COLUMN total_position DECIMAL(15,4) DEFAULT 0,
            ADD COLUMN saleable_quantity DECIMAL(15,4) DEFAULT 0
          `);
          
          // Add indexes
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_unified_custody_total_position 
            ON unified_custody_master(total_position) WHERE total_position > 0
          `);
          
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_unified_custody_saleable 
            ON unified_custody_master(saleable_quantity) WHERE saleable_quantity > 0
          `);
          
          console.log('✅ Added new financial fields to legacy table');
        } else {
          console.log('📝 New financial fields already exist in legacy table');
        }
      }
      
      // Add fields to all daily tables
      const dailyTables = await this.getAllDailyTables();
      
      for (const tableName of dailyTables) {
        console.log(`📝 Checking daily table: ${tableName}`);
        
        // Check if columns already exist
        const columnsCheck = await client.query(`
          SELECT column_name FROM information_schema.columns 
          WHERE table_name = $1 
          AND column_name IN ('total_position', 'saleable_quantity')
        `, [tableName]);
        
        if (columnsCheck.rows.length === 0) {
          await client.query(`
            ALTER TABLE ${tableName} 
            ADD COLUMN total_position DECIMAL(15,4) DEFAULT 0,
            ADD COLUMN saleable_quantity DECIMAL(15,4) DEFAULT 0
          `);
          
          // Add indexes
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableName}_total_position 
            ON ${tableName}(total_position) WHERE total_position > 0
          `);
          
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableName}_saleable 
            ON ${tableName}(saleable_quantity) WHERE saleable_quantity > 0
          `);
          
          await client.query(`
            CREATE INDEX IF NOT EXISTS idx_${tableName}_formula_validation 
            ON ${tableName}(total_position, blocked_quantity, saleable_quantity) WHERE total_position > 0
          `);
          
          console.log(`✅ Added new financial fields to ${tableName}`);
        } else {
          console.log(`📝 New financial fields already exist in ${tableName}`);
        }
      }
      
      console.log('🎉 Migration completed for all existing tables');
      return { success: true, tablesUpdated: dailyTables.length + (legacyExists.rows[0].exists ? 1 : 0) };
      
    } catch (error) {
      console.error('❌ Migration failed:', error.message);
      throw error;
    } finally {
      client.release();
    }
  }

  // Validate financial field relationships
  async validateFinancialRelationships(tableName = null) {
    const client = await this.pool.connect();
    
    try {
      console.log('🔍 Validating financial field relationships...');
      
      const tables = tableName ? [tableName] : await this.getAllDailyTables();
      const results = [];
      
      for (const table of tables) {
        console.log(`📊 Checking table: ${table}`);
        
        // Check formula: saleable_quantity ≈ total_position - blocked_quantity
        const validationResult = await client.query(`
          SELECT 
            COUNT(*) as total_records,
            COUNT(*) FILTER (WHERE total_position > 0) as records_with_position,
            COUNT(*) FILTER (WHERE 
              total_position > 0 AND 
              ABS((total_position - blocked_quantity) - saleable_quantity) <= (total_position * 0.01)
            ) as formula_compliant,
            COUNT(*) FILTER (WHERE 
              total_position > 0 AND 
              ABS((total_position - blocked_quantity) - saleable_quantity) > (total_position * 0.01)
            ) as formula_violations,
            AVG(CASE WHEN total_position > 0 
                THEN ABS((total_position - blocked_quantity) - saleable_quantity) / total_position * 100 
                ELSE 0 END) as avg_deviation_percentage
          FROM ${table}
        `);
        
        const stats = validationResult.rows[0];
        const complianceRate = stats.records_with_position > 0 ? 
          (parseFloat(stats.formula_compliant) / parseFloat(stats.records_with_position) * 100).toFixed(2) : 0;
        
        results.push({
          table: table,
          totalRecords: parseInt(stats.total_records),
          recordsWithPosition: parseInt(stats.records_with_position),
          formulaCompliant: parseInt(stats.formula_compliant),
          formulaViolations: parseInt(stats.formula_violations),
          complianceRate: `${complianceRate}%`,
          avgDeviationPercentage: parseFloat(stats.avg_deviation_percentage || 0).toFixed(4)
        });
        
        console.log(`   📈 Compliance Rate: ${complianceRate}% (${stats.formula_compliant}/${stats.records_with_position})`);
      }
      
      return { success: true, validationResults: results };
      
    } catch (error) {
      console.error('❌ Validation failed:', error.message);
      throw error;
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}

module.exports = NormalizationSchema; 