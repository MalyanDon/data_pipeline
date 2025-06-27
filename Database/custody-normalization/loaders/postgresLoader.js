const { Pool } = require('pg');
const config = require('../../config');
const NormalizationSchema = require('../config/normalization-schema');

class PostgresLoader {
  constructor(pgPool = null) {
    this.pool = pgPool || new Pool(config.postgresql);
    this.schema = new NormalizationSchema(this.pool);
  }

  async loadNormalizedData(normalizedRecords, sourceSystem, fileName) {
    if (!normalizedRecords || normalizedRecords.length === 0) {
      console.log('⚠️  No normalized records to load');
      return { success: true, inserted: 0, updated: 0, errors: [] };
    }

    console.log(`📊 Loading ${normalizedRecords.length} normalized records to PostgreSQL...`);
    
    // Group records by date to create appropriate daily tables
    const recordsByDate = {};
    for (const record of normalizedRecords) {
      const dateKey = record.record_date;
      if (!recordsByDate[dateKey]) {
        recordsByDate[dateKey] = [];
      }
      recordsByDate[dateKey].push(record);
    }

    let totalInserted = 0;
    let totalUpdated = 0;
    const allErrors = [];

    // Process each date group separately
    for (const [recordDate, dateRecords] of Object.entries(recordsByDate)) {
      console.log(`📅 Processing ${dateRecords.length} records for date: ${recordDate}`);
      
      try {
        // Ensure daily table exists for this date
        const tableExists = await this.schema.dailyTableExists(recordDate);
        if (!tableExists) {
          await this.schema.createDailyTable(recordDate);
        }
        
        const tableName = this.schema.getTableName(recordDate);
        const result = await this._loadBatchToTable(dateRecords, tableName, sourceSystem, fileName);
        
        totalInserted += result.inserted;
        totalUpdated += result.updated;
        allErrors.push(...result.errors);
        
        console.log(`   ✅ ${result.inserted} inserted, ${result.updated} updated in ${tableName}`);
        
      } catch (error) {
        console.error(`❌ Failed to load records for date ${recordDate}:`, error.message);
        allErrors.push({
          date: recordDate,
          error: error.message,
          recordCount: dateRecords.length
        });
      }
    }

    const successRate = ((totalInserted + totalUpdated) / normalizedRecords.length * 100).toFixed(2);
    
    console.log(`📈 Load Summary:`);
    console.log(`   📅 Dates processed: ${Object.keys(recordsByDate).length}`);
    console.log(`   ✅ Total inserted: ${totalInserted}`);
    console.log(`   🔄 Total updated: ${totalUpdated}`);
    console.log(`   ❌ Total errors: ${allErrors.length}`);
    console.log(`   📊 Success rate: ${successRate}%`);

    return {
      success: allErrors.length === 0,
      inserted: totalInserted,
      updated: totalUpdated,
      errors: allErrors,
      dateGroups: Object.keys(recordsByDate).length,
      successRate: parseFloat(successRate)
    };
  }

  async _loadBatchToTable(records, tableName, sourceSystem, fileName) {
    const client = await this.pool.connect();
    let inserted = 0;
    let updated = 0;
    const errors = [];

    try {
      await client.query('BEGIN');

      // FIRST: Clear existing data for this source system and file to prevent duplicates
      console.log(`🧹 Clearing existing data for ${sourceSystem} - ${fileName}`);
      const deleteQuery = `
        DELETE FROM ${tableName} 
        WHERE source_system = $1 AND file_name = $2
      `;
      const deleteResult = await client.query(deleteQuery, [sourceSystem, fileName]);
      console.log(`   🗑️  Removed ${deleteResult.rowCount} existing records`);

      // Process in smaller batches for reliability
      const batchSize = 250;
      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize);
        
        for (const record of batch) {
          try {
            // Insert fresh data with enhanced financial fields (no conflict since we cleared existing)
            const query = `
              INSERT INTO ${tableName} (
                client_reference, client_name, instrument_isin, instrument_name, instrument_code,
                blocked_quantity, pending_buy_quantity, pending_sell_quantity,
                total_position, saleable_quantity,
                source_system, file_name, record_date
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            `;

            const values = [
              record.client_reference,
              record.client_name,
              record.instrument_isin,
              record.instrument_name,
              record.instrument_code || null,
              record.blocked_quantity || 0,
              record.pending_buy_quantity || 0,
              record.pending_sell_quantity || 0,
              record.total_position || (record.total_position === null ? null : 0), // Preserve null for Trust PMS
              record.saleable_quantity || 0,
              sourceSystem,
              fileName,
              record.record_date
            ];

            await client.query(query, values);
            inserted++; // All are fresh inserts since we cleared existing data

          } catch (recordError) {
            console.error(`❌ Error inserting record:`, recordError.message);
            errors.push({
              record: record,
              error: recordError.message
            });
          }
        }
        
        // Small delay between batches to reduce load
        if (i + batchSize < records.length) {
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      }

      await client.query('COMMIT');
      
    } catch (error) {
      await client.query('ROLLBACK');
      console.error(`❌ Batch load failed for table ${tableName}:`, error.message);
      throw error;
    } finally {
      client.release();
    }

    return { inserted, updated, errors };
  }

  // Get statistics for a specific date
  async getDailyStats(recordDate) {
    const client = await this.pool.connect();
    
    try {
      const tableName = this.schema.getTableName(recordDate);
      const tableExists = await this.schema.dailyTableExists(recordDate);
      
      if (!tableExists) {
        return {
          date: recordDate,
          exists: false,
          totalRecords: 0,
          uniqueClients: 0,
          uniqueInstruments: 0,
          sourceSystems: []
        };
      }

      const stats = await client.query(`
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT client_reference) as unique_clients,
          COUNT(DISTINCT instrument_isin) as unique_instruments,
          array_agg(DISTINCT source_system) as source_systems,
          SUM(CASE WHEN blocked_quantity > 0 THEN 1 ELSE 0 END) as records_with_blocked,
          SUM(CASE WHEN pending_buy_quantity > 0 THEN 1 ELSE 0 END) as records_with_pending_buy,
          SUM(CASE WHEN pending_sell_quantity > 0 THEN 1 ELSE 0 END) as records_with_pending_sell,
          SUM(CASE WHEN total_position > 0 THEN 1 ELSE 0 END) as records_with_total_position,
          SUM(CASE WHEN saleable_quantity > 0 THEN 1 ELSE 0 END) as records_with_saleable,
          SUM(CASE WHEN total_position IS NOT NULL THEN 1 ELSE 0 END) as records_with_total_position_data,
          AVG(CASE WHEN total_position > 0 AND saleable_quantity >= 0 
              THEN ABS((total_position - blocked_quantity) - saleable_quantity) / total_position * 100 
              ELSE NULL END) as avg_formula_deviation_percentage
        FROM ${tableName}
      `);

      return {
        date: recordDate,
        tableName: tableName,
        exists: true,
        totalRecords: parseInt(stats.rows[0].total_records),
        uniqueClients: parseInt(stats.rows[0].unique_clients),
        uniqueInstruments: parseInt(stats.rows[0].unique_instruments),
        sourceSystems: stats.rows[0].source_systems || [],
        recordsWithBlocked: parseInt(stats.rows[0].records_with_blocked),
        recordsWithPendingBuy: parseInt(stats.rows[0].records_with_pending_buy),
        recordsWithPendingSell: parseInt(stats.rows[0].records_with_pending_sell),
        recordsWithTotalPosition: parseInt(stats.rows[0].records_with_total_position),
        recordsWithSaleable: parseInt(stats.rows[0].records_with_saleable),
        recordsWithTotalPositionData: parseInt(stats.rows[0].records_with_total_position_data),
        avgFormulaDeviationPercentage: parseFloat(stats.rows[0].avg_formula_deviation_percentage || 0).toFixed(4)
      };
      
    } finally {
      client.release();
    }
  }

  // Get overall statistics across all daily tables
  async getOverallStats() {
    const client = await this.pool.connect();
    
    try {
      const tables = await this.schema.getAllDailyTables();
      
      if (tables.length === 0) {
        return {
          totalTables: 0,
          totalRecords: 0,
          uniqueClients: 0,
          uniqueInstruments: 0,
          dateRange: null,
          sourceSystems: []
        };
      }

      // Build union query for all tables with enhanced financial fields
      const unionQueries = tables.map(table => `
        SELECT 
          client_reference, instrument_isin, source_system, record_date,
          blocked_quantity, pending_buy_quantity, pending_sell_quantity,
          total_position, saleable_quantity
        FROM ${table}
      `);

      const unionQuery = unionQueries.join(' UNION ALL ');
      
      const stats = await client.query(`
        WITH all_data AS (${unionQuery})
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT client_reference) as unique_clients,
          COUNT(DISTINCT instrument_isin) as unique_instruments,
          array_agg(DISTINCT source_system) as source_systems,
          MIN(record_date) as min_date,
          MAX(record_date) as max_date,
          SUM(CASE WHEN blocked_quantity > 0 THEN 1 ELSE 0 END) as records_with_blocked,
          SUM(CASE WHEN pending_buy_quantity > 0 THEN 1 ELSE 0 END) as records_with_pending_buy,
          SUM(CASE WHEN pending_sell_quantity > 0 THEN 1 ELSE 0 END) as records_with_pending_sell,
          SUM(CASE WHEN total_position > 0 THEN 1 ELSE 0 END) as records_with_total_position,
          SUM(CASE WHEN saleable_quantity > 0 THEN 1 ELSE 0 END) as records_with_saleable,
          SUM(CASE WHEN total_position IS NOT NULL THEN 1 ELSE 0 END) as records_with_total_position_data,
          COUNT(*) FILTER (WHERE 
            total_position > 0 AND 
            ABS((total_position - blocked_quantity) - saleable_quantity) <= (total_position * 0.01)
          ) as formula_compliant_records,
          AVG(CASE WHEN total_position > 0 AND saleable_quantity >= 0 
              THEN ABS((total_position - blocked_quantity) - saleable_quantity) / total_position * 100 
              ELSE NULL END) as avg_formula_deviation_percentage
        FROM all_data
      `);

      const result = stats.rows[0];
      
      const formulaComplianceRate = result.records_with_total_position_data > 0 ? 
        (parseFloat(result.formula_compliant_records) / parseFloat(result.records_with_total_position_data) * 100).toFixed(2) : 0;

      return {
        totalTables: tables.length,
        tableNames: tables,
        totalRecords: parseInt(result.total_records),
        uniqueClients: parseInt(result.unique_clients),
        uniqueInstruments: parseInt(result.unique_instruments),
        sourceSystems: result.source_systems || [],
        dateRange: {
          from: result.min_date,
          to: result.max_date
        },
        recordsWithBlocked: parseInt(result.records_with_blocked),
        recordsWithPendingBuy: parseInt(result.records_with_pending_buy),
        recordsWithPendingSell: parseInt(result.records_with_pending_sell),
        recordsWithTotalPosition: parseInt(result.records_with_total_position),
        recordsWithSaleable: parseInt(result.records_with_saleable),
        recordsWithTotalPositionData: parseInt(result.records_with_total_position_data),
        formulaCompliantRecords: parseInt(result.formula_compliant_records),
        formulaComplianceRate: `${formulaComplianceRate}%`,
        avgFormulaDeviationPercentage: parseFloat(result.avg_formula_deviation_percentage || 0).toFixed(4)
      };
      
    } finally {
      client.release();
    }
  }

  // Query data from specific date
  async queryByDate(recordDate, filters = {}) {
    const client = await this.pool.connect();
    
    try {
      const tableName = this.schema.getTableName(recordDate);
      const tableExists = await this.schema.dailyTableExists(recordDate);
      
      if (!tableExists) {
        return {
          date: recordDate,
          exists: false,
          records: []
        };
      }

      let whereClause = '1=1';
      const params = [];
      let paramCount = 0;

      if (filters.clientReference) {
        paramCount++;
        whereClause += ` AND client_reference ILIKE $${paramCount}`;
        params.push(`%${filters.clientReference}%`);
      }

      if (filters.instrumentIsin) {
        paramCount++;
        whereClause += ` AND instrument_isin = $${paramCount}`;
        params.push(filters.instrumentIsin);
      }

      if (filters.sourceSystem) {
        paramCount++;
        whereClause += ` AND source_system = $${paramCount}`;
        params.push(filters.sourceSystem);
      }

      if (filters.hasBlocked) {
        whereClause += ' AND blocked_quantity > 0';
      }

      if (filters.hasPending) {
        whereClause += ' AND (pending_buy_quantity > 0 OR pending_sell_quantity > 0)';
      }

      if (filters.hasPosition) {
        whereClause += ' AND total_position > 0';
      }

      if (filters.hasSaleable) {
        whereClause += ' AND saleable_quantity > 0';
      }

      if (filters.hasFormulaDeviation) {
        whereClause += ' AND total_position > 0 AND ABS((total_position - blocked_quantity) - saleable_quantity) > (total_position * 0.01)';
      }

      const limit = filters.limit || 1000;
      paramCount++;
      
      const query = `
        SELECT * FROM ${tableName}
        WHERE ${whereClause}
        ORDER BY created_at DESC
        LIMIT $${paramCount}
      `;
      params.push(limit);

      const result = await client.query(query, params);
      
      return {
        date: recordDate,
        tableName: tableName,
        exists: true,
        records: result.rows,
        totalFound: result.rows.length
      };
      
    } finally {
      client.release();
    }
  }

  async close() {
    await this.pool.end();
    await this.schema.close();
  }

  /**
   * Public method for batch loading (used by multi-threaded pipeline)
   */
  async loadBatchToTable(records, tableName, sourceSystem, fileName) {
    return await this._loadBatchToTable(records, tableName, sourceSystem, fileName);
  }
}

module.exports = PostgresLoader; 