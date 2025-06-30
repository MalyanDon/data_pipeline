const FieldMapper = require('./fieldMapper');

class SmartFileProcessor {
  constructor() {
    this.custodyFieldMapper = new FieldMapper();
  }

  // Intelligent file type detection based on collection names
  detectFileType(collectionName) {
    const name = collectionName.toLowerCase();
    
    // Master Data Types
    if (name.includes('broker_master_data')) return 'broker_master';
    if (name.includes('client_info_data')) return 'client_master';
    if (name.includes('distributor_master_data')) return 'distributor_master';
    if (name.includes('strategy_master_data')) return 'strategy_master';
    
    // Transaction Data Types
    if (name.includes('contract_notes_data')) return 'contract_notes';
    if (name.includes('cash_capital_flow_data')) return 'cash_flow';
    if (name.includes('stock_capital_flow_data')) return 'stock_flow';
    if (name.includes('mf_allocation_data')) return 'mf_allocations';
    
    // Custody Data Types
    if (name.includes('axis') && name.includes('custody')) return 'axis_custody';
    if (name.includes('hdfc') && name.includes('custody')) return 'hdfc_custody';
    if (name.includes('kotak') && name.includes('custody')) return 'kotak_custody';
    if (name.includes('deutsche') && name.includes('custody')) return 'deutsche_custody';
    if (name.includes('orbis') && name.includes('custody')) return 'orbis_custody';
    if (name.includes('trust') && name.includes('custody')) return 'trust_custody';
    if (name.includes('dl_164_ec0000720')) return 'deutsche_custody'; // Deutsche pattern
    
    // Default fallback
    return 'unknown';
  }

  // Get target table for file type
  getTargetTable(fileType) {
    const tableMap = {
      // Master Data
      'broker_master': 'brokers',
      'client_master': 'clients',
      'distributor_master': 'distributors',
      'strategy_master': 'strategies',
      
      // Transaction Data
      'contract_notes': 'contract_notes',
      'cash_flow': 'cash_capital_flow',
      'stock_flow': 'stock_capital_flow',
      'mf_allocations': 'mf_allocations',
      
      // Custody Data (unified table)
      'axis_custody': 'unified_custody_master',
      'hdfc_custody': 'unified_custody_master',
      'kotak_custody': 'unified_custody_master',
      'deutsche_custody': 'unified_custody_master',
      'orbis_custody': 'unified_custody_master',
      'trust_custody': 'unified_custody_master'
    };
    
    return tableMap[fileType] || 'raw_uploads';
  }

  // Process records based on file type
  processRecords(records, fileType, metadata) {
    switch (fileType) {
      case 'broker_master':
        return this.processBrokerMaster(records, metadata);
      case 'client_master':
        return this.processClientMaster(records, metadata);
      case 'distributor_master':
        return this.processDistributorMaster(records, metadata);
      case 'strategy_master':
        return this.processStrategyMaster(records, metadata);
      case 'contract_notes':
        return this.processContractNotes(records, metadata);
      case 'cash_flow':
        return this.processCashFlow(records, metadata);
      case 'stock_flow':
        return this.processStockFlow(records, metadata);
      case 'mf_allocations':
        return this.processMFAllocations(records, metadata);
      default:
        // Handle custody types with existing logic
        if (fileType.includes('custody')) {
          const custodyType = fileType.replace('_custody', '');
          return this.custodyFieldMapper.mapRecords(records, custodyType, metadata);
        }
        return this.processUnknownType(records, metadata);
    }
  }

  // Master Data Processors
  processBrokerMaster(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          broker_code: this.cleanValue(
            record['Broker Code'] || record['BrokerCode'] || record['broker_code'] ||
            record['Broker Master Templete'] || `AUTO_BROKER_${index + 1}`
          ),
          broker_name: this.cleanValue(
            record['Broker Name'] || record['BrokerName'] || record['broker_name'] ||
            record['Broker Master Templete'] || `Auto Broker ${index + 1}`
          ),
          broker_type: this.cleanValue(record['Broker Type'] || record['BrokerType'] || record['broker_type'] || 'Unknown'),
          registration_number: this.cleanValue(record['Registration Number'] || record['RegNumber'] || record['registration_number']),
          contact_person: this.cleanValue(record['Contact Person'] || record['ContactPerson'] || record['contact_person']),
          email: this.cleanValue(record['Email'] || record['email']),
          phone: this.cleanValue(record['Phone'] || record['phone']),
          address: this.cleanValue(record['Address'] || record['address']),
          city: this.cleanValue(record['City'] || record['city']),
          state: this.cleanValue(record['State'] || record['state']),
          country: this.cleanValue(record['Country'] || record['country'] || 'India'),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Auto-generate missing required fields (no strict validation - be flexible)
        if (!mapped.broker_code || mapped.broker_code === '') {
          mapped.broker_code = `AUTO_BROKER_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated broker_code: ${mapped.broker_code}`);
        }
        if (!mapped.broker_name || mapped.broker_name === '') {
          mapped.broker_name = `Auto Broker ${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated broker_name: ${mapped.broker_name}`);
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processClientMaster(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          client_code: this.cleanValue(
            record['Client Code'] || record['ClientCode'] || record['client_code'] ||
            Object.keys(record)[0] || `AUTO_CLIENT_${index + 1}`
          ),
          client_name: this.cleanValue(
            record['Client Name'] || record['ClientName'] || record['client_name'] ||
            Object.values(record).find(val => val && typeof val === 'string' && val.length > 2) ||
            `Auto Client ${index + 1}`
          ),
          client_type: this.cleanValue(record['Client Type'] || record['ClientType'] || record['client_type'] || 'Individual'),
          pan_number: this.cleanValue(record['PAN'] || record['PAN Number'] || record['pan_number']),
          email: this.cleanValue(record['Email'] || record['email']),
          phone: this.cleanValue(record['Phone'] || record['Mobile'] || record['phone']),
          address: this.cleanValue(record['Address'] || record['address']),
          city: this.cleanValue(record['City'] || record['city']),
          state: this.cleanValue(record['State'] || record['state']),
          country: this.cleanValue(record['Country'] || record['country'] || 'India'),
          risk_category: this.cleanValue(record['Risk Category'] || record['RiskCategory'] || record['risk_category'] || 'Medium'),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Auto-generate missing fields (flexible approach)
        if (!mapped.client_code || mapped.client_code === '') {
          mapped.client_code = `AUTO_CLIENT_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
        }
        if (!mapped.client_name || mapped.client_name === '') {
          mapped.client_name = `Auto Client ${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated client_name: ${mapped.client_name}`);
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processDistributorMaster(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          distributor_arn_number: this.cleanValue(
            record['distributor arn number'] || record['Distributor ARN Number'] || 
            record['ARN Number'] || record['ARN'] || record['arn_number'] ||
            record['arn'] || record['AMFI Registration Number'] ||
            `AUTO_ARN_${Date.now()}_${index + 1}`
          ),
          distributor_code: this.cleanValue(
            record['Distributor Code'] || record['DistributorCode'] || record['distributor_code'] ||
            record['email'] || `AUTO_DIST_${index + 1}`
          ),
          distributor_name: this.cleanValue(
            record['Distributor Name'] || record['DistributorName'] || record['distributor_name'] ||
            Object.values(record).find(val => val && typeof val === 'string' && val.length > 2) ||
            `Auto Distributor ${index + 1}`
          ),
          distributor_type: this.cleanValue(record['Distributor Type'] || record['DistributorType'] || record['distributor_type'] || 'External'),
          commission_rate: this.parseNumeric(record['Commission Rate'] || record['CommissionRate'] || record['commission_rate']) || 0,
          contact_person: this.cleanValue(record['Contact Person'] || record['ContactPerson'] || record['contact_person']),
          email: this.cleanValue(record['Email'] || record['email']),
          phone: this.cleanValue(record['Phone'] || record['phone']),
          address: this.cleanValue(record['Address'] || record['address']),
          city: this.cleanValue(record['City'] || record['city']),
          state: this.cleanValue(record['State'] || record['state']),
          country: this.cleanValue(record['Country'] || record['country'] || 'India'),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Auto-generate missing fields (flexible approach)
        if (!mapped.distributor_arn_number || mapped.distributor_arn_number === '' || mapped.distributor_arn_number.startsWith('AUTO_ARN_')) {
          mapped.distributor_arn_number = `AUTO_ARN_${Date.now()}_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated distributor_arn_number: ${mapped.distributor_arn_number}`);
        }
        if (!mapped.distributor_code || mapped.distributor_code === '') {
          mapped.distributor_code = `AUTO_DIST_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated distributor_code: ${mapped.distributor_code}`);
        }
        if (!mapped.distributor_name || mapped.distributor_name === '') {
          mapped.distributor_name = `Auto Distributor ${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated distributor_name: ${mapped.distributor_name}`);
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processStrategyMaster(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          strategy_code: this.cleanValue(
            record['Strategy Code'] || record['StrategyCode'] || record['strategy_code'] ||
            record['Filed Name'] || `AUTO_STRATEGY_${index + 1}`
          ),
          strategy_name: this.cleanValue(
            record['Strategy Name'] || record['StrategyName'] || record['strategy_name'] ||
            record['Data'] || record['Filed Name'] || `Auto Strategy ${index + 1}`
          ),
          strategy_type: this.cleanValue(record['Strategy Type'] || record['StrategyType'] || record['strategy_type'] || 'Equity'),
          description: this.cleanValue(record['Description'] || record['description'] || record['Data']),
          benchmark: this.cleanValue(record['Benchmark'] || record['benchmark']),
          risk_level: this.cleanValue(record['Risk Level'] || record['RiskLevel'] || record['risk_level'] || 'Medium'),
          min_investment: this.parseNumeric(record['Min Investment'] || record['MinInvestment'] || record['min_investment']) || 0,
          max_investment: this.parseNumeric(record['Max Investment'] || record['MaxInvestment'] || record['max_investment']) || 0,
          management_fee: this.parseNumeric(record['Management Fee'] || record['ManagementFee'] || record['management_fee']) || 0,
          performance_fee: this.parseNumeric(record['Performance Fee'] || record['PerformanceFee'] || record['performance_fee']) || 0,
          created_at: new Date(),
          updated_at: new Date()
        };

        // Auto-generate missing fields (flexible approach)
        if (!mapped.strategy_code || mapped.strategy_code === '') {
          mapped.strategy_code = `AUTO_STRATEGY_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated strategy_code: ${mapped.strategy_code}`);
        }
        if (!mapped.strategy_name || mapped.strategy_name === '') {
          mapped.strategy_name = `Auto Strategy ${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated strategy_name: ${mapped.strategy_name}`);
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  // Transaction Data Processors
  processContractNotes(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          ecn_number: this.cleanValue(
            record['ECN No'] || record['ECN Number'] || record['ecn_number'] ||
            record['Contract Note Number'] || record['ContractNoteNumber'] || 
            record['contract_note_number'] || `AUTO_ECN_${Date.now()}_${index + 1}`
          ),
          ecn_status: this.cleanValue(record['ECN Status'] || record['ecn_status'] || 'ACTIVE'),
          ecn_date: this.parseDate(
            record['ECN Date'] || record['ecn_date'] || record['Trade Date'] || 
            record['TradeDate'] || record['trade_date'] || new Date()
          ),
          client_code: this.cleanValue(
            record['Client Exchange Code/UCC'] || record['Client Code'] || 
            record['ClientCode'] || record['client_code'] || `AUTO_CLIENT_${index + 1}`
          ),
          broker_name: this.cleanValue(
            record['Broker Name'] || record['Broker Code'] || record['BrokerCode'] || 
            record['broker_name'] || record['broker_code'] || 'AUTO_BROKER'
          ),
          instrument_isin: this.cleanValue(record['ISIN Code'] || record['ISIN'] || record['isin'] || record['instrument_isin']),
          instrument_name: this.cleanValue(
            record['Security Name'] || record['Instrument Name'] || record['InstrumentName'] || 
            record['instrument_name'] || `Security ${index + 1}`
          ),
          transaction_type: this.cleanValue(
            record['Transaction Type'] || record['TransactionType'] || record['transaction_type'] || 'BUY'
          ),
          delivery_type: this.cleanValue(record['Delivery Type'] || record['delivery_type'] || 'DELIVERY'),
          exchange: this.cleanValue(record['Exchange'] || record['exchange'] || 'NSE'),
          settlement_date: this.parseDate(
            record['Sett. Date'] || record['Settlement Date'] || record['SettlementDate'] || record['settlement_date']
          ),
          quantity: this.parseNumeric(record['Qty'] || record['Quantity'] || record['quantity']) || 0,
          net_amount: this.parseNumeric(
            record['Net Amount'] || record['NetAmount'] || record['net_amount'] || record['gross_amount']
          ) || 0,
          net_rate: this.parseNumeric(record['Net Rate'] || record['Price'] || record['price'] || record['net_rate']) || 0,
          brokerage_amount: this.parseNumeric(record['Brokerage Amount'] || record['Brokerage'] || record['brokerage'] || record['brokerage_amount']) || 0,
          service_tax: this.parseNumeric(record['Service Tax'] || record['Taxes'] || record['taxes'] || record['service_tax']) || 0,
          stt_amount: this.parseNumeric(record['STT Amount'] || record['STT'] || record['stt'] || record['stt_amount']) || 0,
          created_at: new Date(),
          updated_at: new Date()
        };

        // FLEXIBLE validation - auto-generate missing fields
        if (!mapped.ecn_number || mapped.ecn_number === '' || mapped.ecn_number.startsWith('AUTO_ECN_')) {
          mapped.ecn_number = `AUTO_ECN_${Date.now()}_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated ecn_number: ${mapped.ecn_number}`);
        }
        if (!mapped.client_code || mapped.client_code === '') {
          mapped.client_code = `AUTO_CLIENT_${index + 1}`;
          warnings.push(`Row ${index + 1}: Auto-generated client_code: ${mapped.client_code}`);
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processCashFlow(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          transaction_id: this.cleanValue(record['Transaction ID'] || record['TransactionID'] || record['transaction_id']),
          transaction_date: this.parseDate(record['Transaction Date'] || record['TransactionDate'] || record['transaction_date']),
          client_code: this.cleanValue(record['Client Code'] || record['ClientCode'] || record['client_code']),
          transaction_type: this.cleanValue(record['Transaction Type'] || record['TransactionType'] || record['transaction_type']),
          amount: this.parseNumeric(record['Amount'] || record['amount']) || 0,
          currency: this.cleanValue(record['Currency'] || record['currency'] || 'INR'),
          reference_number: this.cleanValue(record['Reference Number'] || record['ReferenceNumber'] || record['reference_number']),
          description: this.cleanValue(record['Description'] || record['description']),
          bank_account: this.cleanValue(record['Bank Account'] || record['BankAccount'] || record['bank_account']),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Validation
        if (!mapped.client_code) {
          errors.push(`Row ${index + 1}: client_code is required`);
          return;
        }
        if (!mapped.amount || mapped.amount === 0) {
          errors.push(`Row ${index + 1}: amount is required and must be non-zero`);
          return;
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processStockFlow(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          transaction_id: this.cleanValue(record['Transaction ID'] || record['TransactionID'] || record['transaction_id']),
          transaction_date: this.parseDate(record['Transaction Date'] || record['TransactionDate'] || record['transaction_date']),
          client_code: this.cleanValue(record['Client Code'] || record['ClientCode'] || record['client_code']),
          instrument_name: this.cleanValue(record['Instrument Name'] || record['InstrumentName'] || record['instrument_name']),
          isin: this.cleanValue(record['ISIN'] || record['isin']),
          transaction_type: this.cleanValue(record['Transaction Type'] || record['TransactionType'] || record['transaction_type']),
          quantity: this.parseNumeric(record['Quantity'] || record['quantity']) || 0,
          price: this.parseNumeric(record['Price'] || record['price']) || 0,
          value: this.parseNumeric(record['Value'] || record['value']) || 0,
          reference_number: this.cleanValue(record['Reference Number'] || record['ReferenceNumber'] || record['reference_number']),
          description: this.cleanValue(record['Description'] || record['description']),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Validation
        if (!mapped.client_code) {
          errors.push(`Row ${index + 1}: client_code is required`);
          return;
        }
        if (!mapped.instrument_name) {
          errors.push(`Row ${index + 1}: instrument_name is required`);
          return;
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processMFAllocations(records, metadata) {
    const mappedRecords = [];
    const errors = [];
    const warnings = [];

    records.forEach((record, index) => {
      try {
        const mapped = {
          allocation_id: this.cleanValue(record['Allocation ID'] || record['AllocationID'] || record['allocation_id']),
          allocation_date: this.parseDate(record['Allocation Date'] || record['AllocationDate'] || record['allocation_date']),
          client_code: this.cleanValue(record['Client Code'] || record['ClientCode'] || record['client_code']),
          fund_name: this.cleanValue(record['Fund Name'] || record['FundName'] || record['fund_name']),
          scheme_code: this.cleanValue(record['Scheme Code'] || record['SchemeCode'] || record['scheme_code']),
          transaction_type: this.cleanValue(record['Transaction Type'] || record['TransactionType'] || record['transaction_type']),
          amount: this.parseNumeric(record['Amount'] || record['amount']) || 0,
          units: this.parseNumeric(record['Units'] || record['units']) || 0,
          nav: this.parseNumeric(record['NAV'] || record['nav']) || 0,
          folio_number: this.cleanValue(record['Folio Number'] || record['FolioNumber'] || record['folio_number']),
          distributor_code: this.cleanValue(record['Distributor Code'] || record['DistributorCode'] || record['distributor_code']),
          created_at: new Date(),
          updated_at: new Date()
        };

        // Validation
        if (!mapped.client_code) {
          errors.push(`Row ${index + 1}: client_code is required`);
          return;
        }
        if (!mapped.fund_name) {
          errors.push(`Row ${index + 1}: fund_name is required`);
          return;
        }

        mappedRecords.push(mapped);
      } catch (error) {
        errors.push(`Row ${index + 1}: ${error.message}`);
      }
    });

    return {
      mappedRecords,
      mappingResults: { errors, warnings, totalRecords: records.length, mappedRecords: mappedRecords.length }
    };
  }

  processUnknownType(records, metadata) {
    // For unknown types, store as-is in raw_uploads table
    const mappedRecords = records.map((record, index) => ({
      ...record,
      _file_name: metadata.fileName,
      _upload_timestamp: new Date(),
      _processing_status: 'unknown_type'
    }));

    return {
      mappedRecords,
      mappingResults: { 
        errors: [], 
        warnings: [`Unknown file type - storing ${records.length} records as raw data`], 
        totalRecords: records.length, 
        mappedRecords: mappedRecords.length 
      }
    };
  }

  // Utility methods
  cleanValue(value) {
    if (value == null) return '';
    return String(value).trim();
  }

  parseNumeric(value) {
    if (value == null || value === '') return null;
    const cleanValue = String(value).replace(/[,\s]/g, '');
    const numericValue = parseFloat(cleanValue);
    return isNaN(numericValue) ? null : numericValue;
  }

  parseDate(value) {
    if (!value) return null;
    const date = new Date(value);
    return isNaN(date.getTime()) ? null : date;
  }
}

module.exports = SmartFileProcessor; 