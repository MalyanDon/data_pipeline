// Custody file type detection patterns and field mappings
const custodyMappings = {
  axis: {
    pattern: /axis.*eod.*custody/i,
    fileExtensions: ['.xlsx', '.xls'],
    headerRow: 1, // 0-indexed
    sourceSystem: 'AXIS',
    fieldMappings: {
      client_reference: ['UCC', 'ClientId', 'Client Id'],
      client_name: ['ClientName', 'Client Name'],
      instrument_isin: ['ISIN'],
      instrument_name: ['SecurityName', 'Security Name'],
      instrument_code: null, // Axis doesn't have instrument codes
      blocked_quantity: ['DematLockedQty', 'PhysicalLocked'],
      pending_buy_quantity: ['PurchaseOutstanding', 'PurchaseUnderProcess'],
      pending_sell_quantity: ['SaleOutstanding', 'SaleUnderProcess'],
      total_position: ['NetBalance'],
      saleable_quantity: ['DematFree']
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      uppercaseClientName: true,
      validateISIN: true,
      sumFinancialFields: {
        blocked_quantity: ['DematLockedQty', 'PhysicalLocked'],
        pending_buy_quantity: ['PurchaseOutstanding', 'PurchaseUnderProcess'],
        pending_sell_quantity: ['SaleOutstanding', 'SaleUnderProcess']
        // total_position and saleable_quantity use single field mappings (NetBalance, DematFree)
      }
    }
  },

  deutsche: {
    pattern: /DL_.*EC\d+/i,
    fileExtensions: ['.xlsx', '.xls'],
    headerRow: 8, // Deutsche Bank has headers at row 8
    sourceSystem: 'DEUTSCHE',
    fieldMappings: {
      client_reference: ['Client Code', 'ClientCode', 'client_code'],
      client_name: ['Master Name', 'MasterName', 'master_name'],
      instrument_isin: ['ISIN', 'isin', 'Isin'],
      instrument_name: ['Instrument Name', 'InstrumentName', 'instrument_name'],
      instrument_code: ['Instrument Code', 'InstrumentCode', 'instrument_code'],
      blocked_quantity: ['Blocked'],
      pending_buy_quantity: ['Pending Purchase'],
      pending_sell_quantity: ['Pending Sale'],
      total_position: ['Logical Position'],
      saleable_quantity: ['Saleable']
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      uppercaseClientName: true,
      validateISIN: true,
      skipEmptyRows: true
    }
  },

  trustpms: {
    pattern: /End_Client_Holding.*TRUSTPMS/i,
    fileExtensions: ['.xls', '.xlsx'],
    headerRow: 2,
    sourceSystem: 'TRUSTPMS',
    fieldMappings: {
      client_reference: ['__EMPTY', 'Client Code'],
      client_name: ['__EMPTY_1', 'Client Name'],
      instrument_isin: ['__EMPTY_4', 'Instrument ISIN'],
      instrument_name: ['__EMPTY_2', 'Instrument Name'],
      instrument_code: ['__EMPTY_6', 'Instrument Code'],
      blocked_quantity: null, // Trust PMS doesn't track blocked quantity
      pending_buy_quantity: ['__EMPTY_7', 'Pending Buy Position'],
      pending_sell_quantity: ['__EMPTY_8', 'Pending Sell Position'],
      total_position: null, // Trust PMS only tracks saleable, not total
      saleable_quantity: ['Saleable Position']
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      uppercaseClientName: true,
      validateISIN: true
    }
  },

  hdfc: {
    pattern: /hdfc.*eod.*custody/i,
    fileExtensions: ['.csv', '.xlsx', '.xls'],
    headerRow: 15, // HDFC has headers at row 15
    sourceSystem: 'HDFC',
    fieldMappings: {
      client_reference: ['Client Code', 'ClientCode', 'client_code'],
      client_name: ['Client Name', 'ClientName', 'client_name'],
      instrument_isin: ['ISIN Code', 'ISINCode', 'isin_code', 'ISIN'],
      instrument_name: ['Instrument Name', 'InstrumentName', 'instrument_name'],
      instrument_code: ['Instrument Code', 'InstrumentCode', 'instrument_code'],
      blocked_quantity: ['Pending Blocked Qty'],
      pending_buy_quantity: ['Pending Purchase'],
      pending_sell_quantity: null, // HDFC doesn't track pending sell
      total_position: ['Book Position'],
      saleable_quantity: ['Total Saleable']
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      uppercaseClientName: true,
      validateISIN: true,
      skipEmptyRows: true
    }
  },

  kotak: {
    pattern: /kotak.*eod.*custody/i,
    fileExtensions: ['.xlsx', '.xls'],
    headerRow: 1,
    sourceSystem: 'KOTAK',
    fieldMappings: {
      client_reference: ['Holding Report as of 09-Jul-2024', 'Cln Code'],
      client_name: ['__EMPTY', 'Cln Name'],
      instrument_isin: ['__EMPTY_2', 'Instr ISIN'],
      instrument_name: ['__EMPTY_3', 'Instr Name'],
      instrument_code: ['__EMPTY_1', 'Instr Code'],
      blocked_quantity: ['__EMPTY_8', 'Blocked'],
      pending_buy_quantity: ['__EMPTY_6', 'Pending Purchase'],
      pending_sell_quantity: ['__EMPTY_7', 'Pending Sale'],
      total_position: ['__EMPTY_5', 'Settled Position'], // Column 6 in original sheet (0-indexed as 5)
      saleable_quantity: ['__EMPTY_15', 'Saleable'] // Column 16 in original sheet (0-indexed as 15)
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      uppercaseClientName: true,
      validateISIN: true
    }
  },

  orbis: {
    pattern: /orbisCustody|orbis.*custody/i,
    fileExtensions: ['.xlsx', '.xls'],
    headerRow: 1,
    sourceSystem: 'ORBIS',
    fieldMappings: {
      client_reference: ['OFIN Code'],
      client_name: null, // Orbis doesn't track client names - will be set to "N/A"
      instrument_isin: ['ISIN'],
      instrument_name: null, // Orbis doesn't have proper instrument names - will be set to NULL
      instrument_code: null, // Orbis doesn't have standard instrument codes - will be set to NULL
      blocked_quantity: ['Blocked/Pledge'],
      pending_buy_quantity: ['Intrasit Purchase : '],
      pending_sell_quantity: ['Intrasit Sale : '],
      total_position: ['Holding Quantity'],
      saleable_quantity: ['Saleble Quantity']
    },
    processingRules: {
      trimWhitespace: true,
      uppercaseClientRef: true,
      validateISIN: true,
      // Special Orbis handling
      setClientNameNA: true, // Set client_name to "N/A"
      setInstrumentNameNull: true, // Set instrument_name to NULL
      setInstrumentCodeNull: true, // Set instrument_code to NULL
      // BSE Code is available but not mapped to instrument_code (kept separate)
      orbisSpecificFields: {
        bse_code: ['BSE Code'], // Keep BSE Code as Orbis-specific field
        asset_class: ['Asset Class'], // Keep Asset Class as Orbis-specific field
        description: ['Description'] // Keep Description as Orbis-specific field
      }
    }
  }
};

// Function to detect custody file type by filename
function detectCustodyFileType(filename) {
  const normalizedFilename = filename.toLowerCase();
  
  for (const [type, config] of Object.entries(custodyMappings)) {
    if (config.pattern.test(filename)) {
      const extension = filename.slice(filename.lastIndexOf('.')).toLowerCase();
      if (config.fileExtensions.includes(extension)) {
        return { type, config };
      }
    }
  }
  
  return null;
}

// Get field mapping for a specific custody type
function getFieldMapping(custodyType, targetField) {
  const mapping = custodyMappings[custodyType];
  if (!mapping || !mapping.fieldMappings[targetField]) {
    return [];
  }
  return mapping.fieldMappings[targetField];
}

// Get all supported custody types
function getSupportedCustodyTypes() {
  return Object.keys(custodyMappings);
}

// Get processing rules for a custody type
function getProcessingRules(custodyType) {
  const mapping = custodyMappings[custodyType];
  return mapping ? mapping.processingRules : {};
}

// Detect custody type from filename
function detectCustodyType(fileName) {
  const lowerFileName = fileName.toLowerCase();
  
  for (const [custodyType, config] of Object.entries(custodyMappings)) {
    if (config.pattern.test(lowerFileName)) {
      return custodyType;
    }
  }
  
  return 'unknown';
}

// Get custody configuration
function getCustodyConfig(custodyType) {
  return custodyMappings[custodyType] || null;
}

// Get supported file types
function getSupportedFileTypes() {
  return Object.keys(custodyMappings).map(type => ({
    type,
    pattern: custodyMappings[type].pattern.toString(),
    extensions: custodyMappings[type].fileExtensions,
    sourceSystem: custodyMappings[type].sourceSystem
  }));
}

// Validate file type
function validateFileType(fileName, custodyType) {
  const config = getCustodyConfig(custodyType);
  if (!config) return false;
  
  const extension = fileName.toLowerCase().substring(fileName.lastIndexOf('.'));
  return config.fileExtensions.includes(extension);
}

// Enhanced function to handle Orbis-specific data gaps
function normalizeOrbisRecord(record, config) {
  const normalized = {
    client_reference: null,
    client_name: "N/A", // Fixed value for Orbis
    instrument_isin: null,
    instrument_name: null, // NULL for Orbis
    instrument_code: null, // NULL for Orbis
    blocked_quantity: 0,
    pending_buy_quantity: 0,
    pending_sell_quantity: 0
  };

  // Extract client reference
  if (config.fieldMappings.client_reference) {
    for (const field of config.fieldMappings.client_reference) {
      if (record[field] !== undefined && record[field] !== null && record[field] !== '') {
        normalized.client_reference = String(record[field]).trim();
        break;
      }
    }
  }

  // Extract ISIN
  if (config.fieldMappings.instrument_isin) {
    for (const field of config.fieldMappings.instrument_isin) {
      if (record[field] !== undefined && record[field] !== null && record[field] !== '') {
        normalized.instrument_isin = String(record[field]).trim();
        break;
      }
    }
  }

  // Extract financial fields
  const extractFinancialField = (fieldMappings) => {
    if (!fieldMappings) return 0;
    for (const field of fieldMappings) {
      if (record[field] !== undefined && record[field] !== null && record[field] !== '') {
        const value = parseFloat(record[field]);
        return isNaN(value) ? 0 : Math.max(0, value); // Ensure non-negative
      }
    }
    return 0;
  };

  normalized.blocked_quantity = extractFinancialField(config.fieldMappings.blocked_quantity);
  normalized.pending_buy_quantity = extractFinancialField(config.fieldMappings.pending_buy_quantity);
  normalized.pending_sell_quantity = extractFinancialField(config.fieldMappings.pending_sell_quantity);

  // Store Orbis-specific fields for reference (not in main normalized record)
  const orbisSpecific = {};
  if (config.processingRules?.orbisSpecificFields) {
    for (const [key, fieldMappings] of Object.entries(config.processingRules.orbisSpecificFields)) {
      for (const field of fieldMappings) {
        if (record[field] !== undefined && record[field] !== null && record[field] !== '') {
          orbisSpecific[key] = String(record[field]).trim();
          break;
        }
      }
    }
  }

  return { normalized, orbisSpecific };
}

// Validation function for custody records
function validateCustodyRecord(record, custodyType) {
  const errors = [];

  // Required field validation
  if (!record.client_reference || record.client_reference.trim() === '') {
    errors.push('Missing client_reference');
  }

  if (!record.instrument_isin || record.instrument_isin.trim() === '') {
    errors.push('Missing instrument_isin');
  }

  // ISIN format validation
  if (record.instrument_isin && !/^[A-Z]{2}[A-Z0-9]{9}[0-9]$/.test(record.instrument_isin)) {
    errors.push(`Invalid ISIN format: ${record.instrument_isin}`);
  }

  // Custody-specific validations
  if (custodyType === 'ORBIS') {
    if (record.client_name !== "N/A") {
      errors.push('Orbis client_name must be "N/A"');
    }
    if (record.instrument_name !== null) {
      errors.push('Orbis instrument_name must be NULL');
    }
    if (record.instrument_code !== null) {
      errors.push('Orbis instrument_code must be NULL');
    }
  } else {
    // For non-Orbis systems, client_name and instrument_name should not be empty
    if (!record.client_name || record.client_name.trim() === '') {
      errors.push('Missing client_name');
    }
    if (!record.instrument_name || record.instrument_name.trim() === '') {
      errors.push('Missing instrument_name');
    }
  }

  // Financial field validation
  const financialFields = ['blocked_quantity', 'pending_buy_quantity', 'pending_sell_quantity'];
  financialFields.forEach(field => {
    if (record[field] !== null && record[field] !== undefined) {
      if (isNaN(record[field]) || record[field] < 0) {
        errors.push(`Invalid ${field}: must be non-negative number`);
      }
    }
  });

  return {
    isValid: errors.length === 0,
    errors
  };
}

module.exports = {
  custodyMappings,
  detectCustodyFileType,
  getFieldMapping,
  getSupportedCustodyTypes,
  getProcessingRules,
  detectCustodyType,
  getCustodyConfig,
  getSupportedFileTypes,
  validateFileType,
  normalizeOrbisRecord,
  validateCustodyRecord
}; 