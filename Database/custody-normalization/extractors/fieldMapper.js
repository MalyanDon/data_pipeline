const { getCustodyConfig } = require('../config/custody-mappings');

class FieldMapper {
  constructor() {
    this.errors = [];
    this.warnings = [];
  }

  mapRecord(record, custodyType, metadata) {
    this.errors = [];
    this.warnings = [];

    const config = getCustodyConfig(custodyType);
    if (!config) {
      this.errors.push(`Unknown custody type: ${custodyType}`);
      return { mappedRecord: null, mappingResults: { errors: this.errors, warnings: this.warnings } };
    }

    const mappedRecord = {
      source_system: config.sourceSystem,
      file_name: metadata.fileName,
      record_date: metadata.recordDate
    };

    // Map required fields
    this._mapField(record, config.fieldMappings.client_reference, 'client_reference', mappedRecord, true);
    this._mapField(record, config.fieldMappings.instrument_isin, 'instrument_isin', mappedRecord, true);
    
    // Handle special Orbis cases for client_name and instrument_name
    if (custodyType === 'orbis') {
      // For Orbis: client_name is always "N/A", instrument_name is always NULL
      mappedRecord.client_name = "N/A";
      mappedRecord.instrument_name = null;
    } else {
      // For all other custody types, these are required fields
      this._mapField(record, config.fieldMappings.client_name, 'client_name', mappedRecord, true);
      this._mapField(record, config.fieldMappings.instrument_name, 'instrument_name', mappedRecord, true);
    }
    
    // Map optional fields - Handle special Orbis case for instrument_code
    if (custodyType === 'orbis') {
      // For Orbis: instrument_code is always NULL (BSE Code is kept as separate field)
      mappedRecord.instrument_code = null;
    } else {
      this._mapField(record, config.fieldMappings.instrument_code, 'instrument_code', mappedRecord, false);
    }
    
    // Map financial data fields with special handling
    this._mapFinancialField(record, config, 'blocked_quantity', mappedRecord);
    this._mapFinancialField(record, config, 'pending_buy_quantity', mappedRecord);
    this._mapFinancialField(record, config, 'pending_sell_quantity', mappedRecord);
    this._mapFinancialField(record, config, 'total_position', mappedRecord);
    this._mapFinancialField(record, config, 'saleable_quantity', mappedRecord);

    // Validate financial relationships
    this._validateFinancialRelationships(mappedRecord, custodyType);

    return {
      mappedRecord,
      mappingResults: {
        errors: this.errors,
        warnings: this.warnings
      }
    };
  }

  _mapField(record, fieldMappings, targetField, mappedRecord, required = false) {
    if (!fieldMappings) {
      if (required) {
        this.errors.push(`No field mapping defined for required field: ${targetField}`);
      }
      return;
    }

    if (Array.isArray(fieldMappings)) {
      // Try each field mapping option
      for (const fieldName of fieldMappings) {
        if (record.hasOwnProperty(fieldName) && record[fieldName] != null && record[fieldName] !== '') {
          mappedRecord[targetField] = this._cleanValue(record[fieldName]);
          return;
        }
      }
      
      if (required) {
        this.errors.push(`Required field '${targetField}' not found. Tried: ${fieldMappings.join(', ')}`);
      } else {
        this.warnings.push(`Optional field '${targetField}' not found. Tried: ${fieldMappings.join(', ')}`);
      }
    } else {
      // Single field mapping
      if (record.hasOwnProperty(fieldMappings) && record[fieldMappings] != null && record[fieldMappings] !== '') {
        mappedRecord[targetField] = this._cleanValue(record[fieldMappings]);
      } else if (required) {
        this.errors.push(`Required field '${targetField}' not found: ${fieldMappings}`);
      }
    }
  }

  _mapFinancialField(record, config, targetField, mappedRecord) {
    const fieldMappings = config.fieldMappings[targetField];
    
    if (fieldMappings === null) {
      // Field is explicitly not available for this custody type (e.g., Trust PMS total_position)
      mappedRecord[targetField] = null;
      return;
    }

    if (!fieldMappings) {
      // No mapping defined - default to 0
      mappedRecord[targetField] = 0;
      return;
    }

    // Check if this custody type needs special summing logic
    const sumFields = config.processingRules?.sumFinancialFields?.[targetField];
    
    if (sumFields && Array.isArray(sumFields)) {
      // Sum multiple fields (AXIS case)
      let totalValue = 0;
      let foundAnyValue = false;
      
      for (const fieldName of sumFields) {
        if (record.hasOwnProperty(fieldName) && record[fieldName] != null && record[fieldName] !== '') {
          const numericValue = this._parseFinancialAmount(record[fieldName]);
          if (numericValue !== null) {
            totalValue += numericValue;
            foundAnyValue = true;
          }
        }
      }
      
      mappedRecord[targetField] = foundAnyValue ? totalValue : 0;
      
    } else if (Array.isArray(fieldMappings)) {
      // Try each field mapping option
      for (const fieldName of fieldMappings) {
        if (record.hasOwnProperty(fieldName) && record[fieldName] != null && record[fieldName] !== '') {
          const numericValue = this._parseFinancialAmount(record[fieldName]);
          mappedRecord[targetField] = numericValue !== null ? numericValue : 0;
          return;
        }
      }
      
      // No value found
      mappedRecord[targetField] = 0;
      
    } else {
      // Single field mapping
      if (record.hasOwnProperty(fieldMappings) && record[fieldMappings] != null && record[fieldMappings] !== '') {
        const numericValue = this._parseFinancialAmount(record[fieldMappings]);
        mappedRecord[targetField] = numericValue !== null ? numericValue : 0;
      } else {
        mappedRecord[targetField] = 0;
      }
    }
  }

  _parseFinancialAmount(value) {
    if (value == null || value === '') return null;
    
    // Convert to string and clean up
    const stringValue = String(value).trim();
    
    // Remove common formatting characters
    const cleanValue = stringValue
      .replace(/[,\s]/g, '') // Remove commas and spaces
      .replace(/[()]/g, ''); // Remove parentheses
    
    // Try to parse as number
    const numericValue = parseFloat(cleanValue);
    
    if (isNaN(numericValue)) {
      this.warnings.push(`Invalid financial amount: '${value}' - setting to 0`);
      return 0;
    }
    
    // Validate reasonable range
    if (numericValue < 0) {
      this.warnings.push(`Negative financial amount: '${value}' - setting to 0`);
      return 0;
    }
    
    if (numericValue > 999999999999.9999) {
      this.warnings.push(`Extremely large financial amount: '${value}' - may need validation`);
    }
    
    return numericValue;
  }

  _validateFinancialRelationships(mappedRecord, custodyType) {
    // Skip validation for Trust PMS since total_position is null
    if (custodyType === 'trustpms' || mappedRecord.total_position === null) {
      return;
    }

    const totalPosition = parseFloat(mappedRecord.total_position) || 0;
    const blockedQuantity = parseFloat(mappedRecord.blocked_quantity) || 0;
    const saleableQuantity = parseFloat(mappedRecord.saleable_quantity) || 0;

    // Skip validation if no positions
    if (totalPosition === 0) {
      return;
    }

    // Validate formula: saleable_quantity ≈ total_position - blocked_quantity
    const expectedSaleable = totalPosition - blockedQuantity;
    const actualSaleable = saleableQuantity;
    const tolerance = Math.max(totalPosition * 0.01, 0.0001); // 1% tolerance or minimum 0.0001

    const deviation = Math.abs(expectedSaleable - actualSaleable);
    
    if (deviation > tolerance) {
      const deviationPercentage = ((deviation / totalPosition) * 100).toFixed(2);
      this.warnings.push(
        `Financial formula validation: Expected saleable ${expectedSaleable.toFixed(4)}, ` +
        `got ${actualSaleable.toFixed(4)} (${deviationPercentage}% deviation). ` +
        `Formula: ${totalPosition} - ${blockedQuantity} = ${expectedSaleable.toFixed(4)}`
      );
    }

    // Validate that saleable quantity doesn't exceed total position
    if (saleableQuantity > totalPosition) {
      this.warnings.push(
        `Saleable quantity (${saleableQuantity}) exceeds total position (${totalPosition})`
      );
    }

    // Validate that blocked quantity doesn't exceed total position significantly
    if (blockedQuantity > totalPosition * 1.01) { // Allow 1% tolerance for rounding
      this.warnings.push(
        `Blocked quantity (${blockedQuantity}) significantly exceeds total position (${totalPosition})`
      );
    }
  }

  _cleanValue(value) {
    if (value == null) return '';
    
    return String(value).trim();
  }

  mapRecords(records, custodyType, metadata) {
    const mappedRecords = [];
    const allErrors = [];
    const allWarnings = [];

    for (let i = 0; i < records.length; i++) {
      const result = this.mapRecord(records[i], custodyType, metadata);
      
      if (result.mappedRecord) {
        mappedRecords.push(result.mappedRecord);
      }
      
      // Collect errors and warnings with row numbers
      result.mappingResults.errors.forEach(error => {
        allErrors.push(`Row ${i + 1}: ${error}`);
      });
      
      result.mappingResults.warnings.forEach(warning => {
        allWarnings.push(`Row ${i + 1}: ${warning}`);
      });
    }

    return {
      mappedRecords,
      mappingResults: {
        errors: allErrors,
        warnings: allWarnings,
        totalRecords: records.length,
        mappedRecords: mappedRecords.length
      }
    };
  }

  getFieldMappingInfo(custodyType) {
    const config = getCustodyConfig(custodyType);
    if (!config) return null;

    return {
      custodyType,
      sourceSystem: config.sourceSystem,
      fieldMappings: config.fieldMappings,
      processingRules: config.processingRules,
      supportedExtensions: config.fileExtensions
    };
  }
}

module.exports = FieldMapper; 