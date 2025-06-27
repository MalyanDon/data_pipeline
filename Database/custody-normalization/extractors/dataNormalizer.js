const NormalizationSchema = require('../config/normalization-schema');

class DataNormalizer {
  constructor() {
    this.schema = new NormalizationSchema();
    this.validationRules = this.schema.getValidationRules();
    this.processingStats = {
      totalRecords: 0,
      validRecords: 0,
      invalidRecords: 0,
      normalizedRecords: 0,
      errors: []
    };
  }

  /**
   * Normalize a single mapped record
   * @param {Object} mappedRecord - Record with mapped fields
   * @returns {Object} - Normalized record with validation results
   */
  normalizeRecord(mappedRecord) {
    const errors = [];
    const warnings = [];
    const normalizedRecord = { ...mappedRecord };

    // Special handling for Orbis and Deutsche records
    const isOrbisRecord = normalizedRecord.source_system === 'ORBIS';
    const isDeutscheRecord = normalizedRecord.source_system === 'DEUTSCHE';

    // Validate and normalize required fields
    for (const field of this.validationRules.requiredFields) {
      // Special validation for Orbis
      if (isOrbisRecord && field === 'client_name') {
        // For Orbis, client_name should be "N/A"
        if (normalizedRecord[field] !== "N/A") {
          normalizedRecord[field] = "N/A";
          warnings.push('client_name: Corrected to "N/A" for Orbis record');
        }
        continue;
      }
      
      if (isOrbisRecord && field === 'instrument_name') {
        // For Orbis, instrument_name should be null
        normalizedRecord[field] = null;
        continue;
      }

      // Special validation for Deutsche Bank (more lenient)
      if (isDeutscheRecord && (field === 'client_name' || field === 'instrument_name')) {
        if (!normalizedRecord[field] || normalizedRecord[field].trim() === '') {
          normalizedRecord[field] = field === 'client_name' ? 'DEUTSCHE_CLIENT' : 'DEUTSCHE_INSTRUMENT';
          warnings.push(`${field}: Set default value for Deutsche record`);
        }
        continue;
      }

      const result = this._validateField(normalizedRecord[field], field, this.validationRules.validation[field]);
      
      if (!result.isValid) {
        errors.push(`${field}: ${result.error}`);
      } else {
        normalizedRecord[field] = result.normalizedValue;
      }
    }

    // Validate and normalize optional fields
    for (const field of this.validationRules.optionalFields) {
      // Special handling for Orbis optional fields
      if (isOrbisRecord && field === 'instrument_code') {
        // For Orbis, instrument_code should always be null
        normalizedRecord[field] = null;
        continue;
      }

      // Special handling for Deutsche optional fields
      if (isDeutscheRecord && field === 'instrument_code') {
        // For Deutsche, set default value if missing
        if (!normalizedRecord[field] || normalizedRecord[field].trim() === '') {
          normalizedRecord[field] = 'DEUTSCHE_CODE';
          warnings.push(`${field}: Set default value for Deutsche record`);
        }
        continue;
      }

      if (normalizedRecord[field] !== undefined && normalizedRecord[field] !== null) {
        const result = this._validateField(normalizedRecord[field], field, this.validationRules.validation[field]);
        
        if (!result.isValid) {
          warnings.push(`${field}: ${result.error}`);
          // For optional fields, set to null/default if validation fails
          normalizedRecord[field] = this._getDefaultValue(field);
        } else {
          normalizedRecord[field] = result.normalizedValue;
        }
      } else {
        // Set default value for missing optional fields
        normalizedRecord[field] = this._getDefaultValue(field);
      }
    }

    // Special handling for financial fields
    this._normalizeFinancialFields(normalizedRecord, errors, warnings);

    return {
      success: errors.length === 0,
      normalizedRecord: errors.length === 0 ? normalizedRecord : null,
      errors,
      warnings
    };
  }

  _normalizeFinancialFields(record, errors, warnings) {
    const financialFields = ['blocked_quantity', 'pending_buy_quantity', 'pending_sell_quantity', 'total_position', 'saleable_quantity'];
    
    for (const field of financialFields) {
      const value = record[field];
      
      if (value === null || value === undefined) {
        // Keep as null for fields that are explicitly not available (e.g., Trust PMS total_position)
        continue;
      }
      
      const normalizedAmount = this.normalizeFinancialAmount(value);
      
      if (normalizedAmount.isValid) {
        record[field] = normalizedAmount.value;
      } else {
        warnings.push(`${field}: ${normalizedAmount.error} - setting to 0`);
        record[field] = 0;
      }
    }
    
    // Enhanced business logic validation including new fields
    this._validateFinancialBusinessRules(record, warnings);
    this._validatePositionRelationships(record, warnings);
  }

  normalizeFinancialAmount(value) {
    if (value === null || value === undefined) {
      return { isValid: true, value: null };
    }
    
    if (typeof value === 'number') {
      if (isNaN(value)) {
        return { isValid: false, error: 'Invalid number (NaN)' };
      }
      
      if (value < 0) {
        return { isValid: false, error: 'Negative amount not allowed' };
      }
      
      if (value > 999999999999.9999) {
        return { isValid: false, error: 'Amount exceeds maximum allowed value' };
      }
      
      // Round to 4 decimal places to match database precision
      const rounded = Math.round(value * 10000) / 10000;
      return { isValid: true, value: rounded };
    }
    
    if (typeof value === 'string') {
      const stringValue = value.trim();
      
      if (stringValue === '' || stringValue.toLowerCase() === 'null') {
        return { isValid: true, value: 0 };
      }
      
      // Remove formatting characters
      const cleanValue = stringValue
        .replace(/[,\s]/g, '')
        .replace(/[()]/g, '');
      
      const numericValue = parseFloat(cleanValue);
      
      if (isNaN(numericValue)) {
        return { isValid: false, error: `Cannot parse '${value}' as number` };
      }
      
      return this.normalizeFinancialAmount(numericValue);
    }
    
    return { isValid: false, error: `Invalid data type for financial amount: ${typeof value}` };
  }

  _validateFinancialBusinessRules(record, warnings) {
    // Check for potentially problematic combinations
    if (record.blocked_quantity > 0 && record.pending_sell_quantity > 0) {
      warnings.push('Financial data warning: Both blocked quantity and pending sell quantity are positive - may need review');
    }
    
    // Check for unreasonably large pending quantities
    const totalPending = (record.pending_buy_quantity || 0) + (record.pending_sell_quantity || 0);
    if (totalPending > 10000000) { // 10 million threshold
      warnings.push('Financial data warning: Very large pending quantities detected');
    }

    // Check for unreasonably large positions
    if (record.total_position > 100000000) { // 100 million threshold
      warnings.push('Financial data warning: Very large total position detected');
    }
    
    // Source-specific business rules
    if (record.source_system === 'TRUSTPMS' && record.blocked_quantity !== null && record.blocked_quantity > 0) {
      warnings.push('Financial data warning: TrustPMS usually does not track blocked quantities');
    }
    
    if (record.source_system === 'HDFC' && record.pending_sell_quantity !== null && record.pending_sell_quantity > 0) {
      warnings.push('Financial data warning: HDFC usually does not track pending sell quantities');
    }

    if (record.source_system === 'TRUSTPMS' && record.total_position !== null) {
      warnings.push('Financial data warning: TrustPMS usually only tracks saleable position, not total position');
    }
  }

  _validatePositionRelationships(record, warnings) {
    // Skip validation for Trust PMS since total_position is null
    if (record.source_system === 'TRUSTPMS' || record.total_position === null) {
      return;
    }

    const totalPosition = parseFloat(record.total_position) || 0;
    const blockedQuantity = parseFloat(record.blocked_quantity) || 0;
    const saleableQuantity = parseFloat(record.saleable_quantity) || 0;

    // Skip validation if no positions
    if (totalPosition === 0 && saleableQuantity === 0) {
      return;
    }

    // Validate core formula: saleable_quantity ≈ total_position - blocked_quantity
    if (totalPosition > 0) {
      const expectedSaleable = totalPosition - blockedQuantity;
      const actualSaleable = saleableQuantity;
      const tolerance = Math.max(totalPosition * 0.01, 0.0001); // 1% tolerance or minimum 0.0001

      const deviation = Math.abs(expectedSaleable - actualSaleable);
      
      if (deviation > tolerance) {
        const deviationPercentage = ((deviation / totalPosition) * 100).toFixed(2);
        warnings.push(
          `Position relationship validation: Expected saleable ${expectedSaleable.toFixed(4)}, ` +
          `got ${actualSaleable.toFixed(4)} (${deviationPercentage}% deviation). ` +
          `Formula: ${totalPosition} - ${blockedQuantity} = ${expectedSaleable.toFixed(4)}`
        );
      }
    }

    // Validate logical constraints
    if (saleableQuantity > totalPosition && totalPosition > 0) {
      const overage = ((saleableQuantity - totalPosition) / totalPosition * 100).toFixed(2);
      warnings.push(
        `Position logic error: Saleable quantity (${saleableQuantity}) exceeds total position (${totalPosition}) by ${overage}%`
      );
    }

    if (blockedQuantity > totalPosition * 1.05 && totalPosition > 0) { // Allow 5% tolerance for rounding
      const overage = ((blockedQuantity - totalPosition) / totalPosition * 100).toFixed(2);
      warnings.push(
        `Position logic error: Blocked quantity (${blockedQuantity}) exceeds total position (${totalPosition}) by ${overage}%`
      );
    }

    // Validate that saleable quantity is not negative when it should be positive
    if (totalPosition > blockedQuantity && saleableQuantity < 0) {
      warnings.push(
        `Position logic error: Negative saleable quantity (${saleableQuantity}) when positive expected (${totalPosition} - ${blockedQuantity})`
      );
    }

    // Validate zero saleable when total equals blocked
    if (Math.abs(totalPosition - blockedQuantity) < 0.0001 && Math.abs(saleableQuantity) > 0.0001) {
      warnings.push(
        `Position logic warning: Expected zero saleable quantity when total equals blocked (${totalPosition} = ${blockedQuantity}), got ${saleableQuantity}`
      );
    }
  }

  _getDefaultValue(field) {
    if (['blocked_quantity', 'pending_buy_quantity', 'pending_sell_quantity', 'total_position', 'saleable_quantity'].includes(field)) {
      return 0;
    }
    return null;
  }

  _validateField(value, fieldName, rules) {
    if (!rules) {
      return { isValid: true, normalizedValue: value };
    }

    // Check if field is required
    if (rules.required && (value === null || value === undefined || value === '')) {
      return { isValid: false, error: 'Required field is missing or empty' };
    }

    // If value is null/undefined and not required, that's okay
    if ((value === null || value === undefined) && !rules.required) {
      return { isValid: true, normalizedValue: null };
    }

    // Convert to string for validation
    const stringValue = String(value).trim();

    // Length validation
    if (rules.maxLength && stringValue.length > rules.maxLength) {
      return { 
        isValid: false, 
        error: `Exceeds maximum length of ${rules.maxLength} characters` 
      };
    }

    // Pattern validation (for ISIN)
    if (rules.pattern && !rules.pattern.test(stringValue)) {
      return { 
        isValid: false, 
        error: 'Does not match required pattern' 
      };
    }

    // Enum validation
    if (rules.enum && !rules.enum.includes(stringValue)) {
      return { 
        isValid: false, 
        error: `Must be one of: ${rules.enum.join(', ')}` 
      };
    }

    // Date validation
    if (rules.type === 'date') {
      const date = new Date(stringValue);
      if (isNaN(date.getTime())) {
        return { isValid: false, error: 'Invalid date format' };
      }
      
      // Check if date is not too far in the future
      const now = new Date();
      const oneYearFromNow = new Date(now.getFullYear() + 1, now.getMonth(), now.getDate());
      
      if (date > oneYearFromNow) {
        return { isValid: false, error: 'Date is too far in the future' };
      }
      
      return { isValid: true, normalizedValue: stringValue };
    }

    // Decimal validation
    if (rules.type === 'decimal') {
      const numericValue = parseFloat(stringValue);
      
      if (isNaN(numericValue)) {
        return { isValid: false, error: 'Invalid decimal number' };
      }
      
      if (rules.min !== undefined && numericValue < rules.min) {
        return { isValid: false, error: `Must be at least ${rules.min}` };
      }
      
      if (rules.max !== undefined && numericValue > rules.max) {
        return { isValid: false, error: `Must not exceed ${rules.max}` };
      }
      
      return { isValid: true, normalizedValue: numericValue };
    }

    return { isValid: true, normalizedValue: stringValue };
  }

  /**
   * Normalize multiple mapped records
   * @param {Array} mappedRecords - Array of mapped records
   * @returns {Object} - Normalization results
   */
  normalizeRecords(mappedRecords) {
    this.resetStats();
    
    const results = {
      success: true,
      totalRecords: mappedRecords.length,
      normalizedRecords: [],
      validCount: 0,
      invalidCount: 0,
      errors: [],
      warnings: [],
      fieldStats: {},
      processingTime: Date.now()
    };

    // Initialize field statistics
    const fields = ['client_reference', 'client_name', 'instrument_isin', 'instrument_name', 'instrument_code'];
    fields.forEach(field => {
      results.fieldStats[field] = {
        total: 0,
        normalized: 0,
        errors: 0,
        percentage: 0
      };
    });

    mappedRecords.forEach((record, index) => {
      const normalizationResult = this.normalizeRecord(record);
      
      if (normalizationResult.success) {
        results.normalizedRecords.push(normalizationResult.normalizedRecord);
        results.validCount++;

        // Update field statistics
        fields.forEach(field => {
          results.fieldStats[field].total++;
          if (normalizationResult.normalizedRecord[field] !== null) {
            results.fieldStats[field].normalized++;
          }
        });

      } else {
        results.invalidCount++;
        results.errors.push({
          recordIndex: index,
          errors: normalizationResult.errors,
          originalRecord: record
        });

        // Track field errors
        fields.forEach(field => {
          results.fieldStats[field].total++;
          results.fieldStats[field].errors++;
        });
      }

      // Collect warnings
      if (normalizationResult.warnings && normalizationResult.warnings.length > 0) {
        results.warnings.push({
          recordIndex: index,
          warnings: normalizationResult.warnings
        });
      }
    });

    // Calculate field percentages
    fields.forEach(field => {
      const stats = results.fieldStats[field];
      stats.percentage = stats.total > 0 ? Math.round((stats.normalized / stats.total) * 100) : 0;
    });

    results.success = results.invalidCount === 0;
    results.processingTime = Date.now() - results.processingTime;

    return results;
  }

  /**
   * Normalize client reference
   * @param {string} value - Raw client reference
   * @returns {string|null} - Normalized client reference
   */
  normalizeClientReference(value) {
    if (!value) return null;
    
    // Convert to string and trim
    let normalized = value.toString().trim();
    
    // Convert to uppercase
    normalized = normalized.toUpperCase();
    
    // Remove special characters except underscore and hyphen
    normalized = normalized.replace(/[^A-Z0-9_\-]/g, '');
    
    // Remove multiple consecutive underscores/hyphens
    normalized = normalized.replace(/[_\-]+/g, match => match[0]);
    
    // Remove leading/trailing underscores/hyphens
    normalized = normalized.replace(/^[_\-]+|[_\-]+$/g, '');
    
    return normalized || null;
  }

  /**
   * Normalize client name
   * @param {string} value - Raw client name
   * @returns {string|null} - Normalized client name
   */
  normalizeClientName(value) {
    if (!value) return null;
    
    // Convert to string and trim
    let normalized = value.toString().trim();
    
    // Convert to uppercase
    normalized = normalized.toUpperCase();
    
    // Replace multiple spaces with single space
    normalized = normalized.replace(/\s+/g, ' ');
    
    // Remove leading/trailing spaces
    normalized = normalized.trim();
    
    return normalized || null;
  }

  /**
   * Normalize ISIN
   * @param {string} value - Raw ISIN
   * @returns {string|null} - Normalized ISIN
   */
  normalizeISIN(value) {
    if (!value) return null;
    
    // Convert to string and trim
    let normalized = value.toString().trim();
    
    // Convert to uppercase
    normalized = normalized.toUpperCase();
    
    // Remove any spaces or special characters
    normalized = normalized.replace(/[^A-Z0-9]/g, '');
    
    // Validate ISIN format (2 letters + 10 alphanumeric)
    if (normalized.length === 12 && /^[A-Z]{2}[A-Z0-9]{10}$/.test(normalized)) {
      return normalized;
    }
    
    // Try to fix common ISIN issues
    if (normalized.length > 12) {
      // Truncate if too long
      normalized = normalized.substring(0, 12);
      if (/^[A-Z]{2}[A-Z0-9]{10}$/.test(normalized)) {
        return normalized;
      }
    }
    
    return null; // Invalid ISIN
  }

  /**
   * Normalize instrument name
   * @param {string} value - Raw instrument name
   * @returns {string|null} - Normalized instrument name
   */
  normalizeInstrumentName(value) {
    if (!value) return null;
    
    // Convert to string and trim
    let normalized = value.toString().trim();
    
    // Replace multiple spaces with single space
    normalized = normalized.replace(/\s+/g, ' ');
    
    // Remove leading/trailing spaces
    normalized = normalized.trim();
    
    // Capitalize first letter of each word
    normalized = normalized.replace(/\b\w/g, l => l.toUpperCase());
    
    return normalized || null;
  }

  /**
   * Normalize instrument code
   * @param {string} value - Raw instrument code
   * @returns {string|null} - Normalized instrument code
   */
  normalizeInstrumentCode(value) {
    if (!value) return null;
    
    // Convert to string and trim
    let normalized = value.toString().trim();
    
    // Convert to uppercase
    normalized = normalized.toUpperCase();
    
    // Remove spaces
    normalized = normalized.replace(/\s/g, '');
    
    return normalized || null;
  }

  /**
   * Normalize source system
   * @param {string} value - Raw source system
   * @returns {string|null} - Normalized source system
   */
  normalizeSourceSystem(value) {
    if (!value) return null;
    
    // Convert to string, trim, and uppercase
    return value.toString().trim().toUpperCase();
  }

  /**
   * Normalize file name
   * @param {string} value - Raw file name
   * @returns {string|null} - Normalized file name
   */
  normalizeFileName(value) {
    if (!value) return null;
    
    // Convert to string and trim
    return value.toString().trim();
  }

  /**
   * Normalize record date
   * @param {string|Date} value - Raw record date
   * @returns {string|null} - Normalized record date in YYYY-MM-DD format
   */
  normalizeRecordDate(value) {
    if (!value) return null;
    
    try {
      const date = new Date(value);
      
      // Check if date is valid
      if (isNaN(date.getTime())) {
        return null;
      }
      
      // Check if date is not in the future
      if (date > new Date()) {
        return null;
      }
      
      // Return in YYYY-MM-DD format
      return date.toISOString().split('T')[0];
      
    } catch (error) {
      return null;
    }
  }

  /**
   * Get data quality report
   * @param {Array} normalizedRecords - Array of normalized records
   * @returns {Object} - Data quality report
   */
  getDataQualityReport(normalizedRecords) {
    const report = {
      totalRecords: normalizedRecords.length,
      fieldCompleteness: {},
      dataQualityScore: 0,
      recommendations: []
    };

    const fields = ['client_reference', 'client_name', 'instrument_isin', 'instrument_name', 'instrument_code'];
    
    fields.forEach(field => {
      const nonNullCount = normalizedRecords.filter(record => 
        record[field] !== null && record[field] !== ''
      ).length;
      
      const completeness = report.totalRecords > 0 
        ? Math.round((nonNullCount / report.totalRecords) * 100) 
        : 0;
      
      report.fieldCompleteness[field] = {
        total: report.totalRecords,
        populated: nonNullCount,
        missing: report.totalRecords - nonNullCount,
        completeness: completeness
      };

      // Generate recommendations
      if (field !== 'instrument_code' && completeness < 80) {
        report.recommendations.push({
          type: 'low_completeness',
          field: field,
          message: `${field} has low completeness (${completeness}%). Consider reviewing field mappings.`,
          severity: completeness < 50 ? 'high' : 'medium'
        });
      }
    });

    // Calculate overall data quality score
    const requiredFields = fields.filter(field => field !== 'instrument_code');
    const avgCompleteness = requiredFields.reduce((sum, field) => 
      sum + report.fieldCompleteness[field].completeness, 0
    ) / requiredFields.length;
    
    report.dataQualityScore = Math.round(avgCompleteness);

    // Overall recommendations
    if (report.dataQualityScore < 70) {
      report.recommendations.push({
        type: 'overall_quality',
        message: `Overall data quality score is ${report.dataQualityScore}%. Consider reviewing file mappings and source data quality.`,
        severity: 'high'
      });
    }

    return report;
  }

  /**
   * Reset processing statistics
   */
  resetStats() {
    this.processingStats = {
      totalRecords: 0,
      validRecords: 0,
      invalidRecords: 0,
      normalizedRecords: 0,
      errors: []
    };
  }

  /**
   * Get processing statistics
   * @returns {Object} - Processing statistics
   */
  getStats() {
    return { ...this.processingStats };
  }
}

module.exports = DataNormalizer; 