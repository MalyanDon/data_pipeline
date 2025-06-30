// Field Mapper for different file types
// Maps various field names to standardized field names

const fieldMappings = {
  // Broker mappings
  broker: {
    'broker_code': ['broker_code', 'brokercode', 'broker_id', 'id'],
    'broker_name': ['broker_name', 'brokername', 'name', 'broker'],
    'contact_person': ['contact_person', 'contact', 'person', 'contact_name'],
    'email': ['email', 'email_id', 'mail', 'email_address'],
    'phone': ['phone', 'phone_number', 'mobile', 'contact_number']
  },
  
  // Client mappings
  client: {
    'client_code': ['client_code', 'clientcode', 'client_id', 'id'],
    'client_name': ['client_name', 'clientname', 'name', 'client'],
    'email': ['email', 'email_id', 'mail', 'email_address'],
    'phone': ['phone', 'phone_number', 'mobile', 'contact_number']
  },
  
  // Generic mappings for unknown types
  generic: {
    'id': ['id', 'code', 'ref', 'reference'],
    'name': ['name', 'title', 'description'],
    'date': ['date', 'created_date', 'updated_date', 'timestamp'],
    'amount': ['amount', 'value', 'price', 'cost'],
    'quantity': ['quantity', 'qty', 'count', 'number']
  }
};

class FieldMapper {
  constructor() {
    this.mappings = fieldMappings;
  }

  /**
   * Maps field names from source data to standardized field names
   * @param {Object} sourceData - The source data object
   * @param {String} dataType - The type of data (broker, client, etc.)
   * @returns {Object} - Mapped data object
   */
  mapFields(sourceData, dataType = 'generic') {
    if (!sourceData || typeof sourceData !== 'object') {
      return sourceData;
    }

    const mappings = this.mappings[dataType] || this.mappings.generic;
    const mappedData = {};
    
    // First, copy all original fields
    Object.assign(mappedData, sourceData);
    
    // Then apply mappings
    for (const [standardField, possibleFields] of Object.entries(mappings)) {
      for (const possibleField of possibleFields) {
        if (sourceData[possibleField] !== undefined) {
          mappedData[standardField] = sourceData[possibleField];
          break;
        }
      }
    }
    
    return mappedData;
  }

  /**
   * Maps multiple records
   * @param {Array} records - Array of records to map
   * @param {String} custodyType - The custody type
   * @param {Object} metadata - Additional metadata
   * @returns {Array} - Array of mapped records
   */
  mapRecords(records, custodyType = 'generic', metadata = {}) {
    if (!Array.isArray(records)) {
      return [];
    }

    return records.map(record => {
      const mappedRecord = this.mapFields(record, custodyType);
      
      // Add metadata if provided
      if (metadata) {
        Object.assign(mappedRecord, metadata);
      }
      
      return mappedRecord;
    });
  }

  /**
   * Detects the data type based on field names
   * @param {Object} sampleData - Sample data object
   * @returns {String} - Detected data type
   */
  detectDataType(sampleData) {
    if (!sampleData || typeof sampleData !== 'object') {
      return 'generic';
    }
    
    const fields = Object.keys(sampleData).map(f => f.toLowerCase());
    
    if (fields.some(f => f.includes('broker'))) {
      return 'broker';
    }
    
    if (fields.some(f => f.includes('client'))) {
      return 'client';
    }
    
    if (fields.some(f => f.includes('distributor'))) {
      return 'distributor';
    }
    
    if (fields.some(f => f.includes('strategy'))) {
      return 'strategy';
    }
    
    return 'generic';
  }
}

module.exports = FieldMapper; 
// Maps various field names to standardized field names

const fieldMappings = {
  // Broker mappings
  broker: {
    'broker_code': ['broker_code', 'brokercode', 'broker_id', 'id'],
    'broker_name': ['broker_name', 'brokername', 'name', 'broker'],
    'contact_person': ['contact_person', 'contact', 'person', 'contact_name'],
    'email': ['email', 'email_id', 'mail', 'email_address'],
    'phone': ['phone', 'phone_number', 'mobile', 'contact_number']
  },
  
  // Client mappings
  client: {
    'client_code': ['client_code', 'clientcode', 'client_id', 'id'],
    'client_name': ['client_name', 'clientname', 'name', 'client'],
    'email': ['email', 'email_id', 'mail', 'email_address'],
    'phone': ['phone', 'phone_number', 'mobile', 'contact_number']
  },
  
  // Generic mappings for unknown types
  generic: {
    'id': ['id', 'code', 'ref', 'reference'],
    'name': ['name', 'title', 'description'],
    'date': ['date', 'created_date', 'updated_date', 'timestamp'],
    'amount': ['amount', 'value', 'price', 'cost'],
    'quantity': ['quantity', 'qty', 'count', 'number']
  }
};

class FieldMapper {
  constructor() {
    this.mappings = fieldMappings;
  }

  /**
   * Maps field names from source data to standardized field names
   * @param {Object} sourceData - The source data object
   * @param {String} dataType - The type of data (broker, client, etc.)
   * @returns {Object} - Mapped data object
   */
  mapFields(sourceData, dataType = 'generic') {
    if (!sourceData || typeof sourceData !== 'object') {
      return sourceData;
    }

    const mappings = this.mappings[dataType] || this.mappings.generic;
    const mappedData = {};
    
    // First, copy all original fields
    Object.assign(mappedData, sourceData);
    
    // Then apply mappings
    for (const [standardField, possibleFields] of Object.entries(mappings)) {
      for (const possibleField of possibleFields) {
        if (sourceData[possibleField] !== undefined) {
          mappedData[standardField] = sourceData[possibleField];
          break;
        }
      }
    }
    
    return mappedData;
  }

  /**
   * Maps multiple records
   * @param {Array} records - Array of records to map
   * @param {String} custodyType - The custody type
   * @param {Object} metadata - Additional metadata
   * @returns {Array} - Array of mapped records
   */
  mapRecords(records, custodyType = 'generic', metadata = {}) {
    if (!Array.isArray(records)) {
      return [];
    }

    return records.map(record => {
      const mappedRecord = this.mapFields(record, custodyType);
      
      // Add metadata if provided
      if (metadata) {
        Object.assign(mappedRecord, metadata);
      }
      
      return mappedRecord;
    });
  }

  /**
   * Detects the data type based on field names
   * @param {Object} sampleData - Sample data object
   * @returns {String} - Detected data type
   */
  detectDataType(sampleData) {
    if (!sampleData || typeof sampleData !== 'object') {
      return 'generic';
    }
    
    const fields = Object.keys(sampleData).map(f => f.toLowerCase());
    
    if (fields.some(f => f.includes('broker'))) {
      return 'broker';
    }
    
    if (fields.some(f => f.includes('client'))) {
      return 'client';
    }
    
    if (fields.some(f => f.includes('distributor'))) {
      return 'distributor';
    }
    
    if (fields.some(f => f.includes('strategy'))) {
      return 'strategy';
    }
    
    return 'generic';
  }
}

module.exports = FieldMapper; 