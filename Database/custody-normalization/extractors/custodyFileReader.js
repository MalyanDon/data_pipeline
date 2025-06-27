const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const { detectCustodyFileType } = require('../config/custody-mappings');

class CustodyFileReader {
  constructor() {
    this.supportedExtensions = ['.xlsx', '.xls', '.csv'];
  }

  /**
   * Auto-detect custody file type and read data
   * @param {string} filePath - Path to the custody file
   * @returns {Object} - { success, type, data, metadata, errors }
   */
  async readCustodyFile(filePath) {
    try {
      // Validate file exists
      if (!fs.existsSync(filePath)) {
        return {
          success: false,
          error: `File not found: ${filePath}`
        };
      }

      const fileName = path.basename(filePath);
      const fileExtension = path.extname(fileName).toLowerCase();

      // Check supported file types
      if (!this.supportedExtensions.includes(fileExtension)) {
        return {
          success: false,
          error: `Unsupported file type: ${fileExtension}. Supported: ${this.supportedExtensions.join(', ')}`
        };
      }

      // Auto-detect custody file type
      const detection = detectCustodyFileType(fileName);
      if (!detection) {
        return {
          success: false,
          error: `Could not detect custody file type for: ${fileName}`
        };
      }

      const { type, config } = detection;
      console.log(`🔍 Detected custody type: ${type.toUpperCase()} for file: ${fileName}`);

      // Read file based on extension
      let rawData = [];
      let metadata = {
        fileName,
        filePath,
        custodyType: type,
        sourceSystem: config.sourceSystem,
        headerRow: config.headerRow,
        fileSize: fs.statSync(filePath).size
      };

      if (fileExtension === '.csv') {
        rawData = await this.readCSVFile(filePath, config);
      } else if (['.xlsx', '.xls'].includes(fileExtension)) {
        rawData = await this.readExcelFile(filePath, config);
      }

      // Apply file-specific processing rules
      const processedData = this.applyProcessingRules(rawData, config);

      return {
        success: true,
        type,
        config,
        data: processedData,
        metadata: {
          ...metadata,
          recordCount: processedData.length,
          headerRow: config.headerRow
        }
      };

    } catch (error) {
      console.error(`❌ Error reading custody file ${filePath}:`, error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Read CSV file with specific header row
   * @param {string} filePath - Path to CSV file
   * @param {Object} config - Custody type configuration
   * @returns {Array} - Array of record objects
   */
  async readCSVFile(filePath, config) {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());

    if (lines.length <= config.headerRow) {
      throw new Error(`File has insufficient rows. Expected header at row ${config.headerRow + 1}`);
    }

    // Get headers from specified row
    const headerLine = lines[config.headerRow];
    const headers = this.parseCSVLine(headerLine);

    // Process data rows
    const dataRows = lines.slice(config.headerRow + 1);
    const records = [];

    for (let i = 0; i < dataRows.length; i++) {
      const line = dataRows[i].trim();
      if (!line) continue; // Skip empty lines

      const values = this.parseCSVLine(line);
      const record = {};

      // Map values to headers
      headers.forEach((header, index) => {
        record[header.trim()] = values[index]?.trim() || '';
      });

      // Skip completely empty records
      if (Object.values(record).some(value => value)) {
        records.push(record);
      }
    }

    return records;
  }

  /**
   * Read Excel file with specific header row
   * @param {string} filePath - Path to Excel file
   * @param {Object} config - Custody type configuration
   * @returns {Array} - Array of record objects
   */
  async readExcelFile(filePath, config) {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0]; // Use first sheet
    const worksheet = workbook.Sheets[sheetName];

    // Get sheet range
    const range = XLSX.utils.decode_range(worksheet['!ref']);
    
    if (range.e.r < config.headerRow) {
      throw new Error(`Sheet has insufficient rows. Expected header at row ${config.headerRow + 1}`);
    }

    // Extract headers from specified row
    const headerRowIndex = config.headerRow;
    const headers = [];
    
    for (let col = range.s.c; col <= range.e.c; col++) {
      const cellAddress = XLSX.utils.encode_cell({ r: headerRowIndex, c: col });
      const cell = worksheet[cellAddress];
      headers.push(cell ? cell.v?.toString().trim() : '');
    }

    // Filter out empty headers and get column positions
    const validHeaders = [];
    headers.forEach((header, index) => {
      if (header) {
        validHeaders.push({ name: header, column: index });
      }
    });

    // Extract data rows
    const records = [];
    
    for (let row = headerRowIndex + 1; row <= range.e.r; row++) {
      const record = {};
      let hasData = false;

      validHeaders.forEach(({ name, column }) => {
        const cellAddress = XLSX.utils.encode_cell({ r: row, c: column });
        const cell = worksheet[cellAddress];
        let value = '';

        if (cell) {
          // Handle different cell types
          if (cell.t === 'n') { // Number
            value = cell.v.toString();
          } else if (cell.t === 's') { // String
            value = cell.v.toString().trim();
          } else if (cell.t === 'd') { // Date
            value = cell.v.toString();
          } else if (cell.v !== undefined) {
            value = cell.v.toString().trim();
          }
        }

        record[name] = value;
        if (value) hasData = true;
      });

      // Only add records that have at least some data
      if (hasData) {
        records.push(record);
      }
    }

    return records;
  }

  /**
   * Parse CSV line handling quoted values
   * @param {string} line - CSV line
   * @returns {Array} - Array of values
   */
  parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      
      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          // Handle escaped quotes
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        result.push(current);
        current = '';
      } else {
        current += char;
      }
    }
    
    result.push(current);
    return result;
  }

  /**
   * Apply file-specific processing rules
   * @param {Array} data - Raw data records
   * @param {Object} config - Custody type configuration
   * @returns {Array} - Processed data records
   */
  applyProcessingRules(data, config) {
    const rules = config.processingRules;
    
    return data.filter(record => {
      // Skip empty rows if configured
      if (rules.skipEmptyRows) {
        const hasNonEmptyValue = Object.values(record).some(value => 
          value && value.toString().trim()
        );
        if (!hasNonEmptyValue) return false;
      }

      return true;
    }).map(record => {
      const processedRecord = {};

      Object.keys(record).forEach(key => {
        let value = record[key];

        if (rules.trimWhitespace && typeof value === 'string') {
          value = value.trim();
        }

        processedRecord[key] = value;
      });

      return processedRecord;
    });
  }

  /**
   * Get available custody files from a directory
   * @param {string} directoryPath - Directory to scan
   * @returns {Array} - Array of custody file info
   */
  async scanCustodyFiles(directoryPath) {
    try {
      if (!fs.existsSync(directoryPath)) {
        throw new Error(`Directory not found: ${directoryPath}`);
      }

      const files = fs.readdirSync(directoryPath);
      const custodyFiles = [];

      for (const file of files) {
        const filePath = path.join(directoryPath, file);
        const stats = fs.statSync(filePath);

        if (stats.isFile()) {
          const detection = detectCustodyFileType(file);
          if (detection) {
            custodyFiles.push({
              fileName: file,
              filePath,
              custodyType: detection.type,
              sourceSystem: detection.config.sourceSystem,
              fileSize: stats.size,
              lastModified: stats.mtime
            });
          }
        }
      }

      return custodyFiles;
    } catch (error) {
      console.error(`❌ Error scanning directory ${directoryPath}:`, error.message);
      throw error;
    }
  }

  /**
   * Get sample data from custody file for preview
   * @param {string} filePath - Path to custody file
   * @param {number} maxRows - Maximum rows to return
   * @returns {Object} - Sample data and metadata
   */
  async getFileSample(filePath, maxRows = 5) {
    const result = await this.readCustodyFile(filePath);
    
    if (!result.success) {
      return result;
    }

    return {
      ...result,
      data: result.data.slice(0, maxRows),
      metadata: {
        ...result.metadata,
        isSample: true,
        sampleSize: Math.min(maxRows, result.data.length),
        totalRecords: result.metadata.recordCount
      }
    };
  }
}

module.exports = CustodyFileReader; 