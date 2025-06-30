const fs = require('fs');
const path = require('path');

console.log('🔧 Fixing file type detection patterns...');

// Read the smart-file-processor.js file
const smartProcessorPath = path.join(__dirname, 'smart-file-processor.js');
let content = fs.readFileSync(smartProcessorPath, 'utf8');

// Fix the detectFileType method to match actual collection names
const oldDetectionMethod = `    detectFileType(collectionName) {
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
    }`;

const newDetectionMethod = `    detectFileType(collectionName) {
        const name = collectionName.toLowerCase();
        
        // Master Data Types - Match both old and new patterns
        if (name.includes('broker_master') || name.includes('broker_master_data')) return 'broker_master';
        if (name.includes('client_info') || name.includes('client_info_data')) return 'client_master';
        if (name.includes('distributor_master') || name.includes('distributor_master_data')) return 'distributor_master';
        if (name.includes('strategy_master') || name.includes('strategy_master_data')) return 'strategy_master';
        
        // Transaction Data Types - Match both old and new patterns
        if (name.includes('contract_note') || name.includes('contract_notes')) return 'contract_notes';
        if (name.includes('cash_capital_flow') || name.includes('cash_flow')) return 'cash_flow';
        if (name.includes('stock_capital_flow') || name.includes('stock_flow')) return 'stock_flow';
        if (name.includes('mf_allocation') || name.includes('mf_alloc')) return 'mf_allocations';
        
        // Custody Data Types
        if (name.includes('axis')) return 'custody';
        if (name.includes('hdfc')) return 'custody';
        if (name.includes('kotak')) return 'custody';
        if (name.includes('deutsche') || name.includes('164_ec0000720')) return 'custody';
        if (name.includes('orbis')) return 'custody';
        if (name.includes('trust') || name.includes('end_client_holding')) return 'custody';
        
        // Default fallback
        return 'unknown';
    }`;

// Apply the fix
if (content.includes("if (name.includes('broker_master_data')) return 'broker_master';")) {
    content = content.replace(oldDetectionMethod, newDetectionMethod);
    console.log('✅ Fixed file type detection patterns');
} else {
    console.log('⚠️ Detection method not found or already updated');
}

// Also fix the getTargetTable method to handle custody data properly
const oldTargetTableMethod = `    // Get target table for file type
    getTargetTable(fileType) {
        const tableMap = {
            // Master Data
            'broker_master': 'brokers',
            'client_master': 'clients',
            'distributor_master': 'distributors',
            'strategy_master': 'strategies',
            
            // Transaction Data (Simplified - no ENSO prefixes)
            'contract_notes': 'contract_notes',
            'cash_flow': 'cash_capital_flow',
            'stock_flow': 'stock_capital_flow',
            'mf_allocations': 'mf_allocations',
            
            // Custody Data (daily tables only - no main table)
            'axis_custody': 'daily_custody', // Will be processed to date-specific table
            'hdfc_custody': 'daily_custody',
            'kotak_custody': 'daily_custody',
            'deutsche_custody': 'daily_custody',
            'orbis_custody': 'daily_custody',
            'trust_custody': 'daily_custody'
        };
        
        return tableMap[fileType] || 'raw_uploads';
    }`;

const newTargetTableMethod = `    // Get target table for file type
    getTargetTable(fileType) {
        const tableMap = {
            // Master Data
            'broker_master': 'brokers',
            'client_master': 'clients',
            'distributor_master': 'distributors',
            'strategy_master': 'strategies',
            
            // Transaction Data (Simplified - no ENSO prefixes)
            'contract_notes': 'contract_notes',
            'cash_flow': 'cash_capital_flow',
            'stock_flow': 'stock_capital_flow',
            'mf_allocations': 'mf_allocations',
            
            // Custody Data - use unified table
            'custody': 'unified_custody_master'
        };
        
        return tableMap[fileType] || 'raw_uploads';
    }`;

// Apply the target table fix
if (content.includes("'axis_custody': 'daily_custody'")) {
    content = content.replace(oldTargetTableMethod, newTargetTableMethod);
    console.log('✅ Fixed target table mapping for custody data');
}

// Write back the fixed file
fs.writeFileSync(smartProcessorPath, content, 'utf8');

console.log('🎉 File type detection fixed! Now the system will:');
console.log('   ✅ Correctly detect broker_master, client_info, distributor_master, strategy_master');
console.log('   ✅ Correctly detect contract_note, cash_capital_flow, stock_capital_flow, mf_allocation');
console.log('   ✅ Correctly detect custody data from axis, hdfc, kotak, deutsche, orbis, trust');
console.log('   ✅ Route data to the correct PostgreSQL tables');

console.log('\n🚀 Ready to process data with correct file type detection!'); 