const SmartFileProcessor = require('./smart-file-processor');
const path = require('path');
const fs = require('fs');

async function testExcelReading() {
    console.log('🧪 Testing Excel file reading...');
    
    // Find files in temp_uploads directory
    const tempDir = './temp_uploads/';
    let testFiles = [];
    
    if (fs.existsSync(tempDir)) {
        const files = fs.readdirSync(tempDir);
        testFiles = files
            .filter(f => !f.startsWith('.')) // Exclude hidden files
            .map(f => path.join(tempDir, f));
        
        console.log(`📁 Found ${testFiles.length} files in temp directory:`, files);
    }
    
    if (testFiles.length === 0) {
        console.log('❌ No files found to test');
        return;
    }
    
    const processor = new SmartFileProcessor();
    
    // Test first 2 files
    for (const filePath of testFiles.slice(0, 2)) {
        console.log(`\n🔍 Testing file: ${filePath}`);
        try {
            // Get file stats to see if it's a valid file
            const stats = fs.statSync(filePath);
            console.log(`📊 File size: ${stats.size} bytes`);
            
            const data = await processor.readFileData(filePath);
            console.log(`📊 Result: ${data.length} records found`);
            if (data.length > 0) {
                console.log(`🔍 Sample record:`, JSON.stringify(data[0], null, 2));
            }
        } catch (error) {
            console.error(`❌ Error reading ${filePath}:`, error.message);
            console.error(`❌ Full error:`, error);
        }
    }
    
    await processor.disconnect();
}

testExcelReading().catch(console.error); 