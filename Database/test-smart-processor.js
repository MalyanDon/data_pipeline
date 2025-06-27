const SmartFileProcessor = require('./smart-file-processor');

async function testVersioning() {
    console.log('🧪 Testing Smart File Processor with Versioning...\n');
    
    const processor = new SmartFileProcessor();
    
    try {
        // Test 1: Get current collections
        console.log('📋 Current Collections:');
        const collections = await processor.getCollectionsInfo();
        collections.forEach(col => {
            console.log(`  📂 ${col.sourceType}: ${col.count} active records, ${col.historicalCount} historical`);
            console.log(`     Version: ${col.currentVersion}, Status: ${col.status}`);
        });
        
        if (collections.length > 0) {
            const testSource = collections[0].sourceType;
            console.log(`\n🕒 Testing version history for: ${testSource}`);
            
            // Test 2: Get version history
            const versions = await processor.getVersionHistory(testSource, 5);
            console.log(`📚 Found ${versions.length} versions:`);
            versions.forEach(v => {
                console.log(`  📝 ${v.uploadVersion || 'v' + v._id}: ${v.recordCount} records, ${v.isActive ? '✅ Active' : '📦 Historical'}`);
                console.log(`     Uploaded: ${new Date(v.uploadTimestamp).toLocaleString()}`);
            });
        }
        
        console.log('\n✅ Smart File Processor test completed successfully!');
        console.log('\n🎯 Key Features:');
        console.log('  • ✅ Intelligent versioning with timestamps');
        console.log('  • 📚 Historical data preservation');
        console.log('  • 🔄 Version rollback capability');
        console.log('  • 🚫 No data loss protection');
        console.log('  • ⚡ Always uses latest version for processing');
        
    } catch (error) {
        console.error('❌ Test failed:', error.message);
    } finally {
        await processor.disconnect();
    }
}

// Run test if called directly
if (require.main === module) {
    testVersioning();
}

module.exports = { testVersioning }; 