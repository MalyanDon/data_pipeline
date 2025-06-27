const mongoose = require('mongoose');
const config = require('./config');

async function checkHierarchicalData() {
  try {
    const mainConnection = await mongoose.connect(config.mongodb.uri + 'admin');
    console.log('🔗 Connected to MongoDB Atlas');
    
    // List all databases
    const databases = await mainConnection.connection.db.admin().listDatabases();
    console.log('\n📂 Available Databases:');
    
    // Look for year-based databases only
    const yearDatabases = databases.databases.filter(db => 
      db.name.startsWith('financial_data_') && /financial_data_\d{4}$/.test(db.name)
    ).sort((a, b) => b.name.localeCompare(a.name));
    
    if (yearDatabases.length === 0) {
      console.log('❌ No financial year databases found');
      return;
    }
    
    // Show Year Databases
    for (const yearDB of yearDatabases) {
      const year = yearDB.name.replace('financial_data_', '');
      console.log(`\n📅 Year Database: ${yearDB.name}`);
      console.log('─'.repeat(50));
      console.log(`   📊 ALL files for year ${year}`);
      
      try {
        const yearConnection = await mongoose.createConnection(config.mongodb.uri + yearDB.name);
        await new Promise((resolve) => {
          yearConnection.once('open', resolve);
        });
        
        const collections = await yearConnection.db.listCollections().toArray();
    
        if (collections.length === 0) {
          console.log('  📭 No collections found');
        } else {
          // Group by file type → month → day
          const fileHierarchy = {};
          
          for (const collection of collections) {
            const name = collection.name;
            const parts = name.split('_');
    
            // Parse collection name: filetype_MM_DD
            if (parts.length >= 3) {
              const fileType = parts.slice(0, -2).join('_');
              const month = parts[parts.length - 2];
              const day = parts[parts.length - 1];
              
              if (!fileHierarchy[fileType]) fileHierarchy[fileType] = {};
              if (!fileHierarchy[fileType][month]) fileHierarchy[fileType][month] = {};
              
              const count = await yearConnection.db.collection(name).countDocuments();
              fileHierarchy[fileType][month][day] = count;
            }
          }
          
          // Show hierarchical structure: File Type → Month → Day
          Object.keys(fileHierarchy).sort().forEach(fileType => {
            const displayName = fileType.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
            console.log(`\n  📁 ${displayName}:`);
            
            Object.keys(fileHierarchy[fileType]).sort((a, b) => b.localeCompare(a)).forEach(month => {
              const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
              console.log(`    📆 ${monthName} (${month}):`);
              
              Object.keys(fileHierarchy[fileType][month]).sort((a, b) => b.localeCompare(a)).forEach(day => {
                const count = fileHierarchy[fileType][month][day];
                console.log(`      📊 Day ${day}: ${count} records`);
              });
        });
      });
    }
    
        yearConnection.close();
      } catch (error) {
        console.log('  📭 No data found');
      }
    }
    
    console.log('\n✨ Financial database structure check complete!');
    console.log('\n💡 Structure:');
    console.log('   📅 financial_data_YYYY → ALL Files (filetype_MM_DD format)');
    console.log('   🗂️  Year-wise segregation: 2024, 2025, etc.');
    
  } catch (error) {
    console.error('❌ Error checking data:', error.message);
  } finally {
    await mongoose.disconnect();
    process.exit(0);
  }
}

checkHierarchicalData();
