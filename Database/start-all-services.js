#!/usr/bin/env node

const { spawn } = require('child_process');
const os = require('os');

console.log('🚀 Starting All Custody Data Services in Parallel...\n');

// Service configurations
const services = [
  {
    name: 'File Upload Dashboard',
    script: 'clean-dashboard.js',
    port: 3002,
    description: 'Upload and view raw files',
    color: '\x1b[32m' // Green
  },
  {
    name: 'Custody Pipeline Dashboard',
    script: 'pg-dashboard.js',
    port: 3005,
    description: 'MongoDB → PostgreSQL pipeline (Multi-threaded)',
    color: '\x1b[34m' // Blue
  },
  {
    name: 'Beautiful Multi-Thread Dashboard',
    script: 'beautiful-dashboard.js',
    port: 3006,
    description: 'Real-time multi-threaded ETL with PostgreSQL viewer',
    color: '\x1b[33m' // Yellow
  },
  {
    name: 'Custody API Server',
    script: 'custody-api-server.js',
    port: 3003,
    description: 'REST API for custody data',
    color: '\x1b[35m' // Magenta
  }
];

const processes = [];
const reset = '\x1b[0m';

// Function to start a service
function startService(service) {
  return new Promise((resolve, reject) => {
    console.log(`${service.color}🔄 Starting ${service.name}...${reset}`);
    
    const process = spawn('node', [service.script], {
      stdio: ['inherit', 'pipe', 'pipe'],
      cwd: __dirname
    });

    // Handle stdout
    process.stdout.on('data', (data) => {
      const output = data.toString().trim();
      if (output) {
        console.log(`${service.color}[${service.name}]${reset} ${output}`);
        
        // Check if service started successfully
        if (output.includes(`running at`) || output.includes(`listening on`) || output.includes(`localhost:${service.port}`)) {
          resolve(process);
        }
      }
    });

    // Handle stderr
    process.stderr.on('data', (data) => {
      const error = data.toString().trim();
      if (error) {
        console.error(`${service.color}[${service.name} ERROR]${reset} ${error}`);
      }
    });

    // Handle process exit
    process.on('close', (code) => {
      console.log(`${service.color}❌ ${service.name} exited with code ${code}${reset}`);
    });

    // Handle errors
    process.on('error', (error) => {
      console.error(`${service.color}💥 Failed to start ${service.name}:${reset}`, error.message);
      reject(error);
    });

    // Store process reference
    processes.push({
      name: service.name,
      process: process,
      port: service.port
    });

    // Timeout if service doesn't start in 30 seconds
    setTimeout(() => {
      if (!process.killed) {
        console.log(`${service.color}✅ ${service.name} started successfully${reset}`);
        resolve(process);
      }
    }, 5000);
  });
}

// Start all services in parallel
async function startAllServices() {
  try {
    console.log(`💻 System: ${os.type()} ${os.release()}`);
    console.log(`🔧 CPU Cores: ${os.cpus().length}`);
    console.log(`💾 Memory: ${Math.round(os.totalmem() / 1024 / 1024 / 1024)}GB\n`);

    // Start services in parallel
    const servicePromises = services.map(service => startService(service));
    
    console.log('⏳ Starting services in parallel...\n');
    
    // Wait for all services to start (or timeout)
    await Promise.allSettled(servicePromises);
    
    // Display service status
    console.log('\n📊 Service Status:');
    console.log('═'.repeat(50));
    
    services.forEach(service => {
      console.log(`${service.color}🌐 ${service.name}${reset}`);
      console.log(`   📍 http://localhost:${service.port}`);
      console.log(`   📝 ${service.description}`);
      console.log('');
    });

    console.log('🎯 Quick Access URLs:');
    console.log('═'.repeat(30));
    console.log('📤 Upload Files:     http://localhost:3002');
    console.log('📊 Pipeline View:    http://localhost:3005');
    console.log('🚀 Multi-Thread:     http://localhost:3006');
    console.log('🔌 API Endpoints:    http://localhost:3003');
    console.log('');
    
    console.log('⚙️  Available Operations:');
    console.log('   • Upload custody files → MongoDB (Port 3002)');
    console.log('   • View pipeline status → MongoDB ↔ PostgreSQL (Port 3005)');
    console.log('   • Real-time multi-threaded ETL → Worker threads (Port 3006)');
    console.log('   • View PostgreSQL normalized data → Live tables (Port 3006)');
    console.log('   • Process data via API → ETL Pipeline (Port 3003)');
    console.log('   • Query normalized data → PostgreSQL (Port 3003)');
    console.log('');
    
    console.log('🛑 To stop all services: Ctrl+C');
    console.log('🔄 Services will auto-restart on file changes (nodemon behavior)');

  } catch (error) {
    console.error('💥 Error starting services:', error.message);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n🛑 Shutting down all services...');
  
  processes.forEach(({ name, process }) => {
    if (process && !process.killed) {
      console.log(`   ⏹️  Stopping ${name}...`);
      process.kill('SIGINT');
    }
  });
  
  setTimeout(() => {
    console.log('✅ All services stopped. Goodbye!');
    process.exit(0);
  }, 2000);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('💥 Uncaught Exception:', error.message);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

// Start everything
startAllServices(); 