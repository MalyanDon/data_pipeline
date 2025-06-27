#!/bin/bash

echo "🚀 Launching Custody Data Platform..."
echo "💻 Detected OS: $(uname -s)"
echo ""

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js not found. Please install Node.js first."
    exit 1
fi

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "❌ npm not found. Please install npm first."
    exit 1
fi

echo "✅ Node.js $(node --version) detected"
echo "✅ npm $(npm --version) detected"
echo ""

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
    echo ""
fi

echo "🔄 Starting all custody data services in parallel..."
echo "📝 This will launch 3 services simultaneously:"
echo "   • File Upload Dashboard (Port 3002)"
echo "   • Pipeline Monitoring Dashboard (Port 3005)" 
echo "   • Custody API Server (Port 3003)"
echo ""

# Start all services
npm start 