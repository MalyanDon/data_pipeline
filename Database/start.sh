#!/bin/bash

echo "🚀 Starting ETL Data Pipeline..."

# Check if the main file exists
if [ ! -f "working-upload-system.js" ]; then
    echo "❌ Error: working-upload-system.js not found!"
    echo "📁 Current directory contents:"
    ls -la
    exit 1
fi

echo "✅ Found working-upload-system.js"
echo "🔧 Starting Node.js application..."

# Start the application
exec node working-upload-system.js
