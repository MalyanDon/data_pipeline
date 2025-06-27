#!/bin/bash

echo "🚀 Starting Unified Custody Data Management System..."
echo "📍 URL: http://localhost:3007"
echo "🔧 Features: Upload, MongoDB, PostgreSQL, ETL Processing, System Status"
echo ""

# Kill any existing processes on port 3007
lsof -ti:3007 | xargs kill -9 2>/dev/null || true

# Start the unified dashboard
npm run unified-dashboard 