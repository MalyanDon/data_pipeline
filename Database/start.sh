#!/bin/bash

# Start script for Financial ETL Pipeline
echo "🚀 Starting Financial ETL Pipeline..."

# Create temp directory if it doesn't exist
mkdir -p temp_uploads

# Start the application
node working-upload-system.js
