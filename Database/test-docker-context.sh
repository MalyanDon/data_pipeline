#!/bin/bash

echo "🐳 Testing Docker build context..."

# Create a temporary directory to simulate Docker context
TEMP_DIR=$(mktemp -d)
echo "📁 Created temp directory: $TEMP_DIR"

# Copy files as Docker would see them (respecting .dockerignore)
echo "📋 Copying files to temp directory..."

# Copy all files except those in .dockerignore
rsync -av --exclude-from=.dockerignore . "$TEMP_DIR/"

# Check what files are in the temp directory
echo "📁 Files in temp directory:"
ls -la "$TEMP_DIR/"

# Check specifically for the main file
echo ""
echo "🔍 Checking for working-upload-system.js:"
if [ -f "$TEMP_DIR/working-upload-system.js" ]; then
    echo "✅ working-upload-system.js found in temp directory"
    echo "📏 Size: $(wc -c < "$TEMP_DIR/working-upload-system.js") bytes"
else
    echo "❌ working-upload-system.js NOT found in temp directory!"
    echo "📁 Available .js files:"
    ls -la "$TEMP_DIR/"*.js 2>/dev/null || echo "No .js files found"
fi

# Check package.json
echo ""
echo "📦 Checking package.json:"
if [ -f "$TEMP_DIR/package.json" ]; then
    echo "✅ package.json found"
    echo "📄 Start script:"
    grep -A 1 -B 1 "start" "$TEMP_DIR/package.json"
else
    echo "❌ package.json NOT found!"
fi

# Clean up
rm -rf "$TEMP_DIR"
echo ""
echo "🧹 Cleaned up temp directory" 