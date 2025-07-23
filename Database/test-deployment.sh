#!/bin/bash

echo "🧪 Testing deployment configuration..."

# Test 1: Check if main file exists and is readable
echo "📄 Test 1: Checking main file..."
if [ -r "working-upload-system.js" ]; then
    echo "✅ working-upload-system.js exists and is readable"
else
    echo "❌ working-upload-system.js missing or not readable"
    exit 1
fi

# Test 2: Check package.json start script
echo "📦 Test 2: Checking package.json..."
START_CMD=$(node -e "console.log(require('./package.json').scripts.start)")
if [ "$START_CMD" = "node working-upload-system.js" ]; then
    echo "✅ package.json start script is correct: $START_CMD"
else
    echo "❌ package.json start script is wrong: $START_CMD"
    exit 1
fi

# Test 3: Test npm start (check if it tries to start)
echo "🚀 Test 3: Testing npm start..."
npm start 2>&1 | head -3 > /tmp/npm_test.log
if grep -q "node working-upload-system.js" /tmp/npm_test.log; then
    echo "✅ npm start command works (tries to start the correct file)"
else
    echo "❌ npm start command failed or wrong file"
    cat /tmp/npm_test.log
    exit 1
fi

# Test 4: Check Dockerfile
echo "🐳 Test 4: Checking Dockerfile..."
if [ -f "Dockerfile.simple" ]; then
    echo "✅ Dockerfile.simple exists"
else
    echo "❌ Dockerfile.simple missing"
    exit 1
fi

# Test 5: Check if Dockerfile copies files correctly
echo "📋 Test 5: Checking Dockerfile content..."
if grep -q "COPY \." Dockerfile.simple; then
    echo "✅ Dockerfile copies all files"
else
    echo "❌ Dockerfile doesn't copy all files"
    exit 1
fi

# Test 6: Check if Dockerfile uses npm start
echo "⚙️ Test 6: Checking Dockerfile CMD..."
if grep -q 'CMD \["npm", "start"\]' Dockerfile.simple; then
    echo "✅ Dockerfile uses npm start"
else
    echo "❌ Dockerfile doesn't use npm start"
    exit 1
fi

echo "🎉 All deployment tests passed!"
echo ""
echo "📋 Deployment Summary:"
echo "  • Main file: working-upload-system.js ✅"
echo "  • Package.json: correct start script ✅"
echo "  • NPM start: works correctly ✅"
echo "  • Dockerfile: copies files and uses npm start ✅"
echo ""
echo "🚀 Ready for Render deployment!" 