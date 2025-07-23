#!/bin/bash

echo "🔍 Verifying deployment configuration..."

# Check if main file exists
if [ -f "working-upload-system.js" ]; then
    echo "✅ working-upload-system.js exists"
else
    echo "❌ working-upload-system.js missing!"
    exit 1
fi

# Check if package.json exists
if [ -f "package.json" ]; then
    echo "✅ package.json exists"
else
    echo "❌ package.json missing!"
    exit 1
fi

# Check if Dockerfile exists
if [ -f "Dockerfile" ]; then
    echo "✅ Dockerfile exists"
else
    echo "❌ Dockerfile missing!"
    exit 1
fi

# Check if start script exists
if [ -f "start.sh" ]; then
    echo "✅ start.sh exists"
else
    echo "❌ start.sh missing!"
    exit 1
fi

# Check if config.js exists
if [ -f "config.js" ]; then
    echo "✅ config.js exists"
else
    echo "❌ config.js missing!"
    exit 1
fi

# Check file permissions
if [ -x "start.sh" ]; then
    echo "✅ start.sh is executable"
else
    echo "❌ start.sh is not executable"
    chmod +x start.sh
    echo "🔧 Made start.sh executable"
fi

# Check package.json start script
START_SCRIPT=$(node -e "console.log(require('./package.json').scripts.start)")
if [ "$START_SCRIPT" = "node working-upload-system.js" ]; then
    echo "✅ package.json start script is correct"
else
    echo "❌ package.json start script is incorrect: $START_SCRIPT"
fi

echo "🎉 Deployment verification complete!"
echo "📋 Files ready for deployment:"
ls -la | grep -E "(working-upload-system\.js|package\.json|Dockerfile|start\.sh|config\.js)" 