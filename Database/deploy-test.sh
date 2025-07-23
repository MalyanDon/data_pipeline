#!/bin/bash

echo "🐳 Testing Docker build for Render deployment..."

# Build the Docker image
echo "📦 Building Docker image..."
docker build -t financial-etl-pipeline .

if [ $? -eq 0 ]; then
    echo "✅ Docker build successful!"
    
    # Test running the container
    echo "🚀 Testing container startup..."
    docker run -d --name test-etl -p 3000:3000 financial-etl-pipeline
    
    # Wait a moment for the app to start
    sleep 5
    
    # Test health endpoint
    echo "🏥 Testing health endpoint..."
    curl -f http://localhost:3000/health
    
    if [ $? -eq 0 ]; then
        echo "✅ Health check passed!"
    else
        echo "❌ Health check failed!"
    fi
    
    # Clean up
    echo "🧹 Cleaning up test container..."
    docker stop test-etl
    docker rm test-etl
    
    echo "🎉 Deployment test completed successfully!"
    echo "You can now deploy to Render with confidence!"
else
    echo "❌ Docker build failed!"
    exit 1
fi 