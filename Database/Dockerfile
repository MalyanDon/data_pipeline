# Use Node.js 18 LTS as base image
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install system dependencies for file processing
RUN apk add --no-cache \
    python3 \
    py3-pip \
    make \
    g++ \
    postgresql-client

# Copy package files first for better caching
COPY Database/package*.json ./

# Install Node.js dependencies
RUN npm install --production

# Copy application files from Database directory
COPY Database/ .

# Create temp uploads directory
RUN mkdir -p temp_uploads

# Set proper permissions
RUN chmod +x working-upload-system.js

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Start the application
CMD ["node", "working-upload-system.js"] 