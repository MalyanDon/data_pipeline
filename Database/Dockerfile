# Clean Dockerfile for ETL Data Pipeline - NO CHMOD NEEDED
FROM node:18-alpine

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache python3 py3-pip make g++ postgresql-client

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install --production

# Copy all application files
COPY . .

# Create upload directory
RUN mkdir -p temp_uploads

# Expose the application port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Start the ETL application
CMD ["node", "working-upload-system.js"] 