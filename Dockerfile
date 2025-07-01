# Financial ETL System Dockerfile - Updated 2025-07-01
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Install system dependencies for file processing
RUN apk add --no-cache \
    python3 \
    py3-pip \
    make \
    g++ \
    postgresql-client \
    curl

# Copy package files first for better caching
COPY package*.json ./

# Install Node.js dependencies
RUN npm install --production

# Copy all application files from Database directory
COPY Database/ ./

# Create temp uploads directory and set proper ownership
RUN mkdir -p temp_uploads && \
    chown -R node:node /app && \
    chmod -R 755 /app

# Switch to non-root user
USER node

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Start the application - NO CHMOD NEEDED
CMD ["node", "working-upload-system.js"] 