# ===================================================
# ETL DATA PIPELINE DOCKERFILE - RENDER DEPLOYMENT
# NO CHMOD COMMANDS - JavaScript files don't need it
# ===================================================

FROM node:18-alpine

# Set up working environment
WORKDIR /app

# Install all system dependencies in one layer
RUN apk add --no-cache \
    python3 \
    py3-pip \
    make \
    g++ \
    postgresql-client

# Copy and install Node.js dependencies
COPY package*.json ./
RUN npm install --production && npm cache clean --force

# Copy the complete application
COPY . .

# Create necessary directories
RUN mkdir -p temp_uploads

# Application runs on port 3000
EXPOSE 3000

# NO CHMOD NEEDED FOR JAVASCRIPT FILES
# Start the ETL data pipeline
CMD ["node", "working-upload-system.js"] 