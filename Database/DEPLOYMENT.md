# Financial ETL Pipeline - Render Deployment Guide

## 🚀 Deploy to Render

This financial data ETL pipeline is configured for deployment on Render using Docker.

### Prerequisites

1. **GitHub Repository**: Push your code to GitHub
2. **Render Account**: Sign up at [render.com](https://render.com)
3. **MongoDB Atlas**: Ensure your MongoDB Atlas database is accessible

### Deployment Steps

1. **Connect GitHub to Render**
   - Go to Render Dashboard
   - Click "New" → "Web Service"
   - Connect your GitHub repository

2. **Configure Service**
   - **Name**: `financial-etl-pipeline`
   - **Environment**: `Docker`
   - **Plan**: `Starter` (or higher)
   - **Docker Command**: Uses `Dockerfile` automatically

3. **Environment Variables** (Auto-configured via `render.yaml`)
   - `NODE_ENV=production`
   - `PORT=3000`
   - `MONGODB_URI=mongodb+srv://...`
   - `MONGODB_DATABASE=financial_data_2025`
   - PostgreSQL vars auto-configured from database

4. **Database Setup**
   - Render will automatically create PostgreSQL database
   - Database connection configured via environment variables

### Features Available After Deployment

✅ **Complete ETL System**
- File upload (CSV, Excel)
- Smart header detection
- MongoDB data storage
- PostgreSQL table creation
- Column mapping interface
- Data viewer

✅ **Supported File Types**
- HDFC custody files
- AXIS custody files
- KOTAK custody files
- ORBIS custody files
- Other financial data formats

✅ **Enhanced Processing**
- 20-row header scanning
- Financial term prioritization
- Multi-format date extraction
- N/A column handling

### Health Check

The service includes a health check endpoint:
- URL: `https://your-app.onrender.com/health`
- Returns: Service status and timestamp

### File Structure

```
├── Dockerfile              # Docker configuration
├── render.yaml             # Render deployment config
├── working-upload-system.js # Main application
├── config.js               # Environment-aware config
├── package.json            # Dependencies
├── start.sh                # Start script
└── .dockerignore           # Docker ignore rules
```

### Post-Deployment

1. **Access your app**: `https://your-service-name.onrender.com`
2. **Upload files**: Use the web interface
3. **View data**: Check MongoDB and PostgreSQL tabs
4. **ETL mapping**: Create unified tables

### Troubleshooting

- **Build fails**: Check Dockerfile dependencies
- **App crashes**: Check logs in Render dashboard
- **DB connection**: Verify environment variables
- **File uploads**: Check temp directory permissions

### Support

For issues with the financial ETL pipeline, check:
1. Render service logs
2. Health check endpoint
3. Database connections
4. File upload permissions
