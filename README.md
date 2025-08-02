# Financial ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for financial data processing with MongoDB and PostgreSQL integration.

## Features

- 📤 File upload and processing (Excel, CSV)
- 🔄 ETL mapping for PostgreSQL tables
- 👁️ Data viewer for uploaded data
- 🎯 Table selection and column mapping
- 📊 MongoDB to PostgreSQL processing
- 🏛️ Custody file normalization
- 💰 Financial data categorization

## API Endpoints

- `GET /` - Health check and API information
- `GET /api/health` - Service health status
- `POST /api/upload` - File upload endpoint
- `GET /api/categories` - Available data categories
- `GET /api/data` - Data retrieval endpoints
- `GET /api/client/:clientId` - Client information lookup
- `GET /api/clients` - List all clients

## Deployment

### Vercel Deployment

This application is configured for deployment on Vercel with the following setup:

1. **API Routes**: The main application is served through `/api/index.js`
2. **Configuration**: `vercel.json` defines the build and routing configuration
3. **Environment Variables**: Configure the following in Vercel dashboard:
   - `MONGODB_URI` - MongoDB connection string
   - `MONGODB_DATABASE` - Database name
   - `POSTGRES_USER` - PostgreSQL username
   - `POSTGRES_PASSWORD` - PostgreSQL password
   - `POSTGRES_HOST` - PostgreSQL host
   - `POSTGRES_PORT` - PostgreSQL port
   - `POSTGRES_DATABASE` - PostgreSQL database name

### Local Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Start production server
npm start
```

## Project Structure

```
├── api/
│   └── index.js          # Vercel API entry point
├── Database/
│   ├── app.js            # Main Express application
│   ├── config.js         # Configuration settings
│   ├── custody-normalization/  # Custody file processing
│   └── package.json      # Database dependencies
├── vercel.json           # Vercel deployment configuration
├── package.json          # Root dependencies
└── README.md            # This file
```

## Technologies Used

- **Backend**: Node.js, Express.js
- **Databases**: MongoDB, PostgreSQL
- **File Processing**: xlsx, csv-parser
- **Deployment**: Vercel
- **File Upload**: Multer

## License

MIT License 