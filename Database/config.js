const config = {
  mongodb: {
    username: 'abhishekmalyan2',
    password: 'STLKamQJJoUWv0Ks',
    cluster: 'database.tu83c8a.mongodb.net',
    uri: process.env.MONGODB_URI || 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/?retryWrites=true&w=majority&appName=Database&ssl=true&tlsAllowInvalidCertificates=true',
    database: process.env.MONGODB_DATABASE || 'financial_data_2025'
  },
  postgresql: process.env.NODE_ENV === 'production' 
    ? {
        host: process.env.POSTGRES_HOST || 'localhost',
        port: parseInt(process.env.POSTGRES_PORT) || 5432,
        database: process.env.POSTGRES_DATABASE || 'financial_data',
        user: process.env.POSTGRES_USER || 'postgres',
        password: process.env.POSTGRES_PASSWORD || '',
        ssl: process.env.POSTGRES_HOST ? { rejectUnauthorized: false } : false,
        connectionTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        max: 20
      }
    : {
        host: 'localhost',
        port: 5432,
        database: 'financial_data',
        user: 'abhishekmalyan',
        password: '',
        ssl: false
      },
  server: {
    port: process.env.PORT || 3000
  }
};

module.exports = config; 