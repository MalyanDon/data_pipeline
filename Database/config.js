const config = {
  mongodb: {
    username: 'abhishekmalyan2',
    password: 'STLKamQJJoUWv0Ks',
    cluster: 'database.tu83c8a.mongodb.net',
    uri: process.env.MONGODB_URI || 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/',
    database: process.env.MONGODB_DATABASE || 'financial_data_2025'
  },
  postgresql: {
    connectionString: process.env.NODE_ENV === 'production' 
      ? `postgresql://${process.env.POSTGRES_USER}:${process.env.POSTGRES_PASSWORD}@${process.env.POSTGRES_HOST}:${process.env.POSTGRES_PORT}/${process.env.POSTGRES_DATABASE}`
      : 'postgresql://abhishekmalyan@localhost:5432/financial_data'
  },
  server: {
    port: process.env.PORT || 3000
  }
};

module.exports = config; 