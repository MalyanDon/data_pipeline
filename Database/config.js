const config = {
  mongodb: {
    username: 'abhishekmalyan2',
    password: 'STLKamQJJoUWv0Ks',
    cluster: 'database.tu83c8a.mongodb.net',
    uri: process.env.MONGODB_URI || 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/',
    database: process.env.MONGODB_DATABASE || 'financial_data_2025'
  },
  postgresql: process.env.NODE_ENV === 'production' 
    ? {
        host: process.env.POSTGRES_HOST,
        port: process.env.POSTGRES_PORT || 5432,
        database: process.env.POSTGRES_DATABASE,
        user: process.env.POSTGRES_USER,
        password: process.env.POSTGRES_PASSWORD,
        ssl: true
      }
    : {
        host: 'localhost',
        port: 5432,
        database: 'financial_data',
        user: 'abhishekmalyan',
        password: ''
      },
  server: {
    port: process.env.PORT || 3000
  }
};

module.exports = config; 