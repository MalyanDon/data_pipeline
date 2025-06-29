const config = {
  mongodb: {
    username: 'abhishekmalyan2',
    password: 'STLKamQJJoUWv0Ks',
    cluster: 'database.tu83c8a.mongodb.net',
    uri: 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/',
    database: 'financial_data'
  },
  postgresql: {
    connectionString: 'postgresql://postgres@localhost:5432/financial_data'
  },
  server: {
    port: process.env.PORT || 3000
  }
};

module.exports = config; 