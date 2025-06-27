const config = {
  mongodb: {
    username: 'abhishekmalyan2',
    password: 'STLKamQJJoUWv0Ks',
    cluster: 'database.tu83c8a.mongodb.net',
    uri: 'mongodb+srv://abhishekmalyan2:STLKamQJJoUWv0Ks@database.tu83c8a.mongodb.net/',
    database: 'financial_data'
  },
  postgresql: {
    connectionString: 'postgresql://neondb_owner:npg_0jJAfrLxdRM7@ep-falling-union-a15mokzs-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'
  },
  server: {
    port: process.env.PORT || 3000
  }
};

module.exports = config; 