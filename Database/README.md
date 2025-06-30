# MongoDB Database Project

A Node.js application with MongoDB Atlas connection using your credentials.

## 📋 Prerequisites

- Node.js (v14 or higher)
- npm or yarn
- MongoDB Atlas account (already configured)

## 🚀 Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Your MongoDB connection is already configured:**
   - **Cluster:** database.tu83c8a.mongodb.net
   - **Username:** abhishekmalyan2-username
   - **Database:** mydatabase

## 🔧 Available Scripts

- `npm start` - Start the production server
- `npm run dev` - Start the development server with nodemon
- `node test-connection.js` - Test the MongoDB connection

## 🧪 Testing the Connection

Run the connection test to verify everything is working:

```bash
node test-connection.js
```

This will:
- Connect to your MongoDB Atlas database
- Create a test user
- Fetch all users
- Clean up the test data
- Disconnect

## 🚀 Starting the API Server

Start the Express API server:

```bash
npm start
```

The server will start on port 3000 and provide the following endpoints:

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | API information and status |
| GET | `/health` | Database connection health check |
| GET | `/users` | Get all users |
| POST | `/users` | Create a new user |
| GET | `/users/:id` | Get user by ID |
| PUT | `/users/:id` | Update user |
| DELETE | `/users/:id` | Delete user |

## 📝 API Usage Examples

### Create a User
```bash
curl -X POST http://localhost:3000/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Abhishek Malyan",
    "email": "abhishek@example.com",
    "age": 25
  }'
```

### Get All Users
```bash
curl http://localhost:3000/users
```

### Get Health Status
```bash
curl http://localhost:3000/health
```

## 📁 Project Structure

```
├── config.js              # MongoDB configuration
├── database.js            # Database connection logic
├── index.js               # Main Express application
├── models/
│   └── User.js            # User model schema
├── test-connection.js     # Connection test script
├── package.json           # Project dependencies
└── README.md             # This file
```

## 🔒 Security Notes

- Your MongoDB credentials are stored in `config.js`
- For production, consider using environment variables
- Make sure to whitelist your IP address in MongoDB Atlas

## 🛠 Customization

You can modify:
- Database name in `config.js`
- User schema in `models/User.js`
- Add new models in the `models/` directory
- Add new API routes in `index.js`

## 📊 MongoDB Atlas Dashboard

You can monitor your database at: [MongoDB Atlas Dashboard](https://cloud.mongodb.com/)

Your cluster: `database.tu83c8a.mongodb.net` 