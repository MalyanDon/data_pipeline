# Ultimate Financial Data Dashboard

A complete ETL pipeline for financial data management with real-time visualization and processing capabilities.

## 🚀 Features

- **Complete ETL Pipeline**: Upload → Process → View → Manage
- **Smart File Detection**: Automatically categorizes and processes different file types
- **Real-time Data Visualization**: Interactive dashboard with live statistics
- **PostgreSQL Integration**: Robust data storage and querying
- **Multi-format Support**: CSV, Excel (XLSX/XLS) file processing
- **Export Capabilities**: Download data as CSV
- **Error-free Processing**: Handles null values and data type conversions

## 📊 Supported Data Types

- **Custody Data**: AXIS, KOTAK, HDFC, Deutsche Bank, Trust PMS, Orbis
- **Master Data**: Brokers, Clients, Distributors, Strategies
- **Transaction Data**: Contract Notes, Cash/Stock Capital Flow, MF Allocations

## 🛠 Installation

1. **Clone the repository**
   ```bash
git clone https://github.com/MalyanDon/data_pipeline.git
cd data_pipeline
```

2. **Install dependencies**
```bash
npm install
```

3. **Setup PostgreSQL**
- Install PostgreSQL
- Create database: `financial_data`
- Update credentials in `config.js` if needed

## 🚀 Quick Start

1. **Start the dashboard**
```bash
npm start
```

2. **Access the dashboard**
Open http://localhost:3000 in your browser

3. **View your data**
- Statistics are displayed immediately
- Select tables from dropdown to view data
- Export data as CSV

## 📁 Project Structure

```
Database/
├── ultimate-dashboard-fixed.js  # Main application
├── package.json                 # Dependencies and scripts
├── config.js                   # Database configuration
├── README.md                   # This file
├── .gitignore                  # Git ignore rules
└── node_modules/               # Dependencies
```

## 🎯 Usage

### Viewing Data
1. Open http://localhost:3000
2. View real-time statistics on the main page
3. Select a table from the dropdown
4. Click "Load Data" to view records
5. Use "Export CSV" to download data

### Data Statistics
Your current data includes:
- **19,080+ Total Records**
- **18,418 Custody Holdings**
- **25 Distributors**
- **18 Strategies**
- **5 Brokers**
- **2 Clients**
- **1 MF Allocation**

## 🔧 Configuration

Update `config.js` for database settings:
```javascript
module.exports = {
    postgresql: {
        user: 'your_username',
        host: 'localhost',
        database: 'financial_data',
        password: 'your_password',
        port: 5432
    }
};
```

## 📈 Data Processing

The system automatically:
- Detects file types based on content
- Routes data to appropriate tables
- Handles data validation and cleaning
- Provides error-free viewing experience

## 🛡 Error Handling

- Graceful handling of null/undefined values
- Proper date formatting
- SQL injection prevention
- Clear error messages

## 👨‍💻 Author

**Abhishek Malyan**

## 📄 License

This project is licensed under the MIT License. 