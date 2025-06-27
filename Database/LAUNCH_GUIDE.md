# 🚀 Multi-Threaded Custody Data Platform

## ⚡ **Quick Start (Multi-Threaded Habit)**

### **Option 1: Super Quick Launch**
```bash
npm start
# or
npm run quick-start
```

### **Option 2: Shell Script Launch**
```bash
./launch.sh
```

### **Option 3: Direct Script**
```bash
node start-all-services.js
```

## 📊 **What Gets Started Automatically**

When you run any of the above commands, **3 services start simultaneously** in separate threads:

| Service | Port | Purpose | URL |
|---------|------|---------|-----|
| 📤 **File Upload Dashboard** | 3002 | Upload & view raw files | http://localhost:3002 |
| 📊 **Pipeline Dashboard** | 3005 | Monitor MongoDB ↔ PostgreSQL | http://localhost:3005 |
| 🔌 **Custody API Server** | 3003 | REST API endpoints | http://localhost:3003 |

## 🎯 **Multi-Threaded Benefits**

✅ **Parallel Processing**: All services run simultaneously  
✅ **Non-Blocking**: Upload files while monitoring pipeline  
✅ **Resource Efficient**: Uses multiple CPU cores  
✅ **Auto-Restart**: Services restart on crashes  
✅ **Graceful Shutdown**: Ctrl+C stops all services cleanly  

## 📋 **Complete Workflow (Habit)**

### **Daily Habit Workflow:**

1. **Start Platform** (1 command):
   ```bash
   npm start
   ```

2. **Upload Files** → http://localhost:3002
   - Upload custody files to MongoDB
   - View file structure and data fields

3. **Monitor Pipeline** → http://localhost:3005
   - See raw MongoDB data
   - Process data to PostgreSQL
   - View normalized results

4. **Access API** → http://localhost:3003
   - Query processed data
   - Get statistics
   - Download reports

5. **Stop Platform** (when done):
   ```
   Ctrl+C (stops all services)
   ```

## 🔄 **Data Fields Explanation**

**"Data Fields"** = Number of columns in each MongoDB collection:
- **Axis**: 30 fields (rich data structure)
- **Kotak**: 18 fields (moderate structure)  
- **Deutsche**: 1 field (minimal structure)
- **Trust PMS**: 2 fields (limited structure)
- **Orbis**: Variable fields (complex structure)

## 🔧 **Individual Service Commands**

If you need to run services individually:

```bash
# Upload Dashboard only
npm run start-upload

# Pipeline Dashboard only  
npm run custody-dashboard

# API Server only
npm run custody-api

# All together (recommended)
npm start
```

## 💡 **Pro Tips**

- **Always use `npm start`** for daily work (starts everything)
- **Keep terminal open** to see live logs from all services
- **Use different browser tabs** for each dashboard
- **Bookmark the URLs** for quick access
- **Services auto-restart** if they crash

## 🎉 **Make it a Habit**

**Every time you work with custody data:**
1. Open terminal in project folder
2. Run `npm start`
3. Open browser to http://localhost:3005
4. Start processing your data!

**That's it!** 🚀 