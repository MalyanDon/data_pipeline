# 🌿 Git Branch Structure

## 📋 **Current Branches**

### **🌿 `main`** (Production/Stable)
- **Purpose**: Production-ready code
- **Status**: Stable, deployed to Render
- **Contains**: Clean, minimal deployment files

### **🚀 `data-pipeline`** (Active Development)
- **Purpose**: Your active data pipeline development
- **Status**: Current working branch
- **Contains**: 
  - Clean deployment structure
  - Essential files only
  - Ready for Render deployment

### **🔧 `working-restore`** (Backup/Archive)
- **Purpose**: Backup of previous work
- **Status**: Archived branch
- **Contains**: Previous versions and configurations

## 🎯 **Recommended Workflow**

### **For Development:**
```bash
# Always work on data-pipeline branch
git checkout data-pipeline

# Make your changes
# ... edit files ...

# Commit and push
git add .
git commit -m "Your changes"
git push origin data-pipeline
```

### **For Deployment:**
```bash
# Ensure you're on data-pipeline branch
git checkout data-pipeline

# Deploy to Render (uses this branch)
# Render will automatically use the latest commit
```

### **For Merging to Main:**
```bash
# When ready to merge to production
git checkout main
git merge data-pipeline
git push origin main
```

## 📁 **Current Clean Structure (data-pipeline branch)**

```
📁 Database/
├── ✅ app.js (main application)
├── ✅ config.js (database configuration)
├── ✅ package.json (dependencies)
├── ✅ package-lock.json (locked versions)
├── ✅ Dockerfile.explicit (deployment)
├── ✅ render.yaml (Render configuration)
├── ✅ .dockerignore (Docker exclusions)
├── ✅ custody-normalization/ (required modules)
├── ✅ temp_uploads/ (upload directory)
└── ✅ node_modules/ (installed packages)
```

## 🚀 **Benefits of This Structure**

1. **Safe Development**: Work on `data-pipeline` without affecting `main`
2. **Easy Rollback**: Can always go back to `main` if needed
3. **Clean History**: `main` stays clean and stable
4. **Parallel Work**: Can work on multiple features in different branches
5. **Deployment Safety**: Render can use `data-pipeline` branch for testing

## 📞 **Quick Commands**

```bash
# Switch to data-pipeline branch
git checkout data-pipeline

# Switch to main branch
git checkout main

# See all branches
git branch -a

# See current branch
git branch
``` 