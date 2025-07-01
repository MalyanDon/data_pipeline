# 🏛️ SmartGov Ex-Gratia Chatbot

An intelligent Telegram chatbot for disaster relief services that combines **rule-based workflows** with **Mistral 7B LLM** for natural language understanding.

## 🎯 Key Features

### **Smart vs Traditional Bots:**

| **Traditional Menu Bot** | **SmartGov (LLM-Powered)** |
|--------------------------|---------------------------|
| User: Types "3" | User: "I want to check my relief application" |
| Bot: What's your App ID? | Bot: ✅ *Understands intent* → "Please share your Application ID" |
| Rigid navigation only | Natural language + Menu options |

### **Core Capabilities:**
- 🧠 **Smart Intent Recognition** - Understands natural language
- 📋 **Ex-Gratia Information** - Norms, eligibility, amounts
- 📝 **Application Assistance** - Procedure + data collection  
- 🔍 **Status Tracking** - Real-time application lookup
- 🤖 **Hybrid Approach** - Menu + conversational interface
- 🔄 **Graceful Fallbacks** - Works even if LLM fails

---

## 🚀 Quick Start

### **1. Prerequisites**
```bash
# Python 3.8+
python --version

# GPU recommended for Mistral 7B (8GB+ VRAM)
nvidia-smi
```

### **2. Get Telegram Bot Token**
✅ **Already configured!** Your bot token has been set up:
- Bot username: `@smartgov_assistant_bot`
- Bot link: https://t.me/smartgov_assistant_bot

### **3. Install & Setup**
```bash
# Install dependencies
pip install -r requirements.txt

# Create environment file
# Note: .env is in .gitignore for security
echo "TELEGRAM_TOKEN=7641958089:AAH2UW5H0EX9pGfE6wZZaURCpkyMHtJK8zw" > .env
echo "MISTRAL_API_URL=http://localhost:8000/generate" >> .env
echo "SUPPORT_PHONE=+91-3592-202401" >> .env
```

### **4. Run the Bot**
```bash
# Terminal 1: Start Mistral 7B Server
python mistral_server.py

# Terminal 2: Start Telegram Bot  
python smartgov_bot.py
```

---

## 📱 User Experience Demo

### **Conversation Flow Examples:**

#### **🎯 Smart Intent Recognition**
```
User: Hi, I need help with disaster relief
Bot: 🙏 Welcome to SmartGov Ex-Gratia Assistance!
     I can help you with:
     1️⃣ Ex-Gratia Norms  2️⃣ Apply for Ex-Gratia  3️⃣ Check Status
     
User: I want to know how much money I can get for my damaged house
Bot: 📋 Ex-Gratia Assistance Norms
     ₹4,000 to ₹25,000 may be sanctioned depending on type of loss...
     
User: How do I apply?
Bot: 📝 How to Apply for Ex-Gratia Assistance
     1. Visit your local Gram Panchayat...
```

#### **🔍 Flexible Status Checking**
```
User: Check my application 23LDM786  
Bot: ✅ Application Found!
     🆔 Application ID: 23LDM786
     👤 Applicant: Rajesh Tamang
     📊 Status: Approved
     💰 Amount: ₹18,000

User: What about application 99XYZ123?
Bot: ❌ Application Not Found
     Please double-check your Application ID...
```

---

## 🏗️ Technical Architecture

### **System Components:**

```
User Input → Intent Classification → Rule Router → Response Generation
    ↓              ↓                     ↓              ↓
Telegram API → Mistral 7B LLM → Business Logic → Structured Output
```

### **File Structure:**
```
smartgov_bot/
├── smartgov_bot.py        # Main bot logic
├── mistral_server.py      # Local LLM API server
├── config.py              # Configuration management
├── requirements.txt       # Dependencies
├── .env                   # Environment variables (create manually)
├── data/
│   ├── info_opt1.txt     # Ex-Gratia norms
│   ├── info_opt2.txt     # Application procedure  
│   ├── status.csv        # Application statuses
│   └── submission.csv    # New applications (auto-created)
└── README.md
```

### **Data Flow:**
1. **User Input** → Telegram receives message
2. **Intent Classification** → Mistral 7B analyzes intent  
3. **Context Management** → Bot tracks conversation state
4. **Business Logic** → Route to appropriate handler
5. **Data Operations** → Read/write CSV files
6. **Response Generation** → Send structured reply

---

## 📊 Features Comparison

| Feature | Basic Menu Bot | SmartGov LLM Bot |
|---------|---------------|------------------|
| **Input Methods** | Numbers only (1,2,3) | Numbers + Natural Language |
| **User Experience** | Rigid navigation | Conversational & flexible |
| **Intent Understanding** | Keyword matching | Deep language understanding |
| **Error Handling** | Generic responses | Context-aware guidance |
| **Data Extraction** | Manual step-by-step | Smart extraction from text |
| **Personalization** | None | Uses names, context |
| **Fallback Strategy** | Show menu again | Multiple fallback levels |

---

## 🔧 Configuration

### **Environment Variables (.env file):**
```env
TELEGRAM_TOKEN=7641958089:AAH2UW5H0EX9pGfE6wZZaURCpkyMHtJK8zw
MISTRAL_API_URL=http://localhost:8000/generate
SUPPORT_PHONE=+91-3592-202401
DEBUG=false
```

### **Data Files:**
- `data/info_opt1.txt` - Ex-Gratia norms and eligibility
- `data/info_opt2.txt` - Application procedure steps
- `data/status.csv` - Sample application statuses for testing
- `data/submission.csv` - New applications (auto-created)

---

## 🚨 Troubleshooting

### **Common Issues:**

**1. Mistral Server Won't Start**
```bash
# Check GPU memory
nvidia-smi

# Try CPU-only mode
# In mistral_server.py, the code automatically detects CPU vs GPU
```

**2. Bot Not Responding**
```bash
# Check if both servers are running
ps aux | grep python

# Test Mistral API
curl -X POST "http://localhost:8000/generate" \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Hello", "max_tokens": 50}'
```

**3. Intent Classification Issues**
- Check Mistral API response format
- Verify fallback_intent_detection() patterns
- Add more training examples in prompt

**4. .env File Issues**
```bash
# Make sure .env file exists in project root
ls -la .env

# Check contents
cat .env
```

---

## 🎯 Testing the Bot

### **Test Commands:**
```
/start - Initialize the bot
/help - Get help information

# Natural Language Tests:
"Hello" - Should trigger greeting
"How much money for house damage?" - Should show norms
"How to apply?" - Should show procedure
"Check status 23LDM786" - Should show application status
"What documents needed?" - Should show application info
```

### **Sample Application IDs for Testing:**
- `23LDM786` - Approved house damage (₹18,000)
- `23LDM787` - Under review crop loss
- `23LDM788` - Approved livestock loss (₹30,000)
- `23LDM789` - Rejected business damage
- `23LDM790` - Approved injury case (₹25,000)

---

## 🎯 Production Deployment

### **For Live Government Use:**

1. **Security Hardening:**
   - Use HTTPS for all API calls
   - Implement rate limiting
   - Add user authentication
   - Encrypt sensitive data

2. **Scalability:**
   - Deploy Mistral on dedicated GPU server
   - Use Redis for conversation state
   - Load balance multiple bot instances
   - Monitor with Prometheus/Grafana

3. **Integration:**
   - Connect to government databases
   - Add webhook notifications
   - Implement audit logging
   - Multi-language support

4. **Compliance:**
   - GDPR/data protection compliance
   - Government security protocols
   - Accessibility standards

---

## 📞 Support & Contributing

**Issues:** Report bugs or feature requests
**Documentation:** Refer to inline code comments  
**Updates:** Check for model improvements

---

## 📜 License

MIT License - Feel free to adapt for your government projects!

---

*Built for citizens, powered by AI* 🤖🏛️ 