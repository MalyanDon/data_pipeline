"""
Configuration management for SmartGov Ex-Gratia Chatbot
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for the SmartGov chatbot"""
    
    # Telegram Bot Configuration
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
    
    # Mistral LLM Configuration
    MISTRAL_API_URL = os.getenv('MISTRAL_API_URL', 'http://localhost:8000/generate')
    
    # Support Information
    SUPPORT_PHONE = os.getenv('SUPPORT_PHONE', '+91-1234567890')
    
    # Debug Mode
    DEBUG = os.getenv('DEBUG', 'false').lower() == 'true'
    
    # Data Paths
    DATA_DIR = 'data'
    EXGRATIA_NORMS_FILE = os.path.join(DATA_DIR, 'info_opt1.txt')
    APPLICATION_PROCEDURE_FILE = os.path.join(DATA_DIR, 'info_opt2.txt')
    STATUS_CSV_FILE = os.path.join(DATA_DIR, 'status.csv')
    SUBMISSION_CSV_FILE = os.path.join(DATA_DIR, 'submission.csv')
    
    # Bot Messages
    WELCOME_MESSAGE = """
🙏 **Welcome to SmartGov Ex-Gratia Assistance!**

I'm here to help you with disaster relief services. You can:

1️⃣ **Ex-Gratia Norms** - Learn about assistance amounts & eligibility
2️⃣ **Apply for Ex-Gratia** - Get help with application process
3️⃣ **Check Status** - Track your application status

💬 You can also just tell me what you need in your own words!

How can I assist you today?
"""
    
    @classmethod
    def validate_config(cls):
        """Validate that all required configuration is present"""
        if not cls.TELEGRAM_TOKEN:
            raise ValueError("TELEGRAM_TOKEN is required in .env file")
        
        if not os.path.exists(cls.DATA_DIR):
            os.makedirs(cls.DATA_DIR)
            print(f"Created data directory: {cls.DATA_DIR}")
        
        print("✅ Configuration validated successfully") 