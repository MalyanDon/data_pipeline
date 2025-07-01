#!/usr/bin/env python3
"""
Simple SmartGov Bot Runner (No LLM Required)
This runs the bot with enhanced rule-based intent detection only.
Perfect for immediate testing and deployment!
"""
import sys
import subprocess
from config import Config

def main():
    print("""
🚀 SmartGov Ex-Gratia Chatbot - Simple Mode
==========================================
Running with enhanced rule-based AI (no LLM server needed)

🤖 Bot Features Active:
✅ Smart Intent Recognition (Rule-based)
✅ Ex-Gratia Information
✅ Application Status Checking  
✅ Help & Support
✅ Natural Language Understanding

📱 Bot Link: https://t.me/smartgov_assistant_bot
""")
    
    try:
        # Validate configuration
        Config.validate_config()
        print("✅ Configuration validated")
        
        # Start the bot
        print("🚀 Starting SmartGov Telegram Bot...")
        print("💡 Bot is running in RULE-BASED mode (works great!)")
        print("📞 Press Ctrl+C to stop")
        print("-" * 50)
        
        # Run the bot
        subprocess.run([sys.executable, "smartgov_bot.py"])
        
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        print("🔧 Check your .env file and try again")

if __name__ == "__main__":
    main() 