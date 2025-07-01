#!/usr/bin/env python3
"""
Setup script for SmartGov Ex-Gratia Chatbot
Quick start script to help users set up the environment
"""
import os
import subprocess
import sys

def print_banner():
    print("""
🏛️ SmartGov Ex-Gratia Chatbot Setup
=====================================
Intelligent Telegram bot for disaster relief services
""")

def check_python_version():
    """Check if Python version is 3.8+"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8+ is required")
        print(f"   Current version: {version.major}.{version.minor}")
        return False
    print(f"✅ Python {version.major}.{version.minor} detected")
    return True

def create_env_file():
    """Create .env file with default configuration"""
    env_content = """TELEGRAM_TOKEN=7641958089:AAH2UW5H0EX9pGfE6wZZaURCpkyMHtJK8zw
MISTRAL_API_URL=http://localhost:8000/generate
SUPPORT_PHONE=+91-3592-202401
DEBUG=false"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ Created .env file with bot configuration")
    else:
        print("ℹ️  .env file already exists")

def install_dependencies():
    """Install Python dependencies"""
    print("📦 Installing Python dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

def check_gpu():
    """Check if GPU is available for Mistral"""
    try:
        import torch
        if torch.cuda.is_available():
            gpu_count = torch.cuda.device_count()
            gpu_name = torch.cuda.get_device_name(0)
            print(f"✅ GPU available: {gpu_name} ({gpu_count} device(s))")
            print("💡 Mistral 7B will use GPU acceleration")
        else:
            print("⚠️  No GPU detected - Mistral will run on CPU")
            print("💡 Consider using a GPU for better performance")
    except ImportError:
        print("ℹ️  PyTorch not installed yet - GPU check will happen after installation")

def show_next_steps():
    """Show instructions for running the bot"""
    print("""
🚀 Setup Complete!

Next Steps:
1. Start the Mistral LLM server:
   python mistral_server.py

2. In a new terminal, start the Telegram bot:
   python smartgov_bot.py

3. Test your bot on Telegram:
   https://t.me/smartgov_assistant_bot

📚 Documentation: See README.md for detailed information
🆘 Support: Check the troubleshooting section in README.md

Happy chatting! 🤖
""")

def main():
    print_banner()
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Create .env file
    create_env_file()
    
    # Check if requirements.txt exists
    if not os.path.exists('requirements.txt'):
        print("❌ requirements.txt not found")
        print("   Make sure you're in the correct directory")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Setup failed during dependency installation")
        sys.exit(1)
    
    # Check GPU availability
    check_gpu()
    
    # Validate configuration
    try:
        from config import Config
        Config.validate_config()
        print("✅ Configuration validated")
    except Exception as e:
        print(f"⚠️  Configuration warning: {e}")
    
    # Show next steps
    show_next_steps()

if __name__ == "__main__":
    main() 