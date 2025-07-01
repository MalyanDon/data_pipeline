"""
SmartGov Ex-Gratia Chatbot - Intelligent Telegram bot for disaster relief services
"""
import csv
import logging
import os
import re
import pandas as pd
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SmartGovBot:
    def __init__(self):
        self.exgratia_norms = self._load_file_content(Config.EXGRATIA_NORMS_FILE)
        self.application_procedure = self._load_file_content(Config.APPLICATION_PROCEDURE_FILE)
        self._initialize_csv_files()
        
    def _load_file_content(self, file_path: str) -> str:
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as file:
                    return file.read()
            return "Information not available. Please contact support."
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return "Error loading information. Please contact support."
    
    def _initialize_csv_files(self):
        if not os.path.exists(Config.SUBMISSION_CSV_FILE):
            with open(Config.SUBMISSION_CSV_FILE, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['submission_id', 'name', 'phone', 'submission_date', 'status', 'details'])
    
    async def get_intent_from_llm(self, message: str) -> str:
        # For now, we'll use the rule-based system directly
        # You can enable LLM later by setting up Hugging Face authentication
        logger.info("Using rule-based intent detection (LLM disabled)")
        return self.fallback_intent_detection(message)
        
        # Uncomment below to enable LLM (requires Hugging Face setup):
        """
        try:
            prompt = f'''
Analyze this message for a government disaster relief chatbot and classify the intent:
"{message}"

Classify as ONE of: exgratia_norms, application_procedure, status_check, apply_start, greeting, help, other

Respond with ONLY the intent name.
'''
            
            response = requests.post(Config.MISTRAL_API_URL, json={
                "prompt": prompt, "max_tokens": 20, "temperature": 0.3
            }, timeout=10)
            
            if response.status_code == 200:
                intent = response.json().get('generated_text', '').strip().lower()
                valid_intents = ['exgratia_norms', 'application_procedure', 'status_check', 'apply_start', 'greeting', 'help', 'other']
                if intent in valid_intents:
                    return intent
            
        except Exception as e:
            logger.error(f"LLM API error: {e}")
        
        return self.fallback_intent_detection(message)
        """
    
    def fallback_intent_detection(self, message: str) -> str:
        """Enhanced rule-based intent detection"""
        message_lower = message.lower()
        
        # Greeting patterns
        greeting_words = ['hello', 'hi', 'start', 'hey', 'namaste', 'good morning', 'good afternoon', 'good evening']
        if any(word in message_lower for word in greeting_words):
            return 'greeting'
        
        # Ex-gratia norms patterns
        norms_phrases = [
            'norms', 'amount', 'money', 'eligibility', 'how much', 'rate', 'compensation',
            'house damage', 'crop loss', 'livestock', 'injury', 'death', 'relief amount',
            'sanction', 'criteria', 'eligible', 'qualify', 'entitle'
        ]
        if any(phrase in message_lower for phrase in norms_phrases):
            return 'exgratia_norms'
        
        # Application procedure patterns
        procedure_phrases = [
            'apply', 'application', 'procedure', 'process', 'how to', 'documents',
            'submit', 'steps', 'gram panchayat', 'ward office', 'requirements',
            'form', 'paperwork', 'where to apply', 'when to apply'
        ]
        if any(phrase in message_lower for phrase in procedure_phrases):
            return 'application_procedure'
        
        # Status check patterns
        status_phrases = [
            'status', 'check', 'track', 'application id', 'app id', 'reference',
            'progress', 'update', 'approved', 'pending', 'rejected', 'sanctioned'
        ]
        # Also check for application ID patterns
        if any(phrase in message_lower for phrase in status_phrases) or re.search(r'\b[a-z0-9]{6,12}\b', message_lower):
            return 'status_check'
        
        # Help patterns
        help_words = ['help', 'support', 'assist', 'guidance', 'information', 'contact', 'phone', 'number']
        if any(word in message_lower for word in help_words):
            return 'help'
        
        # Questions about specific topics
        if any(word in message_lower for word in ['what', 'how', 'when', 'where', 'why', 'which']):
            if any(word in message_lower for word in ['document', 'paper', 'certificate', 'proof']):
                return 'application_procedure'
            elif any(word in message_lower for word in ['money', 'amount', 'rupee', 'compensation']):
                return 'exgratia_norms'
        
        return 'other'
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        keyboard = [
            [InlineKeyboardButton("1️⃣ Ex-Gratia Norms", callback_data="option_1")],
            [InlineKeyboardButton("2️⃣ Apply for Ex-Gratia", callback_data="option_2")],
            [InlineKeyboardButton("3️⃣ Check Status", callback_data="option_3")],
            [InlineKeyboardButton("ℹ️ Help & Support", callback_data="help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(Config.WELCOME_MESSAGE, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = f"""
🆘 **SmartGov Ex-Gratia Assistance Help**

**Available Commands:**
• `/start` - Start the bot and see main menu
• `/help` - Show this help message

**How to Use:**
1️⃣ **Natural Language**: Just type what you need
2️⃣ **Menu Options**: Use the numbered buttons

**Examples:**
• "How much money can I get for house damage?"
• "What documents do I need to apply?"
• "Check status of application 23LDM786"

**Support Contact:**
📞 Helpline: 1077
📞 Phone: {Config.SUPPORT_PHONE}
"""
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if query.data == "option_1":
            await self.show_exgratia_norms(update, context)
        elif query.data == "option_2":
            await self.show_application_procedure(update, context)
        elif query.data == "option_3":
            await self.ask_for_application_id(update, context)
        elif query.data == "help":
            await self.help_command(update, context)
        elif query.data == "back_to_menu":
            await self.start_command(update, context)
    
    async def message_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message = update.message.text
        logger.info(f"User message: {message}")
        
        # Check if waiting for application ID
        if context.user_data.get('waiting_for_app_id'):
            app_id_pattern = r'\b[A-Z0-9]{6,12}\b'
            app_ids = re.findall(app_id_pattern, message.upper())
            if app_ids:
                await self.check_application_status(update, context, app_ids[0])
                return
        
        # Get intent and handle
        intent = await self.get_intent_from_llm(message)
        await self.handle_intent(update, context, intent, message)
    
    async def handle_intent(self, update: Update, context: ContextTypes.DEFAULT_TYPE, intent: str, message: str):
        if intent == "greeting":
            await self.start_command(update, context)
        elif intent == "exgratia_norms":
            await self.show_exgratia_norms(update, context)
        elif intent in ["application_procedure", "apply_start"]:
            await self.show_application_procedure(update, context)
        elif intent == "status_check":
            app_id_pattern = r'\b[A-Z0-9]{6,12}\b'
            app_ids = re.findall(app_id_pattern, message.upper())
            if app_ids:
                await self.check_application_status(update, context, app_ids[0])
            else:
                await self.ask_for_application_id(update, context)
        elif intent == "help":
            await self.help_command(update, context)
        else:
            await self.handle_other_intent(update, context)
    
    async def handle_other_intent(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        response = """
🤔 I'm not sure exactly what you need, but I'm here to help!

Here's what I can assist you with:
1️⃣ **Ex-Gratia Norms** - Information about assistance amounts
2️⃣ **Application Process** - How to apply for ex-gratia
3️⃣ **Status Check** - Track your application

💬 Try asking questions like:
• "How much can I get for house damage?"
• "What documents do I need?"
• "Check my application status"
"""
        
        keyboard = [
            [InlineKeyboardButton("1️⃣ Ex-Gratia Norms", callback_data="option_1")],
            [InlineKeyboardButton("2️⃣ Apply for Ex-Gratia", callback_data="option_2")],
            [InlineKeyboardButton("3️⃣ Check Status", callback_data="option_3")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(response, reply_markup=reply_markup)
    
    async def show_exgratia_norms(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        response = f"""
🏛️ **Government of Sikkim**
{self.exgratia_norms}

📞 **For more information:** {Config.SUPPORT_PHONE}
"""
        
        keyboard = [
            [InlineKeyboardButton("2️⃣ How to Apply", callback_data="option_2")],
            [InlineKeyboardButton("3️⃣ Check Status", callback_data="option_3")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(response, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def show_application_procedure(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        response = f"""
{self.application_procedure}

💡 **Need more help?** Visit your local Gram Panchayat or call {Config.SUPPORT_PHONE}
"""
        
        keyboard = [
            [InlineKeyboardButton("1️⃣ View Norms", callback_data="option_1")],
            [InlineKeyboardButton("3️⃣ Check Status", callback_data="option_3")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(response, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode='Markdown')
    
    async def ask_for_application_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        response = """
🔍 **Application Status Check**

Please share your Application ID to check the status.

**Format:** Usually 8-10 characters (e.g., 23LDM786)

You can find your Application ID in:
📄 Application receipt
📱 SMS confirmation
📧 Email confirmation

Type your Application ID:
"""
        
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(response, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode='Markdown')
        
        context.user_data['waiting_for_app_id'] = True
    
    async def check_application_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE, app_id: str):
        try:
            df = pd.read_csv(Config.STATUS_CSV_FILE)
            result = df[df['application_id'].str.upper() == app_id.upper()]
            
            if not result.empty:
                row = result.iloc[0]
                status_emoji = {'Approved': '✅', 'Under Review': '🔄', 'Pending': '⏳', 'Rejected': '❌'}.get(row['status'], '📋')
                
                response = f"""
{status_emoji} **Application Found!**

🆔 **Application ID:** {row['application_id']}
👤 **Applicant:** {row['applicant_name']}
📱 **Phone:** {row['phone']}
📋 **Type:** {row['type']}
📊 **Status:** {row['status']}
💰 **Amount:** ₹{row['amount']:,}
📅 **Applied:** {row['date_applied']}
📝 **Remarks:** {row['remarks']}

📞 **For queries:** {Config.SUPPORT_PHONE}
"""
            else:
                response = f"""
❌ **Application Not Found**

🔍 Application ID: `{app_id}`

**Possible reasons:**
• Application ID might be incorrect
• Application not yet in system
• Typing error in Application ID

**What to do:**
1. Double-check your Application ID
2. Contact your Gram Panchayat/Ward Office
3. Call helpline: {Config.SUPPORT_PHONE}
"""
            
            keyboard = [
                [InlineKeyboardButton("🔍 Check Another", callback_data="option_3")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(response, reply_markup=reply_markup, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Error checking application status: {e}")
            await update.message.reply_text(
                f"❌ Sorry, there was an error checking the application status. "
                f"Please try again later or contact support: {Config.SUPPORT_PHONE}"
            )
        
        context.user_data.pop('waiting_for_app_id', None)

def main():
    try:
        Config.validate_config()
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("Please create a .env file with your TELEGRAM_TOKEN")
        return
    
    bot = SmartGovBot()
    application = Application.builder().token(Config.TELEGRAM_TOKEN).build()
    
    application.add_handler(CommandHandler("start", bot.start_command))
    application.add_handler(CommandHandler("help", bot.help_command))
    application.add_handler(CallbackQueryHandler(bot.button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.message_handler))
    
    print("🤖 SmartGov Ex-Gratia Chatbot is starting...")
    print("📱 Bot username: @smartgov_assistant_bot")
    print("✅ Bot is running. Press Ctrl+C to stop.")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main() 