import streamlit as st
import asyncio
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
import re
from queue import Queue
import threading
from datetime import datetime
from telethon import TelegramClient
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Schema columns with their expected data types
SCHEMA_COLUMNS = {
    "NamaKonsumen": str, "NoHp": str, "Alamat": str, "Ekspedisi": str, "Memo": str, "Pembayaran": str,
    "Produk1": str, "QTY1": int, "Produk2": str, "QTY2": int, "Produk3": str, "QTY3": int,
    "Produk4": str, "QTY4": int, "Produk5": str, "QTY5": int,
    "NilaiProduk1": int, "NilaiProduk2": int, "NilaiProduk3": int, "NilaiProduk4": int, "NilaiProduk5": int,
    "Ongkir": int, "Diskon": int, "AdminCOD": int, "TotalBayar": int, "NamaCS": str, "NamaADV": str,
    "Note": str, "Hub": str, "Tanggal": str, "JumlahOrderClosing": int, "TypeSales": str,
    "Platfrom": str, "Divisi": str, "Advertiser": str, "Spending": int,
    "OutboundKlik": int, "LPViews": int, "LeadsDashboard": int, "TotalLeadsWA": int,
    "TotalLeadsForm": int, "NPL": int, "VoucherCS": int, "BiayaMarketingCS": int,
    "JumlahTerimaData": int, "VoucherCRM": int, "BiayaMarketingCRM": int,
    "MEIklanShopee": int, "MEIklanLazada": int, "MEIklanTokped": int, "MEIklanCPAS": int,
    "TotalMEAll": int, "MELive": int, "MELain2": int, "MEIklan": int, "Namaakun": str,
    "KomisiAffiliate": int, "BiayaSFPPlatfrom": int, "DiskonToko": int, "BiayaAdm": int,
    "BiayaPenyusutanFO": int, "TotalSalesCRMTiktok": int, "BiayaKOL": int,
    "JumlahPenontonLive": int, "JumlahJamLiveTiktok": int, "idpesan": str,
    "KontenTeruploadHariIni": int, "Timestamp": str, "Namatoko": str
}

# Define required fields for validation bot
AKUISISI_REQUIRED_FIELDS = {
    "NamaKonsumen", "NoHp", "Alamat", "Ekspedisi", "Pembayaran",
    "Produk1", "QTY1", "NilaiProduk1", "Ongkir", "Diskon", "NamaCS", "NamaADV", "Hub", "Tanggal", "JumlahOrderClosing",
    "TypeSales", "Platfrom", "Divisi", "BiayaMarketingCS"
}

CRM_REQUIRED_FIELDS = {
    "NamaKonsumen", "NoHp", "Alamat", "Ekspedisi", "Memo", "Pembayaran",
    "Produk1", "QTY1", "NilaiProduk1", "Ongkir", "Diskon", "NamaCS", "Hub", "Tanggal", "JumlahOrderClosing",
    "TypeSales", "Platfrom", "Divisi", "JumlahTerimaData", "VoucherCRM"
}

MP_REQUIRED_FIELDS = {
    "Tanggal", "Divisi", "Platfrom", "NamaKonsumen", "NoHp", "Alamat",
    "Produk1", "QTY1", "NilaiProduk1", "NamaCS", "NamaADV",
    "JumlahOrderClosing", "TypeSales"
}

TIKTOK_REQUIRED_FIELDS = {
    "Tanggal", "Divisi", "Platfrom", "NamaKonsumen", "NoHp", "Alamat",
    "Produk1", "QTY1", "NilaiProduk1", "NamaCS", "Namaakun", "NamaADV",
    "JumlahOrderClosing", "TypeSales"
}

class TelegramBotValidator:
    def __init__(self, token):
        self.token = token
        self.app = None
        self._running = False
        self.message_queue = Queue()
        self._stop_event = threading.Event()
        self.loop = None

    @staticmethod
    def detect_format(text):
        if not text:
            return None
        
        if "Divisi : Tiktok MP" in text:
            if "Platfrom : MP" in text:
                return "MP"
            elif "Platfrom : Tiktok" in text:
                return "TIKTOK"
            return None
        elif "BiayaMarketingCS" in text:
            return "AKUISISI"
        elif "VoucherCRM" in text or "JumlahTerimaData" in text:
            return "CRM"
        return None

    @staticmethod
    def validate_field_names(text, required_fields):
        errors = []
        lines = text.split('\n')
        
        for line in lines:
            if ':' in line:
                field_name = line.split(':', 1)[0].strip()
                field_name_clean = field_name.replace('*', '')
                field_name_normalized = field_name_clean.replace(' ', '')
                
                if field_name_normalized in required_fields and ' ' in field_name_clean:
                    correct_field = field_name_normalized
                    errors.append(f"❌ Nama kolom '{field_name}' mengandung spasi. "
                                f"Penulisan yang benar: '{correct_field}'")
        
        return errors

    @staticmethod
    def extract_fields(text):
        fields = {}
        lines = text.split('\n')
        
        for line in lines:
            if ':' in line:
                field_name, value = line.split(':', 1)
                field_name_clean = field_name.strip().replace('*', '').replace(' ', '')
                value_clean = value.strip()
                
                if value_clean:
                    fields[field_name_clean] = value_clean
        
        return fields

    @staticmethod
    def validate_numeric_fields(fields):
        numeric_fields = {
            'QTY1', 'QTY2', 'QTY3', 'QTY4', 'QTY5',
            'NilaiProduk1', 'NIlaiProduk1',
            'Ongkir', 'Diskon', 'AdminCOD', 'TotalBayar',
            'JumlahOrderClosing', 'JumlahTerimaData', 'VoucherCRM',
            'BiayaMarketingCS'
        }
        
        errors = []
        for field in numeric_fields:
            if field in fields:
                value = fields[field]
                clean_value = value.replace('.', '').replace(',', '').replace('Rp', '').strip()
                if not clean_value.isdigit() and clean_value != '':
                    errors.append(f"❌ Field {field} harus berupa angka (current: {value})")
        
        return errors

    @staticmethod
    def validate_phone_number(phone):
        if not phone:
            return False
        clean_phone = phone.replace('-', '').replace(' ', '').replace('+', '')
        return bool(re.match(r'^(62|0)\d{8,13}$', clean_phone))

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.message.chat_id
        text = update.message.text
        
        if not text:
            return

        format_type = self.detect_format(text)
        if not format_type:
            await update.message.reply_text(
                "❌ Format pesan tidak dikenali.\n"
                "Format yang tersedia:\n"
                "- AKUISISI\n"
                "- CRM\n"
                "- MP\n"
                "- TIKTOK"
            )
            return

        required_fields = {
            "AKUISISI": AKUISISI_REQUIRED_FIELDS,
            "CRM": CRM_REQUIRED_FIELDS,
            "MP": MP_REQUIRED_FIELDS,
            "TIKTOK": TIKTOK_REQUIRED_FIELDS
        }[format_type]

        fields = self.extract_fields(text)
        field_name_errors = self.validate_field_names(text, required_fields)
        missing_fields = required_fields - set(fields.keys())
        numeric_errors = self.validate_numeric_fields(fields)

        errors = []
        
        if field_name_errors:
            errors.extend(field_name_errors)
            
        if missing_fields:
            errors.append("Field yang wajib diisi tidak ditemukan:")
            errors.extend([f"- {field}" for field in missing_fields])
            
        if numeric_errors:
            errors.extend(numeric_errors)
            
        if 'NoHp' in fields and not self.validate_phone_number(fields['NoHp']):
            errors.append("❌ Format nomor telepon tidak valid. Gunakan format: 08xxx atau 62xxx")

        if errors:
            error_message = "\n".join(errors)
            await update.message.reply_text(f"Ditemukan kesalahan:\n{error_message}")
        else:
            await update.message.reply_text(
                f"✅ Format {format_type} sudah benar dan lengkap!"
            )

        self.message_queue.put(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"Chat {chat_id}: {format_type} format - "
            f"{'Valid' if not errors else 'Invalid'}"
        )

    async def setup(self):
        try:
            self.app = ApplicationBuilder().token(self.token).build()
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))
            await self.app.initialize()
            return True
        except Exception as e:
            st.error(f"Bot setup error: {str(e)}")
            return False

    def run(self):
        def run_bot():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            async def start_bot():
                try:
                    if await self.setup():
                        self._running = True
                        await self.app.start()
                        await self.app.updater.start_polling()
                        
                        while self._running and not self._stop_event.is_set():
                            await asyncio.sleep(1)
                            
                except Exception as e:
                    st.error(f"Bot error: {str(e)}")
                finally:
                    self._running = False
                    if self.app:
                        await self.cleanup()

            self.loop.run_until_complete(start_bot())

        thread = threading.Thread(target=run_bot)
        thread.daemon = True
        thread.start()
        return thread

    async def cleanup(self):
        try:
            if self.app:
                await self.app.updater.stop()
                await self.app.stop()
                await self.app.shutdown()
                self.app = None
        except Exception as e:
            st.error(f"Cleanup error: {str(e)}")
        finally:
            self._running = False
            self._stop_event.set()

class BigQueryScraper:
    def __init__(self, api_id, api_hash, group_chat_id, credential_path, table_id):
        self.api_id = api_id
        self.api_hash = api_hash
        self.group_chat_id = self.process_group_id(group_chat_id)
        self.credential_path = credential_path
        self.table_id = table_id
        self.telegram_client = None
        self.bq_client = None
        self._stop_event = threading.Event()
        self._running = False
        self.message_queue = Queue()
        self.existing_ids = set()
        
    @staticmethod
    def process_group_id(group_id):
        """Process the group ID to make it compatible with Telethon."""
        try:
            # Remove any spaces and convert to string
            group_id = str(group_id).strip()
            
            # If it starts with -100, remove it and return as integer
            if group_id.startswith('-100'):
                return int(group_id[4:])
            # If it's just a negative number, return as is
            elif group_id.startswith('-'):
                return int(group_id)
            # If it's just the number, return as is
            else:
                return int(group_id)
        except ValueError as e:
            logger.error(f"Error processing group ID: {str(e)}")
            return group_id

    def get_existing_message_ids(self):
        """Fetch all existing message IDs from BigQuery."""
        query = f"SELECT DISTINCT idpesan FROM `{self.table_id}`"
        existing_ids = set()
        try:
            query_job = self.bq_client.query(query)
            for row in query_job:
                existing_ids.add(str(row.idpesan))
            return existing_ids
        except Exception as e:
            logger.error(f"Error fetching existing IDs: {str(e)}")
            return set()

    def clean_numeric_value(self, value, field_name):
        """
        Clean numeric values by removing currency symbols and separators.
        Handles Indonesian number format (using dots as thousand separators).
        """
        if not value:
            return None
            
        try:
            # Remove currency symbol, spaces, and other non-numeric characters
            cleaned = re.sub(r'[Rp\s%,]', '', str(value))
            # Replace dots (thousand separators in Indonesian format)
            cleaned = cleaned.replace('.', '')
            
            # Convert to appropriate type based on schema
            if SCHEMA_COLUMNS.get(field_name) == int:
                return int(cleaned) if cleaned else 0
            return cleaned
        except (ValueError, TypeError) as e:
            logger.warning(f"Error converting value '{value}' for field '{field_name}': {str(e)}")
            return 0 if SCHEMA_COLUMNS.get(field_name) == int else None

    def is_valid_format(self, message_text):
        """Check if the message follows the required format."""
        if not re.search(r'\w+\s*:', message_text):
            return False
            
        data = self.parse_message(message_text)
        required_fields_present = all(data.get(field) is not None for field in ["Tanggal", "Divisi"])
        
        return required_fields_present

    def parse_message(self, message_text):
        """Parse message text into structured data according to schema."""
        data = {col: None for col in SCHEMA_COLUMNS.keys()}
        data['Timestamp'] = datetime.utcnow().isoformat()
        
        try:
            lines = message_text.split('\n')
            for line in lines:
                if ':' not in line:
                    continue
                    
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                schema_key = key.replace(' ', '')
                if schema_key in SCHEMA_COLUMNS:
                    # Process value based on schema type
                    if SCHEMA_COLUMNS[schema_key] == int:
                        data[schema_key] = self.clean_numeric_value(value, schema_key)
                    else:
                        data[schema_key] = value if value else None
                        
            return data
            
        except Exception as e:
            logger.error(f"Error parsing message: {str(e)}")
            return data

    async def process_messages(self):
        """Process messages from Telegram group."""
        try:
            messages_processed = 0
            messages_skipped = 0
            errors = 0
            
            try:
                # First verify if we can access the group
                entity = await self.telegram_client.get_entity(self.group_chat_id)
                logger.info(f"Successfully connected to group: {entity.title}")
            except Exception as e:
                error_msg = (
                    f"Error accessing group {self.group_chat_id}. "
                    f"Please verify the group ID and ensure the account has access to it. Error: {str(e)}"
                )
                logger.error(error_msg)
                self.message_queue.put(error_msg)
                return
            
            async for message in self.telegram_client.iter_messages(self.group_chat_id):
                try:
                    message_id = str(message.id)
                    
                    # Check if message already exists in BigQuery
                    if message_id in self.existing_ids:
                        logger.debug(f"Skipping message {message_id} - Already exists in BigQuery")
                        messages_skipped += 1
                        continue
                        
                    if not message.text:
                        logger.debug(f"Skipping message {message_id} - No text content")
                        messages_skipped += 1
                        continue

                    # Skip messages that don't follow the required format
                    if not self.is_valid_format(message.text):
                        logger.debug(f"Skipping message {message_id} - Invalid format")
                        messages_skipped += 1
                        continue

                    # Parse message and prepare row
                    data = self.parse_message(message.text)
                    data['idpesan'] = message_id
                    
                    # Insert into BigQuery
                    insert_errors = self.bq_client.insert_rows_json(
                        self.table_id,
                        [data],
                    )
                    
                    if insert_errors:
                        logger.error(f"Error inserting message {message_id}: {insert_errors}")
                        errors += 1
                    else:
                        logger.info(f"Successfully processed message {message_id}")
                        messages_processed += 1
                        self.existing_ids.add(message_id)
                        
                        # Add to message queue for UI display
                        self.message_queue.put(
                            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                            f"Processed message {message_id} to BigQuery"
                        )

                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {str(e)}")
                    errors += 1
                    continue

            # Print summary
            summary = (
                f"\nScraping Summary:\n"
                f"Messages processed: {messages_processed}\n"
                f"Messages skipped: {messages_skipped}\n"
                f"Errors encountered: {errors}\n"
                f"Total messages checked: {messages_processed + messages_skipped}"
            )
            logger.info(summary)
            self.message_queue.put(summary)

        except Exception as e:
            error_msg = f"An error occurred during message processing: {str(e)}"
            logger.error(error_msg)
            self.message_queue.put(error_msg)

    async def initialize(self):
        try:
            # Initialize BigQuery client
            credentials = service_account.Credentials.from_service_account_file(
                self.credential_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            
            # Initialize Telegram client
            self.telegram_client = TelegramClient('scraper_session', self.api_id, self.api_hash)
            await self.telegram_client.start()
            
            # Initialize existing message IDs set
            self.existing_ids = self.get_existing_message_ids()
            logger.info(f"Loaded {len(self.existing_ids)} existing message IDs from BigQuery")
            
            return True
        except Exception as e:
            logger.error(f"Initialization error: {str(e)}")
            return False

    def run(self):
        def run_scraper():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            async def start_scraper():
                try:
                    if await self.initialize():
                        self._running = True
                        while self._running and not self._stop_event.is_set():
                            await self.process_messages()
                            await asyncio.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Scraper error: {str(e)}")
                finally:
                    self._running = False
                    await self.cleanup()

            loop.run_until_complete(start_scraper())

        thread = threading.Thread(target=run_scraper)
        thread.daemon = True
        thread.start()
        return thread

    async def cleanup(self):
        try:
            if self.telegram_client:
                await self.telegram_client.disconnect()
            if self.bq_client:
                self.bq_client.close()
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")
        finally:
            self._running = False
            self._stop_event.set()

# [Rest of the code (init_session_state, start/stop functions, and main) remains the same]
def init_session_state():
    """Initialize session state variables."""
    if 'validator_bot' not in st.session_state:
        st.session_state.validator_bot = None
    if 'scraper_bot' not in st.session_state:
        st.session_state.scraper_bot = None
    if 'validator_running' not in st.session_state:
        st.session_state.validator_running = False
    if 'scraper_running' not in st.session_state:
        st.session_state.scraper_running = False
    if 'messages' not in st.session_state:
        st.session_state.messages = []

def start_validator_bot(token):
    """Start the validator bot."""
    try:
        if not st.session_state.validator_running:
            st.session_state.validator_bot = TelegramBotValidator(token)
            thread = st.session_state.validator_bot.run()
            st.session_state.validator_running = True
            return True
    except Exception as e:
        st.error(f"Error starting validator bot: {str(e)}")
        return False

def stop_validator_bot():
    """Stop the validator bot."""
    try:
        if st.session_state.validator_running and st.session_state.validator_bot:
            st.session_state.validator_bot._stop_event.set()
            if st.session_state.validator_bot.loop:
                asyncio.run_coroutine_threadsafe(
                    st.session_state.validator_bot.cleanup(),
                    st.session_state.validator_bot.loop
                )
            st.session_state.validator_running = False
            st.session_state.validator_bot = None
            return True
    except Exception as e:
        st.error(f"Error stopping validator bot: {str(e)}")
        return False

def start_scraper_bot(api_id, api_hash, group_chat_id, credential_path, table_id):
    """Start the scraper bot."""
    try:
        if not st.session_state.scraper_running:
            st.session_state.scraper_bot = BigQueryScraper(
                api_id, api_hash, group_chat_id, credential_path, table_id
            )
            thread = st.session_state.scraper_bot.run()
            st.session_state.scraper_running = True
            return True
    except Exception as e:
        st.error(f"Error starting scraper bot: {str(e)}")
        return False

def stop_scraper_bot():
    """Stop the scraper bot."""
    try:
        if st.session_state.scraper_running and st.session_state.scraper_bot:
            st.session_state.scraper_bot._stop_event.set()
            if hasattr(st.session_state.scraper_bot, 'telegram_client'):
                asyncio.run(st.session_state.scraper_bot.cleanup())
            st.session_state.scraper_running = False
            st.session_state.scraper_bot = None
            return True
    except Exception as e:
        st.error(f"Error stopping scraper bot: {str(e)}")
        return False

def main():
    st.set_page_config(
        page_title="Telegram Bot Control Panel",
        page_icon="🤖",
        layout="wide"
    )
    
    st.title("Telegram Bot Control Panel")
    
    # Initialize session state
    init_session_state()
    
    # Create two columns for the main layout
    left_col, right_col = st.columns([2, 1])
    
    with left_col:
        # Validator Bot Section
        st.header("Message Validator Bot")
        validator_token = st.text_input("Enter Validator Bot Token:", type="password")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Start Validator", disabled=st.session_state.validator_running):
                if validator_token:
                    if start_validator_bot(validator_token):
                        st.success("Validator bot started successfully!")
                    else:
                        st.error("Failed to start validator bot")
                else:
                    st.error("Please enter a validator bot token!")
        
        with col2:
            if st.button("Stop Validator", disabled=not st.session_state.validator_running):
                if stop_validator_bot():
                    st.success("Validator bot stopped successfully!")
                else:
                    st.error("Failed to stop validator bot")
        
        # Scraper Bot Section
        st.header("BigQuery Scraper Bot")
        
        # Configuration inputs with default values
        with st.expander("Scraper Configuration", expanded=True):
            api_id = st.text_input("API ID:", value="29571344")
            api_hash = st.text_input("API Hash:", value="83071db94acd379a1242d3be3febd337", type="password")
            group_chat_id = st.text_input("Group Chat ID:", value="-1002269832980")
            credential_path = st.text_input(
                "BigQuery Credential Path:", 
                value="E:/college/BIG DATA &AI/tele-to-bigquery/data-looker-up-media-8b8f1ce55981.json"
            )
            table_id = st.text_input(
                "BigQuery Table ID:", 
                value="data-looker-up-media.UPDMperformance.Databaru"
            )
        
        col3, col4 = st.columns(2)
        with col3:
            if st.button("Start Scraper", disabled=st.session_state.scraper_running):
                if all([api_id, api_hash, group_chat_id, credential_path, table_id]):
                    if start_scraper_bot(api_id, api_hash, group_chat_id, credential_path, table_id):
                        st.success("Scraper bot started successfully!")
                    else:
                        st.error("Failed to start scraper bot")
                else:
                    st.error("Please fill all scraper configuration fields!")
        
        with col4:
            if st.button("Stop Scraper", disabled=not st.session_state.scraper_running):
                if stop_scraper_bot():
                    st.success("Scraper bot stopped successfully!")
                else:
                    st.error("Failed to stop scraper bot")
    
    with right_col:
        # Status Section
        st.header("Bot Status")
        status_col1, status_col2 = st.columns(2)
        with status_col1:
            validator_status = "🟢 Running" if st.session_state.validator_running else "🔴 Stopped"
            st.write(f"Validator Status: {validator_status}")
        
        with status_col2:
            scraper_status = "🟢 Running" if st.session_state.scraper_running else "🔴 Stopped"
            st.write(f"Scraper Status: {scraper_status}")
        
        # Message Log Section
        st.header("Message Log")
        if st.session_state.validator_bot:
            while not st.session_state.validator_bot.message_queue.empty():
                message = st.session_state.validator_bot.message_queue.get()
                st.session_state.messages.append(message)
        
        if st.session_state.messages:
            st.text_area(
                "Recent Messages",
                value="\n".join(reversed(st.session_state.messages[-100:])),
                height=400,
                disabled=True
            )
        
        # Clear log button
        if st.button("Clear Log"):
            st.session_state.messages = []
            st.experimental_rerun()

if __name__ == "__main__":
    main()