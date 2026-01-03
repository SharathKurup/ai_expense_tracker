from dotenv import load_dotenv
import os

#region Configuration
load_dotenv()
INPUT_PDF_DIR = os.getenv("INPUT_PDF_DIR") # Folder containing PDF statements
OUTPUT_JSON_DIR = os.getenv("OUTPUT_JSON_DIR") # Folder for JSON outputs
COMBINED_FILE = os.getenv("COMBINED_FILE") # Combined transactions file
DATE_FORMAT = os.getenv("DATE_FORMAT")  # Expected date format in PDFs
CARRIER_LIST = os.getenv("CARRIER_LIST")
FOOD_DELIVERY = os.getenv("FOOD_DELIVERY")
SHOPPING = os.getenv("SHOPPING")
TRANSPORT = os.getenv("TRANSPORT")
GROCERY = os.getenv("GROCERY")
HEALTHCARE = os.getenv("HEALTHCARE")
RESTAURANTS = os.getenv("RESTAURANTS")
FRUITS_VEGETABLES_FISH = os.getenv("FRUITS_VEGETABLES_FISH")
INTEREST_INCOME = os.getenv("INTEREST_INCOME")
RENT= os.getenv("RENT")
EMI_LIST = os.getenv("EMI_LIST")
CREDIT_CARD_PAYMENT = os.getenv("CREDIT_CARD_PAYMENT")
SUBSCRIPTION_SERVICES = os.getenv("SUBSCRIPTION_SERVICES")
UTILITY_BILLS = os.getenv("UTILITY_BILLS")
RECURRING_PAYMENTS = os.getenv("RECURRING_PAYMENTS")
FOODS_DRINKS = os.getenv("FOODS_DRINKS")
ENTERTAINMENT = os.getenv("ENTERTAINMENT")
MONGODB_URI = os.getenv("MONGO_DB_URI")
DB_NAME = os.getenv("DB_NAME")
ENV = os.getenv("ENV")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
PERSONAL_TYPE = os.getenv("PERSONAL_TYPE")
EDUCATION = os.getenv("EDUCATION")
SPECIAL_EMI = os.getenv("SPECIAL_EMI")
MY_BANKS = os.getenv("MY_BANKS")
#endregion

#region clean Configuration
CARRIER_LIST = [x.strip().upper() for x in (CARRIER_LIST or "").split(",") if x.strip()]
FOOD_DELIVERY_LIST = [x.strip().upper() for x in (FOOD_DELIVERY or "").split(",") if x.strip()]
SHOPPING_LIST = [x.strip().upper() for x in (SHOPPING or "").split(",") if x.strip()]
TRANSPORT_LIST = [x.strip().upper() for x in (TRANSPORT or "").split(",") if x.strip()]
GROCERY_LIST = [x.strip().upper() for x in (GROCERY or "").split(",") if x.strip()]
HEALTHCARE_LIST = [x.strip().upper() for x in (HEALTHCARE or "").split(",") if x.strip()]
RESTAURANTS_LIST = [x.strip().upper() for x in (RESTAURANTS or "").split(",") if x.strip()]
FRUITS_VEGETABLES_FISH_LIST = [x.strip().upper() for x in (FRUITS_VEGETABLES_FISH or "").split(",") if x.strip()]
INTEREST_INCOME_LIST = [x.strip().upper() for x in (INTEREST_INCOME or "").split(",") if x.strip()]
RENT_LIST = [x.strip().upper() for x in (RENT or "").split(",") if x.strip()]
EMI_LIST = [x.strip().upper() for x in (EMI_LIST or "").split(",") if x.strip()]
CREDIT_CARD_PAYMENT_LIST = [x.strip().upper() for x in (CREDIT_CARD_PAYMENT or "").split(",") if x.strip()]
SUBSCRIPTION_SERVICES_LIST = [x.strip().upper() for x in (SUBSCRIPTION_SERVICES or "").split(",") if x.strip()]
UTILITY_BILLS_LIST = [x.strip().upper() for x in (UTILITY_BILLS or "").split(",") if x.strip()]
RECURRING_PAYMENTS_LIST = [x.strip().upper() for x in (RECURRING_PAYMENTS or "").split(",") if x.strip()]
FOODS_DRINKS_LIST = [x.strip().upper() for x in (FOODS_DRINKS or "").split(",") if x.strip()]
ENTERTAINMENT_LIST = [x.strip().upper() for x in (ENTERTAINMENT or "").split(",") if x.strip()]
PERSONAL_TYPE_LIST = [x.strip().upper() for x in (PERSONAL_TYPE or "").split(",") if x.strip()]
EDUCATION_LIST = [x.strip().upper() for x in (EDUCATION or "").split(",") if x.strip()]
SPECIAL_EMI_LIST = [x.strip().upper() for x in (SPECIAL_EMI or "").split(",") if x.strip()]
MY_BANKS_LIST = [x.strip().upper() for x in (MY_BANKS or "").split(",") if x.strip()]
DATE_FORMAT_LIST = [x.strip() for x in (DATE_FORMAT or "").split(",") if x.strip()]
#endregion
