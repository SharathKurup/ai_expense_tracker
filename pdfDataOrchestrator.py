#region Imports
import pdfplumber
import json
import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
import re
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
#endregion

#todo:: Move categorization lists to DB collection for easier management and updates.
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
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
ENV = os.getenv("ENV")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
PERSONAL_TYPE = os.getenv("PERSONAL_TYPE")
EDUCATION = os.getenv("EDUCATION")
SPECIAL_EMI = os.getenv("SPECIAL_EMI")
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
#endregion

def process_all_statements() -> Dict[str, Any]:
    print("Processing all PDF statements...")

    Path(OUTPUT_JSON_DIR).mkdir(exist_ok=True)
    monthly_output_dir=os.path.join(OUTPUT_JSON_DIR, "monthly")
    Path(monthly_output_dir).mkdir(parents=True, exist_ok=True) # parents=True -> will create all parent directories if they do not exist
    
    all_transactions = []
    processing_stats={
        "total_files": 0,
        "processed_files": [],
        "failed_files": 0,
        "total_transactions": 0
    }
    for pdf_file in Path(INPUT_PDF_DIR).glob("*.pdf"):
        try:
            transactions = process_single_statement(pdf_file, monthly_output_dir)
            insert_transactions_to_db(transactions)  # Insert transactions into MongoDB
        except Exception as e:
            print(f"Failed to process {pdf_file.name}: {str(e)}")
            continue

def process_single_statement(pdf_path: Path, output_dir: str) -> Dict[str, Any]:
    print(f"Processing {pdf_path.name}...")
    transactions = []
    doc_id=os.path.splitext(pdf_path.name)[0]  # Extract document ID from filename
    
    with pdfplumber.open(pdf_path) as pdf:

        bank_name = get_bank_name(pdf)

        for page in pdf.pages:
            try:
                tables = page.extract_tables()
                if not tables:
                    continue
                
                for table in tables:
                    for row in table:
                        try:
                            if not row or len(row) < 6 or not row[0] or "Tran Date" in row[0]: # Skip header rows or empty rows
                                continue
                            
                            transaction = process_transaction_row(row, doc_id)
                            if transaction:
                                transaction={"bank_name": bank_name, **transaction} # Add bank name to transaction at the beginning
                                transactions.append(transaction)
                        except (ValueError, IndexError, AttributeError) as e:
                            print(f"Skipping malformed row: {row}. Error: {str(e)}")
                            continue

            except Exception as e:
                print(f"Error processing page {page.page_number} in {pdf_path.name}: {str(e)}")
                continue
    # print(f"{json.dumps(transactions, indent=2)}")
    return transactions

def process_transaction_row(row: List[str], doc_id: str) -> Optional[Dict[str, Any]]:
    try:
        # Clean and convert date
        # print(f"Processing row: {row}")
        date_str = row[0].strip()
        # Skip rows where the first column is not a valid date
        try:
            date_obj = datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            return None  # Not a valid date, skip this row
        
        formatted_date=date_obj.strftime("%Y-%m-%d")
        day_of_week = date_obj.strftime("%A")
        is_weekend = day_of_week in ['Saturday', 'Sunday']
        month_year = formatted_date[:7]
        # month_year = date_obj.strftime("%m/%Y")
        quarter = f"Q{(int(formatted_date[5:7]) - 1) // 3 + 1}"

        raw_description = row[2] if len(row) > 2 else ""
        description = raw_description.strip().replace("\n", " ")

        debit = float(row[3].strip().replace(",", "")) if row[3]  else 0.0
        credit = float(row[4].strip().replace(",", "")) if row[4]  else 0.0
        balance = float(row[5].strip().replace(",", "")) if row[5]  else 0.0
              
        metadata=extract_comprehensive_metadata(
            description=description,
            debit=debit,
            credit=credit,
            date=formatted_date,
            day_of_week=day_of_week,
            is_weekend=is_weekend
        )
        
        transaction = {
            "document_id": doc_id,
            "date": formatted_date,
            "month_year": month_year,
            "quarter": quarter,
            "day_of_week": day_of_week,
            "is_weekend": is_weekend,
            "description": description,
            "debit": debit,
            "credit": credit,
            "balance": balance,
            **metadata,  # Flatten metadata into main transaction dict
        }
    except Exception as e:
        print(f"Error processing transaction row: {str(e)}")
        print(f"ROW: {row}")
        
    return transaction

def extract_comprehensive_metadata(
    description: str,
    debit: float,
    credit: float,
    date: str,
    day_of_week: str,
    is_weekend: bool,
) -> Dict[str, Any]:
    description_upper = description.upper()
    bank_details = extract_bank_details(description_upper)
    payment_method = extract_payment_method(description_upper)
    transaction_category = categorize_transaction(description_upper)
    is_debit = debit > 0
    is_credit = credit > 0
    amount_range = categorize_amount_range(debit if is_debit else credit)
    is_recurrring = is_recurrring_payment(description_upper)
    metadata = {
        "payment_method": payment_method,
        "transaction_category": transaction_category,
        "is_debit": is_debit,
        "is_credit": is_credit,
        "date": date,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "amount_range": amount_range,
        "is_recurrring": is_recurrring,
        "recipient_bank_details": bank_details,
    }
    return metadata
    
def extract_bank_details(description: str) -> Optional[Dict[str, str]]:
    if not description:
        return None
    if "/UPI" in description or description.startswith("UPI/"):
        upi_parts = description.split("/")
        if len(upi_parts) > 1:
            #upi_part = parts[-1].split("/")[0]
            source=upi_parts[0] if len(upi_parts) > 0 else ""
            target_identifier = upi_parts[1] if len(upi_parts) > 1 else ""
            
            transaction_id = upi_parts[2] if len(upi_parts) > 2 else ""
            recepient_name = upi_parts[3] if len(upi_parts) > 3 else ""
            recipient_type = "PERSONAL" if any(x in recepient_name.upper() for x in PERSONAL_TYPE_LIST) else getRecipientType(target_identifier)
            bank_name = upi_parts[5] if len(upi_parts) > 5 else ""

            return {
                "source": source,
                "sendTo": recipient_type,
                "transaction_id": transaction_id,
                "recipient_name": recepient_name.strip().title(),
                "bank_name": bank_name.strip().title()
            }
        
    # if "NEFT" in description:
    if any(x in description for x in ["NEFT", "IMPS", "RTGS"]):
        parts = description.split("/")
        if len(parts)>1:
            source=parts[0] if len(parts) > 0 else ""
            banking_identifier = parts[1] if len(parts) > 1 else "" 
            transaction_id = parts[2] if len(parts) > 2 else ""
            recipient_name = parts[3] if len(parts) > 3 else ""
            bank_name = parts[4] if len(parts) > 4 else ""

            if banking_identifier=="MB":
                banking_type="Mobile Banking"
            elif banking_identifier=="IB":
                banking_type="Internet Banking"
            else:
                banking_type="Other Transfer"

            return{
                "source": source,
                "banking_type": banking_type,
                "transaction_id": transaction_id,
                "recipient_name": recipient_name.strip().title(),
                "bank_name": bank_name.strip().title()
            }
    
    if "ATM-" in description:
        parts = description.split("/")
        #print(parts,len(parts))
        if "AXIS" in parts[0]:
            ATM_Name = ""
            Terminal_id = parts[1].strip()
            Reference_id = parts[2].strip()
            Location = parts[4].strip()
        else:
            ATM_Name = parts[1].strip()
            Terminal_id = ""
            Reference_id = ""
            Location = parts[2].strip()
    
        return{
            "source": "ATM",
            "ATM_Name": ATM_Name,
            "Terminal_id": Terminal_id,
            "Reference_id": Reference_id,
            "Location": Location
        }
#TODO: work on imps, and check some failed UPI cases
    # Cheque pattern - no data found in sample data
    if "CHQ" in description or "CHEQUE" in description:
        match = re.search(r'CHQ\s*(\d+)', description)
        chq_no = match.group(1) if match else None
        return {
            "bank": extract_bank_from_chq_desc(description),
            "source": "CHEQUE",
            "cheque_number": chq_no
        }
    
    return None

def getRecipientType(target_identifier):
    if target_identifier.startswith("P2P") or any(x in target_identifier for x in PERSONAL_TYPE_LIST): # P2P - person to person
        recipient_type = "PERSONAL"
    elif target_identifier.startswith(("P2M", "P2A")): # P2A- person to account, P2M - person to merchant
        recipient_type = "MERCHANT"
    else:
        recipient_type = ""
    return recipient_type

def extract_bank_from_chq_desc(description: str) -> Optional[str]:
    """Extract bank name from cheque description."""
    patterns = [
        r'CHQ\s*(\d+)\s*([A-Z]+)',
        r'CHEQUE\s*(\d+)\s*([A-Z]+)',
        r'([A-Z]+)\s*CHQ'
    ]
    for pattern in patterns:
        match = re.search(pattern, description)
        if match and len(match.groups()) > 1:
            return match.group(2).title()
    return None

def extract_payment_method(description: str) -> str:
    """Detect payment method from transaction description."""
    if "UPI" in description:
        return "UPI"
    elif "POS" in description:
        return "CARD_PAYMENT"
    elif "ATM" in description:
        return "ATM"
    elif any(x in description for x in ["NEFT", "IMPS", "RTGS"]):
        return "BANK_TRANSFER"
    elif "CHEQUE" in description or "CHQ" in description:
        return "CHEQUE"
    elif "NETBANKING" in description or "IB FUND" in description:
        return "ONLINE_BANKING"
    elif "CREDITCARD" in description:
        return "CREDIT_CARD"
    else:
        return "OTHER"

#region Issue with Transaction Categories 
# Issue with utility, as the payment is done via UPI apps., it doesn't provide a clear name to identify the transaction
# probably make it manual for now, update it in DB once the data is loaded.
# future try to make payments via the utility app/website to get a clear name and identity.
# utility to be added in future

# dividend data to be added in future:: I get it Canara currently it won't be available
# insurance data to be added in future:: I get it Canara currently it won't be available

# for now generalize the utility under utlity bills, categorize it later manually/or in future in DB
#endregion
def categorize_transaction(description: str)-> str:
    """Categorize transaction based on description keywords."""
    # print(f"Categorizing transaction: {description}")
    description=description.upper()
    # check for PERSONAL_TYPE_LIST first 
    # if any(x in description for x in PERSONAL_TYPE_LIST):
    #     return "PERSONAL"
    if any(x in description for x in FOOD_DELIVERY_LIST):
        return "FOOD_DELIVERY"
    elif any(x in description for x in GROCERY_LIST):
        return "GROCERY"
    elif any(x in description for x in SHOPPING_LIST):
        return "SHOPPING"
    elif any(x in description for x in TRANSPORT_LIST):
        return "TRANSPORT"
    elif any(x in description for x in HEALTHCARE_LIST):
        return "HEALTHCARE"
    elif any(x in description for x in RESTAURANTS_LIST):
        return "RESTAURANTS"
    elif any(x in description for x in FRUITS_VEGETABLES_FISH_LIST):
        return "FRUITS_VEGETABLES"
    elif any(x in description for x in INTEREST_INCOME_LIST):
        return "INTEREST_INCOME"
    elif any (x in description for x in RENT_LIST):
        return "RENT"
    elif any (x in description for x in ['SALARY']):
        return "SALARY"
    elif any (x in description for x in CARRIER_LIST):
        return "RECHARGE"
    elif any(description.startswith(x) for x in EMI_LIST) or any(x in description for x in SPECIAL_EMI_LIST):
        return "LOAN_PAYMENT"
    elif any (x in description for x in CREDIT_CARD_PAYMENT_LIST):
        return "CREDIT_CARD_PAYMENT"
    elif any (x in description for x in SUBSCRIPTION_SERVICES_LIST):
        return "SUBSCRIPTION_SERVICES"
    elif any (x in description for x in UTILITY_BILLS_LIST):
        return "UTILITY_BILLS"
    elif any (x in description for x in FOODS_DRINKS_LIST):
        return "FOODS_DRINKS"
    elif any (x in description for x in ENTERTAINMENT_LIST):
        return "ENTERTAINMENT"
    elif any (x in description for x in EDUCATION_LIST):
        return "EDUCATION"
    if any(x in description for x in PERSONAL_TYPE_LIST):
        return "PERSONAL"
    else:
        return "OTHER"

def categorize_amount_range(amount:float) -> str:
    if amount < 100:
        return "SMALL"
    elif 100 <= amount < 1000:
        return "MEDIUM"
    elif 1000 <= amount < 10000:
        return "LARGE"
    else:
        return "VERY_LARGE"

def is_recurrring_payment(description: str) -> bool:
    description=description.upper()
    if any (x in description for x in RECURRING_PAYMENTS):
        return True
    return False

def get_effective_collection_name() -> str:
    base = COLLECTION_NAME
    if (ENV or "").strip().lower() == "dev":
        return f"{base}_dev"
    return base

def insert_transactions_to_db(transactions: List[Dict[str, Any]]):
    client = MongoClient(MONGODB_URI)  # Update with your MongoDB URI
    db = client[DB_NAME]  # Database name
    collection = db[get_effective_collection_name()]  # Collection name
    if transactions:
        collection.insert_many(transactions)  # Insert all transactions
    client.close()

def get_bank_name(pdf):
    text = pdf.pages[0].extract_text()
    if text:
        if "AXIS" in text.upper():
            bank_name = "AXIS BANK"
        elif "HDFC" in text.upper():
            bank_name = "HDFC BANK"
        elif "SBI" in text.upper() or "STATE BANK OF INDIA" in text.upper():
            bank_name = "STATE BANK OF INDIA"
        elif "ICICI" in text.upper():
            bank_name = "ICICI BANK"
        else:
            bank_name = "UnknownBank"
    else:
        bank_name = "UnknownBank"
    return bank_name

if __name__ == "__main__":
    # Process all statements when script is run
    print("PDF Orchestrator initialized")
    result = process_all_statements()
    #print("\nSample transaction from combined output:")
    #print(json.dumps(result["transactions"][0], indent=2))
    print("PDF Orchestrator completed processing all statements.")