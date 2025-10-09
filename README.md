
# AI Expense Tracker


A modular pipeline for extracting, storing, and analyzing personal financial transactions using bank statement PDFs, MongoDB, and LLMs.

---

## Project Overview

This project enables you to:
- Parse bank statement PDFs into structured JSON transactions and insert them into MongoDB.
- Analyze expenses and answer natural language queries using LLMs and pandas.

---

## Workflow Sequence

1. **Orchestrate Data Extraction**  
   Parse all PDF bank statements, extract metadata, categorize transactions, and generate structured JSON files. Transactions are also inserted into MongoDB.
   - **Script:** `pdfDataOrchestrator.py`
   - **Output:** Transactions are saved directly to the configured MongoDB collection. (No files are saved in `processed_transactions/`.)

2. **Query and Analyze Expenses**  
   Use natural language to query and analyze your expenses with LLM-powered insights. LLM translates queries to MongoDB, fetches results, and summarizes with pandas and LLM.
   - **Script:** `query_expense.py`

---

## File Descriptions

| File                        | Purpose                                                                                 |
|-----------------------------|-----------------------------------------------------------------------------------------|
| `pdfDataOrchestrator.py`    | Extracts transactions from PDF statements, outputs structured JSON, and inserts into MongoDB. |
| `query_expense.py`          | Translates natural language queries to MongoDB, fetches and analyzes expenses, summarizes with pandas and LLM. |
| `requirements.txt`          | Python dependencies for the project.                                                    |

---

## Setup Instructions

1. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Prepare your data**
   - Place your bank statement PDFs in the `bank_statements/` directory.
   - Create a `.env` file in the project root and fill in the required values (see below).

3. **Run the data orchestrator**
   ```bash
   python pdfDataOrchestrator.py
   ```
   - This will parse PDFs and insert transactions directly into MongoDB. (No files are saved in `processed_transactions/`.)

4. **Query your expenses**
   ```bash
   python query_expense.py
   ```
   - Ask questions like "What did I spend on groceries last month?"
   - The script will:
     - Use LLM to translate your query to a MongoDB query
     - Fetch results from MongoDB
     - Summarize results using pandas and LLM

---

## Notes

- The project previously used ChromaDB for vector storage and semantic search, but now focuses on MongoDB for all data operations.
- LLM analysis requires [Ollama](https://ollama.com/) and a supported model (e.g., llama3).
- All scripts are modular and can be run independently as needed.
- MongoDB is used for storing and querying transactions.

---

## Example Usage

1. **Extract data:**  
   `python pdfDataOrchestrator.py`
   - Parses PDFs, generates JSON, and inserts into MongoDB

2. **Analyze expenses:**  
   `python query_expense.py`
   - Enter natural language queries, get results from MongoDB, summarized with pandas and LLM

---

## pdfDataOrchestrator.py - Key Features

- Reads all PDF statements from the configured directory
- Extracts transaction rows, cleans and parses dates, amounts, and descriptions
- Categorizes transactions (grocery, food delivery, rent, etc.) using keyword lists from `.env`
- Detects payment method (UPI, NEFT, ATM, etc.) and extracts bank details
- Inserts all parsed transactions into MongoDB for persistent storage and querying
  
  
## Additional Info

- All transaction parsing, categorization, and metadata extraction is handled in-memory and saved to MongoDB.
- The `.env` file controls all categorization lists and MongoDB connection details.
- No intermediate or output files are written to the `processed_transactions/` folder; all results are stored in the database.

---

## query_expense.py - Key Features

- Accepts natural language queries (e.g., "Total grocery spend in April 2025")
- Uses LLM to translate user queries into valid MongoDB queries
- Fetches matching transactions or aggregates from MongoDB
- Summarizes results using pandas (totals, averages, trends, category breakdowns)
- Uses LLM to generate human-friendly answers and insights
- Supports advanced analysis: monthly/quarterly/yearly summaries, category trends, merchant analysis

---

## .env Example (add to project root)

```
# PDF and JSON paths
INPUT_PDF_DIR=
OUTPUT_JSON_DIR=
COMBINED_FILE=
DATE_FORMAT=

# Transaction categorization lists (comma-separated)
CARRIER_LIST=
FOOD_DELIVERY=
SHOPPING=
TRANSPORT=
GROCERY=
HEALTHCARE=
RESTAURANTS=
FRUITS_VEGETABLES_FISH=
INTEREST_INCOME=
RENT=
EMI_LIST=
CREDIT_CARD_PAYMENT=
SUBSCRIPTION_SERVICES=
UTILITY_BILLS=
RECURRING_PAYMENTS=
FOODS_DRINKS=
ENTERTAINMENT=
PERSONAL_TYPE=

# MongoDB connection
MONGODB_URI=
DB_NAME=
COLLECTION_NAME=
```

---

## License

MIT License (add your license here if needed)
