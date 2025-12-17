from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json
import ollama
import pandas as pd
from datetime import datetime


load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

ALLOWED_FIELDS = {
    "date", "month_year", "quarter", "day_of_week", "is_weekend",
    "description", "debit", "credit", "balance", "payment_method",
    "transaction_category", "is_debit", "is_credit", "amount_range",
    "is_recurrring", "recipient_bank_details.source", "recipient_bank_details.sendTo",
    "recipient_bank_details.transaction_id", "recipient_bank_details.recipient_name",
    "recipient_bank_details.bank_name"
}

SCHEMA_FIELDS = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Bank Transaction Document",
  "type": "object",
  "description": "Schema for bank transaction records with comprehensive transaction details and metadata",
  "properties": {
    "bank_name": {
      "type": "string",
      "examples": ["AXIS BANK", "HDFC BANK", "SBI", "ICICI BANK"]
    },
    "document_id": {
      "type": "string",
      "examples": ["Account_Statement_april_2025", "Statement_march_2025"]
    },
    "date": {
      "type": "string",
      "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
    },
    "month_year": {
      "type": "string",
      "pattern": "^\\d{4}-\\d{2}$",
      "examples": ["2025-04", "2025-03", "2024-12"]
    },
    "quarter": {
      "type": "string",
      "enum": ["Q1", "Q2", "Q3", "Q4"]
    },
    "day_of_week": {
      "type": "string",
      "enum": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    },
    "is_weekend": {
      "type": "boolean",
    },
    "description": {
      "type": "string",
    },
    "debit": {
      "type": "number",
      "multipleOf": 0.01
    },
    "credit": {
      "type": "number",
      "multipleOf": 0.01
    },
    "balance": {
      "type": "number",
      "multipleOf": 0.01
    },
    "payment_method": {
      "type": "string",
      "enum": ["UPI", "NEFT", "RTGS", "IMPS", "DEBIT_CARD", "CREDIT_CARD", "NET_BANKING", "CHEQUE", "CASH", "ATM", "POS"]
    },
    "transaction_category": {
      "type": "string",
      "enum": [
        "GROCERY", "FOOD", "TRANSPORTATION", "FUEL", "SHOPPING", "ENTERTAINMENT",
        "UTILITIES", "RENT", "MEDICAL", "EDUCATION", "INSURANCE", "INVESTMENT",
        "SALARY", "BUSINESS", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "FEE", "OTHER"
      ]
    },
    "is_debit": {
      "type": "boolean",
    },
    "is_credit": {
      "type": "boolean",
    },
    "amount_range": {
      "type": "string",
      "enum": ["SMALL", "MEDIUM", "LARGE", "VERY_LARGE"]
    },
    "is_recurrring": {
      "type": "boolean",
    },
    "recipient_bank_details": {
      "type": "object",
      "properties": {
        "source": {
          "type": "string",
          "examples": ["UPI", "NEFT", "RTGS", "IMPS", "CARD"]
        },
        "sendTo": {
          "type": "string",
          "enum": ["merchant", "individual", "bank", "government", "organization"]
        },
        "transaction_id": {
          "type": "string",
          "pattern": "^[0-9A-Za-z]+$"
        },
        "recipient_name": {
          "type": "string",
        },
        "bank_name": {
          "type": "string",
        }
      }
    }
  }
}

# {{"operation": "find", "filter": {{"field": "value"}}}}
# {{"operation": "aggregate", "pipeline": [{{"$match": {{"field": "value"}}}}, {{"$group": {{"_id": "$field", "total": {{"$sum": "$amount"}}}}}}]}}

def generate_mongo_query(user_query: str):
    print(f"Generating MongoDB query for user input: {user_query}")
    prompt = f"""You are a MongoDB query translator. Convert user requests into valid MongoDB operations.

        CRITICAL RULES:
        1. Output ONLY valid JSON - no explanations, no extra text
        2. Choose ONE query type based on user intent:
            - Use "find" for listing/filtering documents  
            - Use "aggregate" for calculations, grouping, totals, averages or complex operations
        3. Use collation for case-insensitive text matching

        OUTPUT FORMATS:
        For find queries:
        {{"operation": "find", "filter": {{"field": "value"}}, "collation": {{"locale": "en", "strength": 2}}}}

        For aggregation queries:
        {{"operation": "aggregate", "pipeline": [{{"$match": {{"field": "value"}}}}, {{"$group": {{"_id": "$field", "total": {{"$sum": "$amount"}}}}}}], "collation": {{"locale": "en", "strength": 2}}}}

        FIELD USAGE:
        - Only use these fields: {list(ALLOWED_FIELDS)}
        - Follow this schema: {json.dumps(SCHEMA_FIELDS)}

        DATE QUERIES:
        - Exact date: {{"date": "2025-04-01"}}
        - Date range: {{"date": {{"$gte": "2025-04-01", "$lte": "2025-04-30"}}}}
        - ALWAYS use $ prefix: "$gte", "$lte", "$eq"

        AGGREGATION GUIDELINES:
        - Always start with $match to filter data when conditions are specified
        - For totals: Use {{"sum":"sum": "sum":"debit"}} for expenses or {{"sum": {{"add": ["debit","debit", "debit","credit"]}}}} for all transactions
        - For averages: Use {{"avg":"avg": "avg":"debit"}} for expenses or {{"avg": {{"add": ["debit","debit", "debit","credit"]}}}} for all transactions
        - Valid stages: $match, $group, $project, $sort, $limit, $lookup
        - NEVER use $find - it doesn't exist
        - CRITICAL: ALL pipeline stages MUST start with $ (dollar sign): "$match", "$group", "$project"
        - CRITICAL: ALL operators MUST start with $ (dollar sign): "$gte", "$lte", "$eq", "$sum", "$avg"

        EXAMPLES:
        User: "Show me transactions on April 1st"
        Output: {{"operation": "find", "filter": {{"date": "2025-04-01"}}}}

        User: "Total amount spent in April"  
        Output: {{"operation": "aggregate", "pipeline": [{{"$match": {{"date": {{"$gte": "2025-04-01", "$lte": "2025-04-30"}}}}}}, {{"$group": {{"_id": null, "total": {{"$sum": "$amount"}}}}}}]}}

        Remember: Output ONLY the JSON query, nothing else.

        User request: "{user_query}"
    """

    response = ollama.chat(
        model="llama3", 
        messages=[{"role": "user", "content": prompt}]
    )
    
    try:
        mongo_query = json.loads(response["message"]["content"])
        return mongo_query
    except Exception as e:
        print("Error parsing LLM output:", e)
        return None

def query_expenses(user_query: str):
    print(f"Fetching Expenses")
    client = MongoClient(MONGODB_URI)
    db=client[DB_NAME]
    collection=db[COLLECTION_NAME]
    #results = collection.find({'payment_method':'OTHER'})
    if "find" in user_query or (user_query.get("operation") == "find"):
        if "find" in user_query:
            query = user_query["find"].get("filter", user_query["find"])
        else:
            query = user_query.get("filter", {})
        cursor = collection.find(query)
        results = list(cursor)
    elif "aggregate" in user_query or (user_query.get("operation") == "aggregate"):
        if "aggregate" in user_query:
            pipeline = user_query["aggregate"].get("pipeline", user_query["aggregate"])
        else:
            pipeline = user_query.get("pipeline", [])
        cursor = collection.aggregate(pipeline)
        results = list(cursor)
    else:
        print("Unsupported query type.", user_query)
        
    for doc in results:
        doc['_id'] = str(doc['_id'])  

    print("Results Fetched:", len(results))
    # print(json.dumps(expenses, indent=2))
    client.close()
    return results

def summarize_expenses(expenses, user_query: str):
    print(f"Summarizing {len(expenses)} expense results.")
    
    prompt = f"""
        You are a financial assistant analyzing expense data. Your task is to provide a clear, helpful response based on the user's query and the MongoDB results.

        User Query: "{user_query}"
        Database Results: {expenses}

        Instructions:
        1. Directly answer the user's specific question
        2. If results contain numbers, present them clearly with currency formatting when appropriate
        3. If results are empty or zero, explain what this means
        4. For transaction lists, summarize key insights (total amount, count, categories, etc.)
        5. For aggregated data, explain the totals/averages in context
        6. Use natural, conversational language - avoid technical jargon
        7. If relevant, mention the time period or filters that were applied

        Format Guidelines:
        - Start with a direct answer to their question
        - Use bullet points for multiple transactions or insights
        - Include totals and counts when helpful
        - End with any relevant observations or patterns

        Example responses:
        - "You spent ₹308 on April 1st, 2025 - a single grocery purchase at Om Super Shopee via UPI."
        - "No transactions found for the specified period."
        - "Your total grocery expenses in April were ₹1,250 across 8 transactions."

        Provide a helpful, human-friendly response:
        """
    
    response = ollama.chat(
        model="llama3", 
        messages=[{"role": "user", "content": prompt}]
    )
    return response["message"]["content"]

def analyze_large_dataset_pandas(expenses):
    df = pd.DataFrame(expenses)
    df['date'] = pd.to_datetime(df['date'])
    start= df['date'].min().date(),
    end= df['date'].max().date(),
    panda_analysis = {
        "analysis_id": f"{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "time_period": {"start": start, "end": end},
        "overview_stats": {
            "total_transactions": len(df),
            "total_spent": df['debit'].sum(),
            "total_received": df['credit'].sum(),
            "average_debit": df['debit'].mean(),
            "average_credit": df['credit'].mean(),
            "median_debit": df['debit'].median(),
            "median_credit": df['credit'].median(),
            "max_debit": df['debit'].max(),
            "max_credit": df['credit'].max(),
            "min_debit": df['debit'].min(),
            "min_credit": df['credit'].min(),
            "std_dev_debit": df['debit'].std(),
            "std_dev_credit": df['credit'].std(),
        },
        "temporal_analysis": {
            "yearly_summary": df.groupby(df['date'].dt.year).agg({
                'debit': ['sum', 'mean', 'max'],
                'credit': ['sum', 'mean', 'max'],
                'date': 'count'
            }).rename(columns={'date': 'transaction_count'}).to_dict(orient='index'),
            # "yearly_summary": df.groupby(df['date'].str[:4]).agg({
            #     'debit': ['sum', 'mean', 'max'],
            #     'credit': ['sum', 'mean', 'max'],
            #     'date': 'count'
            # }).rename(columns={'date': 'transaction_count'}).to_dict(orient='index'),
            "monthly_summary": df.groupby('month_year').agg({
                'debit': ['sum', 'mean', 'max'],
                'credit': ['sum', 'mean', 'max'],
                'date': 'count'
            }).rename(columns={'date': 'transaction_count'}).to_dict(orient='index'),
            "weekday_analysis": df.groupby('day_of_week').agg({
                'debit': ['sum', 'mean', 'count'],
                'credit': ['sum', 'mean', 'count'],
                'date': 'count'
            }).rename(columns={'date': 'transaction_count'}).to_dict(orient='index'),
            "yearly_totals": {
            "debit": df.groupby(df["date"].dt.year)["debit"].sum().to_dict(),
            "credit": df.groupby(df["date"].dt.year)["credit"].sum().to_dict(),
            "net": df.groupby(df["date"].dt.year).apply(lambda x: x["credit"].sum() - x["debit"].sum()).to_dict()
            },
            "monthly_totals": {
                "debit": df.groupby(df["date"].dt.to_period("M"))["debit"].sum().to_dict(),
                "credit": df.groupby(df["date"].dt.to_period("M"))["credit"].sum().to_dict(),
                "net": df.groupby(df["date"].dt.to_period("M")).apply(lambda x: x["credit"].sum() - x["debit"].sum()).to_dict()
            },
            "quarterly_totals": {
                "debit": df.groupby(df["date"].dt.to_period("Q"))["debit"].sum().to_dict(),
                "credit": df.groupby(df["date"].dt.to_period("Q"))["credit"].sum().to_dict(),
                "net": df.groupby(df["date"].dt.to_period("Q")).apply(lambda x: x["credit"].sum() - x["debit"].sum()).to_dict()
            },
            # "daily_totals": {
            #     "debit": df.groupby(df["date"].dt.date)["debit"].sum().to_dict(),
            #     "credit": df.groupby(df["date"].dt.date)["credit"].sum().to_dict(),
            #     "net": df.groupby(df["date"].dt.date).apply(lambda x: x["credit"].sum() - x["debit"].sum()).to_dict()
            # },
            # "daily_averages": {
            #     "debit": df.groupby(df["date"].dt.date)["debit"].mean().to_dict(),
            #     "credit": df.groupby(df["date"].dt.date)["credit"].mean().to_dict()
            # },
            "weekend_vs_weekday": df.groupby('is_weekend').agg({
                'debit': ['sum', 'mean', 'count'],
                'credit': ['sum', 'mean', 'count'],
                'date': 'count'
            }).rename(columns={'date': 'transaction_count'}).to_dict(orient='index'),#todo: need to optimize weekend_vs_weekday, to show count in weekday and weekend
        },
        "categorical_analysis": {
            "totals": df.groupby('transaction_category')["debit"].sum().sort_values(ascending=False).to_dict(),
            "counts": df.groupby('transaction_category')['debit'].count().to_dict(),
            "averages": df.groupby('transaction_category')["debit"].mean().to_dict(),
            # "top_expenses": df.sort_values(by='debit', ascending=False).head(3)[['date', 'description', 'debit', 'transaction_category']].to_dict(orient='records'),
            "top_expenses": df[df['transaction_category'] != "PERSONAL"].sort_values(by='debit', ascending=False).head(10)[['date', 'description', 'debit', 'transaction_category']].to_dict(orient='records'),
            "category_trends": calculate_category_trends(df),
            "merchant_trends": analyze_merchant_trends(df),
        }
    }

    print ("Pandas Analysis Result:")
    print(json.dumps(stringify_keys(panda_analysis), indent=2, default=str))

def determine_trend_direction(recent_data) -> str:
    
    first_value = recent_data.iloc[0]
    last_value = recent_data.iloc[-1]
    
    if last_value > first_value * 1.1:  # 10% increase
        return "increasing"
    elif last_value < first_value * 0.9:  # 10% decrease
        return "decreasing"
    else:
        return "stable"

def calculate_category_trends(df):
    debit_df = df[(df['is_debit'] == True) & (df['transaction_category'] != "PERSONAL")].copy()
    # credit_df = df[df['is_credit'] == True].copy()
    credit_df = df[(df['is_credit'] == True) & (df['transaction_category'] != "PERSONAL")].copy()
    trends={
        'expense_trends': {},
        'income_trends': {},
        'net_trends': {},
        "category_summary": {},
        "financial_overview": {}
    }

    # Expense Trends
    if not debit_df.empty:
      monthly_category = df.groupby(['month_year', 'transaction_category'])['debit'].sum().unstack(fill_value=0)
      
      for category in monthly_category.columns:
          category_data= monthly_category[category]
          pct_change=category_data.pct_change().fillna(0).replace([float('inf'), -float('inf')], 0).round(4)
          total_spent = float(category_data.sum())

          if total_spent == 0:
            continue  # Skip categories with no expenses

          recent_months=category_data.tail(3)
          trend_direction="stable"

          if len(recent_months) >= 2:
              if recent_months.iloc[-1] > recent_months.iloc[-2]:
                  trend_direction="increasing"
              elif recent_months.iloc[-1] < recent_months.iloc[-2]:
                  trend_direction="decreasing"

          # trend_direction= determine_trend_direction(recent_months)

          trends['expense_trends'][category] = {
              "category": category,
              "monthly_amounts": category_data.astype(float).to_dict(),
              "average_monthly": float(category_data.mean()),
              "peak_month": str(category_data.idxmax()) if len(category_data) > 0 else None,
              "peak_amount": float(category_data.max()),
              "lowest_month": str(category_data.idxmin()) if len(category_data) > 0 else None,
              "lowest_amount": float(category_data.min()),
              "trend_direction": trend_direction,
              "volatility": float(category_data.std()) if len(category_data) > 1 else 0,
              "growth_rate": float(pct_change.mean()) * 100,
              "recent_3_month_avg": float(recent_months.mean()) if len(recent_months) > 0 else 0,
              "total_spent": total_spent,
              "transaction_type": "expense"
          }
          # print(json.dumps(stringify_keys(trends['expense_trends'][category]), indent=2))

    # Income Trends
    if not credit_df.empty:
      monthly_category = df.groupby(['month_year', 'transaction_category'])['credit'].sum().unstack(fill_value=0)
      
      for category in monthly_category.columns:
          category_data= monthly_category[category]
          
          pct_change=category_data.pct_change().fillna(0).replace([float('inf'), -float('inf')], 0).round(4)
          total_received = float(category_data.sum())

          if total_received == 0:
              continue  # Skip categories with no income

          recent_months=category_data.tail(3)
          trend_direction="stable"

          if len(recent_months) >= 2:
              if recent_months.iloc[-1] > recent_months.iloc[-2]:
                  trend_direction="increasing"
              elif recent_months.iloc[-1] < recent_months.iloc[-2]:
                  trend_direction="decreasing"

          # trend_direction= determine_trend_direction(recent_months)
        
          trends["income_trends"][category] = {
              "category": category,
              "monthly_amounts": category_data.astype(float).to_dict(),
              "average_monthly": float(category_data.mean()),
              "peak_month": str(category_data.idxmax()) if len(category_data) > 0 else None,
              "peak_amount": float(category_data.max()),
              "lowest_month": str(category_data.idxmin()) if len(category_data) > 0 else None,
              "lowest_amount": float(category_data.min()),
              "trend_direction": trend_direction,
              "volatility": float(category_data.std()) if len(category_data) > 1 else 0,
              "growth_rate": float(pct_change.mean()) * 100,
              "recent_3_month_avg": float(recent_months.mean()) if len(recent_months) > 0 else 0,
              "total_received": total_received,
              "transaction_type": "income"
          }
          # print(json.dumps(stringify_keys(trends['income_trends'][category]), indent=2))
          
    # Net Trends      
    all_categories = set()
    if trends['expense_trends']:
        all_categories.update(trends['expense_trends'].keys())
    if trends['income_trends']:
        all_categories.update(trends['income_trends'].keys())

    for category in all_categories:
        total_spent=trends["expense_trends"].get(category, {}).get("total_spent", 0)
        total_received=trends["income_trends"].get(category, {}).get("total_received", 0)
        net_amount= total_received - total_spent

        monthly_net = {}
        expense_monthly = trends["expense_trends"].get(category, {}).get("monthly_amounts", {})
        income_monthly = trends["income_trends"].get(category, {}).get("monthly_amounts", {})
        all_months = set(expense_monthly.keys()).union(set(income_monthly.keys())) # all_months = set(expense_monthly.keys()) | set(income_monthly.keys())

        for month in all_months:
            expense_amt = expense_monthly.get(month, 0)
            income_amt = income_monthly.get(month, 0)
            monthly_net[month] = float(income_amt - expense_amt)

        trends["net_trends"][category] = {
            "category": category,
            "monthly_amounts": monthly_net,
            "total_net": float(net_amount),
            'net_category_type': 'income_generating' if net_amount > 0 else 'expense_category' if net_amount < 0 else 'balanced',
            'average_monthly_net': float(sum(monthly_net.values()) / len(monthly_net)) if monthly_net else 0,
            'net_volatility': float(pd.Series(list(monthly_net.values())).std()) if len(monthly_net) > 1 else 0
        }

        # print(json.dumps(stringify_keys(trends['net_trends'][category]), indent=2))

    # category wise expense and income info
    for category in all_categories:
        expense_info = trends['expense_trends'].get(category)
        income_info = trends['income_trends'].get(category)
        net_info = trends['net_trends'].get(category)

        expense_count = len(debit_df[debit_df["transaction_category"] == category]) if not debit_df.empty else 0
        income_count = len(credit_df[credit_df["transaction_category"] == category]) if not credit_df.empty else 0

        trends["category_summary"][category] = {
            'category': category,
            'total_expense_amount':expense_info.get('total_spent', 0) if expense_info else 0,
            'total_income_amount':income_info.get('total_received', 0) if income_info else 0,
            'net_amount':net_info.get('total_net', 0),
            'expense_transaction_count': expense_count,
            'income_transaction_count': income_count,
            'total_transaction_count': expense_count + income_count,
            'category_type': net_info.get('net_category_type', 'unknown'),
            'has_expense': expense_count > 0,
            'has_income': income_count > 0,
            'expense_trend_direction': expense_info.get('trend_direction', 'N/A') if expense_info else 'N/A',
            'income_trend_direction': income_info.get('trend_direction', 'N/A') if income_info else 'N/A',
            'primary_activity': 'income' if income_count > expense_count else 'expense' if expense_count > 0 else 'inactive'
        }

        # print(json.dumps(stringify_keys(trends['category_summary'][category]), indent=2))

    # Overall financial 
    total_income = sum(info.get('total_received', 0) for info in trends['income_trends'].values())
    total_expense = sum(info.get('total_spent', 0) for info in trends['expense_trends'].values())

    trends["financial_overview"]={
        'total_income_all_categories': float(total_income),
        'total_expense_all_categories': float(total_expense),
        'net_balance_all_categories': float(total_income - total_expense),
        'income_categories_count': len(trends['income_trends']),
        'expense_categories_count': len(trends['expense_trends']),
        'savings_rate': float((total_income - total_expense) / total_income * 100) if total_income > 0 else 0,
        'top_income_categories': sorted(trends['income_trends'].items(), key = lambda x: x[1] .get('total_received', 0), reverse=True)[:3],
        'top_expense_categories': sorted(trends['expense_trends'].items(), key = lambda x: x[1] .get('total_spent', 0), reverse=True)[:3],
        'income_vs_expense_ratio': float(total_income / total_expense) if total_expense > 0 else 0,
        'overall_financial_health': 'positive' if total_income > total_expense else 'negative' if total_expense > total_income else 'balanced'
    }

    # print(json.dumps(stringify_keys(trends['financial_overview']), indent=2))

    #todo: check income more deeply
    #transfer are also considered in income category as it is credit but it is not income
    #need to find a way, to mark income [salary, interest, dividend etc] and transfer [from other account, refund etc]
    #print(f"Category: {category}")
    # print(json.dumps(stringify_keys(trends['expense_trends'][category]), indent=2))
    #print trends
    #print(monthly_category)

    return trends

def analyze_merchant_trends(df): # use only data with sendTo as merchant
    
    df_merchants = df[df['recipient_bank_details'].apply(lambda bd: isinstance(bd, dict) and bd.get('sendTo', '').lower() == 'merchant')].copy()
    # df_merchants = df.loc[df['recipient_bank_details'].apply(lambda bd: isinstance(bd, dict) and bd.get('sendTo', '').lower() == 'merchant')]
    # print(df_merchants["transaction_category"].head(2))
    df_merchants.loc[:, 'merchant_name'] = df_merchants['recipient_bank_details'].apply(extract_merchant_name)
    # merchant_stats = df[df['is_debit'] == True].groupby('merchant_name').agg({
    #     'debit': ['sum', 'mean', 'count', 'max'],
    #     'date': ['min', 'max']
    # }).rename(columns={'date': 'transaction_count'})
    merchant_stats=df_merchants.groupby("merchant_name")["debit"].agg([
        'sum', 'mean', 'count', 'max', 'min', 'std', 'median'
    ]).round(2).rename(columns={
        'sum': 'total_spent',
        'mean': 'average_transaction',
        'count': 'transaction_count',
        'max': 'largest_transaction',
        'min': 'smallest_transaction',
        'std': 'transaction_volatility',
        'median': 'median_transaction'
    })
    merchant_stats=merchant_stats.sort_values(by='total_spent', ascending=False)

    merchant_trends = {}
    monthly_merchant = df_merchants.groupby(['month_year', 'merchant_name'])['debit'].sum().unstack(fill_value=0)
    top_merchants = merchant_stats.head(10).index.tolist()

    for merchant in top_merchants:
        merchant_data = monthly_merchant[merchant]
        merchant_transactions = df_merchants[df_merchants['merchant_name'] == merchant]
        transaction_dates = merchant_transactions['date'].sort_values()
        if len(transaction_dates) > 1:
            date_diffs = transaction_dates.diff().dt.days.dropna()
            avg_days_between = float(date_diffs.mean()) if len(date_diffs) > 0 else 0
        else:
            avg_days_between = 0

        merchant_trends[merchant] = {
            'total_spent': float(merchant_stats.loc[merchant, 'total_spent']),
            'transaction_count': int(merchant_stats.loc[merchant, 'transaction_count']),
            'average_transaction': float(merchant_stats.loc[merchant, 'average_transaction']),
            'median_transaction': float(merchant_stats.loc[merchant, 'median_transaction']),
            'spending_consistency': float(1 / (merchant_stats.loc[merchant, 'transaction_volatility'] + 1)),  # Lower std = more consistent
            'monthly_spending': merchant_data.astype(float).to_dict(),
            'most_active_month': str(merchant_data.idxmax()) if len(merchant_data) > 0 else None,
            'least_active_month': str(merchant_data.idxmin()) if len(merchant_data) > 0 else None,
            'avg_days_between_visits': avg_days_between,
            'visit_frequency': 'regular' if avg_days_between < 14 else 'occasional' if avg_days_between < 30 else 'rare',
            'first_transaction': str(transaction_dates.min().date()) if len(transaction_dates) > 0 else None,
            'last_transaction': str(transaction_dates.max().date()) if len(transaction_dates) > 0 else None,
            'categories_used': merchant_transactions['transaction_category'].unique().tolist(),
            'payment_methods_used': merchant_transactions['payment_method'].unique().tolist()
        }
    
    merchant_analysis = {
        'top_merchants': merchant_trends,
        'total_unique_merchants': int(df_merchants['merchant_name'].nunique()),
        'merchant_concentration': {
            'top_5_share': float(merchant_stats.head(5)['total_spent'].sum() / merchant_stats['total_spent'].sum() * 100),
            'top_10_share': float(merchant_stats.head(10)['total_spent'].sum() / merchant_stats['total_spent'].sum() * 100),
        },
        'merchant_diversity_score': float(1 - (merchant_stats['total_spent'].max() / merchant_stats['total_spent'].sum())),  # 0-1, higher = more diverse
    }
            
    return merchant_analysis
    # print("Merchant Analysis Result:")
    # print(json.dumps(stringify_keys(merchant_analysis), indent=2, default=str))
    # print("-----------------")


def extract_merchant_name(bank_details): # check if sendTo is merchant, then return recipient_name
  
    if isinstance(bank_details, dict):
        return bank_details.get('recipient_name', 'Unknown')
    return 'Unknown'

# Helper function to recursively convert all keys in a nested dict to strings
# used mainly for json dumps
def stringify_keys(obj):
    import pandas as pd
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            # Convert pandas Period to string
            if isinstance(k, pd.Period):
                k = str(k)
            new_dict[str(k)] = stringify_keys(v)
        return new_dict
    elif isinstance(obj, list):
        return [stringify_keys(i) for i in obj]
    else:
        # Also convert Period values to string
        if isinstance(obj, pd.Period):
            return str(obj)
        return obj
        
#List all grocery spendings in April 2025
#What is the total spending on grocery in April 2025?

if __name__ == "__main__":
    # while True:
        # user_query = input("Enter your query (or 'exit' to quit):  ")
        # if user_query.lower() == 'exit':
        #     break
        # user_query = "get record for may 1 and 2 2025"
        # mongo_query=generate_mongo_query(user_query)
        mongo_query={'operation': 'find', 'filter': {'date': {'$gte': '2025-01-01', '$lte': '2025-06-31'}}}
        # print(f"Query Generation Response: {mongo_query}")
        expenses = query_expenses(mongo_query)
        
        # Analyze and summarize the expenses using LLM
        # expense_Analysis = summarize_expenses(expenses, user_query)
        # print(f"Expense Analysis: {expense_Analysis}\n")

        analyze_large_dataset_pandas(expenses)