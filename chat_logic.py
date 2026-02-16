import os
from dotenv import load_dotenv
from openai import OpenAI

# بارگذاری کلید از فایل .env
load_dotenv()
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
)

# تعریف ساختار جدول
TABLE_SCHEMA = """
Table Name: gold.dataset
System: PostgreSQL

Columns:
- gold_record_id (INT)
- booking_id (VARCHAR)
- booking_status (VARCHAR): ['Completed', 'Cancelled by Customer', 'Cancelled by Driver', 'No Driver Found', 'Incomplete']
- customer_id (VARCHAR)
- vehicle_type (VARCHAR): ['Auto', 'eBike', 'Premier Sedan', 'Go Sedan', 'Go Mini', 'Uber XL'] 
- booking_value (FLOAT)
- ride_distance (FLOAT)
- driver_ratings (FLOAT)
- customer_rating (FLOAT)
- timestamp (TIMESTAMP)
- day_name (VARCHAR): ['Monday', 'Tuesday', ..., 'Sunday']
- season (VARCHAR): ['Spring', 'Summer', 'Autumn', 'Winter']
- time_category (VARCHAR): ['Morning', 'Afternoon', 'Evening', 'Night']
- unified_cancellation_reason (VARCHAR)
"""

# پرامپت اصلاح شده با قوانین سخت‌گیرانه برای سلام و احوال‌پرسی
SYSTEM_PROMPT = f"""
You are a PostgreSQL expert. Your mission is to convert English questions into SQL queries.

{TABLE_SCHEMA}

STRICT RULES:
1. Respond ONLY with the raw SQL code. No explanation, no markdown tags.
2. Only English input is allowed. If not in English, say: "Please ask in English."
3. SECURITY: Only generate 'SELECT' queries. Never generate DROP, DELETE, UPDATE, or INSERT. If asked, say: "Access Denied."
4. PERFORMANCE: Always add 'LIMIT 10' to the end of the query unless a specific count or aggregation (like SUM/AVG) is requested.
5. SCOPE & CHIT-CHAT: 
   - Only answer questions explicitly asking for data retrieval from the Uber dataset. 
   - If the user says "Hello", "Hi", "Thanks", or asks general questions (e.g., "How are you?", "What is SQL?"), respond with: "OFF_TOPIC".
   - DO NOT generate a default query for greetings.
6. SYNTAX: Use PostgreSQL syntax. Use 'LIMIT n' at the end.
"""

def get_sql_from_ai(user_question):
    """ارسال سوال به OpenRouter و دریافت پاسخ"""
    try:
        response = client.chat.completions.create(
            model="google/gemini-2.0-flash-001",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_question}
            ],
            temperature=0  # دما را صفر کردیم تا خلاقیت کم شود و دقیق عمل کند
        )
        # استخراج متن خروجی و تمیزکاری نهایی
        sql_output = response.choices[0].message.content.strip()
        import re
        sql_output = re.sub(r"```sql|```", "", sql_output).strip()
        return sql_output
    except Exception as e:
        return f"Error contacting AI: {str(e)}"