from flask import Flask, request, jsonify
import mysql.connector
import requests
import os
from dotenv import load_dotenv
import re

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Database configuration using environment variables
db_config = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USERNAME', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_DATABASE', 'test')
}

# Gemini API Key and endpoint configuration
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

# Ask Gemini for content
def ask_gemini(prompt):
    headers = {'Content-Type': 'application/json'}
    data = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }
    response = requests.post(GEMINI_ENDPOINT, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()
    else:
        return {'error': 'Gemini API error', 'details': response.text}

# Remove markdown/code block syntax
def clean_sql_query(sql_query):
    sql_query = re.sub(r'```sql|```', '', sql_query).strip()
    return sql_query

# Get database schema: tables, columns, and foreign key relationships
def get_database_schema_with_data():
    schema = []
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Get the list of all tables
        cursor.execute("SHOW TABLES;")
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            # Get column names for each table
            cursor.execute(f"SHOW COLUMNS FROM {table};")
            columns = [col[0] for col in cursor.fetchall()]
            schema.append(f"Table: {table} ({', '.join(columns)})")

            # Get some sample rows from each table
            cursor.execute(f"SELECT * FROM {table} LIMIT 3;")
            rows = cursor.fetchall()
            if rows:
                row_data = [dict(zip(columns, row)) for row in rows]
                schema.append(f"Sample rows from {table}: {row_data}")
            else:
                schema.append(f"No data found in table: {table}")

            # Get foreign key relationships
            cursor.execute(f"""
                SELECT k.column_name, k.referenced_table_name, k.referenced_column_name
                FROM information_schema.key_column_usage k
                WHERE k.table_name = '{table}' AND k.referenced_table_name IS NOT NULL;
            """)
            foreign_keys = cursor.fetchall()

            for fk in foreign_keys:
                column_name, referenced_table, referenced_column = fk
                schema.append(f"Foreign Key: {column_name} -> {referenced_table}({referenced_column})")

        cursor.close()
        conn.close()
    except Exception as e:
        schema.append(f"Error: {e}")
    return schema

# Endpoint to chat and execute queries
@app.route('/chat', methods=['POST'])
def chat():
    try:
        user_question = request.json.get('question')
        if not user_question:
            return jsonify({"error": "Question is required"}), 400

        # 1. Get schema including data and foreign keys
        schema_info = get_database_schema_with_data()
        schema_text = "\n".join(schema_info)

        # 2. Build the prompt for Gemini to convert the question to SQL query
        sql_prompt = f"""
Convert this question into a valid MySQL SQL query.
Use the following schema for accuracy:
{schema_text}

Only return the SQL query.

User question: {user_question}
"""
        gemini_response = ask_gemini(sql_prompt)

        if 'error' in gemini_response:
            return jsonify({"error": gemini_response['error'], "details": gemini_response.get('details')}), 500

        # 3. Clean & extract SQL query
        sql_query = gemini_response['candidates'][0]['content']['parts'][0]['text']
        sql_query = clean_sql_query(sql_query)

        # 4. Execute the SQL query
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        try:
            cursor.execute(sql_query)
        except mysql.connector.Error as err:
            # Retry with schema-aware correction
            error_msg = str(err)
            retry_prompt = f"""
The previous SQL query caused an error: "{error_msg}"

Please generate a corrected SQL query for:
"{user_question}"
Using this schema:
{schema_text}

Only return the corrected SQL query.
"""
            retry_response = ask_gemini(retry_prompt)
            if 'error' in retry_response:
                return jsonify({"error": retry_response['error'], "details": retry_response.get('details')}), 500

            retry_sql = clean_sql_query(retry_response['candidates'][0]['content']['parts'][0]['text'])
            cursor.execute(retry_sql)
            sql_query = retry_sql  # update used query

        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in rows]
        cursor.close()
        connection.close()

        # 5. Optional: ask Gemini to explain the results
        explain_prompt = f"Explain these SQL results in simple terms: {results}"
        explanation_response = ask_gemini(explain_prompt)

        explanation = explanation_response['candidates'][0]['content']['parts'][0]['text'] if 'candidates' in explanation_response else ""

        return jsonify({
            'sql_query': sql_query,
            'results': results,
            'explanation': explanation
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=8080)
