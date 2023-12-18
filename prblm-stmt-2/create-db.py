import sqlite3


conn = sqlite3.connect('rates.db')
cursor = conn.cursor()

# Create a table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS rates (
        timestamp DATETIME,
        currency_from TEXT,
        USD_to_currency_rate REAL,
        currency_to_USD_rate REAL,
        currency_to TEXT
    )
''')

conn.commit()
conn.close()
    
    