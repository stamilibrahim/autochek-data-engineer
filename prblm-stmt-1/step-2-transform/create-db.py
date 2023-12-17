import sqlite3
import pandas as pd

# source files for data
BORROWER_SRC = 'https://docs.google.com/spreadsheets/d/1mfm4NUfv4wOJfMdjOIA5k99N79JFJDJeTBhzUfIYtzs/edit?usp=sharing'
LOAN_SRC = 'https://drive.google.com/file/d/1Lid1RDMGNpXWv2PyU0Alsv-sSBnkGMuS/view?usp=sharing'
PAYMENT_SCHEDULE_SRC = 'https://docs.google.com/spreadsheets/d/1LawsJQtLGpO6AQZFDpgSUEn8A_TVFD9I/edit?usp=sharing&ouid=112035781102155860914&rtpof=true&sd=true'
LOAN_PAYMENT_SRC = 'https://drive.google.com/file/d/1qdV_WcVmvo7VsyxJxfU8RXpdOgKZ7V3A/view?usp=sharing'

# define tmp db schema and relationships
conn = sqlite3.connect('autochek.db')
cursor = conn.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS borrowers (
        borrower_id TEXT PRIMARY KEY,
        state TEXT,
        city TEXT,
        zip_code TEXT,
        borrower_credit_score TEXT
    )
'''
)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS loans (
        loan_id TEXT PRIMARY KEY,
        borrower_id TEXT
        date_of_release DATE,
        term REAL,
        interest_rate REAL,
        loan_amount REAL,
        down_payment REAL,
        payment_frequency REAL,
        maturity_date DATE,
        FOREIGN KEY (borrower_id) REFERENCES borrowers (borrower_id)
    )    
'''
)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS payment_schedules (
        schedule_id TEXT PRIMARY KEY,
        loan_id TEXT,
        expected_payment_date DATE,
        expected_payment_amount REAL,
        FOREIGN KEY (loan_id) REFERENCES loans (loan_id)
    )
'''
)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS repayments (
        payment_id TEXT PRIMARY KEY,
        loan_id TEXT,
        date_paid DATE,
        amount_paid REAL,
        FOREIGN KEY (loan_id) REFERENCES loans (loan_id)
    )
'''
)

# read data from source file
# utility function to read from gsheet


# Insert data from the DataFrame into the 'users' table
# df.to_sql('users', conn, if_exists='replace', index=False)

# Commit the changes and close the connection
conn.commit()
conn.close()