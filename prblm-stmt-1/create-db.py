import sqlite3
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import re
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os


# TODO replace with valid credentials and scopes
CRED_FILEPATH = 'creds.json'
SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name(CRED_FILEPATH, SCOPES)

# source files for data
BORROWERS_SRC = 'https://docs.google.com/spreadsheets/d/1mfm4NUfv4wOJfMdjOIA5k99N79JFJDJeTBhzUfIYtzs'
LOANS_SRC =  'https://drive.google.com/file/d/1Lid1RDMGNpXWv2PyU0Alsv-sSBnkGMuS'
PAYMENT_SCHEDULES_SRC = 'https://docs.google.com/spreadsheets/d/1LawsJQtLGpO6AQZFDpgSUEn8A_TVFD9I'
LOAN_PAYMENT_SRC = 'https://drive.google.com/file/d/1qdV_WcVmvo7VsyxJxfU8RXpdOgKZ7V3A' 

BORROWERS_SCHEMA = {
    'borrower_id': 'TEXT',
    'state': 'TEXT',
    'city': 'TEXT',
    'zip_code': 'TEXT',
    'borrower_credit_score': 'TEXT'
}

LOANS_SCHEMA = {
    'borrower_id': 'TEXT',
    'loan_id': 'TEXT',
    'date_of_release': 'DATE',
    'term': 'REAL',
    'interest_rate': 'REAL',
    'loan_amount': 'REAL',
    'down_payment': 'REAL',
    'payment_frequency': 'REAL',
    'maturity_date': 'DATE',
}

PAYMENT_SCHEDULES_SCHEMA = {
    'loan_id': 'TEXT',
    'schedule_id': 'TEXT',
    'expected_payment_date': 'DATE',
    'expected_payment_amount': 'REAL',
}

LOAN_PAYMENT_SCHEMA = {
    'loan_id': 'TEXT',
    'payment_id': 'TEXT',
    'date_paid': 'DATE',
    'amount_paid': 'REAL',
}


# utility functions
def read_google_sheet(sheet_url: str, sheet_name: str) -> pd.DataFrame:
    gc = gspread.authorize(CREDENTIALS)
    workbook = gc.open_by_url(sheet_url)
    worksheet = workbook.worksheet(sheet_name)
    data = worksheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def read_drive_file(file_url: str, file_type:str) -> pd.DataFrame:
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', file_url)
    file_id = match.group(1)
    
    gauth = GoogleAuth()
    gauth.credentials = CREDENTIALS
    drive = GoogleDrive(gauth)
    
    file_obj = drive.CreateFile({'id': file_id})
    if file_type == 'csv':
        file_obj.GetContentFile('temp.csv')
        df = pd.read_csv('temp.csv')
        os.remove('temp.csv')
    elif file_type == 'xlsx':
        file_obj.GetContentFile('temp.xlsx')
        df = pd.read_excel('temp.xlsx')
        os.remove('temp.xlsx')
    else:
        raise TypeError('Invalid File Type in Source')
    
    return df


def main():
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
            borrower_id TEXT
            loan_id TEXT PRIMARY KEY,
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
            loan_id TEXT,
            schedule_id TEXT PRIMARY KEY,
            expected_payment_date DATE,
            expected_payment_amount REAL,
            FOREIGN KEY (loan_id) REFERENCES loans (loan_id)
        )
    '''
    )

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS loan_payments (
            loan_id TEXT,
            payment_id TEXT PRIMARY KEY,
            date_paid DATE,
            amount_paid REAL,
            FOREIGN KEY (loan_id) REFERENCES loans (loan_id)
        )
    '''
    )

    # read data from source files
    # 1. borrowers
    borrowers_df = read_google_sheet(sheet_url=BORROWERS_SRC, sheet_name='Sheet1')
    borrowers_df.columns = BORROWERS_SCHEMA.keys()
    borrowers_df.to_sql('borrowers', conn, if_exists='replace', index=False)
    # 2. loans
    loans_df = read_drive_file(file_url=LOANS_SRC, file_type='csv')
    loans_df.columns = LOANS_SCHEMA.keys()
    loans_df.to_sql('loans', conn, if_exists='replace', index=False)
    # 3. payment_schedules
    payment_schedules_df = read_drive_file(file_url=PAYMENT_SCHEDULES_SRC, file_type='xlsx')
    payment_schedules_df.columns = PAYMENT_SCHEDULES_SCHEMA.keys()
    payment_schedules_df.to_sql('payment_schedules', conn, if_exists='replace', index=False)
    # 4. loan_payments
    loan_payments_df = read_drive_file(file_url=LOAN_PAYMENT_SRC, file_type='csv')
    loan_payments_df.columns = LOAN_PAYMENT_SCHEMA.keys()
    loan_payments_df['date_paid'] = pd.to_datetime(loan_payments_df['date_paid'], format='%m/%d/%Y')
    loan_payments_df.to_sql('loan_payments', conn, if_exists='replace', index=False)

    conn.commit()
    conn.close()
    
    
if __name__ == '__main__':
    main()
    print("Done.")