from xecd_rates_client import XecdClient
import os
import json
import pandas as pd
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

CURRENCY_CODES = 'NGN, GHS, KES, UGX, MAD, XOF, EGP'

def create_xecd_client():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # TODO replace with valid credentials
    xe_cred_file = os.path.join(current_dir, 'xe-creds.json')
    with open(xe_cred_file, 'r') as json_file:
        xe_creds = json.load(json_file)

    account_api_id = xe_creds['account_api_id']
    account_api_key =xe_creds['account_api_key']

    xecd = XecdClient(account_api_id, account_api_key)
    return xecd


def get_rates(xecd: XecdClient) -> pd.DataFrame:
    # get USD to LCY:
    #   - Nigeria - NGN
    # 	- Ghana - GHS
    # 	- Kenya - KSH
    # 	- Uganda - USH
    # 	- Morocco - MAD
    # 	- Ivory Coast - XOF
    # 	- Egypt - EGP
    lcy_to_usd_rates = xecd.convert_from(
        from_currency='USD',
        to_currency=CURRENCY_CODES,
        inverse=True
    )
    rates = lcy_to_usd_rates['to']
    df = pd.DataFrame(rates)
    df.columns = ['currency_to', 'currency_to_USD_rate', 'USD_to_currency_rate']
    df['currency_from'] = 'USD'
    df['timestamp'] = pd.to_datetime('now')
    df = df[['timestamp', 'currency_from', 'USD_to_currency_rate', 'currency_to_USD_rate', 'currency_to']]
    return df

def insert_rates(rates:pd.DataFrame):
    delete_query = '''
        DELETE FROM rates WHERE DATE(timestamp) = DATE('now')
    '''
    try:
        # delete today's records
        conn = sqlite3.connect('rates.db')
        cursor = conn.cursor()
        cursor.execute(delete_query)
        
        # insert fresh records
        rates.to_sql('rates', conn, if_exists='replace', index=False)
        conn.commit()
        
    except Exception as e:
        print("Failed.")
        conn.rollback()
        conn.close()
        raise(e)
        
    finally:
        conn.close()
        

def main():
    xecd = create_xecd_client()
    rates = get_rates(xecd)
    insert_rates(rates)
    print("Done.")
    
if __name__ == '__main__':
    main()