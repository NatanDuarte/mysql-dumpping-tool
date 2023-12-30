import os
import sys
import pandas as pd

from sqlalchemy import create_engine
from dotenv import load_dotenv


load_dotenv()

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python main.py <table_name> <csv_file>")
        sys.exit(1)

    table_name = str(sys.argv[1])
    csv_file = str(sys.argv[2])

    db_username = os.getenv('DB_USERNAME')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    connection_string = \
        f'mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    df = pd.read_csv(csv_file)

    df.to_sql(table_name, con=engine, if_exists='replace')


main()
