import pandas as pd
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path
import os
import os
from pathlib import Path
import yaml

def get_postgres_creds():
    if os.environ.get('PROD_SERVER_FLAG', None) is None:
        cred_file_name = 'local_config.yaml'
    else:
        cred_file_name = 'config.yaml'
    with open(
            os.path.join(Path().absolute().parent, cred_file_name)
            , 'r'
    ) as file:
        data = yaml.safe_load(file)
    return data

class PostgresConnector:

    def __init__(self, host=None, database=None, user=None, password=None, port=None):
        if host is None or database is None or user is None or password is None or port is None:
            data = get_postgres_creds()
            host = data['postgres']['host']
            database = data['postgres']['database']
            user = data['postgres']['user']
            password = data['postgres']['password']
            port = int(data['postgres']['port'])
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port

        # Create connection engine
        self.engine = create_engine(
            f"postgresql+psycopg2://"
            f"{self.user}:{self.password}"
            f"@{self.host}:{self.port}"
            f"/{self.database}"
        )

    def publish_df(self, df, table_name, schema_name='master'):
        # Publish DataFrame to Postgres
        df.columns = [str.lower(_) for _ in df.columns]
        return df.to_sql(
            name=table_name,
            schema=schema_name,
            con=self.engine,
            if_exists="replace",
            index=False
        )

    def create_schema(self, schema_name):
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name)))
            conn.commit()

