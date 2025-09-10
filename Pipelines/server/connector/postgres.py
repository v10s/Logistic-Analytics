import pandas as pd
from sqlalchemy import create_engine


class PostgresConnector:

    def __init__(self, host, database, user, password, port):
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

    def publish_df(self, df, table_name):
        # Publish DataFrame to Postgres
        return df.to_sql(
            table_name,
            self.engine,
            if_exists="replace",
            index=False
        )


