import pandas as pd

from server.connector.postgres import PostgresConnector



class BaseETL:

    def __init__(self):
        self.df = None
        self.p_connect = PostgresConnector()


    def run(self):
        pass

    def ingest_csv_df(self, path, table_name,schema_name):

        df = pd.read_csv(
            path
            , header=1
        )

        self.p_connect.publish_df(df, table_name=table_name, schema_name=schema_name)


if __name__ == "__main__":
    BaseETL().ingest_csv_df(
        path = '.seed_data/ind_avg_temp.csv'
        , table_name="ind_avg_temp"
        , schema_name='international'
    )