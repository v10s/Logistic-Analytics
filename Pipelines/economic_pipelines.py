from base_pipeline import BaseETL
import pandas as pd

import uuid
import os
from pathlib import Path

class EconomicETL(BaseETL):

    def __init__(self):
        super().__init__()

    def run(self):
        super().run()
        self.p_connect.publish_df(
            df=self.get_india_export_data_df(),
            table_name='india_export_by_year',
            schema_name='economy_international_raw'
        )
        self.p_connect.publish_df(df=self.get_india_import_data_df(), table_name='india_import_by_year', schema_name='economy_international_raw')

        self.p_connect.publish_df(df=self.get_inflation_rate_data_df(), table_name='inflation_rate_by_country', schema_name='economy_international_raw')

        self.p_connect.publish_df(df=self.get_gdp_data_df(), table_name='gdp_by_country', schema_name='economy_international_raw')
        self.p_connect.publish_df(df=self.get_gdp_per_capita_data_df(), table_name='gdp_per_capita_by_country', schema_name='economy_international_raw')
        self.p_connect.publish_df(df=self.get_gdp_per_capita_data_df(), table_name='uk_trade_data_by_year',
                                  schema_name='economy_international_raw')

    def get_india_export_data_df(self):
        df_list = []
        year = 2024
        for _ in range(0, 9):

            if _ == 0:
                _ = ''
            elif _ in [1, 5]:
                continue
            else:
                _ = '-{}'.format(_)

            df = pd.read_excel(
                '/Users/vishvajeetthakur/PycharmProjects/Logistic-Analytics/Pipelines/.seed_data/Indian_export_data/eidb-Commoditywise-export{}.xlsx'.format(
                    _),
                engine='openpyxl',
                header=1
            )
            df.index = [uuid.uuid4() for _ in range(len(df.index))]
            df['year'] = df.columns[5]

            columns = list(df.columns)
            columns[3] = "LastYear"
            columns[4] = "LY_Share"
            columns[5] = "CurrentYear"
            columns[6] = "CY_Share"
            columns[7] = "Growth"

            df.columns = columns

            df_list.append(df)
            year -= 1
        df = pd.concat(df_list, axis=0)
        df = df[df['S.No.'].apply(lambda x: len(str(x)) < 5)]
        return df

    def get_india_import_data_df(self):
        df_list = []
        year = 2024
        for _ in range(1, 8):

            if _ == 0:
                _ = ''
            elif _ in [5]:
                continue
            else:
                _ = '-{}'.format(_)

            df = pd.read_excel(
                '/Users/vishvajeetthakur/PycharmProjects/Logistic-Analytics/Pipelines/.seed_data/Indian_import_data/eidb-Commoditywise-import{}.xlsx'.format(
                    _),
                engine='openpyxl',
                header=1
            )
            df.index = [uuid.uuid4() for _ in range(len(df.index))]
            df['year'] = df.columns[5]

            columns = list(df.columns)
            columns[3] = "LastYear"
            columns[4] = "LY_Share"
            columns[5] = "CurrentYear"
            columns[6] = "CY_Share"
            columns[7] = "Growth"

            df.columns = columns

            df_list.append(df)
            year -= 1
        df = pd.concat(df_list, axis=0)

        return df

    def get_inflation_rate_data_df(self):
        df = pd.read_csv(
            os.path.join(Path().parent,'.seed_data/inflation_rate_data/imf-dm-export-20250913.csv'),
        )
        return df

    def get_gdp_data_df(self):
        df = pd.read_csv(
            os.path.join(Path().parent,'.seed_data/GDP_current_prices_data/imf-dm-export-20250913.csv'),
        )
        return df

    def get_gdp_per_capita_data_df(self):
        df = pd.read_csv(
            os.path.join(Path().parent,'.seed_data/GDP_per_capita_current_prices_data/imf-dm-export-20250913.csv'),
        )
        return df

    def get_uk_trade_data(self):
        df = pd.read_excel(
            os.path.join(Path().parent,'.seed_data/uk_trade_data/tradepublicationtables2025jul.xlsx'),
            sheet_name='3 - Annual CP',
            header=3

        )
        print(df.columns)
        return df

if __name__ == "__main__":
    EconomicETL().get_uk_trade_data()