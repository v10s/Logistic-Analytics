from new_york_taxi import NewYorkTaxiETL
from economic_pipelines import EconomicETL
from server.connector.postgres import PostgresConnector

# NewYorkTaxiETL().run()
# PostgresConnector().create_schema('economy_international_raw')
EconomicETL().run()